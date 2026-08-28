"""PRIME receipt file parser for the first Switchboard seam.

Observed receipts are KEY=VALUE text files.  Switchboard validates the
envelope (exists, hash matches if provided, source identity coherent,
mission/stage unambiguous, authority present).  It does not grant PASS.

Live T07 receipts use MISSION_ID / STAGE_ID / SOURCE_HEAD / AUTHORITY_REFERENCE
and EVENT_TARGET=receipt.sealed.  Synthetic fixtures may use the shorter keys
and an optional PAYLOAD_HASH.
"""
from __future__ import annotations

import os
import re
from dataclasses import dataclass
from typing import Mapping

from .contracts import content_sha
from .events import DeadletterReason

_SECRET_SHAPES = (
    re.compile(r"secret_[A-Za-z0-9]{20,}"),
    re.compile(r"\bsk-[A-Za-z0-9\-_]{20,}"),
    re.compile(r"\bAKIA[0-9A-Z]{16}\b"),
    re.compile(r"-----BEGIN [A-Z ]*PRIVATE KEY-----"),
    re.compile(r"\bgh[pousr]_[A-Za-z0-9]{20,}"),
    re.compile(r"\bxox[abpsr]-[A-Za-z0-9\-]{10,}"),
    re.compile(r"\bntn_[A-Za-z0-9]{20,}"),
    re.compile(r"\bsk-or-[A-Za-z0-9\-]{16,}"),
    re.compile(r"Bearer [A-Za-z0-9\-_\.]{30,}"),
)

_HEAD_OK = re.compile(r"^[0-9a-f]{40}$")
_DENIED_AUTHORITY = frozenset({"", "none", "n/a", "null", "self", "unsigned"})
_PR_NUMBER = re.compile(r"\bNUMBER=(\d+)\b")
_PR_URL = re.compile(r"\bURL=(https://\S+)")
_PR_DRAFT = re.compile(r"\bDRAFT=(YES|NO)\b")


@dataclass(frozen=True)
class PrimeReceipt:
    path: str
    fields: Mapping[str, str]
    raw: str
    content_sha: str

    def get(self, key: str, default: str = "") -> str:
        return self.fields.get(key, default)

    def _first(self, *keys: str) -> str:
        for key in keys:
            value = self.fields.get(key, "")
            if value:
                return value
        return ""

    @property
    def mission(self) -> str:
        return self._first("MISSION_ID", "MISSION")

    @property
    def stage(self) -> str:
        return self._first("STAGE_ID", "STAGE", "TASK_ID")

    @property
    def task_id(self) -> str:
        return self._first("TASK_ID", "STAGE_ID", "STAGE")

    @property
    def head(self) -> str:
        return self._first("SOURCE_HEAD", "HEAD").lower()

    @property
    def authority_ref(self) -> str:
        return self._first("AUTHORITY_REFERENCE", "AUTHORITY_REF")

    @property
    def claimer(self) -> str:
        return self._first("CLAIMER", "CLAIMANT", "OWNER_ROLE", "SOURCE_ROLE")

    @property
    def source_role(self) -> str:
        return self._first("SOURCE_ROLE", "CLAIMER", "OWNER_ROLE")

    @property
    def bind(self) -> str:
        return self._first("NETWORK_POLICY", "BIND")

    @property
    def repo(self) -> str:
        return self.get("REPO")

    @property
    def worktree(self) -> str:
        return self.get("WORKTREE")

    @property
    def branch(self) -> str:
        return self.get("BRANCH")

    @property
    def remote(self) -> str:
        return self.get("REMOTE")

    @property
    def sealed_target(self) -> bool:
        return (self.get("EVENT_TARGET") == "receipt.sealed"
                or self.get("SEALED") == "YES")

    @property
    def expected_pr(self) -> str:
        match = _PR_NUMBER.search(self.get("PR"))
        if match:
            return match.group(1)
        return self.get("EXPECTED_GIT_OUTCOME_PR")

    @property
    def expected_pr_url(self) -> str:
        match = _PR_URL.search(self.get("PR"))
        return match.group(1) if match else ""

    @property
    def expected_pr_draft(self) -> str:
        match = _PR_DRAFT.search(self.get("PR"))
        return match.group(1) if match else ""

    @property
    def clipboard_defect(self) -> dict[str, str] | None:
        if (self.get("EVENT_TARGET") == "receipt.sealed"
                and self.get("EVENT_IMPLEMENTATION_STATUS") == "NOT_PROVEN"):
            return {
                "event_target": self.get("EVENT_TARGET"),
                "event_implementation_status": self.get(
                    "EVENT_IMPLEMENTATION_STATUS"),
                "transport": self.get("TRANSPORT"),
                "sentinel_closure_in_receipt": self.get("SENTINEL_CLOSURE"),
            }
        return None


class ReceiptValidation:
    def __init__(self, ok: bool, reason: DeadletterReason | None = None,
                 detail: str = ""):
        self.ok = ok
        self.reason = reason
        self.detail = detail


def parse_prime_receipt(path: str) -> PrimeReceipt:
    with open(path, "r", encoding="utf-8") as fh:
        raw = fh.read()
    fields: dict[str, str] = {}
    for line in raw.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        if "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        fields[key.strip()] = value.strip()
    hashed = {k: v for k, v in fields.items() if k != "PAYLOAD_HASH"}
    return PrimeReceipt(
        path=os.path.abspath(path),
        fields=fields,
        raw=raw,
        content_sha=content_sha(hashed),
    )


def canonical_receipt_sha(fields: Mapping[str, str]) -> str:
    hashed = {k: v for k, v in fields.items() if k != "PAYLOAD_HASH"}
    return content_sha(hashed)


def contains_secret_shape(text: str) -> bool:
    return any(pat.search(text) for pat in _SECRET_SHAPES)


def redact_secrets(text: str) -> str:
    out = text
    for pat in _SECRET_SHAPES:
        out = pat.sub("[REDACTED]", out)
    return out


def looks_like_path(value: str) -> bool:
    return bool(value) and (value.startswith("/") or value.startswith("~")
                            or "/" in value)


def seal_receipt_text(text: str) -> str:
    """Attach PAYLOAD_HASH so a synthetic fixture is tamper-evident."""
    lines = [ln for ln in text.splitlines() if not ln.startswith("PAYLOAD_HASH=")]
    fields: dict[str, str] = {}
    for line in lines:
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        fields[key.strip()] = value.strip()
    digest = canonical_receipt_sha(fields)
    return "\n".join(lines).rstrip() + f"\nPAYLOAD_HASH={digest}\n"


def _is_prime_claimer(receipt: PrimeReceipt) -> bool:
    blob = f"{receipt.claimer} {receipt.source_role}".upper()
    if "SENTINEL" in receipt.claimer.upper() and "PRIME" not in receipt.claimer.upper():
        return False
    return "PRIME" in blob


def _bind_is_loopback(bind: str) -> bool:
    if not bind:
        return True
    lowered = bind.lower()
    return ("127.0.0.1" in lowered or "localhost" in lowered or "::1" in lowered)


def validate_prime_receipt(
    receipt: PrimeReceipt,
    *,
    known_missions: frozenset[str],
) -> ReceiptValidation:
    mission = receipt.mission
    stage = receipt.stage
    head = receipt.head
    authority = receipt.authority_ref

    if not mission or not stage or not receipt.repo:
        return ReceiptValidation(
            False, DeadletterReason.INVALID_SCHEMA,
            "sealed PRIME receipt requires mission, stage/task, and repo")
    if not receipt.sealed_target:
        return ReceiptValidation(
            False, DeadletterReason.INVALID_SCHEMA,
            "receipt is not a sealed target (EVENT_TARGET=receipt.sealed "
            "or SEALED=YES)")
    if "PAYLOAD_HASH" in receipt.fields:
        if receipt.fields["PAYLOAD_HASH"] != receipt.content_sha:
            return ReceiptValidation(
                False, DeadletterReason.BROKEN_HASH,
                "PAYLOAD_HASH does not match canonical receipt body")
    if mission not in known_missions:
        return ReceiptValidation(
            False, DeadletterReason.UNKNOWN_MISSION,
            f"unknown mission {mission!r}")
    if not _HEAD_OK.match(head):
        return ReceiptValidation(
            False, DeadletterReason.UNKNOWN_SOURCE_HEAD,
            "SOURCE_HEAD/HEAD must be exact 40-hex")
    if authority.strip().lower() in _DENIED_AUTHORITY or len(authority.strip()) < 3:
        return ReceiptValidation(
            False, DeadletterReason.AUTHORITY_INVALID,
            "authority_ref missing or invalid")
    if not _is_prime_claimer(receipt):
        return ReceiptValidation(
            False, DeadletterReason.CONTRADICTION,
            "first seam observes PRIME receipts only; a worker may not court "
            "its own receipt")
    if not _bind_is_loopback(receipt.bind):
        return ReceiptValidation(
            False, DeadletterReason.CONTRADICTION,
            "bind/endpoint must be loopback/local only")
    if contains_secret_shape(receipt.raw):
        return ReceiptValidation(
            False, DeadletterReason.CONTRADICTION,
            "credential payload refused")
    return ReceiptValidation(True)
