"""PRIME receipt file parser for the first Switchboard seam.

Observed receipts are KEY=VALUE text files.  Switchboard validates the
envelope (exists, hash matches if provided, source identity coherent,
mission/stage unambiguous, authority present).  It does not grant PASS.
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

_AUTHORITY_OK = re.compile(r"^[a-z0-9][a-z0-9:._/-]{2,127}$")
_HEAD_OK = re.compile(r"^[0-9a-f]{40}$")
_DENIED_AUTHORITY = frozenset({"", "none", "n/a", "null", "self", "unsigned"})


@dataclass(frozen=True)
class PrimeReceipt:
    path: str
    fields: Mapping[str, str]
    raw: str
    content_sha: str

    def get(self, key: str, default: str = "") -> str:
        return self.fields.get(key, default)


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


def seal_receipt_text(text: str) -> str:
    """Attach PAYLOAD_HASH so the file is a sealed PRIME receipt."""
    lines = [ln for ln in text.splitlines() if not ln.startswith("PAYLOAD_HASH=")]
    fields: dict[str, str] = {}
    for line in lines:
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        fields[key.strip()] = value.strip()
    digest = canonical_receipt_sha(fields)
    body = "\n".join(lines).rstrip() + f"\nPAYLOAD_HASH={digest}\n"
    return body


def validate_prime_receipt(
    receipt: PrimeReceipt,
    *,
    known_missions: frozenset[str],
) -> ReceiptValidation:
    fields = receipt.fields
    mission = fields.get("MISSION", "")
    stage = fields.get("STAGE", "")
    head = fields.get("HEAD", "").lower()
    sealed = fields.get("SEALED", "")
    authority = fields.get("AUTHORITY_REF", "")
    source_role = fields.get("SOURCE_ROLE", "")
    bind = fields.get("BIND", "")

    required = ("MISSION", "STAGE", "HEAD", "SEALED", "AUTHORITY_REF",
                "SOURCE_ROLE", "REPO")
    if any(not fields.get(k) for k in required):
        return ReceiptValidation(
            False, DeadletterReason.INVALID_SCHEMA,
            "sealed PRIME receipt requires MISSION, STAGE, HEAD, SEALED, "
            "AUTHORITY_REF, SOURCE_ROLE, REPO")
    if sealed != "YES":
        return ReceiptValidation(
            False, DeadletterReason.INVALID_SCHEMA,
            "receipt is not sealed")
    if "PAYLOAD_HASH" not in fields:
        return ReceiptValidation(
            False, DeadletterReason.INVALID_SCHEMA,
            "sealed receipt must carry PAYLOAD_HASH")
    if fields["PAYLOAD_HASH"] != receipt.content_sha:
        return ReceiptValidation(
            False, DeadletterReason.BROKEN_HASH,
            "PAYLOAD_HASH does not match canonical receipt body")
    if mission not in known_missions:
        return ReceiptValidation(
            False, DeadletterReason.UNKNOWN_MISSION,
            f"unknown mission {mission!r}")
    if not stage:
        return ReceiptValidation(
            False, DeadletterReason.INVALID_SCHEMA,
            "stage is unambiguous only when present")
    if not _HEAD_OK.match(head):
        return ReceiptValidation(
            False, DeadletterReason.UNKNOWN_SOURCE_HEAD,
            "HEAD must be exact 40-hex")
    if authority.lower() in _DENIED_AUTHORITY or not _AUTHORITY_OK.match(authority):
        return ReceiptValidation(
            False, DeadletterReason.AUTHORITY_INVALID,
            "authority_ref missing or invalid")
    if source_role != "PRIME":
        return ReceiptValidation(
            False, DeadletterReason.CONTRADICTION,
            "first seam observes PRIME receipts only; a worker may not court "
            "its own receipt")
    if bind and not (bind.startswith("127.0.0.1") or bind.startswith("localhost")
                     or bind.startswith("[::1]")):
        return ReceiptValidation(
            False, DeadletterReason.CONTRADICTION,
            "bind/endpoint must be loopback/local only")
    if contains_secret_shape(receipt.raw):
        return ReceiptValidation(
            False, DeadletterReason.CONTRADICTION,
            "credential payload refused")
    return ReceiptValidation(True)
