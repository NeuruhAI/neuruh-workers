"""PRIME receipt file parser for the first Switchboard seam.

Observed receipts are KEY=VALUE text files.  Switchboard validates the
envelope (exists, hash matches if provided, source identity coherent,
mission/stage unambiguous, authority present).  It does not grant PASS.

Live T07 receipts use MISSION_ID / STAGE_ID / SOURCE_HEAD / AUTHORITY_REFERENCE
and EVENT_TARGET=receipt.sealed.  Synthetic fixtures may use the shorter keys
and an optional PAYLOAD_HASH.
"""
from __future__ import annotations

import json
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
_FR_DISPATCH_AUTHORITY = re.compile(
    r"^fr-api-v1:approval:[^:\r\n]+:work-spec:[0-9a-f]{64}$")
_SHA256 = re.compile(r"^[0-9a-f]{64}$")

# Receipt-level type discriminator. The event envelope already versions itself as
# nrh.event.v1; receipts had no equivalent, so a transport acknowledgement had to be
# smuggled through the PRIME validator. This names the receipt for what it is.
FR_DISPATCH_RECEIPT_SCHEMA = "nrh.receipt.fr-dispatch.v1"
KNOWN_RECEIPT_SCHEMAS = frozenset({FR_DISPATCH_RECEIPT_SCHEMA})
_FR_DISPATCH_SOURCE_ROLE = "DISPATCH_BRIDGE"
_FR_DISPATCH_SOURCE_COMPONENT = "fr-to-workers-dispatch-bridge"
# A dispatch receipt is only sealed for a job that reached a terminal success.
_FR_DISPATCH_RESULTS = frozenset({"SUCCEEDED"})


@dataclass(frozen=True)
class PrimeReceipt:
    path: str
    fields: Mapping[str, str]
    raw: str
    content_sha: str
    duplicate_fields: tuple[str, ...] = ()

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
    def receipt_schema(self) -> str:
        """Declared receipt type. Empty means an undeclared (legacy PRIME) receipt."""
        return self.get("RECEIPT_SCHEMA")

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
    def code_custody_required(self) -> bool:
        explicit = self.get("CODE_CUSTODY_REQUIRED")
        if explicit:
            return explicit == "YES"
        return bool(self.expected_pr)

    @property
    def candidate(self) -> Mapping[str, object] | None:
        raw = self.get("CANDIDATE_JSON")
        if not raw:
            return None
        value = json.loads(raw)
        return value if isinstance(value, Mapping) else None

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
    duplicates: list[str] = []
    for line in raw.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        if "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        key = key.strip()
        if key in fields:
            duplicates.append(key)
        fields[key] = value.strip()
    hashed = {k: v for k, v in fields.items() if k != "PAYLOAD_HASH"}
    return PrimeReceipt(
        path=os.path.abspath(path),
        fields=fields,
        raw=raw,
        content_sha=content_sha(hashed),
        duplicate_fields=tuple(duplicates),
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
        key = key.strip()
        if key in fields:
            raise ValueError(f"duplicate receipt field: {key}")
        fields[key] = value.strip()
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


def _validate_common(
    receipt: PrimeReceipt,
    *,
    known_missions: frozenset[str],
) -> ReceiptValidation | None:
    """Checks every receipt type owes, whatever its declared schema."""
    if receipt.duplicate_fields:
        return ReceiptValidation(
            False,
            DeadletterReason.INVALID_SCHEMA,
            f"duplicate receipt fields: {sorted(set(receipt.duplicate_fields))}",
        )
    if not receipt.mission or not receipt.stage or not receipt.repo:
        return ReceiptValidation(
            False, DeadletterReason.INVALID_SCHEMA,
            "sealed receipt requires mission, stage/task, and repo")
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
    if receipt.mission not in known_missions:
        return ReceiptValidation(
            False, DeadletterReason.UNKNOWN_MISSION,
            f"unknown mission {receipt.mission!r}")
    if not _HEAD_OK.match(receipt.head):
        return ReceiptValidation(
            False, DeadletterReason.UNKNOWN_SOURCE_HEAD,
            "SOURCE_HEAD/HEAD must be exact 40-hex")
    authority = receipt.authority_ref
    if authority.strip().lower() in _DENIED_AUTHORITY or len(authority.strip()) < 3:
        return ReceiptValidation(
            False, DeadletterReason.AUTHORITY_INVALID,
            "authority_ref missing or invalid")
    if not _bind_is_loopback(receipt.bind):
        return ReceiptValidation(
            False, DeadletterReason.CONTRADICTION,
            "bind/endpoint must be loopback/local only")
    if contains_secret_shape(receipt.raw):
        return ReceiptValidation(
            False, DeadletterReason.CONTRADICTION,
            "credential payload refused")
    candidate_json = receipt.get("CANDIDATE_JSON")
    candidate_sha = receipt.get("CANDIDATE_SHA256")
    if bool(candidate_json) != bool(candidate_sha):
        return ReceiptValidation(
            False, DeadletterReason.INVALID_SCHEMA,
            "candidate JSON and digest must be supplied together")
    if candidate_json:
        try:
            candidate = json.loads(candidate_json)
        except json.JSONDecodeError:
            candidate = None
        if (
            not isinstance(candidate, dict)
            or not _SHA256.fullmatch(candidate_sha)
            or content_sha(candidate) != candidate_sha
        ):
            return ReceiptValidation(
                False, DeadletterReason.BROKEN_HASH,
                "candidate JSON is malformed or differs from its digest")
    custody = receipt.get("CODE_CUSTODY_REQUIRED")
    if custody and custody not in {"YES", "NO"}:
        return ReceiptValidation(
            False, DeadletterReason.INVALID_SCHEMA,
            "CODE_CUSTODY_REQUIRED must be YES or NO")
    return None


def _validate_fr_dispatch_receipt(
    receipt: PrimeReceipt,
    *,
    known_missions: frozenset[str],
) -> ReceiptValidation:
    """Complete validator for nrh.receipt.fr-dispatch.v1.

    This is a transport acknowledgement of an authorised FR dispatch, not a worker's
    claim on PRIME-courted work. Its trust comes from the FR approval and work-spec
    digest named in AUTHORITY_REFERENCE, so every one of those bindings is required
    and none of them may be self-asserted by the bridge.
    """
    common = _validate_common(receipt, known_missions=known_missions)
    if common is not None:
        return common

    required = {
        "RECEIPT_SCHEMA",
        "MISSION_ID",
        "MISSION_VERSION",
        "WORK_SPEC_SHA256",
        "IDEMPOTENCY_KEY",
        "DISPATCH_RESULT",
        "STAGE_ID",
        "TASK_ID",
        "SOURCE_HEAD",
        "AUTHORITY_REFERENCE",
        "EVENT_TARGET",
        "EVENT_IMPLEMENTATION_STATUS",
        "REPO",
        "WORKTREE",
        "BRANCH",
        "REMOTE",
        "LEDGER_TAIL_SHA",
        "JOB_ID",
        "CONTEXT_DIGEST",
        "EVIDENCE_REFS",
        "JOB_SUCCEEDED_SHA",
        "SENTINEL_MODEL_EXECUTION",
        "SENTINEL_CLOSURE",
        "PRIME_VERDICT",
        "NETWORK_POLICY",
        "SOURCE_ROLE",
        "SOURCE_COMPONENT",
        "CODE_CUSTODY_REQUIRED",
        "PAYLOAD_HASH",
    }
    missing = sorted(required - set(receipt.fields))
    if missing:
        return ReceiptValidation(
            False,
            DeadletterReason.INVALID_SCHEMA,
            f"FR dispatch receipt fields missing: {missing}",
        )

    # A transport receipt may not impersonate PRIME, and PRIME ownership fields have
    # no meaning here, so their presence is a lie about what happened.
    if receipt.get("CLAIMER") or receipt.get("CLAIMANT") or receipt.get("OWNER_ROLE"):
        return ReceiptValidation(
            False, DeadletterReason.CONTRADICTION,
            "FR dispatch receipt must not carry PRIME ownership fields "
            "(CLAIMER/CLAIMANT/OWNER_ROLE)")
    if "PRIME" in f"{receipt.source_role} {receipt.get('SOURCE_COMPONENT')}".upper():
        return ReceiptValidation(
            False, DeadletterReason.CONTRADICTION,
            "FR dispatch receipt must not present itself as PRIME")

    if receipt.source_role != _FR_DISPATCH_SOURCE_ROLE:
        return ReceiptValidation(
            False, DeadletterReason.INVALID_SCHEMA,
            f"SOURCE_ROLE must be {_FR_DISPATCH_SOURCE_ROLE}")
    if receipt.get("SOURCE_COMPONENT") != _FR_DISPATCH_SOURCE_COMPONENT:
        return ReceiptValidation(
            False, DeadletterReason.INVALID_SCHEMA,
            f"SOURCE_COMPONENT must be {_FR_DISPATCH_SOURCE_COMPONENT}")
    exact = {
        "RECEIPT_SCHEMA": FR_DISPATCH_RECEIPT_SCHEMA,
        "STAGE_ID": "fr-to-workers-dispatch",
        "EVENT_TARGET": "receipt.sealed",
        "EVENT_IMPLEMENTATION_STATUS": "EMITTED",
        "REPO": "NeuruhAI/neuruh-workers",
        "BRANCH": "main",
        "REMOTE": "origin",
        "SENTINEL_MODEL_EXECUTION": "BLOCKED",
        "SENTINEL_CLOSURE": "NOT_JUDGED",
        "PRIME_VERDICT": "NOT_JUDGED",
        "NETWORK_POLICY": "127.0.0.1:local-only",
    }
    for field, expected in exact.items():
        if receipt.get(field) != expected:
            return ReceiptValidation(
                False,
                DeadletterReason.CONTRADICTION,
                f"FR dispatch receipt {field} is not canonical",
            )
    if (
        receipt.get("TASK_ID") != receipt.get("JOB_ID")
        or not os.path.isabs(receipt.worktree)
        or receipt.get("CODE_CUSTODY_REQUIRED") not in {"YES", "NO"}
    ):
        return ReceiptValidation(
            False,
            DeadletterReason.CONTRADICTION,
            "FR dispatch receipt execution identity is inconsistent",
        )
    for field in (
        "LEDGER_TAIL_SHA", "CONTEXT_DIGEST", "JOB_SUCCEEDED_SHA", "PAYLOAD_HASH",
    ):
        if not _SHA256.fullmatch(receipt.get(field)):
            return ReceiptValidation(
                False,
                DeadletterReason.BROKEN_HASH,
                f"FR dispatch receipt {field} must be lowercase 64-hex",
            )

    authority_match = _FR_DISPATCH_AUTHORITY.fullmatch(receipt.authority_ref)
    if not authority_match:
        return ReceiptValidation(
            False, DeadletterReason.AUTHORITY_INVALID,
            "AUTHORITY_REFERENCE must name an FR approval and work-spec digest")

    work_spec = receipt.get("WORK_SPEC_SHA256")
    if not _SHA256.fullmatch(work_spec):
        return ReceiptValidation(
            False, DeadletterReason.INVALID_SCHEMA,
            "WORK_SPEC_SHA256 must be lowercase 64-hex")
    if not receipt.authority_ref.endswith(f":work-spec:{work_spec}"):
        return ReceiptValidation(
            False, DeadletterReason.CONTRADICTION,
            "WORK_SPEC_SHA256 disagrees with the digest in AUTHORITY_REFERENCE")

    version = receipt.get("MISSION_VERSION")
    if not version.isdigit() or int(version) < 1:
        return ReceiptValidation(
            False, DeadletterReason.INVALID_SCHEMA,
            "MISSION_VERSION must be a positive integer")

    if not _SHA256.fullmatch(receipt.get("IDEMPOTENCY_KEY")):
        return ReceiptValidation(
            False, DeadletterReason.INVALID_SCHEMA,
            "IDEMPOTENCY_KEY must be lowercase 64-hex")

    if receipt.get("DISPATCH_RESULT") not in _FR_DISPATCH_RESULTS:
        return ReceiptValidation(
            False, DeadletterReason.INVALID_SCHEMA,
            f"DISPATCH_RESULT must be one of {sorted(_FR_DISPATCH_RESULTS)}")

    if not receipt.task_id:
        return ReceiptValidation(
            False, DeadletterReason.INVALID_SCHEMA,
            "FR dispatch receipt requires a work identity (TASK_ID/JOB_ID)")
    return ReceiptValidation(True)


def validate_receipt(
    receipt: PrimeReceipt,
    *,
    known_missions: frozenset[str],
) -> ReceiptValidation:
    """Route on the declared receipt type, then run that type's complete validator.

    An undeclared receipt is a legacy PRIME receipt and keeps the PRIME rules
    unchanged. A declared type this build does not know fails closed rather than
    falling back to a laxer path.
    """
    declared = receipt.receipt_schema
    if not declared:
        return _validate_prime_receipt(receipt, known_missions=known_missions)
    if declared == FR_DISPATCH_RECEIPT_SCHEMA:
        return _validate_fr_dispatch_receipt(receipt, known_missions=known_missions)
    return ReceiptValidation(
        False, DeadletterReason.INVALID_SCHEMA,
        f"unknown receipt type {declared!r}")


def _validate_prime_receipt(
    receipt: PrimeReceipt,
    *,
    known_missions: frozenset[str],
) -> ReceiptValidation:
    mission = receipt.mission
    stage = receipt.stage
    head = receipt.head
    authority = receipt.authority_ref

    if receipt.duplicate_fields:
        return ReceiptValidation(
            False,
            DeadletterReason.INVALID_SCHEMA,
            f"duplicate receipt fields: {sorted(set(receipt.duplicate_fields))}",
        )
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
