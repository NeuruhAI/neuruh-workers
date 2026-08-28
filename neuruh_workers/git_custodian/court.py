"""Independent court evidence — never trust a bare PASS string."""
from __future__ import annotations

from typing import Any, Mapping, Protocol

from ..contracts import content_sha
from .constants import COURT_SCHEMA
from .errors import GitCustodianRefused


class CourtStore(Protocol):
    def get(self, ref: str) -> Any: ...


class MemoryCourtStore:
    def __init__(self, records: Mapping[str, Any] | None = None):
        self._records = dict(records or {})

    def get(self, ref: str) -> Any:
        return self._records.get(ref)

    def __setitem__(self, ref: str, value: Any) -> None:
        self._records[ref] = value


def verify_court(evidence: Any, candidate: Mapping[str, Any]) -> dict[str, Any]:
    if evidence is None:
        raise GitCustodianRefused(
            "court receipt is missing",
            {"ref": candidate.get("court_receipt_ref")},
            reason="COURT_MISSING")
    if not isinstance(evidence, dict):
        raise GitCustodianRefused(
            "court evidence must be a structured object; a bare string is not a court",
            {"type": type(evidence).__name__},
            reason="COURT_INVALID")
    required = ("independent_court", "mission_id", "stage", "source_head",
                "candidate")
    missing = [k for k in required if k not in evidence]
    if missing:
        raise GitCustodianRefused(
            "court evidence is incomplete; PASS is not sufficient without "
            "mission/stage/source HEAD/candidate",
            {"missing": missing}, reason="COURT_INVALID")
    schema = evidence.get("schema_version")
    if schema not in (None, COURT_SCHEMA):
        raise GitCustodianRefused(
            "unsupported court schema_version",
            {"schema_version": schema}, reason="COURT_INVALID")
    if evidence["independent_court"] != "PASS":
        raise GitCustodianRefused(
            "independent court is not PASS",
            {"verdict": evidence["independent_court"]},
            reason="COURT_FAILED")
    if evidence["mission_id"] != candidate["mission_id"]:
        raise GitCustodianRefused(
            "court mission_id does not match the candidate",
            {"court": evidence["mission_id"], "candidate": candidate["mission_id"]},
            reason="COURT_MISMATCH")
    if evidence["stage"] != candidate["task_id"]:
        raise GitCustodianRefused(
            "court stage does not match the candidate task_id",
            {"court": evidence["stage"], "candidate": candidate["task_id"]},
            reason="COURT_MISMATCH")
    if evidence["source_head"] != candidate["expected_local_head"]:
        raise GitCustodianRefused(
            "court source HEAD does not match expected_local_head",
            reason="COURT_MISMATCH")
    cand = evidence["candidate"]
    if not isinstance(cand, dict):
        raise GitCustodianRefused(
            "court candidate must be an object", reason="COURT_INVALID")
    if cand.get("repo") != candidate["repo"]:
        raise GitCustodianRefused(
            "court candidate.repo does not match", reason="COURT_MISMATCH")
    if cand.get("branch") != candidate["branch"]:
        raise GitCustodianRefused(
            "court candidate.branch does not match", reason="COURT_MISMATCH")
    if cand.get("head") != candidate["expected_local_head"]:
        raise GitCustodianRefused(
            "court candidate.head does not match expected_local_head",
            reason="COURT_MISMATCH")
    digest = content_sha(evidence)
    if digest != candidate["court_receipt_hash"]:
        raise GitCustodianRefused(
            "court_receipt_hash does not match the evidence object",
            reason="COURT_HASH_MISMATCH")
    return dict(evidence)
