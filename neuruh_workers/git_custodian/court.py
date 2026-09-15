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


#: What the live independent Sentinel organ actually seals (sentinel_court.seal).
#: Git Custodian V1 was written against nrh.court.receipt.v1, a shape no live
#: court produces, so every real PASS it was handed was refused as COURT_INVALID.
SENTINEL_COURT_KIND = "independent_sentinel_court"
_VERIFIED_RELATIONS = frozenset({
    "candidate_is_canonical_base", "candidate_descends_from_canonical_base",
})


def verify_sentinel_court(record: Mapping[str, Any], candidate: Mapping[str, Any]) -> dict[str, Any]:
    """Bind a sealed independent Sentinel record to THIS candidate.

    Same obligations as the legacy court receipt -- independent PASS, mission,
    stage, source head, candidate repo/head, digest -- read from the fields the
    organ seals. Branch is not a court fact (the organ judges a commit); the
    candidate's branch is proven by the push itself (remote head == expected).
    """
    if record.get("independent") is not True or record.get("source_role") != "SENTINEL":
        raise GitCustodianRefused(
            "court record is not an independent Sentinel court", reason="COURT_INVALID")
    if record.get("verdict") != "PASS":
        raise GitCustodianRefused(
            "independent court is not PASS", {"verdict": record.get("verdict")},
            reason="COURT_FAILED")
    for key in ("findings", "exceptions", "required_repairs"):
        if not isinstance(record.get(key), list) or record[key]:
            raise GitCustodianRefused(
                "a PASS with findings, exceptions or repairs is not a clean court",
                {"field": key}, reason="COURT_FAILED")
    if record.get("base_relation") not in _VERIFIED_RELATIONS:
        raise GitCustodianRefused(
            "court PASS rests on an unverified base relation",
            {"relation": record.get("base_relation")}, reason="COURT_INVALID")
    checks = (
        ("mission_id", candidate["mission_id"]),
        ("task_id", candidate["task_id"]),
        ("source_head", candidate["expected_local_head"]),
        ("candidate_sha", candidate["expected_local_head"]),
        ("source_repo", candidate["repo"]),
    )
    for key, expected in checks:
        if record.get(key) != expected:
            raise GitCustodianRefused(
                f"court {key} does not match the candidate", {"field": key},
                reason="COURT_MISMATCH")
    if content_sha(dict(record)) != candidate["court_receipt_hash"]:
        raise GitCustodianRefused(
            "court_receipt_hash does not match the evidence object",
            reason="COURT_HASH_MISMATCH")
    # The shape the rest of Git Custodian (PR body, receipts) reads.
    return {
        "schema_version": record.get("kind"),
        "independent_court": "PASS",
        "mission_id": record["mission_id"],
        "stage": record["task_id"],
        "source_head": record["source_head"],
        "candidate": {"repo": record["source_repo"], "branch": candidate["branch"],
                      "head": record["candidate_sha"]},
        "court_id": record.get("court_id"),
        "sealed_at": record.get("sealed_at"),
    }


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
    if evidence.get("kind") == SENTINEL_COURT_KIND:
        return verify_sentinel_court(evidence, candidate)
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
