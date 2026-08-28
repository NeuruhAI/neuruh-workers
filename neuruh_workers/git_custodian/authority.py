"""Founder candidate authority — narrow git actions, never merge/deploy."""
from __future__ import annotations

from typing import Any, Mapping, Protocol

from .constants import (
    ALLOWED_ACTIONS, AUTHORITY_SCHEMA, FORBIDDEN_ACTIONS, OPTIONAL_ACTIONS,
    PROTECTED_BRANCHES,
)
from .errors import GitCustodianRefused

FOUNDER_CANDIDATE_V1: dict[str, Any] = {
    "schema_version": AUTHORITY_SCHEMA,
    "policy_id": "founder.candidate.v1",
    "allowed": ALLOWED_ACTIONS,
    "forbidden": FORBIDDEN_ACTIONS,
}


class AuthorityStore(Protocol):
    def get(self, ref: str) -> Any: ...


class MemoryAuthorityStore:
    def __init__(self, records: Mapping[str, Any] | None = None):
        self._records = dict(records or {})

    def get(self, ref: str) -> Any:
        return self._records.get(ref)

    def __setitem__(self, ref: str, value: Any) -> None:
        self._records[ref] = value


def is_protected_branch(branch: str) -> bool:
    name = (branch or "").strip().lower()
    if name in PROTECTED_BRANCHES:
        return True
    if name.startswith("release/"):
        return True
    return False


def verify_authority(policy: Any, *, branch: str,
                     extra_needed: tuple[str, ...] = ()) -> dict[str, Any]:
    if policy is None:
        raise GitCustodianRefused(
            "authority receipt is missing", reason="AUTHORITY_MISSING")
    if not isinstance(policy, dict):
        raise GitCustodianRefused(
            "authority must be a structured policy object",
            reason="AUTHORITY_INVALID")
    allowed = tuple(policy.get("allowed") or ())
    forbidden = tuple(policy.get("forbidden") or ())
    if not allowed:
        raise GitCustodianRefused(
            "authority lists no allowed actions", reason="AUTHORITY_INVALID")
    missing_forbid = [a for a in FORBIDDEN_ACTIONS if a not in forbidden]
    if missing_forbid:
        raise GitCustodianRefused(
            "authority is incomplete; forbidden actions must be explicit",
            {"missing_forbidden": missing_forbid}, reason="AUTHORITY_INCOMPLETE")
    for action in FORBIDDEN_ACTIONS:
        if action in allowed:
            raise GitCustodianRefused(
                "authority tries to allow a v1-forbidden action",
                {"action": action}, reason="AUTHORITY_INVALID")
    for needed in ALLOWED_ACTIONS:
        if needed not in allowed:
            raise GitCustodianRefused(
                "authority does not grant a required founder-candidate action",
                {"missing": needed}, reason="AUTHORITY_INVALID")
    for needed in extra_needed:
        if needed not in allowed:
            raise GitCustodianRefused(
                "authority does not grant the requested extra action",
                {"missing": needed}, reason="AUTHORITY_MISSING")
        if needed not in OPTIONAL_ACTIONS and needed not in ALLOWED_ACTIONS:
            raise GitCustodianRefused(
                "unknown extra action", {"action": needed},
                reason="AUTHORITY_INVALID")
    if is_protected_branch(branch):
        raise GitCustodianRefused(
            "candidate branch is protected; DIRECT_MAIN_PUSH is forbidden",
            {"branch": branch}, reason="PROTECTED_BRANCH")
    return {
        "schema_version": policy.get("schema_version", AUTHORITY_SCHEMA),
        "policy_id": policy.get("policy_id", "unknown"),
        "allowed": allowed,
        "forbidden": forbidden,
    }


def assert_action_allowed(policy: Mapping[str, Any], action: str) -> None:
    if action in FORBIDDEN_ACTIONS or action in tuple(policy.get("forbidden") or ()):
        raise GitCustodianRefused(
            "action is forbidden by founder candidate policy",
            {"action": action}, reason=action)
    if action not in tuple(policy.get("allowed") or ()):
        raise GitCustodianRefused(
            "action is not granted",
            {"action": action}, reason="AUTHORITY_MISSING")
