"""Parse and fail-closed validate nrh.git.candidate.v1."""
from __future__ import annotations

from typing import Any, Mapping

from .constants import (
    CANDIDATE_SCHEMA, OPTIONAL_CANDIDATE_FIELDS, REQUIRED_CANDIDATE_FIELDS,
)
from .errors import GitCustodianRefused
from .redact import identity_key, redact_text


_HEX = set("0123456789abcdef")


def require_hex40(value: Any, field: str) -> str:
    if not isinstance(value, str) or len(value) != 40:
        raise GitCustodianRefused(
            f"{field} must be a 40-hex SHA", {"field": field},
            reason="MALFORMED_INPUT")
    lowered = value.lower()
    if any(c not in _HEX for c in lowered):
        raise GitCustodianRefused(
            f"{field} must be a 40-hex SHA", {"field": field},
            reason="MALFORMED_INPUT")
    return lowered


def parse_remote_base(value: Any) -> tuple[str, str]:
    if not isinstance(value, str) or not value.strip():
        raise GitCustodianRefused(
            "expected_remote_base is required",
            {"field": "expected_remote_base"}, reason="MALFORMED_INPUT")
    raw = value.strip()
    if "@" in raw:
        name, sha = raw.rsplit("@", 1)
        if not name:
            raise GitCustodianRefused(
                "expected_remote_base branch is empty",
                {"field": "expected_remote_base"}, reason="MALFORMED_INPUT")
        return name, require_hex40(sha, "expected_remote_base")
    return "main", require_hex40(raw, "expected_remote_base")


def parse_candidate(raw: Any, *, secrets: tuple[str, ...] = ()) -> dict[str, Any]:
    if not isinstance(raw, Mapping):
        raise GitCustodianRefused(
            "candidate must be an object", reason="MALFORMED_INPUT")
    missing = [f for f in REQUIRED_CANDIDATE_FIELDS if f not in raw]
    if missing:
        raise GitCustodianRefused(
            "candidate is missing required fields",
            {"missing": missing}, reason="MALFORMED_INPUT")
    extra = [k for k in raw
             if k not in REQUIRED_CANDIDATE_FIELDS
             and k not in OPTIONAL_CANDIDATE_FIELDS]
    if extra:
        raise GitCustodianRefused(
            "candidate has unknown fields",
            {"unknown": extra}, reason="MALFORMED_INPUT")
    if raw["schema_version"] != CANDIDATE_SCHEMA:
        raise GitCustodianRefused(
            "unsupported candidate schema_version",
            {"schema_version": redact_text(str(raw["schema_version"]), secrets)},
            reason="MALFORMED_INPUT")

    def nonempty_str(field: str) -> str:
        value = raw[field]
        if not isinstance(value, str) or not value.strip():
            raise GitCustodianRefused(
                f"{field} must be a non-empty string",
                {"field": field}, reason="MALFORMED_INPUT")
        return value.strip()

    commit_scope = raw["commit_scope"]
    if isinstance(commit_scope, list):
        if not commit_scope or not all(isinstance(x, str) and x.strip()
                                       for x in commit_scope):
            raise GitCustodianRefused(
                "commit_scope list must contain non-empty strings",
                {"field": "commit_scope"}, reason="MALFORMED_INPUT")
        scope: str | list[str] = [x.strip() for x in commit_scope]
    elif isinstance(commit_scope, str) and commit_scope.strip():
        scope = commit_scope.strip()
    else:
        raise GitCustodianRefused(
            "commit_scope must be a non-empty string or list",
            {"field": "commit_scope"}, reason="MALFORMED_INPUT")

    metadata = raw["pr_body_metadata"]
    if not isinstance(metadata, dict):
        raise GitCustodianRefused(
            "pr_body_metadata must be an object",
            {"field": "pr_body_metadata"}, reason="MALFORMED_INPUT")

    expect_detached = raw.get("expect_detached", False)
    if expect_detached not in (True, False):
        raise GitCustodianRefused(
            "expect_detached must be a boolean",
            {"field": "expect_detached"}, reason="MALFORMED_INPUT")

    remote_raw = nonempty_str("remote_url_or_identity")
    parsed = {
        "schema_version": CANDIDATE_SCHEMA,
        "mission_id": nonempty_str("mission_id"),
        "task_id": nonempty_str("task_id"),
        "repo": nonempty_str("repo"),
        "worktree": nonempty_str("worktree"),
        "branch": nonempty_str("branch"),
        "expected_local_head": require_hex40(
            raw["expected_local_head"], "expected_local_head"),
        "remote_name": nonempty_str("remote_name"),
        "remote_url_or_identity": identity_key(remote_raw),
        "expected_remote_base": nonempty_str("expected_remote_base"),
        "court_receipt_ref": nonempty_str("court_receipt_ref"),
        "court_receipt_hash": nonempty_str("court_receipt_hash"),
        "authority_ref": nonempty_str("authority_ref"),
        "commit_scope": scope,
        "pr_title": nonempty_str("pr_title"),
        "pr_body_metadata": dict(metadata),
        "idempotency_key": nonempty_str("idempotency_key"),
        "expect_detached": bool(expect_detached),
    }
    parse_remote_base(parsed["expected_remote_base"])
    if parsed["remote_name"].startswith("http://") or "://" in parsed["remote_name"]:
        raise GitCustodianRefused(
            "remote_name must be a local remote name, not a URL",
            {"field": "remote_name"}, reason="MALFORMED_INPUT")
    return parsed
