"""Closed vocabularies for Git Custodian V1."""
from __future__ import annotations

CANDIDATE_SCHEMA = "nrh.git.candidate.v1"
COURT_SCHEMA = "nrh.court.receipt.v1"
AUTHORITY_SCHEMA = "nrh.git.authority.v1"

REQUIRED_CANDIDATE_FIELDS = (
    "schema_version", "mission_id", "task_id", "repo", "worktree", "branch",
    "expected_local_head", "remote_name", "remote_url_or_identity",
    "expected_remote_base", "court_receipt_ref", "court_receipt_hash",
    "authority_ref", "commit_scope", "pr_title", "pr_body_metadata",
    "idempotency_key",
)
OPTIONAL_CANDIDATE_FIELDS = ("expect_detached",)

ALLOWED_ACTIONS = (
    "FEATURE_BRANCH_PUSH",
    "PR_CREATE",
    "PR_UPDATE",
    "CI_READ",
)
FORBIDDEN_ACTIONS = (
    "DIRECT_MAIN_PUSH",
    "FORCE_PUSH",
    "AUTO_MERGE",
    "CANONICAL_PROMOTION",
    "TAG_PRODUCTION_RELEASE",
    "PRODUCTION_DEPLOYMENT",
    "DELETE_REMOTE_BRANCH",
    "DELETE_TAG",
    "REWRITE_HISTORY",
)
# Opt-in only; founder candidate policy does not grant this.
OPTIONAL_ACTIONS = ("DETACHED_HEAD",)

PROTECTED_BRANCHES = frozenset({
    "main", "master", "production", "prod", "release",
})

WRITE_BAN_REPOS = frozenset({
    "NeuruhAI/neuruh-axon",
})

CI_STATES = frozenset({
    "NOT_CONFIGURED",
    "NOT_CONFIGURED_OR_NO_RUNS",
    "PENDING",
    "RUNNING",
    "PASS",
    "FAIL",
    "CANCELLED",
    "UNKNOWN",
})

PR_BODY_SECTIONS = (
    "MISSION", "STAGE", "SOURCE_HEAD", "BASE_HEAD",
    "COURT_VERDICT", "TEST_SUMMARY", "RECEIPT_REFERENCE", "KNOWN_RESIDUAL_RISKS",
)

SWITCHBOARD_INPUT = "candidate.push.requested"
SWITCHBOARD_EVENTS = frozenset({
    "candidate.push.requested",
    "candidate.branch.pushed",
    "candidate.pr.opened",
    "ci.started",
    "ci.passed",
    "ci.failed",
    "promotion.ready",
})

T07_REPO = "NeuruhAI/neuruh-axon"
T07_HEAD = "7461eb835b3f979a8243f9f70da634a4e5ab62fc"
