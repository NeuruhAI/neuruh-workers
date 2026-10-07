"""Git Custodian V1 — custody of reviewable candidate state."""
from .adapters import CISnapshot, MockCIAdapter, MockPRAdapter, PullRequest, render_pr_body
from .authority import FOUNDER_CANDIDATE_V1, MemoryAuthorityStore, verify_authority
from .court import MemoryCourtStore, verify_court
from .errors import GitCustodianRefused
from .git_backend import GitIdentity, RealGitBackend, RecordedGitBackend
from .switchboard import (
    STRUCTURED_SENTINEL_PASS_REQUIRED,
    SWITCHAROO_INTEGRATED,
    RecordingSwitchboard,
    accept_candidate_push,
    build_candidate_push_event,
    candidate_from_envelope,
)
from .promotion import (
    FOUNDER_PROMOTION_V1,
    LoopbackGovernanceAuthorize,
    PromotionExecutor,
    custody_record_for,
    parse_promotion,
    promotion_facts,
    verify_governance_allow,
    verify_sentinel_record,
)
from .switchboard import (
    accept_promotion_request,
    build_promotion_request_event,
    promotion_from_envelope,
)
from .worker import GitCustodian

__all__ = [
    "CISnapshot",
    "FOUNDER_CANDIDATE_V1",
    "FOUNDER_PROMOTION_V1",
    "LoopbackGovernanceAuthorize",
    "PromotionExecutor",
    "accept_promotion_request",
    "build_promotion_request_event",
    "custody_record_for",
    "parse_promotion",
    "promotion_facts",
    "promotion_from_envelope",
    "verify_governance_allow",
    "verify_sentinel_record",
    "GitCustodian",
    "GitCustodianRefused",
    "GitIdentity",
    "MemoryAuthorityStore",
    "MemoryCourtStore",
    "MockCIAdapter",
    "MockPRAdapter",
    "PullRequest",
    "RealGitBackend",
    "RecordedGitBackend",
    "RecordingSwitchboard",
    "STRUCTURED_SENTINEL_PASS_REQUIRED",
    "SWITCHAROO_INTEGRATED",
    "accept_candidate_push",
    "build_candidate_push_event",
    "candidate_from_envelope",
    "render_pr_body",
    "verify_authority",
    "verify_court",
]
