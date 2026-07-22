"""Core contracts for the Neuruh autonomous workflow layer.

Everything here is deliberately dependency-free and deterministic:
job identity, receipt hashing and state vocabulary are pure functions of
content, never of wall-clock or randomness.
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Mapping


# --------------------------------------------------------------------------- #
# Canonical hashing


def canonical_json(value: Any) -> str:
    """Deterministic JSON: sorted keys, no whitespace drift, no NaN."""
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def sha256_hex(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def content_sha(value: Any) -> str:
    return sha256_hex(canonical_json(value))


def deterministic_job_id(workflow: str, version: str, params: Mapping[str, Any],
                         dedupe_key: str | None = None, *,
                         dry_run: bool = False) -> str:
    """Job identity is content identity: same workflow+version+params+dedupe
    key always yields the same job ID, which is what makes enqueue idempotent.
    A dry-run is different work from the real run, so it is a different job."""
    seed = canonical_json({
        "workflow": workflow,
        "version": version,
        "params": params,
        "dedupe_key": dedupe_key,
        "dry_run": dry_run,
    })
    return "job-" + sha256_hex(seed)[:24]


# --------------------------------------------------------------------------- #
# State vocabulary — closed sets; anything outside them fails closed.


class JobStatus(str, Enum):
    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    PAUSED = "PAUSED"
    BLOCKED_ON_APPROVAL = "BLOCKED_ON_APPROVAL"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


TERMINAL_STATUSES = {JobStatus.SUCCEEDED, JobStatus.FAILED, JobStatus.CANCELLED}


class ReceiptType(str, Enum):
    JOB_ENQUEUED = "job.enqueued"
    JOB_ENQUEUE_DEDUPLICATED = "job.enqueue_deduplicated"
    JOB_STARTED = "job.started"
    JOB_RECOVERED = "job.recovered"
    JOB_PAUSE_REQUESTED = "job.pause_requested"
    JOB_PAUSED = "job.paused"
    JOB_RESUMED = "job.resumed"
    JOB_SUCCEEDED = "job.succeeded"
    JOB_FAILED = "job.failed"
    JOB_CANCELLED = "job.cancelled"
    STEP_STARTED = "step.started"
    STEP_SUCCEEDED = "step.succeeded"
    STEP_FAILED = "step.failed"
    STEP_TIMEOUT = "step.timeout"
    STEP_RETRY_SCHEDULED = "step.retry_scheduled"
    STEP_REPLAY_SKIPPED = "step.replay_skipped"
    APPROVAL_REQUESTED = "approval.requested"
    APPROVAL_GRANTED = "approval.granted"
    APPROVAL_DENIED = "approval.denied"
    APPROVAL_REJECTED_ATTEMPT = "approval.rejected_attempt"
    EXTERNAL_ACTION_DENIED = "external.action_denied"
    SEAM_DECLARED = "seam.declared"
    SEAM_ACTIVATION_REFUSED = "seam.activation_refused"


# --------------------------------------------------------------------------- #
# Errors — every failure mode is a named, typed refusal. No silent fallback.


class WorkflowError(Exception):
    """Base class; carries a machine-readable code."""
    code = "WORKFLOW_ERROR"

    def __init__(self, message: str, detail: Mapping[str, Any] | None = None):
        super().__init__(f"[{self.code}] {message}")
        self.detail = dict(detail or {})


class DefinitionError(WorkflowError):
    code = "DEFINITION_INVALID"


class CapabilityUnknown(WorkflowError):
    code = "CAPABILITY_UNKNOWN"


class CapabilityViolation(WorkflowError):
    code = "CAPABILITY_VIOLATION"


class ExternalActionDenied(WorkflowError):
    code = "EXTERNAL_ACTION_DENIED"


class LedgerCorrupt(WorkflowError):
    code = "LEDGER_CORRUPT"


class JobStateInvalid(WorkflowError):
    code = "JOB_STATE_INVALID"


class ApprovalInvalid(WorkflowError):
    code = "APPROVAL_INVALID"


class StepTimeout(WorkflowError):
    code = "STEP_TIMEOUT"


class RetryExhausted(WorkflowError):
    code = "RETRY_EXHAUSTED"


class SeamNotAuthorized(WorkflowError):
    code = "SEAM_NOT_AUTHORIZED"


# --------------------------------------------------------------------------- #
# Receipts


@dataclass(frozen=True)
class Receipt:
    """One appended, hash-chained record per state transition.

    ``receipt_sha`` covers seq, type, job_id, payload, observed_at and
    prev_sha — the full record is tamper-evident.  ``canonical_sha`` covers
    the same minus observed_at, so two replays of the same logical run can be
    compared byte-for-byte without their wall clocks having to agree.
    """
    seq: int
    receipt_type: str
    job_id: str
    payload: Mapping[str, Any]
    observed_at: str
    prev_sha: str
    receipt_sha: str = field(default="")

    def hashable_body(self) -> dict[str, Any]:
        return {
            "seq": self.seq,
            "receipt_type": self.receipt_type,
            "job_id": self.job_id,
            "payload": self.payload,
            "observed_at": self.observed_at,
            "prev_sha": self.prev_sha,
        }

    def canonical_body(self) -> dict[str, Any]:
        """Receipt content minus observed_at and minus the timestamped chain
        link — the basis for cross-run determinism comparison."""
        body = self.hashable_body()
        del body["observed_at"]
        del body["prev_sha"]
        return body

    def compute_sha(self) -> str:
        return content_sha(self.hashable_body())

    def to_line(self) -> str:
        record = self.hashable_body()
        record["receipt_sha"] = self.receipt_sha
        return canonical_json(record)


GENESIS_SHA = sha256_hex("neuruh-workers-ledger-genesis-v1")
