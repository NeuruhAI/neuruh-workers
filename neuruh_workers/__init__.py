"""Neuruh autonomous workflow layer (v1).

Governed multi-step work: declarative workflows, an event-sourced append-only
receipt ledger, deterministic job identity, idempotent steps, retry/backoff,
timeouts, pause/resume, human approval gates, dry-run, and a declared-only
registration seam for the Mother and AXON organs.

External actions are structurally disabled in v1 — see registry.WorkerRegistry.
"""
from .contracts import (
    ApprovalInvalid,
    CapabilityUnknown,
    CapabilityViolation,
    DefinitionError,
    ExternalActionDenied,
    JobStateInvalid,
    JobStatus,
    LedgerCorrupt,
    ReceiptType,
    SeamNotAuthorized,
    StepTimeout,
    WorkflowError,
    deterministic_job_id,
)
from .definitions import RetryPolicy, StepDefinition, WorkflowCatalog, WorkflowDefinition
from .ledger import ExecutionLedger
from .queue import JobQueue, JobState
from .registry import Capability, StepContext, WorkerDeclaration, WorkerRegistry
from .runner import WorkflowRunner
from .seams import OrganRegistrationSeam, OrganSeamDeclaration
from .switchboard import HUMAN_CLIPBOARD_REQUIRED, Switchboard
from .git_custodian import GitCustodian, GitCustodianRefused

__version__ = "1.1.0"

__all__ = [
    "ApprovalInvalid", "Capability", "CapabilityUnknown", "CapabilityViolation",
    "DefinitionError", "ExecutionLedger", "ExternalActionDenied", "JobQueue",
    "JobState", "JobStateInvalid", "JobStatus", "LedgerCorrupt", "OrganRegistrationSeam",
    "OrganSeamDeclaration", "ReceiptType", "RetryPolicy", "SeamNotAuthorized",
    "StepContext", "StepDefinition", "StepTimeout", "Switchboard", "WorkerDeclaration",
    "WorkerRegistry", "WorkflowCatalog", "WorkflowDefinition", "WorkflowError",
    "WorkflowRunner", "deterministic_job_id", "HUMAN_CLIPBOARD_REQUIRED",
    "GitCustodian", "GitCustodianRefused",
]
