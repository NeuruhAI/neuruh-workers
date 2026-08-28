"""Job artifacts materialised by Switchboard — not model execution, not git push.

Creating a Sentinel court job artifact is not executing a Sentinel model.
Creating a Honey Pot / Git Custodian artifact is not performing a git push.
"""
from __future__ import annotations

from typing import Any, Mapping

from .definitions import RetryPolicy, StepDefinition, WorkflowCatalog, WorkflowDefinition
from .registry import Capability, StepContext, WorkerDeclaration, WorkerRegistry

SENTINEL_MODEL_EXECUTION = "BLOCKED"
GIT_PUSH = "BLOCKED"

SENTINEL_WORKFLOW = "sentinel-court-artifact"
HONEY_POT_WORKFLOW = "honey-pot-candidate-push"
REMEDIATION_WORKFLOW = "court-fail-remediation"
WORKFLOW_VERSION = "1.0.0"

_RETRY = RetryPolicy(max_attempts=1, base_delay_s=0.0)


def materialize_court_artifact(ctx: StepContext, params: Mapping[str, Any]) -> dict:
    required = ("mission", "stage", "claimer", "receipt_ref",
                "source_head", "required_invariants", "authority_ref")
    missing = [k for k in required if k not in params]
    if missing:
        raise ValueError(f"court artifact missing {missing}")
    court_job_id = params.get("court_job_id") or ctx.job_id
    return {
        "artifact": "court.requested",
        "sentinel_model_execution": SENTINEL_MODEL_EXECUTION,
        "court_job_id": court_job_id,
        "mission": params["mission"],
        "stage": params["stage"],
        "claimer": params["claimer"],
        "receipt_ref": params["receipt_ref"],
        "source_head": params["source_head"],
        "required_invariants": list(params["required_invariants"]),
        "authority_ref": params["authority_ref"],
        "note": "job artifact only; no Sentinel model was invoked",
    }


def materialize_candidate_push(ctx: StepContext, params: Mapping[str, Any]) -> dict:
    required = ("mission_id", "task_id", "repo", "worktree", "branch", "head",
                "remote", "court_receipt", "authority_ref", "idempotency_key")
    missing = [k for k in required if k not in params]
    if missing:
        raise ValueError(f"honey pot artifact missing {missing}")
    return {
        "artifact": "candidate.push.requested",
        "git_push": GIT_PUSH,
        "mission_id": params["mission_id"],
        "task_id": params["task_id"],
        "repo": params["repo"],
        "worktree": params["worktree"],
        "branch": params["branch"],
        "head": params["head"],
        "remote": params["remote"],
        "court_receipt": params["court_receipt"],
        "authority_ref": params["authority_ref"],
        "idempotency_key": params["idempotency_key"],
        "expected_pr": params.get("expected_pr"),
        "expected_pr_url": params.get("expected_pr_url"),
        "expected_pr_draft": params.get("expected_pr_draft"),
        "note": "job artifact only; no git mutation was performed",
    }


def materialize_remediation(ctx: StepContext, params: Mapping[str, Any]) -> dict:
    return {
        "artifact": "task.ready",
        "kind": "remediation",
        "git_push": GIT_PUSH,
        "mission_id": params.get("mission_id", ""),
        "task_id": params.get("task_id", ""),
        "court_receipt": params.get("court_receipt", ""),
        "reason": params.get("reason", "court.failed"),
        "note": "court FAIL produces remediation, never a push request",
    }


def bind_switchboard_workers(registry: WorkerRegistry,
                             catalog: WorkflowCatalog) -> None:
    """Register first-seam artifact workers onto an existing stack."""
    registry.register_worker(WorkerDeclaration(
        worker_id="switchboard-first-seam-v1",
        description="Materialise Sentinel court and Honey Pot job artifacts. "
                    "Read-only; no model execution; no git push.",
        capabilities=(
            Capability(name="sentinel.court_artifact",
                       description="Record a court.requested job artifact",
                       read_only=True, external=False,
                       execute=materialize_court_artifact),
            Capability(name="honey_pot.candidate_push_artifact",
                       description="Record a candidate.push.requested artifact",
                       read_only=True, external=False,
                       execute=materialize_candidate_push),
            Capability(name="switchboard.remediation_artifact",
                       description="Record a court-fail remediation task",
                       read_only=True, external=False,
                       execute=materialize_remediation),
        ),
    ))
    catalog.register(WorkflowDefinition(
        name=SENTINEL_WORKFLOW, version=WORKFLOW_VERSION,
        description="Durable Sentinel court job artifact (model execution blocked).",
        steps=(StepDefinition(step_id="materialize",
                              capability="sentinel.court_artifact",
                              retry=_RETRY, timeout_s=10.0),),
    ))
    catalog.register(WorkflowDefinition(
        name=HONEY_POT_WORKFLOW, version=WORKFLOW_VERSION,
        description="Durable Git Custodian / Honey Pot push-request artifact "
                    "(git push blocked).",
        steps=(StepDefinition(step_id="materialize",
                              capability="honey_pot.candidate_push_artifact",
                              retry=_RETRY, timeout_s=10.0),),
    ))
    catalog.register(WorkflowDefinition(
        name=REMEDIATION_WORKFLOW, version=WORKFLOW_VERSION,
        description="Remediation-ready task after court.failed.",
        steps=(StepDefinition(step_id="materialize",
                              capability="switchboard.remediation_artifact",
                              retry=_RETRY, timeout_s=10.0),),
    ))
