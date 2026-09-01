"""Smallest ownership seam for JGI Founder Runtime over the existing spine.

This is not a second router, event bus, workflow engine, or economic
authorizer. It names who already owns each organ and what this package
may do locally:

- L0/L1 path ranking + read-only dispatch through JobQueue/WorkflowRunner
- pointer-heavy context compilation
- fail-closed handoff to existing owners for everything else

Live Switchboard, Git Custodian, Revenue Factory, IAR, Governance,
Sentinel, and AXON implementations are not copied here.
"""
from __future__ import annotations

from typing import Any, Mapping


# Exact existing owners. Candidate PRs are named; none are treated as merged.
SPINE_OWNERS: dict[str, dict[str, str]] = {
    "INTENT": {
        "owner": "NeuruhAI/neuruh-workers#5 FounderRuntime.MissionIntent",
        "role": "OWN",
        "note": "mission identity + economic hints; not a chat transcript",
    },
    "CONTEXT_COMPILER": {
        "owner": "NeuruhAI/neuruh-workers#5 FounderRuntime.compile_context",
        "role": "OWN",
        "note": "<=4 KiB pointer packet; no chat/transcript field",
    },
    "CHEAPEST_CAPABLE_ROUTER_L0_L1": {
        "owner": "NeuruhAI/neuruh-workers#5 FounderRuntime.rank_candidates",
        "role": "OWN",
        "note": "ranks already-valid deterministic workflow paths only; not IAR",
    },
    "CHEAPEST_CAPABLE_ROUTER_L2_L4": {
        "owner": "NeuruhAI/aegis core/iar + NeuruhAI/neuruh-live-integration#1 revenue_factory/authorization.py",
        "role": "CONSUME",
        "note": "single-use InferenceAuthorization + ConsumptionLedger; IAR routes inference",
    },
    "WORKER": {
        "owner": "NeuruhAI/neuruh-workers main WorkerRegistry + sample workflows",
        "role": "REUSE",
        "note": "existing capabilities; v1 external lockout unchanged",
    },
    "COURT": {
        "owner": "NeuruhAI/neuruh-workers#3 Switchboard court.requested (SENTINEL_MODEL_EXECUTION=BLOCKED)",
        "role": "CONSUME",
        "note": "Switchboard does not declare PASS; independent Sentinel close is injected",
    },
    "RECEIPT": {
        "owner": "NeuruhAI/neuruh-workers main ExecutionLedger",
        "role": "REUSE",
        "note": "hash-chained JSONL; job state is replayed, never stored",
    },
    "AUTOMATIC_PARENT_RESUME": {
        "owner": "UNOWNED",
        "role": "ABSENT",
        "note": "parent_mission_id is compiled; no mission watcher resumes the parent",
    },
    "SWITCHBOARD": {
        "owner": "NeuruhAI/neuruh-workers#3 SWITCHAROO first seam (Switchboard)",
        "role": "CONSUME",
        "note": "wrapper around ExecutionLedger + JobQueue; not a new bus",
    },
    "GIT_CUSTODIAN": {
        "owner": "NeuruhAI/neuruh-workers#4 neuruh_workers.git_custodian",
        "role": "CONSUME",
        "note": "candidate.push.requested only; no merge/deploy/direct-main",
    },
    "REVENUE_FACTORY": {
        "owner": "NeuruhAI/neuruh-live-integration#1 revenue_factory",
        "role": "CONSUME",
        "note": "economic model-call seam; Aegis IAR left untouched there",
    },
    "IAR": {
        "owner": "NeuruhAI/aegis core/iar",
        "role": "CONSUME",
        "note": "NEURUH ROUTER inference authority; this PR must not rebuild it",
    },
    "GOVERNANCE": {
        "owner": "NeuruhAI/autonomous-governance-core",
        "role": "CONSUME",
        "note": "canonical policy/receipt governance; not rewritten here",
    },
    "SENTINEL": {
        "owner": "NeuruhAI/neuruh-workers#3 sentinel-verify job artifact",
        "role": "CONSUME",
        "note": "court job only; model execution blocked in first seam",
    },
    "AXON": {
        "owner": "NeuruhAI/neuruh-axon",
        "role": "CONSUME",
        "note": "governed execution transport; OrganRegistrationSeam.activate refuses in v1",
    },
    "EXECUTION_LEDGER": {
        "owner": "NeuruhAI/neuruh-workers main ExecutionLedger",
        "role": "REUSE",
        "note": "already on main @ 05c669af",
    },
    "WORKFLOW_RUNNER": {
        "owner": "NeuruhAI/neuruh-workers main WorkflowRunner",
        "role": "REUSE",
        "note": "already on main @ 05c669af",
    },
    "MISSION_RECOVERY": {
        "owner": "NeuruhAI/neuruh-workers main WorkflowRunner job.recovered",
        "role": "PARTIAL",
        "note": "job-level crash recovery exists; mission-level stale watcher does not",
    },
    "OUTCOME_LEARNING": {
        "owner": "NeuruhAI/neuruh-outcome-calibration-ledger + neuruh-live-integration#1 reasoning_debt.py",
        "role": "CONSUME",
        "note": "reasoning-debt compile candidates stay auto_apply=False",
    },
}

THIS_PR_MAY_OWN = frozenset({
    "INTENT",
    "CONTEXT_COMPILER",
    "CHEAPEST_CAPABLE_ROUTER_L0_L1",
})

THIS_PR_REUSES = frozenset({
    "WORKER",
    "RECEIPT",
    "EXECUTION_LEDGER",
    "WORKFLOW_RUNNER",
})

THIS_PR_MUST_NOT_OWN = frozenset({
    "SWITCHBOARD",
    "GIT_CUSTODIAN",
    "REVENUE_FACTORY",
    "IAR",
    "GOVERNANCE",
    "SENTINEL",
    "AXON",
    "CHEAPEST_CAPABLE_ROUTER_L2_L4",
    "OUTCOME_LEARNING",
})

GSA_AUTHORITY_PATH = "governance->sentinel->axon"

REASONING_BROKER_BIND: dict[str, Any] = {
    "owner": "NeuruhAI/neuruh-live-integration#1",
    "module": "revenue_factory/authorization.py",
    "symbol": "InferenceAuthorization",
    "consumption": "ConsumptionLedger.consume",
    "iar_owner": "NeuruhAI/aegis",
    "iar_path": "core/iar",
    "invariant": "NO CLOUD MODEL CALL WITHOUT A CONSUMED, MATCHING AUTHORIZATION",
    "budget_rule": "B=min(Bmax, EV*U*R)",
    "authorized": False,
    "model_call": False,
}

GSA_BIND: dict[str, Any] = {
    "path": GSA_AUTHORITY_PATH,
    "governance": "NeuruhAI/autonomous-governance-core",
    "sentinel": "NeuruhAI/neuruh-workers#3 Switchboard court.requested",
    "axon": "NeuruhAI/neuruh-axon",
    "policy_gate": "NeuruhAI/neuruh-policy-gate",
    "seam_activate": "OrganRegistrationSeam.activate refuses in v1",
    "authorized": False,
    "executed": False,
}


def owner_of(organ: str) -> dict[str, str]:
    try:
        return dict(SPINE_OWNERS[organ])
    except KeyError as exc:
        raise KeyError(f"unknown spine organ {organ!r}") from exc


def this_pr_owns(organ: str) -> bool:
    return organ in THIS_PR_MAY_OWN


def this_pr_must_not_own(organ: str) -> bool:
    return organ in THIS_PR_MUST_NOT_OWN


def reasoning_broker_bind(*, context_digest: str) -> dict[str, Any]:
    """Name the existing economic bind. Do not issue or consume authorization."""
    bind = dict(REASONING_BROKER_BIND)
    bind["context_digest"] = context_digest
    bind["required_authorization_id"] = None
    return bind


def gsa_bind(*, reasons: tuple[str, ...]) -> dict[str, Any]:
    bind = dict(GSA_BIND)
    bind["reasons"] = list(reasons)
    return bind


def parent_resume_pointer(parent_mission_id: str | None) -> Mapping[str, Any] | None:
    if not parent_mission_id:
        return None
    return {
        "parent_mission_id": parent_mission_id,
        "automatic": False,
        "status": "PARENT_RESUME_NOT_IMPLEMENTED",
        "owner": owner_of("AUTOMATIC_PARENT_RESUME")["owner"],
    }
