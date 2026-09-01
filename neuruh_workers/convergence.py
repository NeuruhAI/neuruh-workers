"""Ownership seam for JGI Founder Runtime over the existing spine.

This is not a second router, event bus, workflow engine, or economic
authorizer. It names who already owns each organ and what this package
may do locally:

- L0/L1 non-authorizing path pre-filter + read-only dispatch through JobQueue/WorkflowRunner
- pointer-heavy context compilation
- emit receipt.sealed / court.requested onto the SWITCHAROO seam when present
- resume existing parent jobs from ExecutionLedger
- fail-closed handoff to existing owners for everything else

Live Switchboard, Git Custodian, Revenue Factory, IAR, Governance,
Sentinel, and AXON implementations are not copied here.
"""
from __future__ import annotations

from typing import Any

from .organ_consumers import GSA_AUTHORITY_PATH

# Exact existing owners. Candidate PRs are named; none are treated as merged.
SPINE_OWNERS: dict[str, dict[str, str]] = {
    "INTENT": {
        "owner": "NeuruhAI/neuruh-workers#5 FounderRuntime.MissionIntent",
        "role": "OWN",
        "note": "mission identity + economic hints; hints are not spend authority",
    },
    "CONTEXT_COMPILER": {
        "owner": "NeuruhAI/neuruh-workers#5 FounderRuntime.compile_context",
        "role": "OWN",
        "note": "<=4 KiB pointer packet; no chat/transcript field",
    },
    "CHEAPEST_CAPABLE_ROUTER_L0_L1": {
        "owner": "NeuruhAI/neuruh-workers#5 FounderRuntime.rank_candidates",
        "role": "OWN",
        "note": "non-authorizing layer/latency pre-filter; no USD spend authority",
    },
    "CHEAPEST_CAPABLE_ROUTER_L2_L4": {
        "owner": "NeuruhAI/aegis iar_v2 + NeuruhAI/neuruh-live-integration#1 revenue_factory/authorization.py",
        "role": "CONSUME",
        "note": "loopback 127.0.0.1:8090 health JSON + InferenceAuthorization; fail-closed if absent",
    },
    "WORKER": {
        "owner": "NeuruhAI/neuruh-workers main WorkerRegistry + sample workflows",
        "role": "REUSE",
        "note": "existing capabilities; v1 external lockout unchanged",
    },
    "COURT": {
        "owner": "Sentinel independent court organ; SWITCHAROO jobs court.requested",
        "role": "CONSUME",
        "note": "this PR emits court.requested; CI job named court is not Sentinel",
    },
    "RECEIPT": {
        "owner": "NeuruhAI/neuruh-workers main ExecutionLedger",
        "role": "REUSE",
        "note": "hash-chained JSONL; job state is replayed, never stored",
    },
    "RECEIPT_SEALED": {
        "owner": "NeuruhAI/neuruh-workers#3 Switchboard receipt.sealed",
        "role": "CONSUME",
        "note": "emitted onto SWITCHAROO; fail-closed if switchboard.py is not on this branch",
    },
    "AUTOMATIC_PARENT_RESUME": {
        "owner": "NeuruhAI/neuruh-workers main JobQueue.resume + WorkflowRunner job.recovered",
        "role": "CONSUME",
        "note": "child success resumes existing parent jobs from the ledger; no new daemon",
    },
    "SWITCHBOARD": {
        "owner": "NeuruhAI/neuruh-workers#3 SWITCHAROO first seam (Switchboard)",
        "role": "CONSUME",
        "note": "import-if-present; this PR does not copy switchboard.py",
    },
    "GIT_CUSTODIAN": {
        "owner": "NeuruhAI/neuruh-workers#4 neuruh_workers.git_custodian",
        "role": "CONSUME",
        "note": "HONEY POT keeps custody; this PR does not copy git_custodian",
    },
    "REVENUE_FACTORY": {
        "owner": "NeuruhAI/neuruh-live-integration#1 revenue_factory",
        "role": "CONSUME",
        "note": "economic model-call seam; fail-closed if InferenceAuthorization absent",
    },
    "IAR": {
        "owner": "NeuruhAI/aegis iar_v2",
        "role": "CONSUME",
        "note": "contract 127.0.0.1:8090 health JSON; this PR does not rebuild the router",
    },
    "GOVERNANCE": {
        "owner": "NeuruhAI/autonomous-governance-core",
        "role": "CONSUME",
        "note": "evaluate+consume if importable; caller never sets approved=true",
    },
    "SENTINEL": {
        "owner": "Sentinel independent court organ",
        "role": "CONSUME",
        "note": "not a CI job name; verdict stays None until the organ courts",
    },
    "AXON": {
        "owner": "NeuruhAI/neuruh-axon",
        "role": "CONSUME",
        "note": "ToolRegistry/guards/receipts; OrganRegistrationSeam.activate refuses in v1",
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
        "note": "job-level crash recovery + parent resume exist; no stale-mission watcher",
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
    "AUTOMATIC_PARENT_RESUME",
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
    "COURT",
    "RECEIPT_SEALED",
})


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
    """Compatibility wrapper; live bind lives in organ_consumers.bind_iar_rf."""
    from .organ_consumers import bind_iar_rf
    return bind_iar_rf(context_digest=context_digest)
