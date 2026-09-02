"""Ownership seam for FOUNDER_RUNTIME_TO_WORKERS_DISPATCH_BRIDGE.

recipe-engine Founder Runtime (FR_API=:19090, API_VERSION=fr-api-v1) remains
mission-state authority. This package is a consume-only dispatch bridge:

- accept FR APPROVED/QUEUED tickets
- enqueue existing JobQueue work
- seal receipts onto live SWITCHAROO
- resume existing parent jobs from ExecutionLedger
- attempt an FR evidence update (fail closed; this PR does not persist missions)

This is not a second Founder Runtime, scheduler, model router, event bus,
or economic authorizer. Ranker stays DELETED.
"""
from __future__ import annotations

from typing import Any

from .organ_consumers import GSA_AUTHORITY_PATH

SPINE_OWNERS: dict[str, dict[str, str]] = {
    "INTENT": {
        "owner": "NeuruhAI/recipe-engine Founder Runtime FR_API=:19090 fr-api-v1",
        "role": "CONSUME",
        "note": "mission identity stays on recipe-engine; this PR accepts tickets only",
    },
    "MISSION_PERSISTENCE": {
        "owner": "NeuruhAI/recipe-engine Founder Runtime missions.sqlite",
        "role": "CONSUME",
        "note": "this PR must not persist missions",
    },
    "MISSION_IDENTITY": {
        "owner": "NeuruhAI/recipe-engine Founder Runtime mission_id",
        "role": "CONSUME",
        "note": "DispatchTicket.mission_id is FR-issued, never minted here",
    },
    "APPROVAL_STATE": {
        "owner": "NeuruhAI/recipe-engine Founder Runtime approvals",
        "role": "CONSUME",
        "note": "APPROVED/QUEUED is an FR fact; this PR never records allow/deny",
    },
    "DECISION_PLAN": {
        "owner": "NeuruhAI/recipe-engine Founder Runtime proposed plan",
        "role": "CONSUME",
        "note": "this PR does not compile or authorize a decision plan",
    },
    "SCHEDULER": {
        "owner": "NeuruhAI/neuruh-workers main JobQueue",
        "role": "REUSE",
        "note": "no second scheduler",
    },
    "MODEL_ROUTER": {
        "owner": "NeuruhAI/aegis iar_v2 + Revenue Factory InferenceAuthorization",
        "role": "CONSUME",
        "note": "this PR does not own a router; L2/L3/L4 fail closed",
    },
    "CONTEXT_COMPILER": {
        "owner": "NeuruhAI/neuruh-workers#5 DispatchBridge.compile_context",
        "role": "OWN",
        "note": "<=4 KiB pointer packet for dispatch; no chat/transcript; no mission authority",
    },
    "DISPATCH_BRIDGE": {
        "owner": "NeuruhAI/neuruh-workers#5 DispatchBridge",
        "role": "OWN",
        "note": "FR APPROVED/QUEUED ticket → JobQueue → ExecutionLedger",
    },
    "CHEAPEST_CAPABLE_ROUTER_L0_L1": {
        "owner": "NeuruhAI/aegis iar_v2 + Revenue Factory (spend); this PR deleted USD utility",
        "role": "DELETE",
        "note": "score_candidate USD ranker deleted; remaining sort is layer+capability only",
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
        "note": "Switchboard jobs court.requested after receipt.sealed; this PR does not mint it or PASS",
    },
    "RECEIPT": {
        "owner": "NeuruhAI/neuruh-workers main ExecutionLedger",
        "role": "REUSE",
        "note": "hash-chained JSONL; job state is replayed, never stored",
    },
    "RECEIPT_SEALED": {
        "owner": "NeuruhAI/neuruh-workers main Switchboard receipt.sealed",
        "role": "CONSUME",
        "note": "events.build_envelope + Switchboard.ingest_raw + tick; fail-closed if import missing",
    },
    "AUTOMATIC_PARENT_RESUME": {
        "owner": "NeuruhAI/neuruh-workers main JobQueue.resume + WorkflowRunner job.recovered",
        "role": "CONSUME",
        "note": "child success resumes existing parent jobs from the ledger; PARENT_RESUME=PROVEN",
    },
    "SWITCHBOARD": {
        "owner": "NeuruhAI/neuruh-workers main neuruh_workers.switchboard (a0c45700 ancestor)",
        "role": "CONSUME",
        "note": "live import; this PR does not copy switchboard.py or events.py",
    },
    "GIT_CUSTODIAN": {
        "owner": "NeuruhAI/neuruh-workers main neuruh_workers.git_custodian (0923725)",
        "role": "CONSUME",
        "note": "HONEY POT custody; this PR does not copy git_custodian",
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
        "note": "HTTP POST 127.0.0.1:8848/v1/evaluate then /v1/consume on organ ALLOW",
    },
    "SENTINEL": {
        "owner": "Sentinel independent court organ",
        "role": "CONSUME",
        "note": "jobbed by SWITCHAROO; SENTINEL_MODEL_EXECUTION=BLOCKED; CI court job is not Sentinel",
    },
    "AXON": {
        "owner": "NeuruhAI/neuruh-axon",
        "role": "CONSUME",
        "note": "write-banned; no POST /execute from this bridge",
    },
    "FOUNDER_RUNTIME": {
        "owner": "NeuruhAI/recipe-engine Founder Runtime FR_API=:19090 fr-api-v1",
        "role": "CONSUME",
        "note": "mission-state authority; this PR is not a second FR",
    },
    "ETHER": {
        "owner": "none-in-this-repo",
        "role": "ABSENT",
        "note": "no Ether organ is added here",
    },
    "EXECUTION_LEDGER": {
        "owner": "NeuruhAI/neuruh-workers main ExecutionLedger @ 0923725",
        "role": "REUSE",
        "note": "already on main",
    },
    "WORKFLOW_RUNNER": {
        "owner": "NeuruhAI/neuruh-workers main WorkflowRunner @ 0923725",
        "role": "REUSE",
        "note": "already on main",
    },
    "MISSION_RECOVERY": {
        "owner": "NeuruhAI/neuruh-workers main WorkflowRunner job.recovered",
        "role": "PARTIAL",
        "note": "job-level crash recovery + parent resume; no stale-mission watcher",
    },
    "OUTCOME_LEARNING": {
        "owner": "NeuruhAI/neuruh-outcome-calibration-ledger + neuruh-live-integration#1 reasoning_debt.py",
        "role": "CONSUME",
        "note": "reasoning-debt compile candidates stay auto_apply=False",
    },
}

THIS_PR_MAY_OWN = frozenset({
    "CONTEXT_COMPILER",
    "DISPATCH_BRIDGE",
})

THIS_PR_REUSES = frozenset({
    "WORKER",
    "RECEIPT",
    "EXECUTION_LEDGER",
    "WORKFLOW_RUNNER",
    "SCHEDULER",
    "AUTOMATIC_PARENT_RESUME",
})

THIS_PR_MUST_NOT_OWN = frozenset({
    "INTENT",
    "MISSION_PERSISTENCE",
    "MISSION_IDENTITY",
    "APPROVAL_STATE",
    "DECISION_PLAN",
    "MODEL_ROUTER",
    "SWITCHBOARD",
    "GIT_CUSTODIAN",
    "REVENUE_FACTORY",
    "IAR",
    "GOVERNANCE",
    "SENTINEL",
    "AXON",
    "FOUNDER_RUNTIME",
    "ETHER",
    "CHEAPEST_CAPABLE_ROUTER_L0_L1",
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


# Silence unused import while keeping the authority path name stable for packets.
_ = GSA_AUTHORITY_PATH
