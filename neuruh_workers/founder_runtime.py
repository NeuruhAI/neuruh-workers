"""FOUNDER_RUNTIME_TO_WORKERS_DISPATCH_BRIDGE.

recipe-engine Founder Runtime (FR_API=:19090, API_VERSION=fr-api-v1) remains
mission-state authority. This module consumes APPROVED/QUEUED tickets and
dispatches them onto the existing workers spine:

    FR APPROVED/QUEUED
      → JobQueue
      → ExecutionLedger
      → receipt.sealed
      → SWITCHAROO
      → independent Sentinel
      → HONEY POT when code custody is required
      → child completion
      → JobQueue.resume(parent)
      → FR state/evidence update (fail closed; FR owns persistence)

This is not a second Founder Runtime, scheduler, model router, or ranker.
USD `score_candidate` stays deleted. AXON writes are banned. No Ether.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
from enum import Enum
from math import isfinite
from typing import Any, Callable, Mapping, Sequence

from .contracts import JobStatus, canonical_json, content_sha
from .convergence import GSA_AUTHORITY_PATH
from .definitions import WorkflowCatalog
from .organ_consumers import (
    FR_API_VERSION,
    FR_DISPATCHABLE_STATES,
    HttpOpen,
    bind_founder_runtime,
    bind_iar_rf,
    call_gsa,
    emit_receipt_sealed_and_switcharoo,
    job_succeeded_sha,
    resolve_source_head,
    resume_parent_from_receipts,
    update_fr_evidence,
)
from .queue import JobQueue
from .runner import WorkflowRunner


class IntelligenceLayer(str, Enum):
    L0 = "L0_DETERMINISTIC"
    L1 = "L1_NEURUH_DETERMINISTIC"
    L2 = "L2_LOCAL_MODEL"
    L3 = "L3_CHEAP_REMOTE_MODEL"
    L4 = "L4_FRONTIER_MODEL"


_LAYER_ORDER = {
    IntelligenceLayer.L0: 0,
    IntelligenceLayer.L1: 1,
    IntelligenceLayer.L2: 2,
    IntelligenceLayer.L3: 3,
    IntelligenceLayer.L4: 4,
}


class RuntimeStatus(str, Enum):
    PLANNED = "PLANNED"
    QUEUED = "QUEUED"
    SUCCEEDED = "SUCCEEDED"
    ALREADY_SUCCEEDED = "ALREADY_SUCCEEDED"
    APPROVAL_REQUIRED = "APPROVAL_REQUIRED"
    FOUNDER_GATE_REQUIRED = "FOUNDER_GATE_REQUIRED"
    GOVERNANCE_SENTINEL_AXON_REQUIRED = "GOVERNANCE_SENTINEL_AXON_REQUIRED"
    REASONING_BROKER_REQUIRED = "REASONING_BROKER_REQUIRED"
    FR_STATE_NOT_DISPATCHABLE = "FR_STATE_NOT_DISPATCHABLE"
    BLOCKED = "BLOCKED"


@dataclass(frozen=True)
class DispatchTicket:
    """Consumed FR ticket. Not mission persistence, identity, or approval authority."""

    mission_id: str
    objective: str
    fr_state: str = "QUEUED"
    fr_api_version: str = FR_API_VERSION
    expected_value_usd: float = 0.0
    uncertainty: float = 0.0
    relevance: float = 1.0
    model_budget_usd: float = 0.0
    canonical_refs: tuple[str, ...] = ()
    known_failures: tuple[str, ...] = ()
    blockers: tuple[str, ...] = ()
    do_not_rebuild: tuple[str, ...] = ()
    constraints: tuple[str, ...] = ()
    parent_mission_id: str | None = None
    code_custody_required: bool = False

    def __post_init__(self) -> None:
        if not self.mission_id or not self.objective:
            raise ValueError("mission_id and objective are required")
        if self.fr_api_version != FR_API_VERSION:
            raise ValueError(f"fr_api_version must be {FR_API_VERSION}")
        for name in ("uncertainty", "relevance"):
            value = float(getattr(self, name))
            if value < 0.0 or value > 1.0:
                raise ValueError(f"{name} must be between 0 and 1")
        if self.expected_value_usd < 0 or self.model_budget_usd < 0:
            raise ValueError("economic values must be non-negative")


# Compatibility name: this is a consumed ticket, not an owned mission identity.
MissionIntent = DispatchTicket


@dataclass(frozen=True)
class WorkCandidate:
    candidate_id: str
    workflow_name: str
    params: Mapping[str, Any]
    layer: IntelligenceLayer = IntelligenceLayer.L0
    success_probability: float = 1.0
    estimated_execution_cost_usd: float = 0.0
    estimated_model_cost_usd: float = 0.0
    estimated_founder_minutes: float = 0.0
    estimated_latency_minutes: float = 0.0
    ambiguity: float = 0.0
    evidence_refs: tuple[str, ...] = ()
    production_mutation: bool = False
    external_message: bool = False
    money_move: bool = False
    irreversible: bool = False
    private_ip_risk: bool = False

    def __post_init__(self) -> None:
        if not self.candidate_id or not self.workflow_name:
            raise ValueError("candidate_id and workflow_name are required")
        for name in ("success_probability", "ambiguity"):
            value = float(getattr(self, name))
            if value < 0.0 or value > 1.0:
                raise ValueError(f"{name} must be between 0 and 1")
        for name in (
            "estimated_execution_cost_usd", "estimated_model_cost_usd",
            "estimated_founder_minutes", "estimated_latency_minutes",
        ):
            value = float(getattr(self, name))
            if value < 0 or not isfinite(value):
                raise ValueError(f"{name} must be finite and non-negative")


@dataclass(frozen=True)
class RuntimeResult:
    mission_id: str
    status: RuntimeStatus
    candidate_id: str
    workflow_name: str
    layer: str
    utility_usd: float
    founder_gate_reasons: tuple[str, ...]
    context_digest: str
    compiled_context: Mapping[str, Any]
    job_id: str | None = None
    job_status: str | None = None
    ledger_tail_sha: str | None = None
    note: str = ""
    authority_path: str = ""
    broker_bind: Mapping[str, Any] | None = None
    parent_resume: Mapping[str, Any] | None = None
    sealed_receipt: Mapping[str, Any] | None = None
    court_request: Mapping[str, Any] | None = None
    switchboard_emit: Mapping[str, Any] | None = None
    sentinel: Mapping[str, Any] | None = None
    honey_pot: Mapping[str, Any] | None = None
    fr_bind: Mapping[str, Any] | None = None
    fr_evidence_update: Mapping[str, Any] | None = None

    def as_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["status"] = self.status.value
        return data


def founder_gate_reasons(candidate: WorkCandidate) -> tuple[str, ...]:
    reasons: list[str] = []
    if candidate.money_move:
        reasons.append("MONEY_MOVE")
    if candidate.external_message:
        reasons.append("EXTERNAL_MESSAGE")
    if candidate.production_mutation:
        reasons.append("PRODUCTION_MUTATION")
    if candidate.irreversible:
        reasons.append("IRREVERSIBLE")
    if candidate.private_ip_risk:
        reasons.append("PRIVATE_IP_RISK")
    return tuple(reasons)


def rank_candidates(mission: DispatchTicket, candidates: Sequence[WorkCandidate]) -> list[WorkCandidate]:
    """Layer + capability order only. Not spend authority. USD ranker deleted."""
    del mission  # economic hints are metadata, not a local utility function
    if not candidates:
        raise ValueError("at least one work candidate is required")
    return sorted(
        candidates,
        key=lambda candidate: (
            _LAYER_ORDER[candidate.layer],
            candidate.workflow_name,
            candidate.candidate_id,
        ),
    )


def compile_context(
    mission: DispatchTicket,
    candidate: WorkCandidate,
    *,
    state_delta: Mapping[str, Any] | None = None,
    max_bytes: int = 4096,
) -> dict[str, Any]:
    """Create a compact pointer packet; raw chat/transcripts are not a field."""
    packet: dict[str, Any] = {
        "mission_id": mission.mission_id,
        "parent_mission_id": mission.parent_mission_id,
        "objective": mission.objective,
        "fr_state": mission.fr_state,
        "fr_api_version": mission.fr_api_version,
        "candidate": {
            "candidate_id": candidate.candidate_id,
            "workflow": candidate.workflow_name,
            "layer": candidate.layer.value,
        },
        "canonical_refs": list(mission.canonical_refs[-8:]),
        "evidence_refs": list(candidate.evidence_refs[-8:]),
        "known_failures": list(mission.known_failures[-5:]),
        "blockers": list(mission.blockers[-5:]),
        "do_not_rebuild": list(mission.do_not_rebuild[-8:]),
        "constraints": list(mission.constraints[-8:]),
        "state_delta": dict(state_delta or {}),
        "jeramie_manual_handoffs_target": 0,
        "authority_path": GSA_AUTHORITY_PATH,
        "reasoning_broker": "revenue_factory.authorization+aegis.iar_v2",
        "spend_authority": "iar_rf",
        "mission_authority": "recipe-engine.founder_runtime.fr-api-v1",
        "code_custody_required": mission.code_custody_required,
    }
    trim_order = ["canonical_refs", "evidence_refs", "known_failures", "blockers", "do_not_rebuild", "constraints"]
    while len(canonical_json(packet).encode("utf-8")) > max_bytes:
        trimmed = False
        for key in trim_order:
            values = packet[key]
            if values:
                values.pop(0)
                trimmed = True
                break
        if not trimmed:
            raise ValueError("mission packet exceeds max_bytes even after pointer trimming")
    return packet


class DispatchBridge:
    """Consume an FR ticket and dispatch it onto existing workers primitives."""

    def __init__(
        self,
        queue: JobQueue,
        catalog: WorkflowCatalog,
        runner: WorkflowRunner,
        *,
        receipts_dir: str | None = None,
        switchboard_root: str | None = None,
        source_head: str | None = None,
        iar_probe: Callable[[], Mapping[str, Any]] | None = None,
        http_open: HttpOpen | None = None,
        fr_token: str | None = None,
    ) -> None:
        self.queue = queue
        self.catalog = catalog
        self.runner = runner
        self.receipts_dir = receipts_dir
        self.switchboard_root = switchboard_root
        self.source_head = source_head
        self.iar_probe = iar_probe
        self.http_open = http_open
        self.fr_token = fr_token
        self.last_switchboard = None

    def dispatch(
        self,
        mission: DispatchTicket,
        candidates: Sequence[WorkCandidate],
        *,
        auto_execute_read_only: bool = True,
        state_delta: Mapping[str, Any] | None = None,
    ) -> RuntimeResult:
        fr_bind = bind_founder_runtime(opener=self.http_open)
        if mission.fr_state not in FR_DISPATCHABLE_STATES:
            candidate = rank_candidates(mission, candidates)[0]
            context = compile_context(mission, candidate, state_delta=state_delta)
            return RuntimeResult(
                mission_id=mission.mission_id,
                status=RuntimeStatus.FR_STATE_NOT_DISPATCHABLE,
                candidate_id=candidate.candidate_id,
                workflow_name=candidate.workflow_name,
                layer=candidate.layer.value,
                utility_usd=0.0,
                founder_gate_reasons=(),
                context_digest=content_sha(context),
                compiled_context=context,
                authority_path="recipe-engine.founder_runtime",
                fr_bind=fr_bind,
                note="ticket is not FR APPROVED/QUEUED; bridge does not invent approval",
            )

        candidate = rank_candidates(mission, candidates)[0]
        context = compile_context(mission, candidate, state_delta=state_delta)
        context_digest = content_sha(context)
        gates = founder_gate_reasons(candidate)

        if gates:
            bind = call_gsa(reasons=gates, opener=self.http_open)
            return RuntimeResult(
                mission_id=mission.mission_id,
                status=RuntimeStatus.GOVERNANCE_SENTINEL_AXON_REQUIRED,
                candidate_id=candidate.candidate_id,
                workflow_name=candidate.workflow_name,
                layer=candidate.layer.value,
                utility_usd=0.0,
                founder_gate_reasons=gates,
                context_digest=context_digest,
                compiled_context=context,
                authority_path=GSA_AUTHORITY_PATH,
                broker_bind=bind,
                fr_bind=fr_bind,
                note="consequential action; Governance → Sentinel → AXON required; no job enqueued",
            )

        if candidate.layer in {IntelligenceLayer.L2, IntelligenceLayer.L3, IntelligenceLayer.L4}:
            bind = bind_iar_rf(
                context_digest=context_digest,
                health_probe=self.iar_probe,
                opener=self.http_open,
            )
            return RuntimeResult(
                mission_id=mission.mission_id,
                status=RuntimeStatus.REASONING_BROKER_REQUIRED,
                candidate_id=candidate.candidate_id,
                workflow_name=candidate.workflow_name,
                layer=candidate.layer.value,
                utility_usd=0.0,
                founder_gate_reasons=(),
                context_digest=context_digest,
                compiled_context=context,
                authority_path="revenue_factory.authorization->aegis.iar_v2",
                broker_bind=bind,
                fr_bind=fr_bind,
                note="L2/L3/L4 delegated to existing IAR/RF contract; no local spend or model call",
            )

        definition = self.catalog.get(candidate.workflow_name)
        if not definition.read_only:
            reasons = ("NON_READ_ONLY_WORKFLOW",)
            bind = call_gsa(reasons=reasons, opener=self.http_open)
            return RuntimeResult(
                mission_id=mission.mission_id,
                status=RuntimeStatus.GOVERNANCE_SENTINEL_AXON_REQUIRED,
                candidate_id=candidate.candidate_id,
                workflow_name=candidate.workflow_name,
                layer=candidate.layer.value,
                utility_usd=0.0,
                founder_gate_reasons=reasons,
                context_digest=context_digest,
                compiled_context=context,
                authority_path=GSA_AUTHORITY_PATH,
                broker_bind=bind,
                fr_bind=fr_bind,
                note="non-read-only work is consequential; Governance → Sentinel → AXON required",
            )

        dedupe_key = content_sha({
            "mission_id": mission.mission_id,
            "candidate_id": candidate.candidate_id,
            "context_digest": context_digest,
        })
        job_id = self.queue.enqueue(
            definition,
            candidate.params,
            requested_by=f"jgi:{mission.mission_id}",
            dry_run=False,
            dedupe_key=dedupe_key,
        )
        state = self.queue.load(job_id)

        if state.status == JobStatus.SUCCEEDED:
            status = RuntimeStatus.ALREADY_SUCCEEDED
        elif auto_execute_read_only:
            state = self.runner.execute(job_id)
            if state.status == JobStatus.SUCCEEDED:
                status = RuntimeStatus.SUCCEEDED
            elif state.status == JobStatus.BLOCKED_ON_APPROVAL:
                status = RuntimeStatus.APPROVAL_REQUIRED
            else:
                status = RuntimeStatus.BLOCKED
        else:
            status = RuntimeStatus.QUEUED

        chain = self.queue.ledger.verify()
        sealed = None
        court = None
        switchboard_emit = None
        parent_resume = None
        sentinel = None
        honey = None
        fr_update = None
        if status in {RuntimeStatus.SUCCEEDED, RuntimeStatus.ALREADY_SUCCEEDED}:
            emission = emit_receipt_sealed_and_switcharoo(
                mission_id=mission.mission_id,
                job_id=job_id,
                ledger_tail_sha=chain["tail_sha"],
                context_digest=context_digest,
                evidence_refs=candidate.evidence_refs,
                job_succeeded_sha=job_succeeded_sha(self.queue, job_id),
                source_head=resolve_source_head(self.source_head),
                receipts_dir=self.receipts_dir,
                switchboard_root=self.switchboard_root,
                code_custody_required=mission.code_custody_required,
            )
            sealed = emission["sealed"]
            court = emission["court_request"]
            switchboard_emit = emission["emit"]
            sentinel = emission["sentinel"]
            honey = emission["honey_pot"]
            self.last_switchboard = emission.get("board")
            if mission.parent_mission_id:
                parent_resume = resume_parent_from_receipts(
                    self.queue,
                    self.runner,
                    parent_mission_id=mission.parent_mission_id,
                    child_job_id=job_id,
                )
                chain = self.queue.ledger.verify()
            fr_update = update_fr_evidence(
                mission.mission_id,
                {
                    "job_id": job_id,
                    "ledger_tail_sha": chain["tail_sha"],
                    "job_status": state.status.value,
                    "parent_resume": parent_resume,
                    "switchboard_bind": (switchboard_emit or {}).get("bind_status"),
                    "sentinel_bind": (sentinel or {}).get("bind_status"),
                },
                opener=self.http_open,
                token=self.fr_token,
            )
        elif mission.parent_mission_id:
            parent_resume = {
                "parent_mission_id": mission.parent_mission_id,
                "automatic": False,
                "proven": False,
                "status": "CHILD_NOT_RECEIPTED",
                "note": "parent resume waits for a receipted child success",
            }

        return RuntimeResult(
            mission_id=mission.mission_id,
            status=status,
            candidate_id=candidate.candidate_id,
            workflow_name=candidate.workflow_name,
            layer=candidate.layer.value,
            utility_usd=0.0,
            founder_gate_reasons=(),
            context_digest=context_digest,
            compiled_context=context,
            job_id=job_id,
            job_status=state.status.value,
            ledger_tail_sha=chain["tail_sha"],
            authority_path="workflow_runner->execution_ledger",
            parent_resume=parent_resume,
            sealed_receipt=sealed,
            court_request=court,
            switchboard_emit=switchboard_emit,
            sentinel=sentinel,
            honey_pot=honey,
            fr_bind=fr_bind,
            fr_evidence_update=fr_update,
            note="FR ticket dispatched onto existing queue/runner/ledger; no second FR",
        )


# Compatibility alias: this class is the dispatch bridge, not a new Founder Runtime.
FounderRuntime = DispatchBridge
