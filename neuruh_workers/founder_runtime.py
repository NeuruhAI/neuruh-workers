"""JGI Founder Runtime v0: cheapest-capable mission planning over existing workers.

This module is deliberately a CONTROL PLANE, not another workflow engine.
It reuses WorkflowCatalog, WorkerRegistry, JobQueue, WorkflowRunner and the
hash-chained ExecutionLedger already owned by neuruh-workers.

V0 autonomy boundary:
- safe deterministic/read-only workflows may be selected, enqueued and run
  without a clerical founder handoff;
- approval-gated work stops at the existing content-bound approval gate;
- production mutation, money movement, external messages, irreversible work
  and private-IP-risk operations stop at a founder gate;
- L2/L3/L4 reasoning is routed but not invoked here. A reasoning broker must
  satisfy its own authorization/cost court (e.g. Revenue Factory/IAR).
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
from enum import Enum
from math import isfinite
from typing import Any, Mapping, Sequence

from .contracts import JobStatus, canonical_json, content_sha
from .convergence import (
    GSA_AUTHORITY_PATH,
    gsa_bind,
    parent_resume_pointer,
    reasoning_broker_bind,
)
from .definitions import WorkflowCatalog
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
    BLOCKED = "BLOCKED"


@dataclass(frozen=True)
class MissionIntent:
    mission_id: str
    objective: str
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

    def __post_init__(self) -> None:
        if not self.mission_id or not self.objective:
            raise ValueError("mission_id and objective are required")
        for name in ("uncertainty", "relevance"):
            value = float(getattr(self, name))
            if value < 0.0 or value > 1.0:
                raise ValueError(f"{name} must be between 0 and 1")
        if self.expected_value_usd < 0 or self.model_budget_usd < 0:
            raise ValueError("economic values must be non-negative")


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
class CandidateScore:
    candidate_id: str
    utility_usd: float
    gross_expected_value_usd: float
    cost_penalty_usd: float
    founder_penalty_usd: float
    latency_penalty_usd: float
    layer: str


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


def score_candidate(
    mission: MissionIntent,
    candidate: WorkCandidate,
    *,
    founder_minute_value_usd: float = 2.0,
    latency_minute_value_usd: float = 0.05,
) -> CandidateScore:
    """Economic utility used only to rank already-valid candidate paths."""
    gross = mission.expected_value_usd * mission.relevance * candidate.success_probability
    direct = candidate.estimated_execution_cost_usd + candidate.estimated_model_cost_usd
    founder = candidate.estimated_founder_minutes * founder_minute_value_usd
    latency = candidate.estimated_latency_minutes * latency_minute_value_usd
    utility = gross - direct - founder - latency
    return CandidateScore(
        candidate_id=candidate.candidate_id,
        utility_usd=round(utility, 4),
        gross_expected_value_usd=round(gross, 4),
        cost_penalty_usd=round(direct, 4),
        founder_penalty_usd=round(founder, 4),
        latency_penalty_usd=round(latency, 4),
        layer=candidate.layer.value,
    )


def rank_candidates(mission: MissionIntent, candidates: Sequence[WorkCandidate]) -> list[tuple[WorkCandidate, CandidateScore]]:
    if not candidates:
        raise ValueError("at least one work candidate is required")
    ranked: list[tuple[WorkCandidate, CandidateScore]] = []
    for candidate in candidates:
        # A candidate that exceeds the explicit model budget is not a valid path.
        if candidate.estimated_model_cost_usd > mission.model_budget_usd:
            continue
        ranked.append((candidate, score_candidate(mission, candidate)))
    if not ranked:
        raise ValueError("no candidate fits the mission model budget")
    ranked.sort(key=lambda pair: (-pair[1].utility_usd, _LAYER_ORDER[pair[0].layer], pair[0].candidate_id))
    return ranked


def compile_context(
    mission: MissionIntent,
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
        "reasoning_broker": "revenue_factory.authorization+aegis.iar",
    }
    # Trim pointer arrays before ever truncating semantic mission/task fields.
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


class FounderRuntime:
    """Select and autonomously run the best safe existing workflow path."""

    def __init__(
        self,
        queue: JobQueue,
        catalog: WorkflowCatalog,
        runner: WorkflowRunner,
    ) -> None:
        self.queue = queue
        self.catalog = catalog
        self.runner = runner

    def dispatch(
        self,
        mission: MissionIntent,
        candidates: Sequence[WorkCandidate],
        *,
        auto_execute_read_only: bool = True,
        state_delta: Mapping[str, Any] | None = None,
    ) -> RuntimeResult:
        candidate, score = rank_candidates(mission, candidates)[0]
        context = compile_context(mission, candidate, state_delta=state_delta)
        context_digest = content_sha(context)
        gates = founder_gate_reasons(candidate)

        if gates:
            return RuntimeResult(
                mission_id=mission.mission_id,
                status=RuntimeStatus.GOVERNANCE_SENTINEL_AXON_REQUIRED,
                candidate_id=candidate.candidate_id,
                workflow_name=candidate.workflow_name,
                layer=candidate.layer.value,
                utility_usd=score.utility_usd,
                founder_gate_reasons=gates,
                context_digest=context_digest,
                compiled_context=context,
                authority_path=GSA_AUTHORITY_PATH,
                broker_bind=gsa_bind(reasons=gates),
                note="consequential action; Governance → Sentinel → AXON required; no job enqueued",
            )

        if candidate.layer in {IntelligenceLayer.L2, IntelligenceLayer.L3, IntelligenceLayer.L4}:
            bind = reasoning_broker_bind(context_digest=context_digest)
            return RuntimeResult(
                mission_id=mission.mission_id,
                status=RuntimeStatus.REASONING_BROKER_REQUIRED,
                candidate_id=candidate.candidate_id,
                workflow_name=candidate.workflow_name,
                layer=candidate.layer.value,
                utility_usd=score.utility_usd,
                founder_gate_reasons=(),
                context_digest=context_digest,
                compiled_context=context,
                authority_path="revenue_factory.authorization->aegis.iar",
                broker_bind=bind,
                note="L2/L3/L4 must consume existing Revenue Factory InferenceAuthorization; this layer does not issue or call models",
            )

        definition = self.catalog.get(candidate.workflow_name)
        if not definition.read_only:
            reasons = ("NON_READ_ONLY_WORKFLOW",)
            return RuntimeResult(
                mission_id=mission.mission_id,
                status=RuntimeStatus.GOVERNANCE_SENTINEL_AXON_REQUIRED,
                candidate_id=candidate.candidate_id,
                workflow_name=candidate.workflow_name,
                layer=candidate.layer.value,
                utility_usd=score.utility_usd,
                founder_gate_reasons=reasons,
                context_digest=context_digest,
                compiled_context=context,
                authority_path=GSA_AUTHORITY_PATH,
                broker_bind=gsa_bind(reasons=reasons),
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
        return RuntimeResult(
            mission_id=mission.mission_id,
            status=status,
            candidate_id=candidate.candidate_id,
            workflow_name=candidate.workflow_name,
            layer=candidate.layer.value,
            utility_usd=score.utility_usd,
            founder_gate_reasons=(),
            context_digest=context_digest,
            compiled_context=context,
            job_id=job_id,
            job_status=state.status.value,
            ledger_tail_sha=chain["tail_sha"],
            authority_path="workflow_runner->execution_ledger",
            parent_resume=parent_resume_pointer(mission.parent_mission_id),
            note="existing deterministic workflow reused; queue/runner/ledger remain canonical",
        )
