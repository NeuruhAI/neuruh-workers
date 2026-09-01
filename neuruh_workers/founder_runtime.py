"""JGI Founder Runtime v0: cheapest-capable mission planning over existing workers.

This module is deliberately a CONTROL PLANE, not another workflow engine.
It reuses WorkflowCatalog, WorkerRegistry, JobQueue, WorkflowRunner and the
hash-chained ExecutionLedger already owned by neuruh-workers.

V0 autonomy boundary:
- safe deterministic/read-only workflows may be selected, enqueued and run
  without a clerical founder handoff;
- approval-gated work stops at the existing content-bound approval gate;
- consequential work (money, external message, production mutation,
  irreversible, private-IP risk, non-read-only) fails closed to
  Governance → Sentinel → AXON only;
- L2/L3/L4 reasoning is delegated to the existing IAR / Revenue Factory
  contract and fails closed when that organ is absent. This layer does not
  issue spend, call models, or rebuild a router.
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
    bind_iar_rf,
    emit_receipt_sealed_and_court_request,
    gsa_bind,
    job_succeeded_sha,
    resolve_source_head,
    resume_parent_from_receipts,
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
    """Non-authorizing L0/L1 path pre-filter. Not spend authority."""

    candidate_id: str
    layer: str
    layer_rank: int
    latency_minutes: float
    authorizes_spend: bool = False
    authorizes_model_call: bool = False


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
) -> CandidateScore:
    """Pre-filter only. Does not price work or authorize spend/model calls.

    Mission economic hints (expected_value_usd, model_budget_usd) stay on
    the intent as metadata. Spend authority is IAR / Revenue Factory.
    """
    del mission  # economic hints are not spend authority here
    return CandidateScore(
        candidate_id=candidate.candidate_id,
        layer=candidate.layer.value,
        layer_rank=_LAYER_ORDER[candidate.layer],
        latency_minutes=candidate.estimated_latency_minutes,
        authorizes_spend=False,
        authorizes_model_call=False,
    )


def rank_candidates(mission: MissionIntent, candidates: Sequence[WorkCandidate]) -> list[tuple[WorkCandidate, CandidateScore]]:
    if not candidates:
        raise ValueError("at least one work candidate is required")
    ranked = [(candidate, score_candidate(mission, candidate)) for candidate in candidates]
    ranked.sort(key=lambda pair: (pair[1].layer_rank, pair[1].latency_minutes, pair[0].candidate_id))
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
        "reasoning_broker": "revenue_factory.authorization+aegis.iar_v2",
        "spend_authority": "iar_rf",
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
        *,
        receipts_dir: str | None = None,
        switchboard_root: str | None = None,
        source_head: str | None = None,
        iar_probe: Callable[[], Mapping[str, Any]] | None = None,
    ) -> None:
        self.queue = queue
        self.catalog = catalog
        self.runner = runner
        self.receipts_dir = receipts_dir
        self.switchboard_root = switchboard_root
        self.source_head = source_head
        self.iar_probe = iar_probe

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
            bind = gsa_bind(reasons=gates)
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
                note="consequential action; Governance → Sentinel → AXON required; no job enqueued",
            )

        if candidate.layer in {IntelligenceLayer.L2, IntelligenceLayer.L3, IntelligenceLayer.L4}:
            bind = bind_iar_rf(context_digest=context_digest, health_probe=self.iar_probe)
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
                note="L2/L3/L4 delegated to existing IAR/RF contract; no local spend or model call",
            )

        definition = self.catalog.get(candidate.workflow_name)
        if not definition.read_only:
            reasons = ("NON_READ_ONLY_WORKFLOW",)
            bind = gsa_bind(reasons=reasons)
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
                note="non-read-only work is consequential; Governance → Sentinel → AXON required",
            )

        # Local ranker must not authorize spend even for L0/L1 path choice.
        if score.authorizes_spend or score.authorizes_model_call:
            bind = bind_iar_rf(context_digest=context_digest, health_probe=self.iar_probe)
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
                note="path ranker attempted spend/model authority; refused",
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
        parent_resume = None
        if status in {RuntimeStatus.SUCCEEDED, RuntimeStatus.ALREADY_SUCCEEDED}:
            emission = emit_receipt_sealed_and_court_request(
                mission_id=mission.mission_id,
                job_id=job_id,
                ledger_tail_sha=chain["tail_sha"],
                context_digest=context_digest,
                evidence_refs=candidate.evidence_refs,
                job_succeeded_sha=job_succeeded_sha(self.queue, job_id),
                source_head=resolve_source_head(self.source_head),
                receipts_dir=self.receipts_dir,
                switchboard_root=self.switchboard_root,
            )
            sealed = emission["sealed"]
            court = emission["court_request"]
            if mission.parent_mission_id:
                parent_resume = resume_parent_from_receipts(
                    self.queue,
                    self.runner,
                    parent_mission_id=mission.parent_mission_id,
                    child_job_id=job_id,
                )
                chain = self.queue.ledger.verify()
        elif mission.parent_mission_id:
            parent_resume = {
                "parent_mission_id": mission.parent_mission_id,
                "automatic": False,
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
            note="existing deterministic workflow reused; queue/runner/ledger remain canonical",
        )
