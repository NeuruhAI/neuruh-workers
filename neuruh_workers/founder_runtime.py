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

import hashlib
import json
import os
from dataclasses import asdict, dataclass
from enum import Enum
from math import isfinite
from typing import Any, Callable, Mapping, Sequence

from .contracts import DefinitionError, JobStatus, canonical_json, content_sha
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
    pull_authoritative_mission,
    resolve_dispatch_source_identity,
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
    mission_version: int
    dispatch_approval_id: str
    work_spec_sha256: str
    execution_authorized: bool
    work_spec_execution_authorized: bool
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
        # `.strip()` here tests emptiness only -- the stored objective is never
        # rewritten. Widening the newline rule below must not make "\n" a
        # substitute for the mission intent this field is supposed to carry.
        if not self.mission_id or not self.objective.strip():
            raise ValueError("mission_id and objective are required")
        if type(self.mission_version) is not int or self.mission_version < 1:
            raise ValueError("mission_version must be a positive integer")
        # Identity stays single-line: these are keys, log fields and members of the
        # dispatch authority identity tuple, where an embedded newline is an
        # injection surface. `objective` is not identity -- it is Founder Runtime's
        # authored mission intent, carried verbatim. Real missions state their
        # "Done when:" clause on its own line, and Workers has no licence to join,
        # truncate or otherwise reinterpret what the mission author wrote.
        for name in ("mission_id", "dispatch_approval_id"):
            if "\r" in getattr(self, name) or "\n" in getattr(self, name):
                raise ValueError(f"{name} must be a single-line value")
        if not self.dispatch_approval_id:
            raise ValueError("dispatch_approval_id is required")
        if (
            len(self.work_spec_sha256) != 64
            or any(char not in "0123456789abcdef" for char in self.work_spec_sha256)
        ):
            raise ValueError("work_spec_sha256 must be lowercase 64-hex")
        if (
            self.execution_authorized is not True
            or self.work_spec_execution_authorized is not True
        ):
            raise ValueError("execution and work-spec authority must both be true")
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


FR_WORK_SPEC_SCHEMA = "fr.work_spec.v1"
_WORK_SPEC_FIELDS = frozenset({
    "schema",
    "candidate_id",
    "workflow_name",
    "params",
    "intelligence_layer",
    "evidence_refs",
    "production_mutation",
    "external_message",
    "money_move",
    "irreversible",
    "private_ip_risk",
    "code_custody_required",
})
_WORK_SPEC_FLAGS = (
    "production_mutation",
    "external_message",
    "money_move",
    "irreversible",
    "private_ip_risk",
    "code_custody_required",
)


def _validate_work_params(value: Any, path: str = "$.params") -> Any:
    if value is None or type(value) in (bool, int, str):
        return value
    if isinstance(value, float):
        raise ValueError(f"{path}: floats are not valid FR work-spec params")
    if isinstance(value, list):
        return [
            _validate_work_params(item, f"{path}[{index}]")
            for index, item in enumerate(value)
        ]
    if isinstance(value, Mapping):
        normalized = {}
        for key, item in value.items():
            if not isinstance(key, str) or not key:
                raise ValueError(f"{path}: parameter keys must be non-empty strings")
            normalized[key] = _validate_work_params(item, f"{path}.{key}")
        return normalized
    raise ValueError(f"{path}: value is not JSON-compatible")


def _fr_work_spec_sha256(work_spec: Mapping[str, Any]) -> str:
    serialized = json.dumps(
        work_spec,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
        allow_nan=False,
    )
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def map_founder_runtime_dispatch(
    envelope: Mapping[str, Any],
    *,
    catalog: WorkflowCatalog,
) -> tuple[DispatchTicket, WorkCandidate]:
    """Map one authority-bound FR envelope into Workers types without inference."""
    if not isinstance(envelope, Mapping):
        raise ValueError("Founder Runtime envelope must be an object")
    if envelope.get("state") != "QUEUED":
        raise ValueError("Founder Runtime mission is not QUEUED")
    if envelope.get("api_version") != FR_API_VERSION:
        raise ValueError(f"Founder Runtime api_version must be {FR_API_VERSION}")

    authority = envelope.get("authority")
    if not isinstance(authority, Mapping):
        raise ValueError("Founder Runtime authority truth is missing")
    if authority.get("state") != "APPROVED":
        raise ValueError("Founder Runtime execution gate is not approved")
    if authority.get("execution_authorized") is not True:
        raise ValueError("Founder Runtime execution authorization is absent")
    if authority.get("work_spec_execution_authorized") is not True:
        raise ValueError("Founder Runtime work spec is not execution-authorized")
    if not isinstance(authority.get("dispatch_approval_id"), str) or not authority["dispatch_approval_id"]:
        raise ValueError("Founder Runtime consumed dispatch approval identity is missing")

    work_spec = envelope.get("work_spec")
    if not isinstance(work_spec, Mapping):
        raise ValueError("Founder Runtime work_spec is missing")
    if set(work_spec) != _WORK_SPEC_FIELDS:
        raise ValueError("Founder Runtime work_spec fields are incomplete or extended")
    if work_spec.get("schema") != FR_WORK_SPEC_SCHEMA:
        raise ValueError(f"work_spec schema must be {FR_WORK_SPEC_SCHEMA}")

    work_spec_sha256 = envelope.get("work_spec_sha256")
    if not isinstance(work_spec_sha256, str) or len(work_spec_sha256) != 64:
        raise ValueError("Founder Runtime work_spec_sha256 is malformed")
    if authority.get("work_spec_sha256") != work_spec_sha256:
        raise ValueError("Founder Runtime authority is bound to a different work spec")
    if _fr_work_spec_sha256(work_spec) != work_spec_sha256:
        raise ValueError("Founder Runtime work_spec hash mismatch")

    candidate_id = work_spec.get("candidate_id")
    workflow_name = work_spec.get("workflow_name")
    if not isinstance(candidate_id, str) or not candidate_id:
        raise ValueError("work_spec candidate_id is required")
    if not isinstance(workflow_name, str) or not workflow_name:
        raise ValueError("work_spec workflow_name is required")
    try:
        catalog.get(workflow_name)
    except DefinitionError as exc:
        raise ValueError("work_spec workflow_name is not registered") from exc

    params = work_spec.get("params")
    if not isinstance(params, Mapping):
        raise ValueError("work_spec params must be an object")
    params = _validate_work_params(params)
    evidence_refs = work_spec.get("evidence_refs")
    if (
        not isinstance(evidence_refs, list)
        or any(
            not isinstance(ref, str)
            or not ref
            or "\r" in ref
            or "\n" in ref
            for ref in evidence_refs
        )
    ):
        raise ValueError("work_spec evidence_refs must be non-empty strings")
    for flag in _WORK_SPEC_FLAGS:
        if type(work_spec.get(flag)) is not bool:
            raise ValueError(f"work_spec {flag} must be boolean")
    try:
        layer = IntelligenceLayer(work_spec.get("intelligence_layer"))
    except (TypeError, ValueError) as exc:
        raise ValueError("work_spec intelligence_layer is unsupported") from exc

    ticket = DispatchTicket(
        mission_id=str(envelope.get("mission_id") or ""),
        objective=str(envelope.get("objective") or ""),
        mission_version=envelope.get("version"),
        dispatch_approval_id=authority["dispatch_approval_id"],
        work_spec_sha256=work_spec_sha256,
        execution_authorized=authority["execution_authorized"],
        work_spec_execution_authorized=authority["work_spec_execution_authorized"],
        fr_state=str(envelope["state"]),
        fr_api_version=str(envelope["api_version"]),
        parent_mission_id=(
            str(envelope["parent_mission_id"])
            if envelope.get("parent_mission_id") else None
        ),
        code_custody_required=work_spec["code_custody_required"],
    )
    candidate = WorkCandidate(
        candidate_id=candidate_id,
        workflow_name=workflow_name,
        params=params,
        layer=layer,
        evidence_refs=tuple(evidence_refs),
        production_mutation=work_spec["production_mutation"],
        external_message=work_spec["external_message"],
        money_move=work_spec["money_move"],
        irreversible=work_spec["irreversible"],
        private_ip_risk=work_spec["private_ip_risk"],
    )
    return ticket, candidate


def _dispatch_ticket_authority_identity(ticket: DispatchTicket) -> tuple[Any, ...]:
    return (
        ticket.mission_id,
        ticket.objective,
        ticket.mission_version,
        ticket.dispatch_approval_id,
        ticket.work_spec_sha256,
        ticket.execution_authorized,
        ticket.work_spec_execution_authorized,
        ticket.fr_state,
        ticket.fr_api_version,
        ticket.parent_mission_id,
    )


def _candidate_execution_identity(candidate: WorkCandidate) -> tuple[Any, ...]:
    return (
        candidate.candidate_id,
        candidate.workflow_name,
        dict(candidate.params),
        candidate.layer,
        tuple(candidate.evidence_refs),
        candidate.production_mutation,
        candidate.external_message,
        candidate.money_move,
        candidate.irreversible,
        candidate.private_ip_risk,
    )


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
        source_root: str | None = None,
        iar_probe: Callable[[], Mapping[str, Any]] | None = None,
        http_open: HttpOpen | None = None,
        fr_token: str | None = None,
        fr_timeout_s: float = 0.25,
    ) -> None:
        self.queue = queue
        self.catalog = catalog
        self.runner = runner
        self.receipts_dir = receipts_dir
        self.switchboard_root = switchboard_root
        self.source_head = source_head
        self.source_root = source_root
        self.iar_probe = iar_probe
        self.http_open = http_open
        self.fr_token = fr_token
        self.fr_timeout_s = fr_timeout_s
        self.last_switchboard = None

    def dispatch(
        self,
        mission: DispatchTicket,
        candidates: Sequence[WorkCandidate],
        *,
        auto_execute_read_only: bool = True,
        state_delta: Mapping[str, Any] | None = None,
    ) -> RuntimeResult:
        fr_bind = bind_founder_runtime(
            opener=self.http_open, timeout_s=self.fr_timeout_s)
        if len(candidates) != 1:
            raise ValueError(
                "DispatchBridge requires exactly one FR-mapped WorkCandidate")
        candidate = candidates[0]
        context = compile_context(mission, candidate)
        context_digest = content_sha(context)
        if state_delta:
            return RuntimeResult(
                mission_id=mission.mission_id,
                status=RuntimeStatus.BLOCKED,
                candidate_id=candidate.candidate_id,
                workflow_name=candidate.workflow_name,
                layer=candidate.layer.value,
                utility_usd=0.0,
                founder_gate_reasons=(),
                context_digest=context_digest,
                compiled_context=context,
                authority_path="recipe-engine.founder_runtime",
                fr_bind=fr_bind,
                note="caller state_delta is not part of the authority-bound FR work spec",
            )
        if mission.fr_state not in FR_DISPATCHABLE_STATES:
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

        if not fr_bind.get("present"):
            return RuntimeResult(
                mission_id=mission.mission_id,
                status=RuntimeStatus.BLOCKED,
                candidate_id=candidate.candidate_id,
                workflow_name=candidate.workflow_name,
                layer=candidate.layer.value,
                utility_usd=0.0,
                founder_gate_reasons=(),
                context_digest=context_digest,
                compiled_context=context,
                authority_path="recipe-engine.founder_runtime",
                fr_bind=fr_bind,
                note="Founder Runtime became unavailable before execution",
            )

        try:
            verified_source = resolve_dispatch_source_identity(
                self.source_root or os.getcwd(), self.source_head)
        except RuntimeError as exc:
            return RuntimeResult(
                mission_id=mission.mission_id,
                status=RuntimeStatus.BLOCKED,
                candidate_id=candidate.candidate_id,
                workflow_name=candidate.workflow_name,
                layer=candidate.layer.value,
                utility_usd=0.0,
                founder_gate_reasons=(),
                context_digest=context_digest,
                compiled_context=context,
                authority_path="recipe-engine.founder_runtime",
                fr_bind=fr_bind,
                note=f"Workers source provenance refused: {exc}",
            )

        refreshed = pull_authoritative_mission(
            mission.mission_id,
            opener=self.http_open,
            token=self.fr_token,
            timeout_s=self.fr_timeout_s,
        )
        refreshed_envelope = refreshed.get("envelope")
        refreshed_authority = (
            refreshed_envelope.get("authority")
            if isinstance(refreshed_envelope, Mapping) else None
        )
        authority_matches = (
            refreshed.get("ok") is True
            and isinstance(refreshed_envelope, Mapping)
            and isinstance(refreshed_authority, Mapping)
            and refreshed_envelope.get("mission_id") == mission.mission_id
            and refreshed_envelope.get("version") == mission.mission_version
            and refreshed_envelope.get("state") == "QUEUED"
            and refreshed_envelope.get("work_spec_sha256") == mission.work_spec_sha256
            and refreshed_authority.get("state") == "APPROVED"
            and refreshed_authority.get("execution_authorized") is True
            and refreshed_authority.get("work_spec_execution_authorized") is True
            and refreshed_authority.get("dispatch_approval_id")
            == mission.dispatch_approval_id
            and refreshed_authority.get("work_spec_sha256")
            == mission.work_spec_sha256
        )
        if not authority_matches:
            return RuntimeResult(
                mission_id=mission.mission_id,
                status=RuntimeStatus.BLOCKED,
                candidate_id=candidate.candidate_id,
                workflow_name=candidate.workflow_name,
                layer=candidate.layer.value,
                utility_usd=0.0,
                founder_gate_reasons=(),
                context_digest=context_digest,
                compiled_context=context,
                authority_path="recipe-engine.founder_runtime",
                fr_bind=fr_bind,
                note="authenticated Founder Runtime mission authority recheck failed",
            )

        try:
            trusted_mission, trusted_candidate = map_founder_runtime_dispatch(
                refreshed_envelope, catalog=self.catalog)
        except (TypeError, ValueError) as exc:
            return RuntimeResult(
                mission_id=mission.mission_id,
                status=RuntimeStatus.BLOCKED,
                candidate_id=candidate.candidate_id,
                workflow_name=candidate.workflow_name,
                layer=candidate.layer.value,
                utility_usd=0.0,
                founder_gate_reasons=(),
                context_digest=context_digest,
                compiled_context=context,
                authority_path="recipe-engine.founder_runtime",
                fr_bind=fr_bind,
                note=f"authenticated Founder Runtime envelope was rejected: {exc}",
            )
        if (
            _dispatch_ticket_authority_identity(trusted_mission)
            != _dispatch_ticket_authority_identity(mission)
            or _candidate_execution_identity(trusted_candidate)
            != _candidate_execution_identity(candidate)
        ):
            return RuntimeResult(
                mission_id=mission.mission_id,
                status=RuntimeStatus.BLOCKED,
                candidate_id=candidate.candidate_id,
                workflow_name=candidate.workflow_name,
                layer=candidate.layer.value,
                utility_usd=0.0,
                founder_gate_reasons=(),
                context_digest=context_digest,
                compiled_context=context,
                authority_path="recipe-engine.founder_runtime",
                fr_bind=fr_bind,
                note="caller ticket/candidate differ from authenticated Founder Runtime",
            )
        mission = trusted_mission
        candidate = trusted_candidate
        context = compile_context(mission, candidate)
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

        if candidate.layer in {
            IntelligenceLayer.L2, IntelligenceLayer.L3, IntelligenceLayer.L4,
        }:
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

        try:
            execution_source = resolve_dispatch_source_identity(
                self.source_root or os.getcwd(), self.source_head)
        except RuntimeError as exc:
            return RuntimeResult(
                mission_id=mission.mission_id,
                status=RuntimeStatus.BLOCKED,
                candidate_id=candidate.candidate_id,
                workflow_name=candidate.workflow_name,
                layer=candidate.layer.value,
                utility_usd=0.0,
                founder_gate_reasons=(),
                context_digest=context_digest,
                compiled_context=context,
                authority_path="recipe-engine.founder_runtime",
                fr_bind=fr_bind,
                note=f"Workers source provenance drifted before enqueue: {exc}",
            )
        if execution_source != verified_source:
            return RuntimeResult(
                mission_id=mission.mission_id,
                status=RuntimeStatus.BLOCKED,
                candidate_id=candidate.candidate_id,
                workflow_name=candidate.workflow_name,
                layer=candidate.layer.value,
                utility_usd=0.0,
                founder_gate_reasons=(),
                context_digest=context_digest,
                compiled_context=context,
                authority_path="recipe-engine.founder_runtime",
                fr_bind=fr_bind,
                note="Workers source identity changed before enqueue",
            )

        dedupe_key = content_sha({
            "mission_id": mission.mission_id,
            "candidate_id": candidate.candidate_id,
            "context_digest": context_digest,
            "dispatch_approval_id": mission.dispatch_approval_id,
            "work_spec_sha256": mission.work_spec_sha256,
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
        emission_complete = False
        if status in {RuntimeStatus.SUCCEEDED, RuntimeStatus.ALREADY_SUCCEEDED}:
            emission = emit_receipt_sealed_and_switcharoo(
                mission_id=mission.mission_id,
                job_id=job_id,
                ledger_tail_sha=chain["tail_sha"],
                context_digest=context_digest,
                evidence_refs=candidate.evidence_refs,
                job_succeeded_sha=job_succeeded_sha(self.queue, job_id),
                source_head=verified_source.head,
                source_root=self.source_root,
                source_identity=verified_source,
                dispatch_approval_id=mission.dispatch_approval_id,
                work_spec_sha256=mission.work_spec_sha256,
                mission_version=mission.mission_version,
                idempotency_key=dedupe_key,
                # Immutable receipt records the stable terminal job result.
                dispatch_result=state.status.value,
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
            emission_complete = (
                isinstance(sealed, Mapping)
                and sealed.get("event_type") == "receipt.sealed"
                and isinstance(court, Mapping)
                and court.get("event_type") == "court.requested"
                and isinstance(switchboard_emit, Mapping)
                and switchboard_emit.get("consumed") is True
            )
            if not emission_complete:
                status = RuntimeStatus.BLOCKED
            elif sentinel and sentinel.get("verdict") == "FAIL":
                status = RuntimeStatus.BLOCKED
            elif sentinel and sentinel.get("verdict") == "PASS":
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
            note=(
                "FR ticket dispatched, independently courted, and sealed"
                if emission_complete and sentinel and sentinel.get("verdict") == "PASS"
                else "FR ticket execution sealed; independent court is pending"
                if emission_complete and not (sentinel or {}).get("verdict")
                else "independent court failed; downstream effects refused"
                if emission_complete and (sentinel or {}).get("verdict") == "FAIL"
                else "Workers execution did not complete the sealed Switchboard seam"
            ),
        )


# Compatibility alias: this class is the dispatch bridge, not a new Founder Runtime.
FounderRuntime = DispatchBridge
