"""Consume Switchboard `nrh.event.v1` — do not copy or reimplement the envelope.

Inbound: `neuruh_workers.events.validate_envelope` + `candidate.push.requested`.
Outbound: a recorder of Git Custodian receipts. This is not a second event bus.
"""
from __future__ import annotations

from typing import Any, Mapping

from ..events import (
    AckState,
    EventError,
    EventType,
    SCHEMA_VERSION,
    build_envelope,
    validate_envelope,
)
from ..switchboard_jobs import GIT_PUSH
from .constants import PROMOTION_INPUT, SWITCHBOARD_EVENTS, SWITCHBOARD_INPUT
from .errors import GitCustodianRefused

SWITCHAROO_INTEGRATED = True
STRUCTURED_SENTINEL_PASS_REQUIRED = True


class RecordingSwitchboard:
    """Outbound receipt log. Inbound consumption is nrh.event.v1."""

    def __init__(self) -> None:
        self.events: list[dict[str, Any]] = []

    @property
    def integrated(self) -> bool:
        return SWITCHAROO_INTEGRATED

    def emit(self, event_type: str, payload: Mapping[str, Any] | None = None) -> None:
        if event_type not in SWITCHBOARD_EVENTS:
            raise GitCustodianRefused(
                "unknown switchboard event type",
                {"event_type": event_type}, reason="SWITCHBOARD_UNKNOWN")
        self.events.append({"type": event_type, "payload": dict(payload or {})})


def accept_candidate_push(event: Any) -> dict[str, Any]:
    """Fail closed unless this is a real Switchboard `nrh.event.v1` envelope."""
    try:
        envelope = validate_envelope(event)
    except EventError as exc:
        raise GitCustodianRefused(
            "Git Custodian consumes nrh.event.v1 candidate.push.requested; "
            "homemade {type, candidate} dicts are refused",
            {"error": str(exc)},
            reason="SWITCHBOARD_UNKNOWN",
        ) from exc
    if envelope["event_type"] != EventType.CANDIDATE_PUSH_REQUESTED.value:
        raise GitCustodianRefused(
            "Git Custodian accepts only candidate.push.requested",
            {"event_type": envelope["event_type"]},
            reason="SWITCHBOARD_UNKNOWN",
        )
    if envelope["schema_version"] != SCHEMA_VERSION:
        raise GitCustodianRefused(
            "unsupported envelope schema_version",
            {"schema_version": envelope["schema_version"]},
            reason="SWITCHBOARD_UNKNOWN",
        )
    return envelope


def candidate_from_envelope(envelope: Mapping[str, Any]) -> Mapping[str, Any]:
    payload = envelope.get("payload_ref_or_payload")
    if not isinstance(payload, Mapping):
        raise GitCustodianRefused(
            "candidate.push.requested payload must be an object",
            reason="MALFORMED_INPUT")
    candidate = payload.get("candidate")
    if not isinstance(candidate, Mapping):
        raise GitCustodianRefused(
            "candidate.push.requested must carry nrh.git.candidate.v1 "
            "under payload.candidate",
            reason="MALFORMED_INPUT")
    return candidate


def accept_promotion_request(event: Any) -> dict[str, Any]:
    """Fail closed unless this is a real Switchboard `promotion.requested` envelope."""
    try:
        envelope = validate_envelope(event)
    except EventError as exc:
        raise GitCustodianRefused(
            "Promotion Executor consumes nrh.event.v1 promotion.requested; "
            "homemade dicts are refused",
            {"error": str(exc)},
            reason="SWITCHBOARD_UNKNOWN",
        ) from exc
    if envelope["event_type"] != EventType.PROMOTION_REQUESTED.value:
        raise GitCustodianRefused(
            "Promotion Executor accepts only promotion.requested",
            {"event_type": envelope["event_type"]},
            reason="SWITCHBOARD_UNKNOWN",
        )
    if envelope["schema_version"] != SCHEMA_VERSION:
        raise GitCustodianRefused(
            "unsupported envelope schema_version",
            {"schema_version": envelope["schema_version"]},
            reason="SWITCHBOARD_UNKNOWN",
        )
    return envelope


def promotion_from_envelope(envelope: Mapping[str, Any]) -> Mapping[str, Any]:
    payload = envelope.get("payload_ref_or_payload")
    if not isinstance(payload, Mapping):
        raise GitCustodianRefused(
            "promotion.requested payload must be an object",
            reason="MALFORMED_INPUT")
    candidate = payload.get("promotion")
    if not isinstance(candidate, Mapping):
        raise GitCustodianRefused(
            "promotion.requested must carry nrh.git.promotion.v1 under "
            "payload.promotion",
            reason="MALFORMED_INPUT")
    return candidate


def build_promotion_request_event(
    promotion: Mapping[str, Any],
    *,
    created_at: str = "2026-09-02T00:00:00.000Z",
    source_role: str = "BOB",
    source_component: str = "bob-orchestration",
    source_repo: str = "NeuruhAI/neuruh-workers",
    causation_id: str = "evt-promotion-ready-fixture",
    correlation_id: str | None = None,
    attempt: int = 1,
) -> dict[str, Any]:
    """The real Switchboard envelope for a promotion request (Bob orchestrates)."""
    mission = str(promotion["mission_id"])
    task = str(promotion["task_id"])
    head = str(promotion["expected_head"])
    corr = correlation_id or f"corr-{mission}-{task}"
    payload = {
        "artifact": PROMOTION_INPUT,
        "mission_id": mission,
        "task_id": task,
        "repo": promotion["repo"],
        "branch": promotion["branch"],
        "base_branch": promotion.get("base_branch", "main"),
        "head": head,
        "pr_number": promotion["pr_number"],
        "authority_ref": promotion.get("authority_ref", ""),
        "idempotency_key": promotion["idempotency_key"],
        "promotion": dict(promotion),
    }
    return build_envelope(
        event_type=EventType.PROMOTION_REQUESTED,
        mission_id=mission,
        plan_id=task,
        task_id=task,
        correlation_id=corr,
        causation_id=causation_id,
        source_role=source_role,
        source_component=source_component,
        source_repo=source_repo,
        source_head=head,
        created_at=created_at,
        authority_ref=str(promotion.get("authority_ref") or "unsigned"),
        payload=payload,
        idempotency_key=f"{PROMOTION_INPUT}|{promotion['idempotency_key']}",
        attempt=attempt,
        ack_state=AckState.PENDING,
    )


def build_candidate_push_event(
    candidate: Mapping[str, Any],
    *,
    created_at: str = "2026-08-28T00:00:00.000Z",
    source_role: str = "SWITCHAROO",
    source_component: str = "switchboard-first-seam",
    source_repo: str = "NeuruhAI/neuruh-workers",
    causation_id: str = "evt-court-passed-fixture",
    correlation_id: str | None = None,
    attempt: int = 1,
) -> dict[str, Any]:
    """Build the real Switchboard envelope. Uses events.build_envelope."""
    mission = str(candidate["mission_id"])
    task = str(candidate["task_id"])
    head = str(candidate["expected_local_head"])
    corr = correlation_id or f"corr-{mission}-{task}"
    payload = {
        "artifact": SWITCHBOARD_INPUT,
        "git_push": GIT_PUSH,
        "mission_id": mission,
        "task_id": task,
        "repo": candidate["repo"],
        "worktree": candidate["worktree"],
        "branch": candidate["branch"],
        "head": head,
        "remote": candidate.get("remote_name", "origin"),
        "court_receipt": candidate.get("court_receipt_ref", ""),
        "authority_ref": candidate.get("authority_ref", ""),
        "idempotency_key": candidate["idempotency_key"],
        "candidate": dict(candidate),
    }
    return build_envelope(
        event_type=EventType.CANDIDATE_PUSH_REQUESTED,
        mission_id=mission,
        plan_id=task,
        task_id=task,
        correlation_id=corr,
        causation_id=causation_id,
        source_role=source_role,
        source_component=source_component,
        source_repo=source_repo,
        source_head=head,
        created_at=created_at,
        authority_ref=str(candidate.get("authority_ref") or "unsigned"),
        payload=payload,
        idempotency_key=f"{SWITCHBOARD_INPUT}|{candidate['idempotency_key']}",
        attempt=attempt,
        ack_state=AckState.PENDING,
    )
