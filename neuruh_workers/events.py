"""nrh.event.v1 envelope for the Switchboard first seam.

An event is data, not authority.  Switchboard validates the envelope and
routes; it never independently declares a receipt PASS (Sentinel's court).
"""
from __future__ import annotations

from enum import Enum
from typing import Any, Mapping

from .contracts import WorkflowError, canonical_json, content_sha, sha256_hex


SCHEMA_VERSION = "nrh.event.v1"

ENVELOPE_FIELDS = (
    "schema_version",
    "event_id",
    "event_type",
    "mission_id",
    "plan_id",
    "task_id",
    "correlation_id",
    "causation_id",
    "source_role",
    "source_component",
    "source_repo",
    "source_head",
    "created_at",
    "authority_ref",
    "payload_hash",
    "payload_ref_or_payload",
    "idempotency_key",
    "attempt",
    "max_attempts",
    "ack_state",
)


class EventType(str, Enum):
    RECEIPT_CREATED = "receipt.created"
    RECEIPT_SEALED = "receipt.sealed"
    RECEIPT_REJECTED = "receipt.rejected"
    COURT_REQUESTED = "court.requested"
    COURT_STARTED = "court.started"
    COURT_PASSED = "court.passed"
    COURT_FAILED = "court.failed"
    CANDIDATE_PUSH_REQUESTED = "candidate.push.requested"
    DEADLETTER_CREATED = "deadletter.created"
    DEADLETTER_REQUEUED = "deadletter.requeued"
    TASK_READY = "task.ready"
    TASK_FAILED = "task.failed"


class AckState(str, Enum):
    PENDING = "PENDING"
    LEASED = "LEASED"
    PROCESSING = "PROCESSING"
    ACKED = "ACKED"
    RETRY = "RETRY"
    DEADLETTER = "DEADLETTER"
    CANCELLED = "CANCELLED"


class DeadletterReason(str, Enum):
    INVALID_SCHEMA = "INVALID_SCHEMA"
    BROKEN_HASH = "BROKEN_HASH"
    UNKNOWN_MISSION = "UNKNOWN_MISSION"
    UNKNOWN_SOURCE_HEAD = "UNKNOWN_SOURCE_HEAD"
    AUTHORITY_INVALID = "AUTHORITY_INVALID"
    CONSUMER_FAILURE = "CONSUMER_FAILURE"
    MAX_RETRIES = "MAX_RETRIES"
    CONTRADICTION = "CONTRADICTION"
    UNROUTABLE_EVENT = "UNROUTABLE_EVENT"


class EventError(WorkflowError):
    code = "EVENT_INVALID"


def event_id_for(*, event_type: str, idempotency_key: str,
                 causation_id: str = "") -> str:
    return "evt-" + sha256_hex(
        canonical_json({
            "event_type": event_type,
            "idempotency_key": idempotency_key,
            "causation_id": causation_id,
        }))[:24]


def payload_hash_of(payload: Any) -> str:
    return content_sha(payload)


def validate_envelope(record: Mapping[str, Any]) -> dict[str, Any]:
    """Fail closed on missing/extra fields, wrong schema, or unknown type."""
    if not isinstance(record, dict):
        raise EventError("envelope must be an object")
    keys = tuple(record)
    if set(keys) != set(ENVELOPE_FIELDS):
        raise EventError(
            "envelope field set does not match nrh.event.v1",
            {"got": sorted(keys), "expected": list(ENVELOPE_FIELDS)})
    if record["schema_version"] != SCHEMA_VERSION:
        raise EventError(
            "unsupported schema_version",
            {"schema_version": record["schema_version"]})
    try:
        EventType(record["event_type"])
    except ValueError as exc:
        raise EventError(
            "unknown event_type",
            {"event_type": record["event_type"]}) from exc
    try:
        AckState(record["ack_state"])
    except ValueError as exc:
        raise EventError(
            "unknown ack_state",
            {"ack_state": record["ack_state"]}) from exc
    for key in ("event_id", "mission_id", "plan_id", "task_id",
                "correlation_id", "causation_id", "source_role",
                "source_component", "source_repo", "source_head",
                "created_at", "idempotency_key"):
        if not isinstance(record[key], str) or not record[key]:
            if key == "causation_id" and record[key] == "":
                continue
            raise EventError(f"{key} must be a non-empty string")
    if not isinstance(record["attempt"], int) or record["attempt"] < 1:
        raise EventError("attempt must be an integer >= 1")
    if not isinstance(record["max_attempts"], int) or record["max_attempts"] < 1:
        raise EventError("max_attempts must be an integer >= 1")
    expected_hash = payload_hash_of(record["payload_ref_or_payload"])
    if record["payload_hash"] != expected_hash:
        raise EventError(
            "payload_hash does not match payload",
            {"reason": DeadletterReason.BROKEN_HASH.value})
    return dict(record)


def build_envelope(
    *,
    event_type: EventType,
    mission_id: str,
    plan_id: str,
    task_id: str,
    correlation_id: str,
    causation_id: str,
    source_role: str,
    source_component: str,
    source_repo: str,
    source_head: str,
    created_at: str,
    authority_ref: str,
    payload: Any,
    idempotency_key: str,
    attempt: int = 1,
    max_attempts: int = 3,
    ack_state: AckState = AckState.PENDING,
) -> dict[str, Any]:
    body = {
        "schema_version": SCHEMA_VERSION,
        "event_id": event_id_for(
            event_type=event_type.value,
            idempotency_key=idempotency_key,
            causation_id=causation_id),
        "event_type": event_type.value,
        "mission_id": mission_id,
        "plan_id": plan_id,
        "task_id": task_id,
        "correlation_id": correlation_id,
        "causation_id": causation_id,
        "source_role": source_role,
        "source_component": source_component,
        "source_repo": source_repo,
        "source_head": source_head,
        "created_at": created_at,
        "authority_ref": authority_ref,
        "payload_hash": payload_hash_of(payload),
        "payload_ref_or_payload": payload,
        "idempotency_key": idempotency_key,
        "attempt": attempt,
        "max_attempts": max_attempts,
        "ack_state": ack_state.value,
    }
    return validate_envelope(body)
