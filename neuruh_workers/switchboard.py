"""Switchboard first seam: observe a sealed PRIME receipt, emit court.requested,
and on court.passed materialise exactly one Honey Pot / Git Custodian artifact.

This is a wrapper around the existing ExecutionLedger + JobQueue.  It is not
a new event bus, scheduler, orchestrator, workflow engine, or database.
OrganRegistrationSeam.activate() remains refused — this wrap does not need it.

Delivery is AT_LEAST_ONCE.  Consequential consumers are idempotent via
deterministic job IDs.  Persistence is JSONL (ledger) + a cursor file.
Loopback/local file observation only; no public listener.
"""
from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from typing import Any, Callable, Mapping

from .contracts import DefinitionError, JobStatus, content_sha
from .definitions import WorkflowCatalog
from .events import (
    AckState,
    DeadletterReason,
    EventError,
    EventType,
    build_envelope,
    validate_envelope,
)
from .ledger import Clock, ExecutionLedger, utc_clock
from .prime_receipts import (
    contains_secret_shape,
    looks_like_path,
    parse_prime_receipt,
    redact_secrets,
    validate_prime_receipt,
)
from .queue import JobQueue
from .registry import WorkerRegistry
from .runner import WorkflowRunner
from .switchboard_jobs import (
    GIT_PUSH,
    HONEY_POT_WORKFLOW,
    REMEDIATION_WORKFLOW,
    SENTINEL_MODEL_EXECUTION,
    SENTINEL_WORKFLOW,
    bind_switchboard_workers,
)

EVENT_JOB = "switchboard-events"
CURSOR_NAME = "cursor.json"
EVENTS_NAME = "events.jsonl"
JOBS_NAME = "jobs.jsonl"
SOURCE_COMPONENT = "switchboard-first-seam"
SOURCE_ROLE = "SWITCHAROO"
SOURCE_REPO = "NeuruhAI/neuruh-workers"
CONSUMER_HANDLER_VERSION = "switchboard-first-seam-v1"

T07_MISSION = "NEURUH_GROK_SOVEREIGN_BINDING_V4"
T07_STAGE = "T07_LOCAL_GROK_OBSERVER_BIND"
T07_HEAD = "7461eb835b3f979a8243f9f70da634a4e5ab62fc"
T07_AXON_PR = "4"
T07_AXON_PR_URL = "https://github.com/NeuruhAI/neuruh-axon/pull/4"
T07_AXON_BRANCH = "feat/grok-sovereign-mcp-v4-20260827"

DEFAULT_MISSIONS = frozenset({T07_MISSION})

# First seam closes the clipboard.  Court PASS is never derived here.
HUMAN_CLIPBOARD_REQUIRED = "NO"
INDEPENDENT_COURT_KIND = "independent_sentinel_court"
_FAKE_EVIDENCE_KINDS = frozenset({
    "reconstruction",
    "injected",
    "prime_verdict",
    "sentinel_closure_in_receipt",
    "clipboard",
})


class SwitchboardError(DefinitionError):
    code = "SWITCHBOARD_REFUSED"


@dataclass
class ConsumerSpec:
    consumer_id: str
    event_type: EventType
    mission_scope: frozenset[str] | None  # None = all known
    handler_version: str = CONSUMER_HANDLER_VERSION
    max_attempts: int = 3


@dataclass
class TickReport:
    observed: list[str] = field(default_factory=list)
    events_emitted: list[str] = field(default_factory=list)
    jobs_created: list[str] = field(default_factory=list)
    acked: list[str] = field(default_factory=list)
    deadlettered: list[str] = field(default_factory=list)
    recovered: list[str] = field(default_factory=list)


def _atomic_write(path: str, text: str) -> None:
    parent = os.path.dirname(os.path.abspath(path))
    os.makedirs(parent, exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        fh.write(text)
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(tmp, path)


class Switchboard:
    """Durable observe → validate → court job → ack, plus court.passed wrap."""

    def __init__(
        self,
        root: str,
        *,
        receipts_dir: str,
        known_missions: frozenset[str] | None = None,
        clock: Clock | None = None,
        crash_after_lease: bool = False,
        fail_handler: Callable[[str], None] | None = None,
        bind_host: str = "127.0.0.1",
        git_custodian: Any = None,
    ):
        if bind_host not in ("127.0.0.1", "localhost", "::1"):
            raise SwitchboardError(
                f"switchboard bind must be loopback, got {bind_host!r}")
        self.bind_host = bind_host
        self.root = os.path.abspath(root)
        self.receipts_dir = os.path.abspath(receipts_dir)
        os.makedirs(self.root, exist_ok=True)
        os.makedirs(self.receipts_dir, exist_ok=True)
        self._clock = clock or utc_clock
        self.known_missions = known_missions or DEFAULT_MISSIONS
        self.crash_after_lease = crash_after_lease
        self.fail_handler = fail_handler
        self.git_custodian = git_custodian
        self.event_ledger = ExecutionLedger(
            os.path.join(self.root, EVENTS_NAME), clock=self._clock)
        self.job_ledger = ExecutionLedger(
            os.path.join(self.root, JOBS_NAME), clock=self._clock)
        self.registry = WorkerRegistry()
        self.catalog = WorkflowCatalog()
        bind_switchboard_workers(self.registry, self.catalog)
        self.queue = JobQueue(self.job_ledger, self.registry)
        self.runner = WorkflowRunner(self.queue, self.registry, self.catalog)
        self.cursor_path = os.path.join(self.root, CURSOR_NAME)
        self._cursor = self._load_cursor()
        self._rebuild_seen_from_ledger()
        consumers: list[ConsumerSpec] = [
            ConsumerSpec("switchboard.court-on-sealed.v1",
                         EventType.RECEIPT_SEALED, self.known_missions),
            ConsumerSpec("switchboard.push-on-court-passed.v1",
                         EventType.COURT_PASSED, self.known_missions),
            ConsumerSpec("switchboard.remediate-on-court-failed.v1",
                         EventType.COURT_FAILED, self.known_missions),
        ]
        if git_custodian is not None:
            consumers.append(ConsumerSpec(
                "git-custodian.consume-push-requested.v1",
                EventType.CANDIDATE_PUSH_REQUESTED, self.known_missions))
        self.consumers = tuple(consumers)

    # -- persistence ------------------------------------------------------- #

    def _load_cursor(self) -> dict[str, Any]:
        if not os.path.exists(self.cursor_path):
            return {
                "observed": {},
                "consumers": {},
                "seen_event_ids": [],
                "seen_idempotency_keys": [],
            }
        with open(self.cursor_path, "r", encoding="utf-8") as fh:
            return json.load(fh)

    def _save_cursor(self) -> None:
        _atomic_write(self.cursor_path, json.dumps(self._cursor, indent=2,
                                                   sort_keys=True))

    def _rebuild_seen_from_ledger(self) -> None:
        seen_ids = self._cursor.setdefault("seen_event_ids", [])
        seen_keys = self._cursor.setdefault("seen_idempotency_keys", [])
        for envelope in self.events():
            eid = envelope.get("event_id")
            key = envelope.get("idempotency_key")
            if eid and eid not in seen_ids:
                seen_ids.append(eid)
            if key and key not in seen_keys:
                seen_keys.append(key)

    def _consumer_state(self, consumer_id: str) -> dict[str, Any]:
        consumers = self._cursor.setdefault("consumers", {})
        if consumer_id not in consumers:
            consumers[consumer_id] = {
                "last_ack": 0,
                "lease": None,
                "attempt_count": 0,
                "deadletter_policy": "retain_original",
            }
        return consumers[consumer_id]

    # -- public ------------------------------------------------------------ #

    def listen(self, host: str, port: int) -> None:
        """First seam has no network listener.  Public binds are refused."""
        if host not in ("127.0.0.1", "localhost", "::1"):
            raise SwitchboardError(
                f"public bind refused; loopback/local only, got {host!r}:{port}")
        raise SwitchboardError(
            "no network listener in first seam; file observation only")

    def tick(self) -> TickReport:
        report = TickReport()
        self._observe_receipts(report)
        self._deliver(report)
        self._save_cursor()
        return report

    def reopen(self) -> "Switchboard":
        """A fresh process over the same durable files."""
        return Switchboard(
            self.root,
            receipts_dir=self.receipts_dir,
            known_missions=self.known_missions,
            clock=self._clock,
            fail_handler=self.fail_handler,
            bind_host=self.bind_host,
            git_custodian=self.git_custodian,
        )

    def events(self, event_type: str | EventType | None = None) -> list[dict]:
        wanted = event_type.value if isinstance(event_type, EventType) else event_type
        out = []
        for receipt in self.event_ledger.receipts(EVENT_JOB):
            payload = dict(receipt.payload)
            if wanted and payload.get("event_type") != wanted:
                continue
            out.append(payload)
        return out

    def deadletters(self) -> list[dict]:
        return [e for e in self.events(EventType.DEADLETTER_CREATED)]

    def jobs_for(self, workflow: str) -> list[str]:
        ids = []
        for job_id in self.queue.job_ids():
            try:
                state = self.queue.load(job_id)
            except Exception:  # noqa: BLE001 — skip non-job ids
                continue
            if state.workflow == workflow:
                ids.append(job_id)
        return ids

    def record_court_outcome(
        self,
        *,
        passed: bool,
        court_job_id: str,
        mission_id: str,
        plan_id: str,
        task_id: str,
        correlation_id: str,
        causation_id: str,
        source_head: str,
        authority_ref: str,
        source_repo: str,
        receipt_ref: str,
        evidence: Mapping[str, Any],
        verdict_note: str = "",
        extra_payload: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Ingest Sentinel's independent verdict.  Switchboard does not PASS."""
        normalized = self._require_independent_court_evidence(
            evidence=evidence,
            court_job_id=court_job_id,
            correlation_id=correlation_id,
        )
        event_type = EventType.COURT_PASSED if passed else EventType.COURT_FAILED
        payload = {
            "court_job_id": court_job_id,
            "verdict": "PASS" if passed else "FAIL",
            "receipt_ref": receipt_ref,
            "sentinel_model_execution": SENTINEL_MODEL_EXECUTION,
            "note": verdict_note or (
                "independent Sentinel verdict ingested; Switchboard did not judge"),
        }
        if extra_payload:
            payload.update(dict(extra_payload))
        # Evidence and verdict are pinned after extra_payload so a caller
        # cannot overwrite them into a fake PASS.
        payload["evidence"] = normalized
        payload["verdict"] = "PASS" if passed else "FAIL"
        payload["sentinel_model_execution"] = SENTINEL_MODEL_EXECUTION
        envelope = self._emit(
            event_type,
            mission_id=mission_id,
            plan_id=plan_id,
            task_id=task_id,
            correlation_id=correlation_id,
            causation_id=causation_id,
            source_role="SENTINEL",
            source_component="sentinel-court",
            source_repo=source_repo,
            source_head=source_head,
            authority_ref=authority_ref,
            payload=payload,
            idempotency_key=f"{event_type.value}|{court_job_id}|{source_head}",
        )
        self._save_cursor()
        return envelope

    def ingest_court_fixture(self, path: str) -> dict[str, Any]:
        """Ingest independent Sentinel court evidence.  Never opens SECRET_POINTER.

        Reconstruction / injected fixtures are refused — they are not a
        Sentinel PASS.  SENTINEL_CLOSURE / PRIME_VERDICT on the PRIME
        receipt are also not a PASS.
        """
        with open(path, "r", encoding="utf-8") as fh:
            data = json.load(fh)
        pointer = data.get("secret_pointer")
        if pointer is not None:
            if not isinstance(pointer, str) or not looks_like_path(pointer):
                raise SwitchboardError(
                    "secret_pointer must be a path string only")
            # Path only.  Do not stat, open, log, or copy the secret file.
        dumped = json.dumps(data)
        scanned = dumped.replace(pointer, "") if isinstance(pointer, str) else dumped
        if contains_secret_shape(scanned):
            raise SwitchboardError(
                "court fixture must not carry secret file contents")
        if data.get("verdict") not in {"PASS", "FAIL"}:
            raise SwitchboardError("court fixture verdict must be PASS or FAIL")
        if data.get("reconstruction"):
            raise SwitchboardError(
                "reconstruction fixtures are not independent Sentinel court "
                "evidence; refusing fake Sentinel PASS")
        if data.get("independent") is False:
            raise SwitchboardError(
                "fixture marked independent=false is not court evidence")
        mission_id = str(data.get("mission_id", ""))
        stage_id = str(data.get("stage_id", ""))
        source_head = str(data.get("source_head", "")).lower()
        sealed = None
        for envelope in self.events(EventType.RECEIPT_SEALED):
            if (envelope.get("mission_id") == mission_id
                    and envelope.get("plan_id") == stage_id
                    and envelope.get("source_head") == source_head):
                sealed = envelope
        if sealed is None:
            raise SwitchboardError(
                "court fixture has no matching sealed receipt on the chain")
        court_events = [
            e for e in self.events(EventType.COURT_REQUESTED)
            if e.get("correlation_id") == sealed["correlation_id"]
        ]
        if not court_events:
            raise SwitchboardError(
                "court fixture requires a prior court.requested job artifact")
        court = court_events[-1]
        summary = data.get("summary") or {}
        expected = data.get("honey_pot_expected") or {}
        evidence = {
            "kind": data.get("kind") or INDEPENDENT_COURT_KIND,
            "source_role": data.get("source_role") or "SENTINEL",
            "independent": data.get("independent", True),
            "ref": os.path.abspath(path),
        }
        return self.record_court_outcome(
            passed=data["verdict"] == "PASS",
            court_job_id=court["payload_ref_or_payload"]["court_job_id"],
            mission_id=sealed["mission_id"],
            plan_id=sealed["plan_id"],
            task_id=sealed["task_id"],
            correlation_id=sealed["correlation_id"],
            causation_id=court["event_id"],
            source_head=sealed["source_head"],
            authority_ref=sealed["authority_ref"],
            source_repo=str(data.get("repo") or sealed["source_repo"]),
            receipt_ref=sealed["payload_ref_or_payload"]["receipt_ref"],
            evidence=evidence,
            verdict_note=(
                "independent Sentinel court evidence ingested from fixture; "
                "Switchboard did not judge; SENTINEL_CLOSURE / PRIME_VERDICT "
                "in the PRIME receipt were not treated as PASS"
            ),
            extra_payload={
                "summary": {
                    "pass_count": summary.get("pass_count"),
                    "fail_count": summary.get("fail_count"),
                    "local_observer_bind": summary.get("local_observer_bind"),
                    "remote_grok_bind": summary.get("remote_grok_bind"),
                    "bind": summary.get("bind"),
                    "pid": summary.get("pid"),
                    "observer_tools": summary.get("observer_tools"),
                    "mutations": summary.get("mutations"),
                },
                "secret_pointer": pointer,
                "honey_pot_expected": expected,
                "secret_file_ingested": False,
            },
        )

    def _require_independent_court_evidence(
        self,
        *,
        evidence: Mapping[str, Any],
        court_job_id: str,
        correlation_id: str,
    ) -> dict[str, Any]:
        """Fail closed unless evidence is independent Sentinel court, not a fake."""
        if not isinstance(evidence, Mapping) or not evidence:
            raise SwitchboardError(
                "court.passed/failed requires independent court evidence")
        if evidence.get("reconstruction"):
            raise SwitchboardError(
                "reconstruction is not independent Sentinel court evidence; "
                "refusing fake Sentinel PASS")
        if evidence.get("independent") is False:
            raise SwitchboardError(
                "court evidence marked independent=false is refused")
        kind = str(evidence.get("kind") or evidence.get("evidence_kind") or "").strip()
        if not kind or kind.lower() in _FAKE_EVIDENCE_KINDS:
            raise SwitchboardError(
                "court evidence kind is missing or is a fake/reconstruction PASS")
        source_role = str(evidence.get("source_role") or "").strip().upper()
        if source_role != "SENTINEL":
            raise SwitchboardError(
                "court outcome source_role must be independent SENTINEL")
        ref = evidence.get("ref") or evidence.get("evidence_ref")
        if not isinstance(ref, str) or not ref.strip():
            raise SwitchboardError("court evidence requires a non-empty ref")
        sealed = self._prior_on_chain(correlation_id, EventType.RECEIPT_SEALED)
        if sealed is None:
            raise SwitchboardError(
                "court outcome requires a sealed receipt on the correlation chain")
        court_events = [
            e for e in self.events(EventType.COURT_REQUESTED)
            if (e.get("payload_ref_or_payload") or {}).get("court_job_id")
            == court_job_id
        ]
        if not court_events:
            raise SwitchboardError(
                "court.passed/failed requires a prior court.requested job artifact")
        receipt_payload = sealed.get("payload_ref_or_payload") or {}
        forbidden_refs = {
            "PRIME_VERDICT",
            "SENTINEL_CLOSURE",
            str(receipt_payload.get("prime_verdict_in_receipt") or ""),
            str(receipt_payload.get("sentinel_closure_in_receipt") or ""),
        }
        if ref in forbidden_refs:
            raise SwitchboardError(
                "PRIME_VERDICT / SENTINEL_CLOSURE in the receipt is not court evidence")
        return {
            "kind": kind,
            "source_role": "SENTINEL",
            "independent": True,
            "ref": ref,
        }

    def ingest_raw(self, record: Mapping[str, Any]) -> dict[str, Any]:
        """Ingest a caller-supplied envelope.  Invalid schema is deadlettered."""
        try:
            envelope = validate_envelope(record)
        except EventError as exc:
            original = dict(record) if isinstance(record, Mapping) else {
                "raw": redact_secrets(repr(record))}
            return self._deadletter(
                original_event=original,
                reason=DeadletterReason.INVALID_SCHEMA,
                detail=str(exc),
                mission_id=str(record.get("mission_id", "UNKNOWN")) if isinstance(
                    record, Mapping) else "UNKNOWN",
                plan_id=str(record.get("plan_id", "UNKNOWN")) if isinstance(
                    record, Mapping) else "UNKNOWN",
                task_id=str(record.get("task_id", "UNKNOWN")) if isinstance(
                    record, Mapping) else "UNKNOWN",
                correlation_id=str(record.get("correlation_id", "UNKNOWN"))
                if isinstance(record, Mapping) else "UNKNOWN",
                causation_id=str(record.get("event_id", "")) if isinstance(
                    record, Mapping) else "",
                source_head=str(record.get("source_head", "0" * 40)) if isinstance(
                    record, Mapping) and _looks_head(record.get("source_head"))
                else "0" * 40,
                authority_ref="deadletter:invalid-schema",
            )
        try:
            EventType(envelope["event_type"])
        except ValueError:
            return self._deadletter_envelope(
                envelope, DeadletterReason.UNROUTABLE_EVENT, "unknown event_type")
        known_types = {t.value for t in EventType}
        if envelope["event_type"] not in known_types:
            return self._deadletter_envelope(
                envelope, DeadletterReason.UNROUTABLE_EVENT, "unroutable")
        stored = self._append_event(envelope)
        self._save_cursor()
        return stored

    # -- observe ----------------------------------------------------------- #

    def _observe_receipts(self, report: TickReport) -> None:
        names = sorted(
            n for n in os.listdir(self.receipts_dir)
            if os.path.isfile(os.path.join(self.receipts_dir, n))
            and n.endswith(".txt")
            and not n.startswith(".")
            and not n.endswith(".secret")
        )
        observed = self._cursor.setdefault("observed", {})
        for name in names:
            path = os.path.join(self.receipts_dir, name)
            receipt = parse_prime_receipt(path)
            prior = observed.get(name)
            if prior and prior.get("sha") == receipt.content_sha:
                continue
            if prior and prior.get("sha") != receipt.content_sha:
                self._deadletter(
                    original_event={
                        "receipt_ref": path,
                        "prior_sha": prior["sha"],
                        "new_sha": receipt.content_sha,
                        "raw_retained": True,
                    },
                    reason=DeadletterReason.BROKEN_HASH,
                    detail="receipt file changed after observation",
                    mission_id=receipt.mission or "UNKNOWN",
                    plan_id=receipt.stage or "UNKNOWN",
                    task_id=receipt.task_id or name,
                    correlation_id=prior.get("correlation_id", "UNKNOWN"),
                    causation_id="",
                    source_head=receipt.head if _looks_head(receipt.head) else "0" * 40,
                    authority_ref=receipt.authority_ref or "deadletter",
                )
                report.deadlettered.append(name)
                continue
            report.observed.append(name)
            self._admit_receipt(receipt, name, report)

    def _admit_receipt(self, receipt, name: str, report: TickReport) -> None:
        mission = receipt.mission or "UNKNOWN"
        stage = receipt.stage or "UNKNOWN"
        task_id = receipt.task_id or stage
        head = receipt.head if _looks_head(receipt.head) else "0" * 40
        correlation_id = "corr-" + content_sha(
            {"mission": mission, "stage": stage, "head": head})[:24]
        created = self._emit(
            EventType.RECEIPT_CREATED,
            mission_id=mission,
            plan_id=stage,
            task_id=task_id,
            correlation_id=correlation_id,
            causation_id="",
            source_role="PRIME" if "PRIME" in receipt.source_role.upper() else SOURCE_ROLE,
            source_component=receipt.get("SOURCE_COMPONENT") or "prime-receipt",
            source_repo=receipt.repo or SOURCE_REPO,
            source_head=head,
            authority_ref=receipt.authority_ref or "unsigned",
            payload={
                "receipt_ref": receipt.path,
                "receipt_sha": receipt.content_sha,
                "sentinel_closure_in_receipt": receipt.get("SENTINEL_CLOSURE"),
                "prime_verdict_in_receipt": receipt.get("PRIME_VERDICT"),
                "clipboard_defect": receipt.clipboard_defect,
            },
            idempotency_key=f"receipt.created|{receipt.content_sha}",
        )
        report.events_emitted.append(created["event_id"])
        verdict = validate_prime_receipt(receipt, known_missions=self.known_missions)
        self._cursor.setdefault("observed", {})[name] = {
            "sha": receipt.content_sha,
            "correlation_id": correlation_id,
        }
        if not verdict.ok:
            rejected = self._emit(
                EventType.RECEIPT_REJECTED,
                mission_id=mission,
                plan_id=stage,
                task_id=task_id,
                correlation_id=correlation_id,
                causation_id=created["event_id"],
                source_role=SOURCE_ROLE,
                source_component=SOURCE_COMPONENT,
                source_repo=SOURCE_REPO,
                source_head=head,
                authority_ref=receipt.authority_ref or "unsigned",
                payload={
                    "receipt_ref": receipt.path,
                    "receipt_sha": receipt.content_sha,
                    "reason": verdict.reason.value if verdict.reason else "UNKNOWN",
                    "detail": verdict.detail,
                    "original_retained": True,
                },
                idempotency_key=f"receipt.rejected|{receipt.content_sha}",
            )
            report.events_emitted.append(rejected["event_id"])
            self._deadletter(
                original_event={
                    "receipt_ref": receipt.path,
                    "receipt_sha": receipt.content_sha,
                    "fields": dict(receipt.fields),
                    "original_retained": True,
                },
                reason=verdict.reason or DeadletterReason.INVALID_SCHEMA,
                detail=verdict.detail,
                mission_id=mission,
                plan_id=stage,
                task_id=task_id,
                correlation_id=correlation_id,
                causation_id=rejected["event_id"],
                source_head=head,
                authority_ref=receipt.authority_ref or "deadletter",
            )
            report.deadlettered.append(name)
            return
        sealed = self._emit(
            EventType.RECEIPT_SEALED,
            mission_id=mission,
            plan_id=stage,
            task_id=task_id,
            correlation_id=correlation_id,
            causation_id=created["event_id"],
            source_role="PRIME",
            source_component=receipt.get("SOURCE_COMPONENT") or "prime-builder",
            source_repo=receipt.repo or SOURCE_REPO,
            source_head=head,
            authority_ref=receipt.authority_ref,
            payload={
                "receipt_ref": receipt.path,
                "receipt_sha": receipt.content_sha,
                "mission": mission,
                "stage": stage,
                "claimer": receipt.claimer or "PRIME",
                "repo": receipt.repo,
                "worktree": receipt.worktree,
                "branch": receipt.branch,
                "head": head,
                "remote": receipt.remote,
                "expected_pr": receipt.expected_pr,
                "expected_pr_url": receipt.expected_pr_url,
                "expected_pr_draft": receipt.expected_pr_draft,
                "bind": receipt.bind,
                "sentinel_closure_in_receipt": receipt.get("SENTINEL_CLOSURE"),
                "clipboard_defect": receipt.clipboard_defect,
                "required_invariants": [
                    "envelope.valid",
                    "hash.matches_if_provided",
                    "source_identity.coherent",
                    "mission_stage.unambiguous",
                    "authority.present",
                    "loopback_only",
                    "sentinel_closure_in_receipt_is_not_pass",
                ],
            },
            idempotency_key=f"receipt.sealed|{receipt.content_sha}",
        )
        report.events_emitted.append(sealed["event_id"])

    # -- deliver ----------------------------------------------------------- #

    def _deliver(self, report: TickReport) -> None:
        all_events = [
            (r.seq, dict(r.payload))
            for r in self.event_ledger.receipts(EVENT_JOB)
        ]
        for consumer in self.consumers:
            state = self._consumer_state(consumer.consumer_id)
            lease = state.get("lease")
            if lease and lease.get("state") in {
                    AckState.LEASED.value, AckState.PROCESSING.value,
                    AckState.RETRY.value}:
                report.recovered.append(lease["event_id"])
                self._run_leased(consumer, lease, report)
            if state.get("lease"):
                # Still in-flight (retry); do not lease a second event.
                continue
            last_ack = int(state.get("last_ack") or 0)
            for seq, envelope in all_events:
                if seq <= last_ack:
                    continue
                if envelope.get("event_type") != consumer.event_type.value:
                    continue
                mission = envelope.get("mission_id")
                if consumer.mission_scope is not None and mission not in consumer.mission_scope:
                    self._deadletter_envelope(
                        envelope, DeadletterReason.UNKNOWN_MISSION,
                        "event mission outside consumer scope")
                    report.deadlettered.append(envelope["event_id"])
                    state["last_ack"] = seq
                    continue
                state["lease"] = {
                    "event_id": envelope["event_id"],
                    "seq": seq,
                    "state": AckState.LEASED.value,
                    "attempt_count": 1,
                }
                self._save_cursor()
                if self.crash_after_lease:
                    raise _CrashSimulated(
                        f"killed after lease of {envelope['event_id']}")
                self._run_leased(consumer, state["lease"], report)
                if state.get("lease"):
                    break

    def _run_leased(self, consumer: ConsumerSpec, lease: dict,
                    report: TickReport) -> None:
        state = self._consumer_state(consumer.consumer_id)
        seq = lease["seq"]
        envelope = None
        for receipt in self.event_ledger.receipts(EVENT_JOB):
            if receipt.seq == seq:
                envelope = dict(receipt.payload)
                break
        if envelope is None:
            state["lease"] = None
            return
        attempt = int(lease.get("attempt_count") or 1)
        lease["state"] = AckState.PROCESSING.value
        try:
            if self.fail_handler is not None:
                self.fail_handler(consumer.consumer_id)
            job_id = self._handle(consumer, envelope)
            if job_id:
                report.jobs_created.append(job_id)
            state["last_ack"] = seq
            state["lease"] = None
            state["attempt_count"] = 0
            report.acked.append(envelope["event_id"])
        except _CrashSimulated:
            raise
        except Exception as exc:  # noqa: BLE001 — receipted, never swallowed
            if attempt >= consumer.max_attempts:
                self._deadletter_envelope(
                    envelope, DeadletterReason.MAX_RETRIES, str(exc))
                report.deadlettered.append(envelope["event_id"])
                state["last_ack"] = seq
                state["lease"] = None
                state["attempt_count"] = 0
            else:
                lease["state"] = AckState.RETRY.value
                lease["attempt_count"] = attempt + 1
                state["lease"] = lease
                state["attempt_count"] = attempt + 1
                lease["last_error"] = redact_secrets(str(exc))

    def _handle(self, consumer: ConsumerSpec, envelope: dict) -> str | None:
        event_type = EventType(envelope["event_type"])
        if event_type is EventType.RECEIPT_SEALED:
            return self._on_receipt_sealed(envelope)
        if event_type is EventType.COURT_PASSED:
            return self._on_court_passed(envelope)
        if event_type is EventType.COURT_FAILED:
            return self._on_court_failed(envelope)
        if event_type is EventType.RECEIPT_CREATED:
            return self._unroutable(consumer, event_type)
        if event_type is EventType.RECEIPT_REJECTED:
            return self._unroutable(consumer, event_type)
        if event_type is EventType.COURT_REQUESTED:
            return self._unroutable(consumer, event_type)
        if event_type is EventType.COURT_STARTED:
            return self._unroutable(consumer, event_type)
        if event_type is EventType.CANDIDATE_PUSH_REQUESTED:
            return self._on_candidate_push_requested(envelope)
        if event_type is EventType.DEADLETTER_CREATED:
            return self._unroutable(consumer, event_type)
        if event_type is EventType.DEADLETTER_REQUEUED:
            return self._unroutable(consumer, event_type)
        if event_type is EventType.TASK_READY:
            return self._unroutable(consumer, event_type)
        if event_type is EventType.TASK_FAILED:
            return self._unroutable(consumer, event_type)
        _assert_never(event_type)
        return None

    def _unroutable(self, consumer: ConsumerSpec, event_type: EventType) -> None:
        raise SwitchboardError(
            f"consumer {consumer.consumer_id} cannot route {event_type.value}",
            {"reason": DeadletterReason.UNROUTABLE_EVENT.value})

    def _on_receipt_sealed(self, envelope: dict) -> str:
        payload = envelope["payload_ref_or_payload"]
        if envelope.get("authority_ref") in {"", "unsigned", "none"}:
            self._deadletter_envelope(
                envelope, DeadletterReason.AUTHORITY_INVALID,
                "consequential court dispatch requires authority")
            raise SwitchboardError("authority invalid")
        params = {
            "mission": payload["mission"],
            "stage": payload["stage"],
            "claimer": payload["claimer"],
            "receipt_ref": payload["receipt_ref"],
            "source_head": payload["head"],
            "required_invariants": list(payload["required_invariants"]),
            "authority_ref": envelope["authority_ref"],
        }
        dedupe = envelope["idempotency_key"]
        wf = self.catalog.get(SENTINEL_WORKFLOW)
        enqueued = self.queue.enqueue(
            wf, params, requested_by=SOURCE_ROLE, dedupe_key=dedupe)
        state = self.queue.load(enqueued)
        if state.status == JobStatus.QUEUED:
            self.runner.execute(enqueued)
        court_payload = {
            "court_job_id": enqueued,
            "mission": params["mission"],
            "stage": params["stage"],
            "claimer": params["claimer"],
            "receipt_ref": params["receipt_ref"],
            "source_head": params["source_head"],
            "required_invariants": params["required_invariants"],
            "authority_ref": params["authority_ref"],
            "sentinel_model_execution": SENTINEL_MODEL_EXECUTION,
        }
        self._emit(
            EventType.COURT_REQUESTED,
            mission_id=envelope["mission_id"],
            plan_id=envelope["plan_id"],
            task_id=envelope["task_id"],
            correlation_id=envelope["correlation_id"],
            causation_id=envelope["event_id"],
            source_role=SOURCE_ROLE,
            source_component=SOURCE_COMPONENT,
            source_repo=SOURCE_REPO,
            source_head=envelope["source_head"],
            authority_ref=envelope["authority_ref"],
            payload=court_payload,
            idempotency_key=f"court.requested|{payload['receipt_sha']}",
        )
        return enqueued

    def _on_court_passed(self, envelope: dict) -> str:
        if envelope.get("authority_ref") in {"", "unsigned", "none"}:
            self._deadletter_envelope(
                envelope, DeadletterReason.AUTHORITY_INVALID,
                "candidate push requires authority")
            raise SwitchboardError("authority invalid")
        # Find the sealed receipt payload on this correlation chain.
        sealed = self._prior_on_chain(envelope["correlation_id"],
                                      EventType.RECEIPT_SEALED)
        if sealed is None:
            self._deadletter_envelope(
                envelope, DeadletterReason.CONTRADICTION,
                "court.passed without a sealed receipt on the chain")
            raise SwitchboardError("missing sealed receipt")
        src = sealed["payload_ref_or_payload"]
        court = envelope["payload_ref_or_payload"]
        expected = court.get("honey_pot_expected") or {}
        params = {
            "mission_id": envelope["mission_id"],
            "task_id": envelope["task_id"],
            "repo": expected.get("repo") or src.get("repo", ""),
            "worktree": src.get("worktree", ""),
            "branch": expected.get("branch") or src.get("branch", ""),
            "head": expected.get("head") or src.get("head", envelope["source_head"]),
            "remote": src.get("remote", "origin"),
            "court_receipt": court.get("court_job_id", envelope["causation_id"]),
            "authority_ref": envelope["authority_ref"],
            "idempotency_key": f"candidate.push.requested|{src.get('receipt_sha')}",
            "expected_pr": expected.get("pr_number") or src.get("expected_pr") or None,
            "expected_pr_url": expected.get("pr_url") or src.get("expected_pr_url") or None,
            "expected_pr_draft": expected.get("pr_draft") or src.get("expected_pr_draft") or None,
        }
        if params["expected_pr"] == "":
            params["expected_pr"] = None
        if expected.get("candidate"):
            params["candidate"] = expected["candidate"]
        wf = self.catalog.get(HONEY_POT_WORKFLOW)
        job_id = self.queue.enqueue(
            wf, params, requested_by=SOURCE_ROLE,
            dedupe_key=params["idempotency_key"])
        state = self.queue.load(job_id)
        if state.status == JobStatus.QUEUED:
            self.runner.execute(job_id)
        self._emit(
            EventType.CANDIDATE_PUSH_REQUESTED,
            mission_id=envelope["mission_id"],
            plan_id=envelope["plan_id"],
            task_id=envelope["task_id"],
            correlation_id=envelope["correlation_id"],
            causation_id=envelope["event_id"],
            source_role=SOURCE_ROLE,
            source_component=SOURCE_COMPONENT,
            source_repo=SOURCE_REPO,
            source_head=envelope["source_head"],
            authority_ref=envelope["authority_ref"],
            payload={**params, "git_push": GIT_PUSH, "honey_pot_job_id": job_id},
            idempotency_key=params["idempotency_key"],
        )
        return job_id

    def _on_candidate_push_requested(self, envelope: dict) -> str | None:
        """Honey Pot consume: Git Custodian handles the Switchboard envelope."""
        worker = self.git_custodian
        if worker is None:
            return None
        receipt = worker.handle(envelope)
        if isinstance(receipt, Mapping):
            return str(receipt.get("job_id") or "") or None
        return None

    def _on_court_failed(self, envelope: dict) -> str:
        court = envelope["payload_ref_or_payload"]
        params = {
            "mission_id": envelope["mission_id"],
            "task_id": envelope["task_id"],
            "court_receipt": court.get("court_job_id", envelope["causation_id"]),
            "reason": court.get("verdict", "FAIL"),
        }
        wf = self.catalog.get(REMEDIATION_WORKFLOW)
        job_id = self.queue.enqueue(
            wf, params, requested_by=SOURCE_ROLE,
            dedupe_key=f"remediation|{envelope['correlation_id']}")
        state = self.queue.load(job_id)
        if state.status == JobStatus.QUEUED:
            self.runner.execute(job_id)
        self._emit(
            EventType.TASK_READY,
            mission_id=envelope["mission_id"],
            plan_id=envelope["plan_id"],
            task_id=envelope["task_id"],
            correlation_id=envelope["correlation_id"],
            causation_id=envelope["event_id"],
            source_role=SOURCE_ROLE,
            source_component=SOURCE_COMPONENT,
            source_repo=SOURCE_REPO,
            source_head=envelope["source_head"],
            authority_ref=envelope["authority_ref"] or "remediation",
            payload={**params, "git_push": GIT_PUSH, "kind": "remediation",
                     "remediation_job_id": job_id},
            idempotency_key=f"task.ready|remediation|{envelope['correlation_id']}",
        )
        return job_id

    def _prior_on_chain(self, correlation_id: str,
                        event_type: EventType) -> dict | None:
        found = None
        for envelope in self.events():
            if (envelope.get("correlation_id") == correlation_id
                    and envelope.get("event_type") == event_type.value):
                found = envelope
        return found

    # -- emit / deadletter ------------------------------------------------- #

    def _emit(self, event_type: EventType, **kwargs) -> dict[str, Any]:
        envelope = build_envelope(
            event_type=event_type,
            created_at=self._clock(),
            **kwargs,
        )
        return self._append_event(envelope)

    def _append_event(self, envelope: dict[str, Any]) -> dict[str, Any]:
        seen_ids = self._cursor.setdefault("seen_event_ids", [])
        seen_keys = self._cursor.setdefault("seen_idempotency_keys", [])
        if envelope["event_id"] in seen_ids:
            return envelope
        if envelope["idempotency_key"] in seen_keys:
            return envelope
        self.event_ledger.append(
            envelope["event_type"], EVENT_JOB, envelope)
        seen_ids.append(envelope["event_id"])
        seen_keys.append(envelope["idempotency_key"])
        return envelope

    def _deadletter(self, *, original_event: Mapping[str, Any],
                    reason: DeadletterReason, detail: str,
                    mission_id: str, plan_id: str, task_id: str,
                    correlation_id: str, causation_id: str,
                    source_head: str, authority_ref: str) -> dict[str, Any]:
        payload = {
            "reason": reason.value,
            "detail": redact_secrets(detail),
            "original_event": json.loads(redact_secrets(json.dumps(original_event))),
            "original_retained": True,
        }
        return self._emit(
            EventType.DEADLETTER_CREATED,
            mission_id=mission_id or "UNKNOWN",
            plan_id=plan_id or "UNKNOWN",
            task_id=task_id or "UNKNOWN",
            correlation_id=correlation_id or "UNKNOWN",
            causation_id=causation_id,
            source_role=SOURCE_ROLE,
            source_component=SOURCE_COMPONENT,
            source_repo=SOURCE_REPO,
            source_head=source_head if _looks_head(source_head) else "0" * 40,
            authority_ref=authority_ref or "deadletter",
            payload=payload,
            idempotency_key=(
                f"deadletter|{reason.value}|"
                f"{content_sha(dict(original_event))[:16]}|{detail[:40]}"),
        )

    def _deadletter_envelope(self, envelope: Mapping[str, Any],
                             reason: DeadletterReason, detail: str) -> dict[str, Any]:
        return self._deadletter(
            original_event=dict(envelope),
            reason=reason,
            detail=detail,
            mission_id=str(envelope.get("mission_id", "UNKNOWN")),
            plan_id=str(envelope.get("plan_id", "UNKNOWN")),
            task_id=str(envelope.get("task_id", "UNKNOWN")),
            correlation_id=str(envelope.get("correlation_id", "UNKNOWN")),
            causation_id=str(envelope.get("event_id", "")),
            source_head=str(envelope.get("source_head", "0" * 40)),
            authority_ref=str(envelope.get("authority_ref", "deadletter")),
        )


def _looks_head(value: Any) -> bool:
    return isinstance(value, str) and len(value) == 40 and all(
        c in "0123456789abcdef" for c in value.lower())


def _assert_never(value: EventType) -> None:
    raise SwitchboardError(f"unhandled event type {value.value}")


class _CrashSimulated(BaseException):
    """Process kill analogue: not caught by handler Exception wrapping."""
