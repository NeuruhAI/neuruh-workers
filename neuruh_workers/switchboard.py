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
import shutil

import fcntl
import functools
import hashlib
import json
import os
import stat
import subprocess
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any, Callable, Mapping

from .contracts import DefinitionError, JobStatus, ReceiptType, content_sha
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
    FR_DISPATCH_RECEIPT_SCHEMA,
    contains_secret_shape,
    looks_like_path,
    parse_prime_receipt,
    redact_secrets,
    validate_receipt,
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
#: The base a board names when it could not observe one. The organ's packet
#: gate REQUIRES canonical_sha, so omitting the key made the request
#: unparseable and the court deadlettered on MAX_RETRIES instead of failing
#: closed on the gap. Naming the unobserved base explicitly lets the organ
#: refuse it on the evidence, and lets this board refuse a PASS against it.
_UNOBSERVED_BASE = "0" * 40
# The consumer that carries court.requested to the independent Sentinel. It is
# registered only when an adjudicator is injected; workers never imports the organ.
ADJUDICATE_CONSUMER_ID = "switchboard.adjudicate-on-court-requested.v1"
COURTS_DIR = "courts"
DEFAULT_CANONICAL_BASE_REF = "refs/heads/main"
GIT = os.environ.get("NEURUH_GIT") or shutil.which("git") or "git"
# Only these relations may stand under a PASS (mirrors the organ's VERIFIED_RELATIONS).
_VERIFIED_RELATIONS = frozenset({
    "candidate_is_canonical_base",
    "candidate_descends_from_canonical_base",
})
# Evidence kinds the organ accepts (A04 §4). The board names only what it can bind.
_KIND_EVENTS = "switchboard_events_jsonl"
_KIND_EXECUTION_LEDGER = "workers_execution_ledger_jsonl"
_KIND_RECEIPT = "prime_receipt_txt"
_KIND_GOVERNANCE = "governance_authorize_receipts_jsonl"
# A FAIL whose only blocking finding is that the model could not be reached is not
# a judgement of the candidate. It stays open (retry, then deadletter) rather than
# becoming a false terminal court.failed.
_ENVIRONMENTAL_ONLY = frozenset({"MODEL_UNAVAILABLE"})
_FAKE_EVIDENCE_KINDS = frozenset({
    "reconstruction",
    "injected",
    "prime_verdict",
    "sentinel_closure_in_receipt",
    "clipboard",
})


class SwitchboardError(DefinitionError):
    code = "SWITCHBOARD_REFUSED"


class AdjudicatorUnavailable(SwitchboardError):
    """The independent Sentinel could not be reached, so no court happened.

    This is emphatically NOT a verdict. It is raised by an adjudicator adapter
    (see neuruh_workers.adjudicator_http) when the organ is down, times out, or
    answers with a non-2xx, and the board treats it as a VISIBLE blocked state:
    the court stays open, the cause is named on the tick report and in the
    consumer lease, and no retry budget is burned toward a MAX_RETRIES
    deadletter. Burning that budget is precisely how the live T02 court
    disappeared -- an unreachable organ became an anonymous deadletter that read
    like a judged candidate. An absent court must look absent.
    """

    code = "SENTINEL_COURT_UNAVAILABLE"


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
    #: Events whose consumer is blocked on an unreachable dependency. Open, not
    #: judged, not deadlettered -- and named, so a stalled seam is legible from
    #: `run_switchboard.py tick|status` instead of looking like quiet success.
    blocked: list[dict[str, Any]] = field(default_factory=list)


def _atomic_write(path: str, text: str) -> None:
    parent = os.path.dirname(os.path.abspath(path))
    os.makedirs(parent, exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        fh.write(text)
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(tmp, path)


def _synchronized(method):
    @functools.wraps(method)
    def wrapper(self, *args, **kwargs):
        with self._exclusive_lock():
            self._cursor = self._load_cursor()
            self._rebuild_seen_from_ledger()
            return method(self, *args, **kwargs)

    return wrapper


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
        promotion_executor: Any = None,
        adjudicator: Callable[[Mapping[str, Any]], Mapping[str, Any]] | None = None,
        authority_resolver: Callable[[str], Mapping[str, Any] | None] | None = None,
        execution_ledger_resolver: Callable[[str], str | None] | None = None,
        canonical_base_ref: str = DEFAULT_CANONICAL_BASE_REF,
        canonical_observer: Callable[[str, str, str], tuple[str, str]] | None = None,
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
        self.promotion_executor = promotion_executor
        # Injected, never imported: the independent Sentinel, and the two facts the
        # board cannot know on its own (the Governance record for a mission, and the
        # dispatch bridge's execution ledger for a job). Absent means absent -- the
        # request then names a gap instead of inventing a binding.
        self.adjudicator = adjudicator
        self.authority_resolver = authority_resolver
        self.execution_ledger_resolver = execution_ledger_resolver
        self.canonical_base_ref = canonical_base_ref
        # Observing the canonical base means reading a remote. That is injected,
        # never implied: with no observer the board names the gap, the organ fails
        # closed on it, and _require_observed_relation refuses every PASS against
        # it. Both production composition roots inject observe_canonical_base_via_git:
        # scripts/run_switchboard.py (_board) and the dispatch bridge
        # (organ_consumers.emit_receipt_sealed_and_switcharoo).
        self.canonical_observer = canonical_observer
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
        self.lock_path = os.path.join(self.root, ".switchboard.lock")
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
        if adjudicator is not None:
            consumers.append(ConsumerSpec(
                ADJUDICATE_CONSUMER_ID, EventType.COURT_REQUESTED, self.known_missions))
        if git_custodian is not None:
            consumers.append(ConsumerSpec(
                "git-custodian.consume-push-requested.v1",
                EventType.CANDIDATE_PUSH_REQUESTED, self.known_missions))
        if promotion_executor is not None:
            # Bob emits promotion.requested; the Honey Pot's promotion executor
            # consumes it. Without an executor the event is unroutable — the
            # Switchboard never merges anything itself.
            consumers.append(ConsumerSpec(
                "git-custodian.consume-promotion-requested.v1",
                EventType.PROMOTION_REQUESTED, self.known_missions))
        self.consumers = tuple(consumers)

    # -- persistence ------------------------------------------------------- #

    @contextmanager
    def _exclusive_lock(self):
        with open(self.lock_path, "a+", encoding="utf-8") as handle:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)

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

    @_synchronized
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
            promotion_executor=self.promotion_executor,
            adjudicator=self.adjudicator,
            authority_resolver=self.authority_resolver,
            execution_ledger_resolver=self.execution_ledger_resolver,
            canonical_base_ref=self.canonical_base_ref,
            canonical_observer=self.canonical_observer,
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

    @_synchronized
    def record_court_outcome(self, **kwargs) -> dict[str, Any]:
        """Ingest Sentinel's independent verdict.  Switchboard does not PASS."""
        return self._record_court_outcome_locked(**kwargs)

    def _record_court_outcome_locked(
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
            passed=passed,
            court_job_id=court_job_id,
            mission_id=mission_id,
            plan_id=plan_id,
            task_id=task_id,
            correlation_id=correlation_id,
            causation_id=causation_id,
            source_head=source_head,
            authority_ref=authority_ref,
            source_repo=source_repo,
            receipt_ref=receipt_ref,
        )
        event_type = EventType.COURT_PASSED if passed else EventType.COURT_FAILED
        prior = [
            event for event in (
                self.events(EventType.COURT_PASSED)
                + self.events(EventType.COURT_FAILED)
            )
            if event.get("causation_id") == causation_id
        ]
        if prior:
            if (
                len(prior) == 1
                and prior[0].get("event_type") == event_type.value
                and (prior[0].get("payload_ref_or_payload") or {})
                .get("evidence", {}).get("record_sha") == normalized["record_sha"]
            ):
                return prior[0]
            raise SwitchboardError(
                "court request already has a different terminal outcome")
        payload = {
            "court_job_id": court_job_id,
            "verdict": "PASS" if passed else "FAIL",
            "receipt_ref": receipt_ref,
            "sentinel_model_execution": SENTINEL_MODEL_EXECUTION,
            "note": verdict_note or (
                "independent Sentinel verdict ingested; Switchboard did not judge"),
        }
        if extra_payload:
            forbidden_extra = {
                "honey_pot_expected",
                "evidence",
                "verdict",
                "sentinel_model_execution",
            }
            if forbidden_extra.intersection(extra_payload):
                raise SwitchboardError(
                    "court outcome extras cannot redirect custody or override verdict evidence")
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
        evidence = {
            "kind": data.get("kind"),
            "source_role": data.get("source_role"),
            "independent": data.get("independent"),
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
            source_repo=sealed["source_repo"],
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
                "secret_file_ingested": False,
            },
        )

    def _require_independent_court_evidence(
        self,
        *,
        evidence: Mapping[str, Any],
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
    ) -> dict[str, Any]:
        """Fail closed unless evidence is independent Sentinel court, not a fake."""
        if not isinstance(evidence, Mapping) or not evidence:
            raise SwitchboardError(
                "court.passed/failed requires independent court evidence")
        if evidence.get("reconstruction"):
            raise SwitchboardError(
                "reconstruction is not independent Sentinel court evidence; "
                "refusing fake Sentinel PASS")
        if evidence.get("independent") is not True:
            raise SwitchboardError(
                "court evidence must explicitly declare independent=true")
        kind = str(evidence.get("kind") or evidence.get("evidence_kind") or "").strip()
        if kind != INDEPENDENT_COURT_KIND or kind.lower() in _FAKE_EVIDENCE_KINDS:
            raise SwitchboardError(
                "court evidence kind is not the independent Sentinel contract")
        source_role = str(evidence.get("source_role") or "").strip().upper()
        if source_role != "SENTINEL":
            raise SwitchboardError(
                "court outcome source_role must be independent SENTINEL")
        ref = evidence.get("ref") or evidence.get("evidence_ref")
        if (
            not isinstance(ref, str)
            or not os.path.isabs(ref)
            or not os.path.isfile(ref)
        ):
            raise SwitchboardError("court evidence requires an existing absolute file ref")
        court_events = [
            event for event in self.events(EventType.COURT_REQUESTED)
            if (
                event.get("event_id") == causation_id
                and event.get("correlation_id") == correlation_id
                and (event.get("payload_ref_or_payload") or {}).get("court_job_id")
                == court_job_id
            )
        ]
        if len(court_events) != 1:
            raise SwitchboardError(
                "court outcome requires one exact prior court.requested event")
        court = court_events[0]
        sealed_events = [
            event for event in self.events(EventType.RECEIPT_SEALED)
            if event.get("event_id") == court.get("causation_id")
        ]
        if len(sealed_events) != 1:
            raise SwitchboardError(
                "court outcome requires one exact causative sealed receipt")
        sealed = sealed_events[0]
        receipt_payload = sealed.get("payload_ref_or_payload") or {}
        expected = {
            "mission_id": mission_id,
            "plan_id": plan_id,
            "task_id": task_id,
            "correlation_id": correlation_id,
            "source_head": source_head,
            "authority_ref": authority_ref,
        }
        for field, value in expected.items():
            if sealed.get(field) != value or court.get(field) != value:
                raise SwitchboardError(
                    f"court outcome {field} differs from its exact receipt chain")
        if sealed.get("source_repo") != source_repo:
            raise SwitchboardError(
                "court outcome source_repo differs from its sealed receipt")
        if os.path.realpath(str(receipt_payload.get("receipt_ref") or "")) != os.path.realpath(
                receipt_ref):
            raise SwitchboardError("court outcome receipt_ref differs from sealed receipt")
        try:
            current_receipt = parse_prime_receipt(receipt_ref)
            receipt_stat = os.lstat(receipt_ref)
        except OSError as exc:
            raise SwitchboardError("sealed receipt is no longer readable") from exc
        if current_receipt.content_sha != receipt_payload.get("receipt_sha"):
            raise SwitchboardError("sealed receipt bytes changed before court outcome")
        if (
            current_receipt.receipt_schema == FR_DISPATCH_RECEIPT_SCHEMA
            and (
                not stat.S_ISREG(receipt_stat.st_mode)
                or receipt_stat.st_mode & 0o222
            )
        ):
            raise SwitchboardError(
                "FR dispatch receipt is not an immutable regular file")
        forbidden_refs = {
            "PRIME_VERDICT",
            "SENTINEL_CLOSURE",
            str(receipt_payload.get("prime_verdict_in_receipt") or ""),
            str(receipt_payload.get("sentinel_closure_in_receipt") or ""),
        }
        if ref in forbidden_refs:
            raise SwitchboardError(
                "PRIME_VERDICT / SENTINEL_CLOSURE in the receipt is not court evidence")
        try:
            with open(ref, encoding="utf-8") as handle:
                record = json.load(handle)
        except (OSError, json.JSONDecodeError) as exc:
            raise SwitchboardError("Sentinel evidence record is unreadable") from exc
        if not isinstance(record, Mapping):
            raise SwitchboardError("Sentinel evidence record must be an object")
        record_expected = {
            "kind": INDEPENDENT_COURT_KIND,
            "source_role": "SENTINEL",
            "independent": True,
            "mission_id": mission_id,
            "job_id": court_job_id,
            "receipt_ref": receipt_ref,
            "receipt_sha": receipt_payload.get("receipt_sha"),
            "candidate_sha": source_head,
            "plan_id": plan_id,
            "task_id": task_id,
            "correlation_id": correlation_id,
            "causation_id": causation_id,
            "source_head": source_head,
            "source_repo": source_repo,
            "authority_ref": authority_ref,
            "workers_sentinel_model_execution": SENTINEL_MODEL_EXECUTION,
        }
        for field, value in record_expected.items():
            if record.get(field) != value:
                raise SwitchboardError(
                    f"Sentinel evidence {field} is not bound to the court request")
        # The organ seals verdicts this board does not ingest directly
        # (PASS_WITH_EXCEPTIONS). Where it projected one, the PROJECTION is what
        # the outcome must agree with -- it is the organ's own answer to
        # "PASS or FAIL", and re-deriving that answer here is exactly the
        # divergence this seam removed. A record sealed without a projection --
        # the offline record-court path, and every record already on a live
        # ledger -- is still compared on its raw verdict, unchanged.
        ingest = record.get("switchboard_ingest")
        verdict = str(record.get("verdict") or "")
        if isinstance(ingest, Mapping) and ingest.get("ingest_verdict"):
            verdict = str(ingest["ingest_verdict"])
        if verdict != ("PASS" if passed else "FAIL"):
            raise SwitchboardError("Sentinel evidence verdict differs from outcome")
        court_id = record.get("court_id")
        if (
            not isinstance(court_id, str)
            or len(court_id) != 30
            or not court_id.startswith("court-")
            or any(char not in "0123456789abcdef" for char in court_id[6:])
        ):
            raise SwitchboardError("Sentinel evidence court_id is missing")
        if os.path.realpath(str(record.get("ref") or "")) != os.path.realpath(ref):
            raise SwitchboardError("Sentinel evidence ref is not self-bound")
        if (
            type(record.get("findings")) is not list
            or type(record.get("exceptions")) is not list
            or type(record.get("required_repairs")) is not list
            or not isinstance(record.get("observed_sha"), str)
            or len(record["observed_sha"]) != 64
            or any(char not in "0123456789abcdef" for char in record["observed_sha"])
        ):
            raise SwitchboardError(
                "Sentinel evidence findings and observed digest are malformed")
        # Binding runs for EVERY verdict; only the relation and authority are
        # PASS-only. An unbound terminal FAIL forecloses the correct verdict just
        # as surely as an unearned PASS grants one.
        self._require_request_binding(record, court=court, passed=passed)
        if passed:
            if (
                record["findings"]
                or record["exceptions"]
                or record["required_repairs"]
                or record.get("base_relation") == "unverified"
            ):
                raise SwitchboardError("Sentinel PASS carries findings or exceptions")
            self._require_observed_relation(record, court=court, source_head=source_head)
            iar = record.get("iar_provenance")
            binding = iar.get("binding") if isinstance(iar, Mapping) else None
            if (
                not isinstance(iar, Mapping)
                or iar.get("authorized") is not True
                or not isinstance(iar.get("authorization_ref"), str)
                or not iar["authorization_ref"].startswith("iar:")
                or iar.get("provenance_intact") is not True
                or not isinstance(binding, Mapping)
                or not _looks_head(binding.get("repo_sha"))
                or type(binding.get("ledger_line_count")) is not int
                or binding["ledger_line_count"] < 1
                or not isinstance(binding.get("ledger_sha256_prefix"), str)
                or len(binding["ledger_sha256_prefix"]) < 4
            ):
                raise SwitchboardError(
                    "Sentinel PASS lacks IAR authorization provenance")
        return {
            "kind": kind,
            "source_role": "SENTINEL",
            "independent": True,
            "ref": ref,
            "court_id": court_id,
            "record_sha": content_sha(record),
        }

    @_synchronized
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
            event_type = EventType(envelope["event_type"])
        except ValueError:
            return self._deadletter_envelope(
                envelope, DeadletterReason.UNROUTABLE_EVENT, "unknown event_type")
        if event_type in {EventType.COURT_PASSED, EventType.COURT_FAILED}:
            return self._deadletter_envelope(
                envelope,
                DeadletterReason.CONTRADICTION,
                "court outcomes must enter through validated record_court_outcome",
            )
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
        verdict = validate_receipt(receipt, known_missions=self.known_missions)
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
            source_role=receipt.source_role,
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
                "code_custody_required": receipt.code_custody_required,
                "candidate": receipt.candidate,
                "candidate_sha256": receipt.get("CANDIDATE_SHA256") or None,
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
        except AdjudicatorUnavailable as exc:
            # No court happened. Keep it open and make the block visible: do NOT
            # advance attempt_count, because three quiet retries against a dead
            # organ would terminate a court that was never heard, under
            # MAX_RETRIES -- a reason that names the board's patience rather than
            # the actual cause. The lease stays in RETRY, so the next tick after
            # the organ returns picks the very same event back up.
            lease["state"] = AckState.RETRY.value
            lease["last_error"] = redact_secrets(str(exc))
            state["lease"] = lease
            report.blocked.append({
                "event_id": envelope["event_id"],
                "event_type": envelope["event_type"],
                "consumer_id": consumer.consumer_id,
                "reason": AdjudicatorUnavailable.code,
                "detail": redact_secrets(str(exc)),
                "attempt": attempt,
            })
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
            if self.adjudicator is None:
                return self._unroutable(consumer, event_type)
            return self._on_court_requested(envelope)
        if event_type is EventType.COURT_STARTED:
            return self._unroutable(consumer, event_type)
        if event_type is EventType.CANDIDATE_PUSH_REQUESTED:
            return self._on_candidate_push_requested(envelope)
        if event_type is EventType.PROMOTION_REQUESTED:
            if self.promotion_executor is None:
                return self._unroutable(consumer, event_type)
            return self._on_promotion_requested(envelope)
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
            "bindings": self._court_bindings(envelope),
        }
        dedupe = envelope["idempotency_key"]
        wf = self.catalog.get(SENTINEL_WORKFLOW)
        enqueued = self.queue.enqueue(
            wf, params, requested_by=SOURCE_ROLE, dedupe_key=dedupe)
        state = self.queue.load(enqueued)
        if state.status == JobStatus.QUEUED:
            self.runner.execute(enqueued)
            state = self.queue.load(enqueued)
        succeeded = [
            receipt for receipt in self.job_ledger.receipts(enqueued)
            if receipt.receipt_type == ReceiptType.JOB_SUCCEEDED
        ]
        if state.status != JobStatus.SUCCEEDED or len(succeeded) != 1:
            raise SwitchboardError(
                "Sentinel court artifact job did not seal exactly one success")
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
        court_payload.update(self._court_bindings(envelope))
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

    # -- court bindings (A04 §6): what the organ needs, named from immutable facts -- #

    def _court_bindings(self, sealed: Mapping[str, Any]) -> dict[str, Any]:
        """Everything the independent Sentinel needs to retrieve and verify the
        evidence for this receipt, derived from facts the board already holds or
        can observe. Nothing here is invented: when a fact is unavailable the
        request names the gap, and the organ fails closed on it."""
        payload = sealed.get("payload_ref_or_payload") or {}
        gaps: list[str] = []
        refs: list[dict[str, Any]] = []
        out: dict[str, Any] = {}

        # The receipt, under its declared kind only.
        receipt_ref = str(payload.get("receipt_ref") or "")
        if receipt_ref and os.path.isfile(receipt_ref):
            refs.append(self._typed_ref(_KIND_RECEIPT, receipt_ref))
        else:
            gaps.append("receipt")
        out["receipt_sha"] = payload.get("receipt_sha")

        # The causal chain, up to and including the sealed line: final at emission,
        # so the digest it carries still reproduces after this request is appended.
        sealed_seq = next(
            (r.seq for r in self.event_ledger.receipts(EVENT_JOB)
             if (r.payload or {}).get("event_id") == sealed.get("event_id")), 0)
        events_path = os.path.join(self.root, EVENTS_NAME)
        if sealed_seq and os.path.isfile(events_path):
            refs.append(self._typed_ref(_KIND_EVENTS, events_path, line_start=1, line_end=sealed_seq))
        else:
            gaps.append("causal_chain")

        # The dispatch bridge's execution ledger for this job (injected resolver).
        task_id = str(sealed.get("task_id") or "")
        out["task_id"] = task_id
        ledger_path = None
        if self.execution_ledger_resolver is not None and task_id:
            try:
                ledger_path = self.execution_ledger_resolver(task_id)
            except Exception:  # noqa: BLE001 -- a resolver fault is a gap, not a binding
                ledger_path = None
        if ledger_path and os.path.isfile(str(ledger_path)):
            refs.append(self._typed_ref(_KIND_EXECUTION_LEDGER, str(ledger_path)))
        else:
            gaps.append("execution_ledger")

        # The Governance record this dispatch's authority rests on (injected resolver).
        mission_id = str(sealed.get("mission_id") or "")
        authority = None
        if self.authority_resolver is not None and mission_id:
            try:
                authority = self.authority_resolver(mission_id)
            except Exception:  # noqa: BLE001
                authority = None
        ledger_ref = str((authority or {}).get("ledger_ref") or "")
        if (isinstance(authority, Mapping) and authority.get("authorization_id")
                and authority.get("entry_hash") and os.path.isfile(ledger_ref)):
            out["authority_record_ref"] = f"{authority['authorization_id']}:{authority['entry_hash']}"
            refs.append(self._typed_ref(
                _KIND_GOVERNANCE, ledger_ref,
                line_start=int(authority.get("line_start") or 0),
                line_end=int(authority.get("line_end") or 0)))
        else:
            gaps.append("authority_record")

        # Work spec: only the FR authority form carries one.
        authority_ref = str(sealed.get("authority_ref") or "")
        if ":work-spec:" in authority_ref:
            out["work_spec_sha256"] = authority_ref.rsplit(":work-spec:", 1)[1]

        # The candidate is the sealed head; the tree it ran in is where the organ
        # observes it.
        out["candidate_sha"] = str(sealed.get("source_head") or "").lower()
        worktree = str(payload.get("worktree") or "")
        if worktree and os.path.isabs(worktree) and os.path.isdir(worktree):
            out["source_repo_path"] = worktree
        else:
            gaps.append("source_repo_path")

        # The canonical base is OBSERVED on the worktree's remote. Declared here,
        # re-observed by the organ, never trusted from either side alone.
        repo_url, observed = "", ""
        if self.canonical_observer is not None and worktree:
            try:
                repo_url, observed = self.canonical_observer(
                    worktree, str(payload.get("remote") or "origin"), self.canonical_base_ref)
            except Exception:  # noqa: BLE001 -- an observer fault is a gap, not a base
                repo_url, observed = "", ""
        out["canonical_base_ref"] = self.canonical_base_ref
        if repo_url:
            out["canonical_repo"] = repo_url
        if observed:
            out["canonical_sha"] = observed
            out["canonical_base_observed"] = True
        else:
            # Name the gap AND a parseable base: the organ requires canonical_sha,
            # so an omitted key is not a fail-closed gap, it is an unreadable packet.
            out["canonical_sha"] = _UNOBSERVED_BASE
            out["canonical_base_observed"] = False
            gaps.append("canonical_base")

        out["evidence_refs"] = refs
        out["evidence_sha256"] = _aggregate_evidence_sha256([r["sha256"] for r in refs])
        out["binding_gaps"] = gaps
        return out

    @staticmethod
    def _typed_ref(kind: str, path: str, *, line_start: int = 0, line_end: int = 0) -> dict[str, Any]:
        with open(path, "rb") as handle:
            body = handle.read()
        entry: dict[str, Any] = {"kind": kind, "ref": os.path.abspath(path)}
        if line_start or line_end:
            lines = body.splitlines(keepends=True)
            if line_start < 1 or line_end < line_start or line_end > len(lines):
                raise SwitchboardError(
                    f"evidence line range {line_start}..{line_end} is outside {path}")
            body = b"".join(lines[line_start - 1:line_end])
            entry["line_start"], entry["line_end"] = line_start, line_end
        entry["sha256"] = hashlib.sha256(body).hexdigest()
        return entry

    # -- court.requested consumer (A04 §12) ----------------------------------- #

    def _on_court_requested(self, envelope: dict) -> str | None:
        """Carry the request to the independent Sentinel and ingest what it seals.

        The packet IS the event. No field is added, chosen, or assembled here; if
        the request did not name something, the organ does not get it, and fails
        closed on the gap. The board writes the sealed record it receives beside
        its own ledgers and ingests the verdict through the same verifier every
        other outcome goes through.
        """
        court = envelope.get("payload_ref_or_payload") or {}
        record = self.adjudicator(json.loads(json.dumps(envelope)))
        if not isinstance(record, Mapping):
            raise SwitchboardError("adjudicator returned no sealed record")
        if (record.get("kind") != INDEPENDENT_COURT_KIND
                or str(record.get("source_role") or "").upper() != "SENTINEL"
                or record.get("independent") is not True):
            raise SwitchboardError("adjudicator record is not an independent Sentinel court")
        court_id = str(record.get("court_id") or "")
        if not court_id.startswith("court-") or len(court_id) != 30:
            raise SwitchboardError("adjudicator record has no court_id")
        verdict = str(record.get("verdict") or "")
        if verdict not in {"PASS", "PASS_WITH_EXCEPTIONS", "FAIL"}:
            raise SwitchboardError("adjudicator record verdict is not PASS|PASS_WITH_EXCEPTIONS|FAIL")
        findings = [f for f in (record.get("findings") or []) if isinstance(f, Mapping)]
        blocking = {str(f.get("code")) for f in findings if f.get("severity", "blocking") == "blocking"}
        if verdict == "FAIL" and blocking and blocking <= _ENVIRONMENTAL_ONLY:
            # The organ was reached but its model was not. Same shape as an
            # unreachable organ: a block, not a judgement. Raised as
            # AdjudicatorUnavailable so the court genuinely stays open and says
            # why -- the plain SwitchboardError this used to raise burned the
            # retry budget and terminated under MAX_RETRIES, which is the
            # opposite of the sentence above it.
            raise AdjudicatorUnavailable(
                "Sentinel could not reach its model; the court stays open rather than "
                "recording a verdict the candidate did not earn")
        # PROJECTION OWNERSHIP (ruled 2026-09-09): the organ owns the PASS|FAIL
        # projection (sentinel_court.project.project_for_switchboard_ingest) and
        # ships it on the wire beside the record. The board reads that answer; it
        # does not re-derive one. Two implementations of this mapping is exactly
        # how a PASS carrying exceptions came to be scored one way inside the
        # organ and another way here.
        ingest = record.get("switchboard_ingest")
        if not isinstance(ingest, Mapping):
            raise SwitchboardError(
                "adjudicator record carries no switchboard_ingest projection; the "
                "PASS|FAIL projection is owned by the Sentinel organ and must arrive "
                "with the record it projects")
        passed = ingest.get("switchboard_passed")
        if not isinstance(passed, bool):
            raise SwitchboardError(
                "switchboard_ingest.switchboard_passed is not a boolean; the board "
                "does not infer a verdict the organ did not project")

        courts_dir = os.path.join(self.root, COURTS_DIR)
        os.makedirs(courts_dir, exist_ok=True)
        ref = os.path.join(courts_dir, f"{court_id}.json")
        sealed = dict(record)
        sealed["ref"] = ref
        text = json.dumps(sealed, sort_keys=True, indent=2) + "\n"
        # The record must be on disk before the verdict is ingested: the outcome
        # verifier requires an existing absolute file ref. What must NOT happen is
        # the old byte-for-byte refusal -- the organ re-adjudicates on redelivery
        # when it holds no ledger, and court_id_for deliberately excludes
        # sealed_at, so a re-seal differing only in its timestamp used to wedge the
        # court forever: attempt 1 failed at ingest, every later attempt died on
        # the comparison, and the receipt deadlettered with no verdict at all.
        # Only a genuinely different VERDICT for the same court_id is a conflict.
        if os.path.exists(ref):
            try:
                with open(ref, "r", encoding="utf-8") as handle:
                    prior = json.load(handle)
            except (OSError, ValueError):
                prior = {}
            identity = ("verdict", "evidence_sha256", "observed_sha", "canonical_sha", "base_relation")
            if any(prior.get(key) != sealed.get(key) for key in identity):
                raise SwitchboardError(f"court {court_id} was already sealed with a different verdict")
        _atomic_write(ref, text)

        self._record_court_outcome_locked(
            passed=passed,
            court_job_id=str(court.get("court_job_id") or ""),
            mission_id=envelope["mission_id"],
            plan_id=envelope["plan_id"],
            task_id=envelope["task_id"],
            correlation_id=envelope["correlation_id"],
            causation_id=envelope["event_id"],
            source_head=envelope["source_head"],
            authority_ref=envelope["authority_ref"],
            source_repo=envelope["source_repo"],
            receipt_ref=str(court.get("receipt_ref") or ""),
            evidence={"kind": INDEPENDENT_COURT_KIND, "source_role": "SENTINEL",
                      "independent": True, "ref": ref},
            verdict_note=("independent Sentinel verdict carried by the court.requested "
                          "consumer; Switchboard did not judge"),
        )

        return None

    def _require_request_binding(self, record: Mapping[str, Any], *, court: Mapping[str, Any],
                                 passed: bool) -> None:
        """Every verdict must answer the question this court actually asked.

        This runs for PASS and FAIL alike. A terminal FAIL is as consequential as a
        PASS -- it is the one outcome for this causation_id forever, and a later
        correct verdict is refused and deadlettered -- so a FAIL that judged other
        evidence may not be ingested either. Absence is fatal rather than
        permissive: a request that named no evidence digest can corroborate
        nothing, so nothing may be recorded against it.
        """
        payload = court.get("payload_ref_or_payload") or {}
        requested = str(payload.get("evidence_sha256") or "")
        if requested and str(record.get("evidence_sha256") or "") != requested:
            raise SwitchboardError(
                "Sentinel verdict evidence_sha256 differs from the evidence the court requested")
        declared = str(payload.get("canonical_sha") or "").lower()
        canonical = str(record.get("canonical_sha") or "").lower()
        # _UNOBSERVED_BASE is the board saying "I read no base", not a rival claim,
        # so it cannot contradict the record. A PASS against it is refused outright
        # in _require_observed_relation, which is where that belongs.
        if declared and declared != _UNOBSERVED_BASE and canonical and canonical != declared:
            raise SwitchboardError("Sentinel verdict judged a different canonical base than was requested")
        if passed:
            # Only a PASS must be positively bound: the organ's honest NO_EVIDENCE
            # FAIL legitimately binds nothing and carries no observed digest.
            # observed_sha is already required to be 64-hex on EVERY record by
            # _require_independent_court_evidence; only boundness is PASS-specific.
            if not record.get("evidence_refs"):
                raise SwitchboardError("Sentinel PASS names no bound evidence")
        elif not (record.get("findings") or record.get("exceptions")
                  or record.get("required_repairs")):
            raise SwitchboardError("Sentinel FAIL names no finding, exception or repair")

    def _require_observed_relation(self, record: Mapping[str, Any], *, court: Mapping[str, Any],
                                   source_head: str) -> None:
        """A PASS may rest only on a base THIS board observed.

        canonical_sha equal to the head is not a relation; it is two copies of one
        string. Nor is the record's own `canonical_base_observed_on_remote` the
        relation: that flag is the judged party's self-report, and a record that
        merely sets it to true must not thereby certify itself. The board grants a
        PASS only when its own request carried a base it read off the remote, and
        the record agrees with that base -- so a board with no canonical observer
        can never certify a PASS, which is the fail-closed answer. That last clause
        is enforced here, not merely described: a request that named the
        canonical_base gap or declared the unobserved sentinel refuses every PASS.
        """
        payload = court.get("payload_ref_or_payload") or {}
        binding = record.get("base_binding") if isinstance(record.get("base_binding"), Mapping) else {}
        relation = str(record.get("base_relation") or "")
        canonical = str(record.get("canonical_sha") or "").lower()
        observed_base = str(binding.get("observed_base_sha") or "").lower()
        declared = str(payload.get("canonical_sha") or "").lower()
        gaps = [str(gap) for gap in (payload.get("binding_gaps") or [])]
        remote_observed = binding.get("canonical_base_observed_on_remote") is True
        if relation not in _VERIFIED_RELATIONS:
            raise SwitchboardError(f"Sentinel PASS relation {relation!r} is not a verified relation")
        # The board's own observation is the only ground a PASS may stand on. A
        # request that named the canonical_base gap, or declared the unobserved
        # sentinel, or carries no 40-hex base at all, read nothing off the remote
        # -- and no record can supply what this board did not read, however
        # coherent it is. Each clause stands alone: a gap named beside a base is a
        # contradiction, and the sentinel without a gap is still no observation.
        if "canonical_base" in gaps or declared == _UNOBSERVED_BASE or not _looks_head(declared):
            raise SwitchboardError(
                "Sentinel PASS rests on a canonical base this board never observed")
        # A record that CLAIMS observation must show it. Without this, the whole
        # proof of an observed base was one boolean the judged party set itself:
        # `canonical_base_observed_on_remote: true` with no observed sha at all was
        # enough to carry a PASS whose canonical_sha equalled the candidate -- the
        # 2026-09-04 shape exactly.
        if remote_observed and not _looks_head(observed_base):
            raise SwitchboardError(
                "Sentinel PASS claims an observed canonical base but names no observed sha")
        if canonical == source_head and not remote_observed:
            raise SwitchboardError(
                "Sentinel PASS asserts the candidate is its own canonical base without "
                "an observed remote ref; equality by construction is not a relation")
        if remote_observed and observed_base and canonical != observed_base:
            raise SwitchboardError("Sentinel PASS canonical_sha is not the base it observed")
        # `declared` is now known to be a base this board read; the record must
        # have judged exactly that one.
        if canonical != declared:
            raise SwitchboardError("Sentinel PASS judged a different canonical base than was requested")

    def _on_court_passed(self, envelope: dict) -> str | None:
        if envelope.get("authority_ref") in {"", "unsigned", "none"}:
            self._deadletter_envelope(
                envelope, DeadletterReason.AUTHORITY_INVALID,
                "candidate push requires authority")
            raise SwitchboardError("authority invalid")
        court_requested = self._event_by_id(envelope.get("causation_id", ""))
        sealed = (
            self._event_by_id(court_requested.get("causation_id", ""))
            if court_requested is not None
            and court_requested.get("event_type") == EventType.COURT_REQUESTED.value
            else None
        )
        if (
            sealed is None
            or sealed.get("event_type") != EventType.RECEIPT_SEALED.value
            or court_requested.get("correlation_id") != envelope.get("correlation_id")
            or sealed.get("correlation_id") != envelope.get("correlation_id")
            or sealed.get("mission_id") != envelope.get("mission_id")
            or sealed.get("task_id") != envelope.get("task_id")
        ):
            self._deadletter_envelope(
                envelope, DeadletterReason.CONTRADICTION,
                "court.passed is not bound through exact causation to its sealed receipt")
            raise SwitchboardError("court.passed receipt chain mismatch")
        src = sealed["payload_ref_or_payload"]
        court = envelope["payload_ref_or_payload"]
        if src.get("code_custody_required") is not True:
            return None
        params = {
            "mission_id": envelope["mission_id"],
            "task_id": envelope["task_id"],
            "repo": src.get("repo", ""),
            "worktree": src.get("worktree", ""),
            "branch": src.get("branch", ""),
            "head": src.get("head", envelope["source_head"]),
            "remote": src.get("remote", "origin"),
            "court_receipt": court.get("court_job_id", envelope["causation_id"]),
            "authority_ref": envelope["authority_ref"],
            "idempotency_key": f"candidate.push.requested|{src.get('receipt_sha')}",
            "expected_pr": src.get("expected_pr") or None,
            "expected_pr_url": src.get("expected_pr_url") or None,
            "expected_pr_draft": src.get("expected_pr_draft") or None,
        }
        if src.get("candidate") is not None:
            params["candidate"] = src["candidate"]
        if params["expected_pr"] == "":
            params["expected_pr"] = None
        wf = self.catalog.get(HONEY_POT_WORKFLOW)
        job_id = self.queue.enqueue(
            wf, params, requested_by=SOURCE_ROLE,
            dedupe_key=params["idempotency_key"])
        state = self.queue.load(job_id)
        if state.status == JobStatus.QUEUED:
            self.runner.execute(job_id)
            state = self.queue.load(job_id)
        succeeded = [
            receipt for receipt in self.job_ledger.receipts(job_id)
            if receipt.receipt_type == ReceiptType.JOB_SUCCEEDED
        ]
        if state.status != JobStatus.SUCCEEDED or len(succeeded) != 1:
            raise SwitchboardError(
                "Honey Pot custody artifact did not seal exactly one success")
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

    def _on_promotion_requested(self, envelope: dict) -> str | None:
        """Promotion consume: the Honey Pot executor handles the envelope."""
        receipt = self.promotion_executor.handle(envelope)
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

    def _event_by_id(self, event_id: str) -> dict | None:
        matches = [
            envelope for envelope in self.events()
            if envelope.get("event_id") == event_id
        ]
        return matches[0] if len(matches) == 1 else None

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


def _aggregate_evidence_sha256(item_shas) -> str:
    """One digest over the set of per-item digests; order-independent (matches the organ)."""
    return hashlib.sha256(
        json.dumps(sorted(str(s) for s in item_shas), separators=(",", ":")).encode("utf-8")).hexdigest()


def observe_canonical_base_via_git(worktree: str, remote: str, base_ref: str) -> tuple[str, str]:
    """Default canonical observer: (canonical_repo, observed sha) via read-only git.

    The remote is the one the executed worktree itself calls canonical; the sha is
    what that remote's ref points at right now. Empty sha means: not observed.
    This reads a remote, so it is injected into the Switchboard rather than
    assumed -- a unit test must never reach a network from inside a tick."""
    if not worktree or not os.path.isdir(worktree):
        return "", ""
    # The artefact under judgement names a remote NAME, never a URL. A receipt
    # carrying `REMOTE=origin https://elsewhere/x` would otherwise choose both the
    # repository this court calls canonical and the host this tick reaches.
    tokens = remote.split()
    if len(tokens) != 1:
        return "", ""
    rc, out = _git_read(["-C", worktree, "remote", "get-url", tokens[0]])
    url = out.strip() if rc == 0 else ""
    if not url:
        return "", ""
    rc, out = _git_read(["ls-remote", "--", url, base_ref])
    if rc != 0:
        return url, ""
    for line in out.splitlines():
        parts = line.strip().split()
        if len(parts) == 2 and parts[1] == base_ref and _looks_head(parts[0].lower()):
            return url, parts[0].lower()
    return url, ""


def _git_read(args: list[str], *, timeout: int = 30) -> tuple[int, str]:
    """Read-only git; no prompts, no locks, no writes."""
    try:
        proc = subprocess.run(
            [GIT, "--no-optional-locks", *args], capture_output=True, text=True, timeout=timeout,
            env={**os.environ, "GIT_TERMINAL_PROMPT": "0", "GIT_OPTIONAL_LOCKS": "0"})
        return proc.returncode, proc.stdout
    except (OSError, subprocess.SubprocessError):
        return 127, ""


def _looks_head(value: Any) -> bool:
    return isinstance(value, str) and len(value) == 40 and all(
        c in "0123456789abcdef" for c in value.lower())


def _assert_never(value: EventType) -> None:
    raise SwitchboardError(f"unhandled event type {value.value}")


class _CrashSimulated(BaseException):
    """Process kill analogue: not caught by handler Exception wrapping."""
