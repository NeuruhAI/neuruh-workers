"""Durable mission continuation: a parent job waits on a deterministic child.

Owner: the existing Workers spine.  A parent and its child are both plain
ledger jobs, so continuation state is never stored — it is replayed from the
same hash-chained ExecutionLedger that already gives jobs pause/resume,
idempotent steps and crash recovery.  This module adds no daemon, no mission
table and no second ledger.

    parent  QUEUED
      -> tick: run parent until its `continuation.await_child` step
      -> child enqueued with a deterministic id (a second dispatch is the
         queue's receipted no-op) + `child.dispatched` on the parent
      -> parent JOB_PAUSED before await_child           (= WAITING_ON_CHILD)
      -> child runs to JOB_SUCCEEDED — same tick, a later tick, or another
         process over the same ledger file
      -> `child.result.ingested` on the parent, bound to the child's
         job.succeeded receipt sha (idempotent: one receipt per child)
      -> JOB_RESUMED; await_child serves the ingested result; the remaining
         steps run; parent JOB_SUCCEEDED   (= READY_FOR_REVIEW; read-only
         parents publish nothing and carry no public authority)

Blocked stays blocked: a FAILED/CANCELLED child leaves the parent PAUSED, an
approval-gated parent stays BLOCKED_ON_APPROVAL, a terminal parent is never
re-run and never re-dispatches its child.  No founder transport exists on
this path; the human gate is only the existing approval receipt.

Mission identity is the job's ``requested_by`` (``jgi:<mission_id>``), the
same convention the Founder Runtime dispatch bridge already uses, so the two
paths address the same jobs on the same ledger.
"""
from __future__ import annotations

from typing import Any, Callable, Mapping

from .contracts import (
    DefinitionError,
    JobStateInvalid,
    JobStatus,
    ReceiptType,
    content_sha,
    deterministic_job_id,
)
from .definitions import RetryPolicy, StepDefinition, WorkflowCatalog, WorkflowDefinition
from .ledger import ExecutionLedger
from .queue import JobQueue, JobState
from .registry import Capability, StepContext, WorkerDeclaration, WorkerRegistry
from .runner import WorkflowRunner
from .workflows import build_catalog, build_registry

MISSION_REQUESTED_BY_PREFIX = "jgi:"  # == organ_consumers.PARENT_REQUESTED_BY_PREFIX
AWAIT_CAPABILITY = "continuation.await_child"
PARENT_WORKFLOW = "mission-continuation-parent"
GATED_PARENT_WORKFLOW = "mission-continuation-parent-gated"
CHILD_RECEIPT_SCHEMA = "nrh.continuation.child_receipt.v1"
CHILD_DEDUPE_SCHEMA = "nrh.continuation.child.v1"

# Mission-level vocabulary projected from job state (never stored).
PHASE_RUNNABLE = "RUNNABLE"
PHASE_EXECUTING = "EXECUTING"
PHASE_WAITING_ON_CHILD = "WAITING_ON_CHILD"
PHASE_PAUSED = "PAUSED"
PHASE_BLOCKED_ON_APPROVAL = "BLOCKED_ON_APPROVAL"
PHASE_READY_FOR_REVIEW = "READY_FOR_REVIEW"
PHASE_FAILED = "FAILED"
PHASE_CANCELLED = "CANCELLED"


class ContinuationRefused(JobStateInvalid):
    code = "CONTINUATION_REFUSED"


# --------------------------------------------------------------------------- #
# Deterministic identity


def mission_requested_by(mission_id: str, *, child: bool = False) -> str:
    """``jgi:<mission_id>``; a child mission id is ``<parent>::<role>`` and is
    the only place '::' may appear."""
    if not mission_id or "\n" in mission_id or "\r" in mission_id:
        raise ContinuationRefused("mission_id must be a single-line value")
    if not child and "::" in mission_id:
        raise ContinuationRefused("parent mission_id must not contain '::'")
    return f"{MISSION_REQUESTED_BY_PREFIX}{mission_id}"


def child_mission_id(parent_mission_id: str, child_role: str) -> str:
    if not child_role or "::" in child_role or "\n" in child_role:
        raise ContinuationRefused("child_role must be a single-line value without '::'")
    return f"{parent_mission_id}::{child_role}"


def child_dedupe_key(parent_job_id: str, child_role: str) -> str:
    return content_sha({
        "schema": CHILD_DEDUPE_SCHEMA,
        "parent_job_id": parent_job_id,
        "child_role": child_role,
    })


def expected_child_job_id(parent_job_id: str, child_role: str,
                          child_definition: WorkflowDefinition,
                          child_params: Mapping[str, Any]) -> str:
    """Child identity is a pure function of parent job + role + child work."""
    return deterministic_job_id(
        child_definition.name, child_definition.version, dict(child_params),
        child_dedupe_key(parent_job_id, child_role), dry_run=False)


# --------------------------------------------------------------------------- #
# Parent workflow definitions (read-only)

_RETRY = RetryPolicy(max_attempts=1, base_delay_s=0.0)


def parent_workflow_definition() -> WorkflowDefinition:
    return WorkflowDefinition(
        name=PARENT_WORKFLOW, version="1.0.0",
        description="Declare a bounded mission, wait on one deterministic "
                    "specialist child, compose a review packet. Publishes "
                    "nothing.",
        steps=(
            StepDefinition("declare_mission", "continuation.declare",
                           retry=_RETRY, timeout_s=10.0),
            StepDefinition("await_child", AWAIT_CAPABILITY,
                           params={"child_role": "specialist"},
                           retry=_RETRY, timeout_s=10.0),
            StepDefinition("review_packet", "continuation.review_packet",
                           retry=_RETRY, timeout_s=10.0),
        ))


def gated_parent_workflow_definition() -> WorkflowDefinition:
    """Same continuation, but the review packet is founder-gated: the mission
    must stop at BLOCKED_ON_APPROVAL after the child returns."""
    return WorkflowDefinition(
        name=GATED_PARENT_WORKFLOW, version="1.0.0",
        description="Continuation parent whose review packet requires a "
                    "founder approval receipt.",
        steps=(
            StepDefinition("declare_mission", "continuation.declare",
                           retry=_RETRY, timeout_s=10.0),
            StepDefinition("await_child", AWAIT_CAPABILITY,
                           params={"child_role": "specialist"},
                           retry=_RETRY, timeout_s=10.0),
            StepDefinition("review_packet", "continuation.review_packet",
                           retry=_RETRY, timeout_s=10.0,
                           requires_approval=True, approvers=("founder",)),
        ))


def _require_str(params: Mapping[str, Any], key: str) -> str:
    value = params.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"required parameter {key!r} must be a non-empty string")
    return value


def declare_mission(ctx: StepContext, params: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "objective": _require_str(params, "objective"),
        "done_predicate": _require_str(params, "done_predicate"),
        "economic_link": _require_str(params, "economic_link"),
        "children": sorted((params.get("children") or {}).keys()),
        "public_authority": "none",
    }


def review_packet(ctx: StepContext, params: Mapping[str, Any]) -> dict[str, Any]:
    declared = ctx.results["declare_mission"]
    ingested = ctx.results["await_child"]
    return {
        "state": PHASE_READY_FOR_REVIEW,
        "objective": declared["objective"],
        "done_predicate": declared["done_predicate"],
        "child_role": ingested["child_role"],
        "child_job_id": ingested["child_job_id"],
        "child_job_succeeded_sha": ingested["child_job_succeeded_sha"],
        "child_results_sha": ingested["child_results_sha"],
        "published": False,
        "public_authority": "none",
    }


# --------------------------------------------------------------------------- #
# Coordinator


class MissionContinuation:
    """The mission-control seam over one ledger. Stateless between calls."""

    def __init__(self, ledger: ExecutionLedger, *,
                 registry: WorkerRegistry | None = None,
                 catalog: WorkflowCatalog | None = None,
                 sleep_fn: Callable[[float], None] | None = None):
        self.ledger = ledger
        self.registry = registry or build_registry()
        self.registry.register_worker(self.worker_declaration())
        self.catalog = catalog or build_catalog()
        self.catalog.register(parent_workflow_definition())
        self.catalog.register(gated_parent_workflow_definition())
        self.queue = JobQueue(ledger, self.registry)
        self.runner = WorkflowRunner(
            self.queue, self.registry, self.catalog,
            sleep_fn=sleep_fn or (lambda _s: None),
            step_hook=self._before_step)

    # -- capabilities ------------------------------------------------------ #

    def worker_declaration(self) -> WorkerDeclaration:
        def cap(name, fn, description):
            return Capability(name=name, description=description,
                              read_only=True, external=False, execute=fn)
        return WorkerDeclaration(
            worker_id="mission-continuation-v1",
            description="Parent/child continuation over the execution ledger. "
                        "Read-only; dispatches ledger jobs only.",
            capabilities=(
                cap("continuation.declare", declare_mission,
                    "Declare a bounded mission (objective, done predicate)"),
                cap(AWAIT_CAPABILITY, self._await_child,
                    "Serve the ingested child result from the ledger"),
                cap("continuation.review_packet", review_packet,
                    "Compose the READY_FOR_REVIEW packet; publishes nothing"),
            ))

    def _await_child(self, ctx: StepContext, params: Mapping[str, Any]) -> dict[str, Any]:
        role = _require_str(params, "child_role")
        state = self.queue.load(ctx.job_id)
        record = state.child_results.get(role)
        if record is None:
            # Unreachable through tick(): the hook pauses the parent first.
            raise ContinuationRefused(
                f"child {role!r} result is not ingested; parent must wait",
                {"job_id": ctx.job_id, "child_role": role})
        return {**record, "served_from": "ledger"}

    # -- identity ---------------------------------------------------------- #

    def parent_job_id(self, mission_id: str) -> str:
        wanted = mission_requested_by(mission_id)
        found = []
        for job_id in self.queue.job_ids():
            state = self._safe_load(job_id)
            if state is not None and state.requested_by == wanted:
                found.append(job_id)
        if not found:
            raise ContinuationRefused(
                f"mission {mission_id!r} has no parent job on this ledger",
                {"mission_id": mission_id})
        if len(found) > 1:
            raise ContinuationRefused(
                f"mission {mission_id!r} maps to {len(found)} parent jobs",
                {"mission_id": mission_id, "jobs": found})
        return found[0]

    def _safe_load(self, job_id: str) -> JobState | None:
        try:
            return self.queue.load(job_id)
        except Exception:  # noqa: BLE001 — non-job ledger rows
            return None

    def _child_spec(self, state: JobState, role: str) -> tuple[WorkflowDefinition, dict[str, Any]]:
        children = state.params.get("children")
        if not isinstance(children, Mapping) or role not in children:
            raise ContinuationRefused(
                f"parent params declare no child {role!r}",
                {"job_id": state.job_id, "child_role": role})
        spec = children[role]
        if not isinstance(spec, Mapping) or not isinstance(spec.get("workflow"), str):
            raise ContinuationRefused(
                f"child {role!r} must declare a workflow name",
                {"job_id": state.job_id, "child_role": role})
        try:
            definition = self.catalog.get(spec["workflow"])
        except DefinitionError as exc:
            raise ContinuationRefused(str(exc), {"child_role": role}) from exc
        if not definition.read_only:
            raise ContinuationRefused(
                f"child {role!r} binds non-read-only workflow {definition.name!r}",
                {"job_id": state.job_id, "child_role": role})
        params = spec.get("params") or {}
        if not isinstance(params, Mapping):
            raise ContinuationRefused("child params must be a mapping")
        return definition, dict(params)

    def _await_roles(self, state: JobState) -> list[str]:
        return [step["params"]["child_role"] for step in state.spec["steps"]
                if step["capability"] == AWAIT_CAPABILITY]

    # -- mission lifecycle ------------------------------------------------- #

    def open_mission(self, mission_id: str, params: Mapping[str, Any], *,
                     workflow: str = PARENT_WORKFLOW) -> dict[str, Any]:
        """Idempotent: the same mission opens the same parent job once."""
        requested_by = mission_requested_by(mission_id)
        definition = self.catalog.get(workflow)
        before = self.ledger.verify()["receipts"]
        existing = [job_id for job_id in self.queue.job_ids()
                    if (self._safe_load(job_id) or JobState(job_id)).requested_by == requested_by]
        if existing:
            same = deterministic_job_id(
                definition.name, definition.version, dict(params),
                content_sha({"mission_id": mission_id}), dry_run=False)
            if existing != [same]:
                raise ContinuationRefused(
                    f"mission {mission_id!r} is already open with different work",
                    {"mission_id": mission_id, "existing": existing})
        job_id = self.queue.enqueue(
            definition, params, requested_by=requested_by,
            dedupe_key=content_sha({"mission_id": mission_id}))
        last = self.ledger.receipts(job_id)[-1]
        return {
            "mission_id": mission_id,
            "parent_job_id": job_id,
            "status": ("OPENED" if last.receipt_type == ReceiptType.JOB_ENQUEUED
                       else "ALREADY_OPEN"),
            "receipts_before": before,
            "ledger_tail_sha": self.ledger.verify()["tail_sha"],
        }

    def dispatch_child(self, mission_id: str, role: str) -> dict[str, Any]:
        """Dispatch is refused for a terminal parent and deduplicated by
        the queue for an already-dispatched child."""
        parent_id = self.parent_job_id(mission_id)
        state = self.queue.load(parent_id)
        if state.terminal:
            return {
                "mission_id": mission_id, "child_role": role,
                "status": "DISPATCH_REFUSED_PARENT_TERMINAL",
                "parent_status": state.status.value,
                "child_job_id": (state.children.get(role) or {}).get("child_job_id"),
            }
        return self._dispatch(state, role)

    def _dispatch(self, state: JobState, role: str) -> dict[str, Any]:
        definition, params = self._child_spec(state, role)
        expected = expected_child_job_id(state.job_id, role, definition, params)
        child_id = self.queue.enqueue(
            definition, params,
            requested_by=mission_requested_by(
                child_mission_id(state.requested_by[len(MISSION_REQUESTED_BY_PREFIX):], role),
                child=True),
            dedupe_key=child_dedupe_key(state.job_id, role))
        if child_id != expected:  # cannot happen; identity is the same function
            raise ContinuationRefused("child identity drifted at enqueue")
        last = self.ledger.receipts(child_id)[-1]
        duplicate = last.receipt_type == ReceiptType.JOB_ENQUEUE_DEDUPLICATED
        already = state.children.get(role)
        if already is None:
            self.ledger.append(ReceiptType.CHILD_DISPATCHED, state.job_id, {
                "child_role": role,
                "child_mission_id": child_mission_id(
                    state.requested_by[len(MISSION_REQUESTED_BY_PREFIX):], role),
                "child_job_id": child_id,
                "child_workflow": definition.name,
                "child_version": definition.version,
                "child_params_sha": content_sha(params),
                "dedupe_key": child_dedupe_key(state.job_id, role),
            })
        elif already["child_job_id"] != child_id:
            raise ContinuationRefused(
                "recorded child identity does not match deterministic identity",
                {"recorded": already["child_job_id"], "expected": child_id})
        return {
            "mission_id": state.requested_by[len(MISSION_REQUESTED_BY_PREFIX):],
            "child_role": role,
            "child_job_id": child_id,
            "status": "DUPLICATE_REFUSED" if duplicate else "DISPATCHED",
            "child_status": self.queue.load(child_id).status.value,
        }

    def _before_step(self, job_id: str, step_id: str) -> None:
        state = self.queue.load(job_id)
        step = next((s for s in state.spec["steps"] if s["step_id"] == step_id), None)
        if step is None or step["capability"] != AWAIT_CAPABILITY:
            return
        role = step["params"]["child_role"]
        if role in state.child_results:
            return  # child returned and was ingested; let the step serve it
        self._dispatch(state, role)
        # WAITING_ON_CHILD: the runner honours this at the step boundary and
        # receipts JOB_PAUSED {paused_before_step: await_child}.
        self.queue.pause(job_id)

    def ingest_child(self, mission_id: str, role: str) -> dict[str, Any]:
        """Bind the child's job.succeeded receipt to the parent. Idempotent."""
        parent_id = self.parent_job_id(mission_id)
        state = self.queue.load(parent_id)
        if role in state.child_results:
            return {"mission_id": mission_id, "child_role": role,
                    "status": "ALREADY_INGESTED",
                    "child_job_id": state.child_results[role]["child_job_id"],
                    "child_job_succeeded_sha":
                        state.child_results[role]["child_job_succeeded_sha"]}
        if state.terminal:
            raise ContinuationRefused("parent is terminal; nothing to ingest",
                                      {"job_id": parent_id})
        dispatched = state.children.get(role)
        if dispatched is None:
            raise ContinuationRefused(f"child {role!r} was never dispatched",
                                      {"job_id": parent_id, "child_role": role})
        definition, params = self._child_spec(state, role)
        expected = expected_child_job_id(parent_id, role, definition, params)
        child_id = dispatched["child_job_id"]
        if child_id != expected:
            raise ContinuationRefused(
                "recorded child identity does not match deterministic identity",
                {"recorded": child_id, "expected": expected})
        child = self.queue.load(child_id)
        if child.status != JobStatus.SUCCEEDED:
            raise ContinuationRefused(
                f"child {role!r} is {child.status.value}, not SUCCEEDED; "
                "parent stays where it is",
                {"job_id": parent_id, "child_job_id": child_id,
                 "child_status": child.status.value})
        succeeded = [r for r in self.ledger.receipts(child_id)
                     if r.receipt_type == ReceiptType.JOB_SUCCEEDED][-1]
        self.ledger.append(ReceiptType.CHILD_RESULT_INGESTED, parent_id, {
            "child_role": role,
            "child_mission_id": dispatched["child_mission_id"],
            "child_job_id": child_id,
            "child_job_succeeded_sha": succeeded.receipt_sha,
            "child_job_succeeded_seq": succeeded.seq,
            "child_results_sha": succeeded.payload["results_sha"],
            "child_workflow": child.workflow,
            "child_version": child.version,
        })
        return {"mission_id": mission_id, "child_role": role,
                "status": "INGESTED", "child_job_id": child_id,
                "child_job_succeeded_sha": succeeded.receipt_sha}

    # -- sealed child receipts (portable, replay-safe) ---------------------- #

    def export_child_receipt(self, mission_id: str, role: str) -> dict[str, Any]:
        parent_id = self.parent_job_id(mission_id)
        state = self.queue.load(parent_id)
        dispatched = state.children.get(role)
        if dispatched is None:
            raise ContinuationRefused(f"child {role!r} was never dispatched")
        child_id = dispatched["child_job_id"]
        child = self.queue.load(child_id)
        if child.status != JobStatus.SUCCEEDED:
            raise ContinuationRefused(
                f"child is {child.status.value}; only a SUCCEEDED child seals a receipt")
        succeeded = [r for r in self.ledger.receipts(child_id)
                     if r.receipt_type == ReceiptType.JOB_SUCCEEDED][-1]
        body = {
            "schema": CHILD_RECEIPT_SCHEMA,
            "parent_mission_id": mission_id,
            "parent_job_id": parent_id,
            "child_role": role,
            "child_mission_id": dispatched["child_mission_id"],
            "child_job_id": child_id,
            "child_workflow": child.workflow,
            "child_version": child.version,
            "child_job_succeeded_sha": succeeded.receipt_sha,
            "child_job_succeeded_seq": succeeded.seq,
            "child_results_sha": succeeded.payload["results_sha"],
            "child_canonical_chain_tail": self.ledger.canonical_chain(child_id)[-1],
            "ledger_tail_sha": self.ledger.verify()["tail_sha"],
        }
        return {**body, "receipt_sha": content_sha(body)}

    def ingest_sealed_receipt(self, receipt: Mapping[str, Any]) -> dict[str, Any]:
        """Replay a sealed child receipt. Verified against the ledger, never
        trusted on its own; a second replay is a receipted no-op."""
        body = {k: v for k, v in receipt.items() if k != "receipt_sha"}
        if receipt.get("schema") != CHILD_RECEIPT_SCHEMA:
            raise ContinuationRefused("unknown child receipt schema")
        if content_sha(body) != receipt.get("receipt_sha"):
            raise ContinuationRefused("child receipt sha does not match its body")
        mission_id = body["parent_mission_id"]
        role = body["child_role"]
        parent_id = self.parent_job_id(mission_id)
        if parent_id != body["parent_job_id"]:
            raise ContinuationRefused("receipt parent job does not match this ledger")
        child_id = body["child_job_id"]
        succeeded = [r for r in self.ledger.receipts(child_id)
                     if r.receipt_type == ReceiptType.JOB_SUCCEEDED]
        if not succeeded or succeeded[-1].receipt_sha != body["child_job_succeeded_sha"]:
            raise ContinuationRefused(
                "receipt does not bind to a job.succeeded receipt on this ledger",
                {"child_job_id": child_id})
        if self.ledger.canonical_chain(child_id)[-1] != body["child_canonical_chain_tail"]:
            raise ContinuationRefused("child canonical chain does not match the receipt")
        result = self.ingest_child(mission_id, role)
        return {**result, "receipt_sha": receipt["receipt_sha"]}

    # -- the tick (no daemon; call it from anything) ----------------------- #

    def tick(self, mission_id: str, *, run_children: bool = True) -> dict[str, Any]:
        """Advance one mission by exactly what the ledger permits."""
        parent_id = self.parent_job_id(mission_id)
        state = self.queue.load(parent_id)
        report: dict[str, Any] = {
            "mission_id": mission_id, "parent_job_id": parent_id,
            "before": state.status.value, "actions": [],
        }
        if state.terminal:
            report["action"] = "PARENT_TERMINAL"
        elif state.status == JobStatus.BLOCKED_ON_APPROVAL:
            report["action"] = "PARENT_BLOCKED_ON_APPROVAL"
        elif state.status == JobStatus.PAUSED:
            dispatched = [r for r in self._await_roles(state) if r in state.children]
            if not dispatched:
                report["action"] = "PARENT_PAUSED_NOT_WAITING"
            else:
                waiting = [r for r in dispatched if r not in state.child_results]
                report["action"] = self._advance_waiting(state, waiting, run_children, report)
        elif state.status in (JobStatus.QUEUED, JobStatus.RUNNING):
            self.runner.execute(parent_id)
            report["actions"].append({"parent": "EXECUTED_TO_BOUNDARY"})
            after = self.queue.load(parent_id)
            if after.status == JobStatus.PAUSED and run_children:
                for role in self._await_roles(after):
                    child = after.children.get(role)
                    if child and role not in after.child_results:
                        report["actions"].append(self._run_child(child["child_job_id"]))
            report["action"] = "PARENT_ADVANCED"
        report["after"] = self.queue.load(parent_id).status.value
        report["phase"] = self.status(mission_id)["phase"]
        report["ledger_tail_sha"] = self.ledger.verify()["tail_sha"]
        return report

    def _advance_waiting(self, state: JobState, waiting: list[str],
                         run_children: bool, report: dict[str, Any]) -> str:
        for role in waiting:
            child_id = state.children[role]["child_job_id"]
            child = self.queue.load(child_id)
            if child.status in (JobStatus.QUEUED, JobStatus.RUNNING) and run_children:
                report["actions"].append(self._run_child(child_id))
                child = self.queue.load(child_id)
            if child.status == JobStatus.SUCCEEDED:
                report["actions"].append(self.ingest_child(
                    state.requested_by[len(MISSION_REQUESTED_BY_PREFIX):], role))
            elif child.status in (JobStatus.FAILED, JobStatus.CANCELLED):
                report["actions"].append({"child_job_id": child_id,
                                          "child_status": child.status.value})
                return "CHILD_FAILED_PARENT_STAYS_PAUSED"
            else:
                report["actions"].append({"child_job_id": child_id,
                                          "child_status": child.status.value})
                return "WAITING_ON_CHILD"
        # Every awaited child is ingested: resume the parent from the ledger.
        self.queue.resume(state.job_id)
        self.runner.execute(state.job_id)
        report["actions"].append({"parent": "JOB_RESUMED_AND_EXECUTED"})
        return "PARENT_RESUMED"

    def _run_child(self, child_id: str) -> dict[str, Any]:
        before = self.queue.load(child_id).status.value
        try:
            state = self.runner.execute(child_id)
            return {"child_job_id": child_id, "child_before": before,
                    "child_after": state.status.value}
        except Exception as exc:  # noqa: BLE001 — receipted by the runner
            return {"child_job_id": child_id, "child_before": before,
                    "child_after": self.queue.load(child_id).status.value,
                    "error": type(exc).__name__}

    def run_until_settled(self, mission_id: str, *, max_ticks: int = 8) -> list[dict[str, Any]]:
        reports = []
        for _ in range(max_ticks):
            report = self.tick(mission_id)
            reports.append(report)
            if report["action"] in ("PARENT_TERMINAL", "PARENT_BLOCKED_ON_APPROVAL",
                                    "CHILD_FAILED_PARENT_STAYS_PAUSED",
                                    "PARENT_PAUSED_NOT_WAITING"):
                break
        return reports

    # -- projection -------------------------------------------------------- #

    def status(self, mission_id: str) -> dict[str, Any]:
        parent_id = self.parent_job_id(mission_id)
        state = self.queue.load(parent_id)
        waiting = [r for r in self._await_roles(state)
                   if r in state.children and r not in state.child_results]
        if state.status == JobStatus.SUCCEEDED:
            phase = PHASE_READY_FOR_REVIEW
        elif state.status == JobStatus.FAILED:
            phase = PHASE_FAILED
        elif state.status == JobStatus.CANCELLED:
            phase = PHASE_CANCELLED
        elif state.status == JobStatus.BLOCKED_ON_APPROVAL:
            phase = PHASE_BLOCKED_ON_APPROVAL
        elif state.status == JobStatus.PAUSED:
            phase = PHASE_WAITING_ON_CHILD if waiting else PHASE_PAUSED
        elif state.status == JobStatus.RUNNING:
            phase = PHASE_EXECUTING
        else:
            phase = PHASE_RUNNABLE
        children = {}
        for role, rec in state.children.items():
            child = self._safe_load(rec["child_job_id"])
            children[role] = {
                "child_mission_id": rec["child_mission_id"],
                "child_job_id": rec["child_job_id"],
                "status": child.status.value if child else "ABSENT",
                "ingested": role in state.child_results,
            }
        return {
            "mission_id": mission_id,
            "parent_job_id": parent_id,
            "parent_workflow": state.workflow,
            "parent_status": state.status.value,
            "phase": phase,
            "waiting_on": waiting,
            "children": children,
            "review_packet": state.step_results.get("review_packet"),
            "ledger": self.ledger.verify(),
        }
