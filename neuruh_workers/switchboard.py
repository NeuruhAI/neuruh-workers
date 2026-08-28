"""Switchboard V1: wrap ExecutionLedger + JobQueue so a Prime sealed receipt
becomes a Sentinel verification job without a human clipboard.

This is a ledger watcher + enqueue adapter, not an event bus, not Ether, not
an orchestrator, and not Autonomous Governance Core.  Organ seams are not
touched: ``OrganRegistrationSeam.activate()`` still refuses in its own lane.

Loopback-only.  The watcher polls (or is ticked) over a configurable ledger
path, holds an idempotent cursor, and is replay-safe: re-reading a sealed
receipt will not double-enqueue because job identity is deterministic and
Switchboard receipts record which source seq was already routed.

EVENT_IMPLEMENTATION_STATUS
---------------------------
Status is either PROVEN (asserted by tests in this repo) or TARGET_CONTRACT
(named so a later bus can carry it; not a live event bus in this wrap).

| Name                              | Status           | Role in V1                                      |
|-----------------------------------|------------------|-------------------------------------------------|
| receipt.sealed                    | TARGET_CONTRACT  | Bus name; V1 maps it to prime.receipt_sealed    |
| prime.receipt_sealed              | PROVEN           | Watch trigger: sealed Prime receipt             |
| sentinel-verify (workflow)        | PROVEN           | Job type enqueued onto the existing JobQueue    |
| sentinel.verification_pass        | PROVEN           | Watch trigger: Sentinel PASS for a receipt      |
| job.succeeded (sentinel-verify)   | PROVEN           | Existing ledger type treated as equivalent PASS |
| candidate.push.ready              | TARGET_CONTRACT  | Bus name; carried as git-custody job payload    |
| git-custody (workflow)            | PROVEN           | Payload-only job; no git push is implemented    |
| switchboard.job_created           | PROVEN           | Switchboard's own receipt: first enqueue        |
| switchboard.job_already_existed   | PROVEN           | Switchboard's own receipt: idempotent hit       |
| job.enqueued / enqueue_deduplicated | PROVEN (existing) | Queue receipts reused, not replaced          |
"""
from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from typing import Any, Literal, Mapping, Never

from .contracts import Receipt, ReceiptType, WorkflowError, canonical_json, deterministic_job_id
from .definitions import RetryPolicy, StepDefinition, WorkflowCatalog, WorkflowDefinition
from .ledger import ExecutionLedger
from .queue import JobQueue
from .registry import Capability, StepContext, WorkerDeclaration, WorkerRegistry

SWITCHBOARD_JOB_ID = "switchboard-v1"
SWITCHBOARD_REQUESTED_BY = "switchboard-v1"

SENTINEL_WORKFLOW = "sentinel-verify"
GIT_CUSTODY_WORKFLOW = "git-custody"
WORKFLOW_VERSION = "1.0.0"

ACTION_SENTINEL_VERIFY = "enqueue_sentinel_verify"
ACTION_GIT_CUSTODY = "enqueue_git_custody"

CONTRACT_RECEIPT_SEALED = "receipt.sealed"
CONTRACT_CANDIDATE_PUSH_READY = "candidate.push.ready"

_SHA256_HEX_LEN = 64
_RETRY = RetryPolicy(max_attempts=1, base_delay_s=0.0)

EVENT_IMPLEMENTATION_STATUS: dict[str, dict[str, str]] = {
    "receipt.sealed": {
        "status": "TARGET_CONTRACT",
        "role": "Named bus event. V1 watches prime.receipt_sealed instead.",
    },
    "prime.receipt_sealed": {
        "status": "PROVEN",
        "role": "V1 ledger receipt type that triggers a sentinel-verify job.",
    },
    "sentinel-verify": {
        "status": "PROVEN",
        "role": "Workflow/job type enqueued on the existing JobQueue.",
    },
    "sentinel.verification_pass": {
        "status": "PROVEN",
        "role": "V1 ledger receipt type that triggers a git-custody job.",
    },
    "job.succeeded": {
        "status": "PROVEN",
        "role": "Existing type; a succeeded sentinel-verify job is a PASS.",
    },
    "candidate.push.ready": {
        "status": "TARGET_CONTRACT",
        "role": "Named bus event. V1 carries it on the git-custody payload.",
    },
    "git-custody": {
        "status": "PROVEN",
        "role": "Workflow/job type. Payload only; Git Custodian is another lane.",
    },
    "switchboard.job_created": {
        "status": "PROVEN",
        "role": "Switchboard receipt written when a job is first enqueued.",
    },
    "switchboard.job_already_existed": {
        "status": "PROVEN",
        "role": "Switchboard receipt written when enqueue is a deduplicated hit.",
    },
}


class SwitchboardRefused(WorkflowError):
    code = "SWITCHBOARD_REFUSED"


class SwitchboardPathRefused(SwitchboardRefused):
    code = "SWITCHBOARD_PATH_REFUSED"


class SwitchboardCursorCorrupt(SwitchboardRefused):
    code = "SWITCHBOARD_CURSOR_CORRUPT"


@dataclass
class TickReport:
    """Facts from one poll of the ledger.  Never a command to other organs."""
    scanned: int = 0
    ignored: int = 0
    cursor_seq: int = 0
    sentinel_jobs_created: list[str] = field(default_factory=list)
    sentinel_jobs_existing: list[str] = field(default_factory=list)
    git_custody_jobs_created: list[str] = field(default_factory=list)
    git_custody_jobs_existing: list[str] = field(default_factory=list)


def _is_under_root(path: str, root: str) -> bool:
    real_path = os.path.realpath(path)
    real_root = os.path.realpath(root)
    try:
        common = os.path.commonpath([real_path, real_root])
    except ValueError:
        return False
    return common == real_root


def _require_sha256_hex(value: Any) -> str | None:
    if not isinstance(value, str) or len(value) != _SHA256_HEX_LEN:
        return None
    if any(c not in "0123456789abcdef" for c in value):
        return None
    return value


def _require_nonempty_str(value: Any) -> str | None:
    if not isinstance(value, str) or not value.strip():
        return None
    return value


def sentinel_verify_params(receipt_id: str, content_sha: str) -> dict[str, Any]:
    """Canonical params for the sentinel-verify job (also the dedupe seed)."""
    return {
        "receipt_id": receipt_id,
        "content_sha": content_sha,
        "contract": CONTRACT_RECEIPT_SEALED,
    }


def git_custody_params(receipt_id: str, content_sha: str,
                       sentinel_job_id: str) -> dict[str, Any]:
    """Canonical params for the git-custody job.  git_push is always false."""
    return {
        "receipt_id": receipt_id,
        "content_sha": content_sha,
        "sentinel_job_id": sentinel_job_id,
        "contract": CONTRACT_CANDIDATE_PUSH_READY,
        "git_push": False,
        "custody": "payload_only",
    }


def _plan_sentinel(ctx: StepContext, params: Mapping[str, Any]) -> dict:
    return {"plan": "would verify sealed receipt in loopback",
            "params": dict(params)}


def _execute_sentinel(ctx: StepContext, params: Mapping[str, Any]) -> dict:
    """Loopback schema check only.  Live Sentinel is a separate lane."""
    receipt_id = _require_nonempty_str(params.get("receipt_id"))
    content_sha = _require_sha256_hex(params.get("content_sha"))
    if receipt_id is None or content_sha is None:
        raise ValueError("sentinel-verify params are not a sealed receipt")
    return {
        "result": "PASS",
        "receipt_id": receipt_id,
        "content_sha": content_sha,
        "note": "loopback schema verification only; live Sentinel is a separate lane",
    }


def _plan_git_custody(ctx: StepContext, params: Mapping[str, Any]) -> dict:
    return {"plan": "would record git-custody payload; no push",
            "params": dict(params)}


def _execute_git_custody(ctx: StepContext, params: Mapping[str, Any]) -> dict:
    """Record the custody payload.  Never invokes git.  Never pushes."""
    if params.get("git_push") is True:
        raise ValueError("git_push is not authorized in switchboard v1")
    receipt_id = _require_nonempty_str(params.get("receipt_id"))
    if receipt_id is None:
        raise ValueError("git-custody params require receipt_id")
    return {
        "status": "PAYLOAD_RECORDED",
        "git_push": False,
        "contract": CONTRACT_CANDIDATE_PUSH_READY,
        "receipt_id": receipt_id,
        "sentinel_job_id": str(params.get("sentinel_job_id") or ""),
        "note": "Git Custodian is a separate repo/lane; no push attempted",
    }


def build_switchboard_registry() -> WorkerRegistry:
    registry = WorkerRegistry()
    registry.register_worker(WorkerDeclaration(
        worker_id="switchboard-loopback-worker-v1",
        description="Loopback-only Switchboard V1 capabilities.  No network, "
                    "no git, no organ activation.",
        capabilities=(
            Capability(
                name="switchboard.sentinel_verify",
                description="Loopback schema verification of a sealed receipt",
                read_only=True, external=False,
                execute=_execute_sentinel, plan=_plan_sentinel,
            ),
            Capability(
                name="switchboard.git_custody_payload",
                description="Record a git-custody payload; never push",
                read_only=True, external=False,
                execute=_execute_git_custody, plan=_plan_git_custody,
            ),
        ),
    ))
    return registry


def build_switchboard_catalog() -> WorkflowCatalog:
    catalog = WorkflowCatalog()
    catalog.register(WorkflowDefinition(
        name=SENTINEL_WORKFLOW, version=WORKFLOW_VERSION,
        description="Sentinel verification job for a Prime sealed receipt. "
                    "Loopback schema check only; live Sentinel is another lane.",
        read_only=True,
        steps=(StepDefinition(
            step_id="verify_sealed_receipt",
            capability="switchboard.sentinel_verify",
            retry=_RETRY, timeout_s=10.0,
        ),),
    ))
    catalog.register(WorkflowDefinition(
        name=GIT_CUSTODY_WORKFLOW, version=WORKFLOW_VERSION,
        description="Git custody payload for a Sentinel PASS.  Does not push. "
                    "Git Custodian is a separate repo/lane.",
        read_only=True,
        steps=(StepDefinition(
            step_id="record_custody_payload",
            capability="switchboard.git_custody_payload",
            retry=_RETRY, timeout_s=10.0,
        ),),
    ))
    return catalog


SwitchboardKind = Literal["prime_sealed", "sentinel_pass", "job_succeeded", "ignore"]


def _classify(receipt: Receipt) -> SwitchboardKind:
    """Closed set of watcher kinds.  Anything else is ignore, not a bus event."""
    rt = receipt.receipt_type
    if rt == ReceiptType.PRIME_RECEIPT_SEALED:
        return "prime_sealed"
    if rt == ReceiptType.SENTINEL_VERIFICATION_PASS:
        return "sentinel_pass"
    if rt == ReceiptType.JOB_SUCCEEDED:
        return "job_succeeded"
    return "ignore"


def _never(value: Never) -> None:
    raise SwitchboardRefused(
        f"unknown switchboard kind {value!r}", {"kind": value})


class Switchboard:
    """Poll the execution ledger and enqueue Sentinel / git-custody jobs.

    Construction refuses unless ``loopback=True`` and both the ledger path and
    cursor path resolve under ``allowed_root``.  The existing
    ``ExecutionLedger`` is reused (verified, append-only, fail-closed).  The
    existing ``JobQueue.enqueue`` is reused (deterministic ids, dedupe).
    """

    def __init__(self, ledger: ExecutionLedger, *, ledger_path: str,
                 allowed_root: str, cursor_path: str | None = None,
                 loopback: bool = True):
        if loopback is not True:
            raise SwitchboardRefused(
                "switchboard v1 is loopback-only; refusing non-loopback mode",
                {"loopback": loopback})
        if not allowed_root:
            raise SwitchboardPathRefused(
                "allowed_root is required", {"allowed_root": allowed_root})
        if not os.path.isdir(os.path.realpath(allowed_root)):
            raise SwitchboardPathRefused(
                "allowed_root must be an existing directory",
                {"allowed_root": allowed_root})

        self._ledger_path = os.path.realpath(ledger_path)
        self._allowed_root = os.path.realpath(allowed_root)
        self._assert_under_root(self._ledger_path, "ledger_path")

        cursor_path = cursor_path or (self._ledger_path + ".switchboard-cursor.json")
        # The cursor file may not exist yet; constrain its resolved parent.
        cursor_parent = os.path.dirname(os.path.abspath(cursor_path)) or os.curdir
        self._assert_under_root(os.path.realpath(cursor_parent), "cursor_path")
        # If the cursor already exists, the file itself must also stay inside.
        if os.path.exists(cursor_path):
            self._assert_under_root(os.path.realpath(cursor_path), "cursor_path")
        self._cursor_path = os.path.abspath(cursor_path)

        self._ledger = ledger
        self._registry = build_switchboard_registry()
        self._catalog = build_switchboard_catalog()
        self._queue = JobQueue(ledger, self._registry)
        self._cursor_seq = self._load_cursor()

    @property
    def queue(self) -> JobQueue:
        return self._queue

    @property
    def catalog(self) -> WorkflowCatalog:
        return self._catalog

    @property
    def registry(self) -> WorkerRegistry:
        return self._registry

    @property
    def cursor_seq(self) -> int:
        return self._cursor_seq

    @property
    def cursor_path(self) -> str:
        return self._cursor_path

    def _assert_under_root(self, path: str, label: str) -> None:
        if not _is_under_root(path, self._allowed_root):
            raise SwitchboardPathRefused(
                f"{label} is outside allowed_root",
                {"path": path, "allowed_root": self._allowed_root})

    def _load_cursor(self) -> int:
        if not os.path.exists(self._cursor_path):
            return 0
        try:
            with open(self._cursor_path, encoding="utf-8") as fh:
                data = json.load(fh)
        except (OSError, json.JSONDecodeError) as exc:
            raise SwitchboardCursorCorrupt(
                "switchboard cursor is not valid JSON",
                {"cursor_path": self._cursor_path}) from exc
        if not isinstance(data, dict):
            raise SwitchboardCursorCorrupt(
                "switchboard cursor must be an object",
                {"cursor_path": self._cursor_path})
        last_seq = data.get("last_seq")
        if not isinstance(last_seq, int) or isinstance(last_seq, bool) or last_seq < 0:
            raise SwitchboardCursorCorrupt(
                "switchboard cursor last_seq is invalid",
                {"cursor_path": self._cursor_path})
        recorded_path = data.get("ledger_path")
        if recorded_path and os.path.realpath(str(recorded_path)) != self._ledger_path:
            raise SwitchboardCursorCorrupt(
                "switchboard cursor belongs to a different ledger",
                {"cursor_path": self._cursor_path,
                 "recorded_ledger": recorded_path,
                 "ledger_path": self._ledger_path})
        return last_seq

    def _save_cursor(self, seq: int) -> None:
        payload = {
            "version": 1,
            "last_seq": seq,
            "ledger_path": self._ledger_path,
        }
        body = canonical_json(payload) + "\n"
        tmp = self._cursor_path + ".tmp"
        os.makedirs(os.path.dirname(self._cursor_path) or ".", exist_ok=True)
        with open(tmp, "w", encoding="utf-8") as fh:
            fh.write(body)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, self._cursor_path)
        self._cursor_seq = seq

    def tick(self) -> TickReport:
        """Process receipts newer than the cursor.  Replay-safe and idempotent."""
        report = TickReport(cursor_seq=self._cursor_seq)
        receipts = self._ledger.receipts()
        for receipt in receipts:
            if receipt.seq <= self._cursor_seq:
                continue
            report.scanned += 1
            self._route(receipt, report)
            self._save_cursor(receipt.seq)
        # Receipts we appended while routing (job.enqueued, switchboard.*)
        # are not new Prime/Sentinel signals; park the cursor at the tail so
        # a retick is a true no-op.
        tail = self._ledger.verify()["receipts"]
        if tail > self._cursor_seq:
            self._save_cursor(tail)
        report.cursor_seq = self._cursor_seq
        return report

    def _route(self, receipt: Receipt, report: TickReport) -> None:
        kind = _classify(receipt)
        if kind == "prime_sealed":
            self._on_prime_sealed(receipt, report)
        elif kind == "sentinel_pass":
            self._on_sentinel_pass(receipt, report)
        elif kind == "job_succeeded":
            self._on_job_succeeded(receipt, report)
        elif kind == "ignore":
            report.ignored += 1
        else:
            _never(kind)

    def _parse_prime_sealed(self, receipt: Receipt) -> tuple[str, str] | None:
        """Return (receipt_id, content_sha) or None if unsigned/malformed.

        A ledger record is always hash-chained; that is not enough.  V1 requires
        an explicit sealed Prime payload: sealed=true, receipt_id, content_sha.
        """
        payload = receipt.payload
        if not isinstance(payload, Mapping):
            return None
        if payload.get("sealed") is not True:
            return None
        receipt_id = _require_nonempty_str(payload.get("receipt_id"))
        content_sha = _require_sha256_hex(payload.get("content_sha"))
        if receipt_id is None or content_sha is None:
            return None
        contract = payload.get("contract")
        if contract is not None and contract != CONTRACT_RECEIPT_SEALED:
            return None
        return receipt_id, content_sha

    def _parse_sentinel_pass(self, receipt: Receipt) -> tuple[str, str, str] | None:
        """Return (receipt_id, content_sha, sentinel_job_id) or None.

        content_sha / sentinel_job_id may be recovered from a prior sentinel
        enqueue when the PASS record only names receipt_id.
        """
        payload = receipt.payload
        if not isinstance(payload, Mapping):
            return None
        result = payload.get("result")
        if result != "PASS":
            return None
        receipt_id = _require_nonempty_str(payload.get("receipt_id"))
        if receipt_id is None:
            return None
        content_sha = _require_sha256_hex(payload.get("content_sha")) or ""
        sentinel_job_id = _require_nonempty_str(payload.get("sentinel_job_id")) or ""
        if not content_sha or not sentinel_job_id:
            recovered = self._lookup_sentinel_job(receipt_id)
            if recovered is None:
                return None
            job_id, params = recovered
            content_sha = content_sha or params["content_sha"]
            sentinel_job_id = sentinel_job_id or job_id
        return receipt_id, content_sha, sentinel_job_id

    def _lookup_sentinel_job(self, receipt_id: str) -> tuple[str, dict[str, Any]] | None:
        for r in self._ledger.receipts():
            if r.receipt_type != ReceiptType.JOB_ENQUEUED:
                continue
            if r.payload.get("workflow") != SENTINEL_WORKFLOW:
                continue
            params = r.payload.get("params") or {}
            if params.get("receipt_id") == receipt_id:
                return r.job_id, dict(params)
        return None

    def _already_routed(self, source_seq: int, action: str) -> bool:
        for r in self._ledger.receipts(SWITCHBOARD_JOB_ID):
            if r.receipt_type not in (
                    ReceiptType.SWITCHBOARD_JOB_CREATED,
                    ReceiptType.SWITCHBOARD_JOB_ALREADY_EXISTED):
                continue
            if r.payload.get("source_seq") == source_seq and r.payload.get("action") == action:
                return True
        return False

    def _has_enqueued(self, job_id: str) -> bool:
        return any(
            r.receipt_type == ReceiptType.JOB_ENQUEUED
            for r in self._ledger.receipts(job_id))

    def _enqueue_and_receipt(
            self, *, workflow: str, params: Mapping[str, Any],
            dedupe_key: str, source: Receipt, action: str,
            extra_payload: Mapping[str, Any],
            created_bucket: list[str], existing_bucket: list[str]) -> None:
        if self._already_routed(source.seq, action):
            return
        definition = self._catalog.get(workflow)
        job_id = deterministic_job_id(
            definition.name, definition.version, params, dedupe_key,
            dry_run=False)
        had = self._has_enqueued(job_id)
        enqueued_id = self._queue.enqueue(
            definition, params, requested_by=SWITCHBOARD_REQUESTED_BY,
            dry_run=False, dedupe_key=dedupe_key)
        if enqueued_id != job_id:
            raise SwitchboardRefused(
                "queue returned a different job id than the deterministic seed",
                {"expected": job_id, "got": enqueued_id})
        kind = (ReceiptType.SWITCHBOARD_JOB_ALREADY_EXISTED if had
                else ReceiptType.SWITCHBOARD_JOB_CREATED)
        self._ledger.append(kind, SWITCHBOARD_JOB_ID, {
            "action": action,
            "source_seq": source.seq,
            "source_receipt_sha": source.receipt_sha,
            "source_receipt_type": source.receipt_type,
            "target_job_id": job_id,
            "target_workflow": workflow,
            **dict(extra_payload),
        })
        if had:
            existing_bucket.append(job_id)
        else:
            created_bucket.append(job_id)

    def _on_prime_sealed(self, receipt: Receipt, report: TickReport) -> None:
        parsed = self._parse_prime_sealed(receipt)
        if parsed is None:
            report.ignored += 1
            return
        receipt_id, content_sha = parsed
        params = sentinel_verify_params(receipt_id, content_sha)
        self._enqueue_and_receipt(
            workflow=SENTINEL_WORKFLOW,
            params=params,
            dedupe_key=f"sentinel-verify:{receipt_id}:{content_sha}",
            source=receipt,
            action=ACTION_SENTINEL_VERIFY,
            extra_payload={
                "contract": CONTRACT_RECEIPT_SEALED,
                "receipt_id": receipt_id,
                "content_sha": content_sha,
            },
            created_bucket=report.sentinel_jobs_created,
            existing_bucket=report.sentinel_jobs_existing,
        )

    def _on_sentinel_pass(self, receipt: Receipt, report: TickReport) -> None:
        parsed = self._parse_sentinel_pass(receipt)
        if parsed is None:
            report.ignored += 1
            return
        receipt_id, content_sha, sentinel_job_id = parsed
        self._enqueue_git_custody(
            source=receipt, report=report, receipt_id=receipt_id,
            content_sha=content_sha, sentinel_job_id=sentinel_job_id)

    def _on_job_succeeded(self, receipt: Receipt, report: TickReport) -> None:
        enqueued = None
        for r in self._ledger.receipts(receipt.job_id):
            if r.receipt_type == ReceiptType.JOB_ENQUEUED:
                enqueued = r
                break
        if enqueued is None or enqueued.payload.get("workflow") != SENTINEL_WORKFLOW:
            report.ignored += 1
            return
        if enqueued.payload.get("dry_run"):
            report.ignored += 1
            return
        params = dict(enqueued.payload.get("params") or {})
        receipt_id = _require_nonempty_str(params.get("receipt_id"))
        content_sha = _require_sha256_hex(params.get("content_sha"))
        if receipt_id is None or content_sha is None:
            report.ignored += 1
            return
        self._enqueue_git_custody(
            source=receipt, report=report, receipt_id=receipt_id,
            content_sha=content_sha, sentinel_job_id=receipt.job_id)

    def _enqueue_git_custody(
            self, *, source: Receipt, report: TickReport, receipt_id: str,
            content_sha: str, sentinel_job_id: str) -> None:
        params = git_custody_params(receipt_id, content_sha, sentinel_job_id)
        self._enqueue_and_receipt(
            workflow=GIT_CUSTODY_WORKFLOW,
            params=params,
            dedupe_key=f"git-custody:{receipt_id}:{content_sha}",
            source=source,
            action=ACTION_GIT_CUSTODY,
            extra_payload={
                "contract": CONTRACT_CANDIDATE_PUSH_READY,
                "receipt_id": receipt_id,
                "content_sha": content_sha,
                "sentinel_job_id": sentinel_job_id,
                "git_push": False,
            },
            created_bucket=report.git_custody_jobs_created,
            existing_bucket=report.git_custody_jobs_existing,
        )
