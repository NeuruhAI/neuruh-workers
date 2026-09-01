"""Thin consumers for organs this PR must not rebuild.

Founder Runtime may import or probe existing seams. It may not vendor
Switchboard, Git Custodian, IAR, Revenue Factory, Governance, Sentinel,
or AXON. Missing organs fail closed. Callers never set approved=true.
"""
from __future__ import annotations

import importlib
import json
import os
import subprocess
import urllib.error
import urllib.request
from typing import Any, Callable, Mapping, Sequence

from .contracts import JobStatus, ReceiptType, content_sha
from .queue import JobQueue
from .runner import WorkflowRunner

IAR_HEALTH_HOST = "127.0.0.1"
IAR_HEALTH_PORT = 8090
IAR_HEALTH_PATHS = ("/health", "/v1/health")

RF_IMPORT_PATHS = (
    "revenue_factory.authorization",
    "revenue_factory",
)
GOVERNANCE_IMPORT_PATHS = (
    "autonomous_governance_core",
    "governance_core",
    "neuruh_governance",
)
SENTINEL_IMPORT_PATHS = (
    "neuruh_sentinel",
    "sentinel",
)
AXON_IMPORT_PATHS = (
    "neuruh_axon",
    "axon",
)
SWITCHBOARD_MODULE = "neuruh_workers.switchboard"
EVENTS_MODULE = "neuruh_workers.events"
GIT_CUSTODIAN_MODULE = "neuruh_workers.git_custodian"

GSA_AUTHORITY_PATH = "governance->sentinel->axon"
PARENT_REQUESTED_BY_PREFIX = "jgi:"


def try_import(module_name: str) -> Any | None:
    try:
        return importlib.import_module(module_name)
    except ImportError:
        return None


def _looks_head(value: Any) -> bool:
    return (
        isinstance(value, str)
        and len(value) == 40
        and all(c in "0123456789abcdef" for c in value.lower())
    )


def resolve_source_head(explicit: str | None = None) -> str:
    if explicit and _looks_head(explicit):
        return explicit.lower()
    try:
        out = subprocess.check_output(
            ["git", "rev-parse", "HEAD"],
            text=True,
            stderr=subprocess.DEVNULL,
        ).strip().lower()
        if _looks_head(out):
            return out
    except (OSError, subprocess.CalledProcessError):
        pass
    return "0" * 40


def default_iar_health_probe(timeout_s: float = 0.25) -> dict[str, Any]:
    """Loopback IAR health JSON only. Not a model router or provider client."""
    last_reason = "unreachable"
    for path in IAR_HEALTH_PATHS:
        url = f"http://{IAR_HEALTH_HOST}:{IAR_HEALTH_PORT}{path}"
        try:
            request = urllib.request.Request(url, method="GET")
            with urllib.request.urlopen(request, timeout=timeout_s) as resp:
                raw = resp.read().decode("utf-8")
            data = json.loads(raw)
            if not isinstance(data, dict):
                return {
                    "present": False,
                    "url": url,
                    "reason": "health_not_json_object",
                    "authorized": False,
                }
            allowed = ("status", "ok", "healthy", "service", "name", "version")
            return {
                "present": True,
                "url": url,
                "health": {key: data[key] for key in allowed if key in data},
                "authorized": False,
            }
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError, OSError) as exc:
            last_reason = type(exc).__name__
            continue
    return {
        "present": False,
        "url": f"http://{IAR_HEALTH_HOST}:{IAR_HEALTH_PORT}/health",
        "reason": last_reason,
        "authorized": False,
    }


def import_inference_authorization() -> dict[str, Any]:
    for name in RF_IMPORT_PATHS:
        module = try_import(name)
        if module is None:
            continue
        symbol = getattr(module, "InferenceAuthorization", None)
        if symbol is not None:
            return {
                "present": True,
                "module": name,
                "symbol": "InferenceAuthorization",
                "authorized": False,
            }
    return {
        "present": False,
        "module": "revenue_factory/authorization.py",
        "symbol": "InferenceAuthorization",
        "authorized": False,
        "gap": "Revenue Factory InferenceAuthorization is not importable on this branch",
    }


def bind_iar_rf(
    *,
    context_digest: str,
    health_probe: Callable[[], Mapping[str, Any]] | None = None,
) -> dict[str, Any]:
    """Name and probe the existing IAR/RF contract. Never issue or consume auth."""
    iar = dict((health_probe or default_iar_health_probe)())
    rf = import_inference_authorization()
    organs_present = bool(iar.get("present")) and bool(rf.get("present"))
    return {
        "owner": "NeuruhAI/neuruh-live-integration#1",
        "module": rf["module"],
        "symbol": rf["symbol"],
        "consumption": "ConsumptionLedger.consume",
        "iar_owner": "NeuruhAI/aegis",
        "iar_path": "iar_v2",
        "iar_contract": f"http://{IAR_HEALTH_HOST}:{IAR_HEALTH_PORT}/health",
        "iar_health": iar,
        "rf_present": bool(rf.get("present")),
        "organs_present": organs_present,
        "organ_absent": not organs_present,
        "invariant": "NO CLOUD MODEL CALL WITHOUT A CONSUMED, MATCHING AUTHORIZATION",
        "budget_rule": "B=min(Bmax, EV*U*R) owned by IAR/RF, not this ranker",
        "authorized": False,
        "model_call": False,
        "required_authorization_id": None,
        "context_digest": context_digest,
        "gap": None if organs_present else (
            "IAR loopback health and/or Revenue Factory InferenceAuthorization absent; "
            "L2/L3/L4 fail closed"
        ),
    }


def _first_import(paths: Sequence[str]) -> tuple[str | None, Any | None]:
    for name in paths:
        module = try_import(name)
        if module is not None:
            return name, module
    return None, None


def try_governance_evaluate(action: Mapping[str, Any]) -> dict[str, Any]:
    """Consume Governance evaluate+consume if present. Caller never approves."""
    name, module = _first_import(GOVERNANCE_IMPORT_PATHS)
    bind: dict[str, Any] = {
        "owner": "NeuruhAI/autonomous-governance-core",
        "present": module is not None,
        "module": name,
        "caller_approved": False,
        "authorized": False,
        "consumed": False,
    }
    if module is None:
        bind["gap"] = "autonomous-governance-core is not importable on this branch"
        return bind
    evaluate = getattr(module, "evaluate", None)
    consume = getattr(module, "consume", None)
    if not callable(evaluate):
        bind["gap"] = "governance module present but evaluate() is missing"
        return bind
    try:
        verdict = evaluate(dict(action))
    except Exception as exc:  # noqa: BLE001 — fail closed; do not approve
        bind["gap"] = f"governance.evaluate refused ({type(exc).__name__})"
        return bind
    bind["evaluate_result"] = verdict if isinstance(verdict, (str, int, bool)) else type(verdict).__name__
    allowed = verdict in {"ALLOW", "ALLOWED", True} or (
        isinstance(verdict, Mapping) and verdict.get("decision") in {"ALLOW", "ALLOWED"}
    )
    if allowed and callable(consume):
        try:
            consume(dict(action))
            bind["consumed"] = True
        except Exception as exc:  # noqa: BLE001 — fail closed
            bind["gap"] = f"governance.consume refused ({type(exc).__name__})"
            return bind
    # Even after a foreign ALLOW, this caller still does not set approved=true.
    bind["organ_allow"] = bool(allowed)
    bind["authorized"] = False
    return bind


def try_sentinel_bind() -> dict[str, Any]:
    name, module = _first_import(SENTINEL_IMPORT_PATHS)
    switchboard = try_import(SWITCHBOARD_MODULE)
    return {
        "owner": "Sentinel independent court organ",
        "present": module is not None,
        "module": name,
        "ci_job_named_court_is_not_sentinel": True,
        "switchboard_court_job_seam": switchboard is not None,
        "verdict": None,
        "authorized": False,
        "gap": None if module is not None else (
            "Sentinel court organ is not importable; CI unittest job is not Sentinel"
        ),
    }


def try_axon_bind() -> dict[str, Any]:
    name, module = _first_import(AXON_IMPORT_PATHS)
    tool_registry = getattr(module, "ToolRegistry", None) if module is not None else None
    return {
        "owner": "NeuruhAI/neuruh-axon",
        "present": module is not None,
        "module": name,
        "tool_registry": tool_registry is not None,
        "activated": False,
        "authorized": False,
        "executed": False,
        "gap": None if module is not None else (
            "neuruh-axon is not importable; OrganRegistrationSeam.activate remains refused in v1"
        ),
    }


def gsa_bind(*, reasons: tuple[str, ...]) -> dict[str, Any]:
    """Governance → Sentinel → AXON only. Missing organs fail closed."""
    action = {
        "kind": "consequential",
        "reasons": list(reasons),
        "path": GSA_AUTHORITY_PATH,
    }
    governance = try_governance_evaluate(action)
    sentinel = try_sentinel_bind()
    axon = try_axon_bind()
    organs_present = bool(
        governance.get("present") and sentinel.get("present") and axon.get("present")
    )
    organ_allow = bool(governance.get("organ_allow")) and sentinel.get("verdict") == "PASS"
    # Execution requires all three live organs plus their own allow. This
    # package never supplies approved=true and never activates AXON itself.
    may_execute = (
        organs_present
        and organ_allow
        and bool(governance.get("consumed"))
        and bool(axon.get("activated"))
        and governance.get("caller_approved") is False
    )
    return {
        "path": GSA_AUTHORITY_PATH,
        "governance": governance,
        "sentinel": sentinel,
        "axon": axon,
        "policy_gate": "NeuruhAI/neuruh-policy-gate",
        "reasons": list(reasons),
        "organs_present": organs_present,
        "authorized": False,
        "executed": False,
        "enqueued": False,
        "caller_approved": False,
        "may_execute": bool(may_execute),
        "gap": None if organs_present else (
            "Governance, Sentinel, and/or AXON absent; consequential work not enqueued"
        ),
    }


def honey_pot_bind() -> dict[str, Any]:
    module = try_import(GIT_CUSTODIAN_MODULE)
    return {
        "owner": "NeuruhAI/neuruh-workers#4 neuruh_workers.git_custodian",
        "module": GIT_CUSTODIAN_MODULE,
        "present": module is not None,
        "role": "CONSUME",
        "custody": "HONEY_POT",
        "this_pr_copies_custodian": False,
        "bind_status": (
            "GIT_CUSTODIAN_CONSUMED" if module is not None
            else "GIT_CUSTODIAN_ABSENT_FAIL_CLOSED"
        ),
        "gap": None if module is not None else (
            "git_custodian lives on sibling PR #4; this PR does not vendor a second custodian"
        ),
    }


def switchboard_present() -> bool:
    return try_import(SWITCHBOARD_MODULE) is not None


def _prime_receipt_text(
    *,
    mission_id: str,
    job_id: str,
    ledger_tail_sha: str,
    context_digest: str,
    evidence_refs: Sequence[str],
    job_succeeded_sha: str | None,
    source_head: str,
) -> str:
    refs = ",".join(evidence_refs)
    lines = [
        f"MISSION_ID={mission_id}",
        "STAGE_ID=jgi-founder-runtime",
        f"TASK_ID={job_id}",
        f"SOURCE_HEAD={source_head}",
        "AUTHORITY_REFERENCE=jgi-founder-runtime-l0-readonly",
        "EVENT_TARGET=receipt.sealed",
        "EVENT_IMPLEMENTATION_STATUS=EMITTED",
        "REPO=NeuruhAI/neuruh-workers",
        "BRANCH=feature/jgi-founder-runtime-v0-20260901",
        f"LEDGER_TAIL_SHA={ledger_tail_sha}",
        f"JOB_ID={job_id}",
        f"CONTEXT_DIGEST={context_digest}",
        f"EVIDENCE_REFS={refs}",
        f"JOB_SUCCEEDED_SHA={job_succeeded_sha or ''}",
        "SENTINEL_MODEL_EXECUTION=BLOCKED",
        "SENTINEL_CLOSURE=NOT_JUDGED",
        "PRIME_VERDICT=NOT_JUDGED",
        "NETWORK_POLICY=loopback_only",
        "SOURCE_ROLE=JGI_FOUNDER_RUNTIME",
        "SOURCE_COMPONENT=founder-runtime",
        "CLAIMER=JGI_FOUNDER_RUNTIME",
    ]
    return "\n".join(lines) + "\n"


def emit_receipt_sealed_and_court_request(
    *,
    mission_id: str,
    job_id: str,
    ledger_tail_sha: str,
    context_digest: str,
    evidence_refs: Sequence[str],
    job_succeeded_sha: str | None,
    source_head: str | None = None,
    receipts_dir: str | None = None,
    switchboard_root: str | None = None,
) -> dict[str, Any]:
    """Emit receipt.sealed + COURT_REQUEST onto the SWITCHAROO seam.

    If ``neuruh_workers.switchboard`` is on this branch, tick it. If it exists
    only on sibling PR #3, keep the packet and optional PRIME observe file and
    fail closed rather than vendoring a fork. This is not a new event bus.
    """
    head = resolve_source_head(source_head)
    evidence = list(evidence_refs)
    receipt_ref = ""
    if receipts_dir:
        os.makedirs(receipts_dir, exist_ok=True)
        receipt_ref = os.path.abspath(os.path.join(receipts_dir, f"{job_id}.txt"))
        with open(receipt_ref, "w", encoding="utf-8") as fh:
            fh.write(_prime_receipt_text(
                mission_id=mission_id,
                job_id=job_id,
                ledger_tail_sha=ledger_tail_sha,
                context_digest=context_digest,
                evidence_refs=evidence,
                job_succeeded_sha=job_succeeded_sha,
                source_head=head,
            ))

    idempotency = f"receipt.sealed|{job_id}|{ledger_tail_sha}"
    sealed = {
        "event_type": "receipt.sealed",
        "mission_id": mission_id,
        "job_id": job_id,
        "ledger_tail_sha": ledger_tail_sha,
        "context_digest": context_digest,
        "evidence_refs": evidence,
        "job_succeeded_sha": job_succeeded_sha,
        "receipt_ref": receipt_ref or None,
        "source_head": head,
        "source_role": "JGI_FOUNDER_RUNTIME",
        "source_component": "founder-runtime",
        "idempotency_key": idempotency,
        "event_id": "evt-" + content_sha({
            "event_type": "receipt.sealed",
            "idempotency_key": idempotency,
        })[:24],
        "sentinel_verdict": None,
        "sentinel_model_execution": "BLOCKED",
    }
    court = {
        "event_type": "court.requested",
        "mission_id": mission_id,
        "job_id": job_id,
        "ledger_tail_sha": ledger_tail_sha,
        "context_digest": context_digest,
        "evidence_refs": evidence,
        "job_succeeded_sha": job_succeeded_sha,
        "source_head": head,
        "sentinel_model_execution": "BLOCKED",
        "verdict": None,
        "causation_id": sealed["event_id"],
        "idempotency_key": f"court.requested|{job_id}|{ledger_tail_sha}",
    }
    court["event_id"] = "evt-" + content_sha({
        "event_type": "court.requested",
        "idempotency_key": court["idempotency_key"],
        "causation_id": sealed["event_id"],
    })[:24]

    sb_mod = try_import(SWITCHBOARD_MODULE)
    events_mod = try_import(EVENTS_MODULE)
    honey = honey_pot_bind()
    bind_status = "SWITCHBOARD_ABSENT_FAIL_CLOSED"
    consumed = False
    if sb_mod is not None and switchboard_root and receipts_dir:
        switchboard_cls = getattr(sb_mod, "Switchboard", None)
        if switchboard_cls is not None:
            board = switchboard_cls(
                switchboard_root,
                receipts_dir=receipts_dir,
                known_missions=frozenset({mission_id}),
            )
            board.tick()
            bind_status = "SWITCHBOARD_CONSUMED"
            consumed = True
    elif sb_mod is not None:
        bind_status = "SWITCHBOARD_PRESENT_UNWIRED_FAIL_CLOSED"
    elif events_mod is not None:
        bind_status = "EVENTS_PRESENT_SWITCHBOARD_ABSENT_FAIL_CLOSED"

    sealed["switchboard_module"] = SWITCHBOARD_MODULE
    sealed["switchboard_present"] = sb_mod is not None
    sealed["events_module_present"] = events_mod is not None
    sealed["bind_status"] = bind_status
    sealed["consumed"] = consumed
    sealed["honey_pot"] = honey
    court["bind_status"] = bind_status
    court["switchboard_present"] = sb_mod is not None
    return {"sealed": sealed, "court_request": court}


def job_succeeded_sha(queue: JobQueue, job_id: str) -> str | None:
    for receipt in queue.ledger.receipts(job_id):
        if receipt.receipt_type == ReceiptType.JOB_SUCCEEDED:
            return receipt.receipt_sha
    return None


def find_jobs_for_mission(queue: JobQueue, mission_id: str) -> list[str]:
    wanted = f"{PARENT_REQUESTED_BY_PREFIX}{mission_id}"
    found: list[str] = []
    for job_id in queue.job_ids():
        try:
            state = queue.load(job_id)
        except Exception:  # noqa: BLE001 — skip non-job ledger rows
            continue
        if state.requested_by == wanted:
            found.append(job_id)
    return found


def resume_parent_from_receipts(
    queue: JobQueue,
    runner: WorkflowRunner,
    *,
    parent_mission_id: str,
    child_job_id: str | None,
) -> dict[str, Any]:
    """Resume existing parent jobs from the ledger. No new recovery daemon."""
    parent_ids = [
        job_id for job_id in find_jobs_for_mission(queue, parent_mission_id)
        if job_id != child_job_id
    ]
    if not parent_ids:
        return {
            "parent_mission_id": parent_mission_id,
            "automatic": False,
            "status": "PARENT_JOB_ABSENT_IN_LEDGER",
            "jobs": [],
            "owner": "neuruh-workers main JobQueue.resume + WorkflowRunner job.recovered",
            "note": "parent_mission_id is wired; no existing parent job on ExecutionLedger",
        }

    actions: list[dict[str, Any]] = []
    automatic = False
    for job_id in parent_ids:
        state = queue.load(job_id)
        action = {
            "job_id": job_id,
            "before": state.status.value,
            "action": "NONE",
        }
        try:
            if state.status == JobStatus.PAUSED:
                queue.resume(job_id)
                runner.execute(job_id)
                action["action"] = "JOB_RESUMED"
                automatic = True
            elif state.status == JobStatus.RUNNING and state.open_steps:
                runner.execute(job_id)
                action["action"] = "JOB_RECOVERED"
                automatic = True
            elif state.status == JobStatus.QUEUED:
                runner.execute(job_id)
                action["action"] = "JOB_STARTED_FROM_LEDGER"
                automatic = True
            elif state.status == JobStatus.BLOCKED_ON_APPROVAL:
                action["action"] = "PARENT_BLOCKED_ON_APPROVAL"
            elif state.terminal:
                action["action"] = "ALREADY_TERMINAL"
            else:
                action["action"] = f"LEFT_{state.status.value}"
            action["after"] = queue.load(job_id).status.value
        except Exception as exc:  # noqa: BLE001 — child already receipted
            action["action"] = "RESUME_FAILED"
            action["error"] = type(exc).__name__
            action["after"] = queue.load(job_id).status.value
        actions.append(action)

    status = "PARENT_RESUMED_FROM_RECEIPTS" if automatic else "PARENT_NOT_RESUMABLE"
    return {
        "parent_mission_id": parent_mission_id,
        "automatic": automatic,
        "status": status,
        "jobs": actions,
        "owner": "neuruh-workers main JobQueue.resume + WorkflowRunner job.recovered",
        "note": "existing job-level resume/recovery; no new mission watcher",
    }
