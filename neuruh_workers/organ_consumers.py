"""Thin consumers for organs this PR must not rebuild.

Calls existing seams. Does not vendor Switchboard, Git Custodian, IAR,
Revenue Factory, Governance, Sentinel, or AXON. Missing organs fail from
the attempted call, not from a pre-filled authorized=False struct.
Callers never set approved=true.
"""
from __future__ import annotations

import importlib
import json
import os
import subprocess
import urllib.error
import urllib.request
from datetime import datetime, timezone
from typing import Any, Callable, Mapping, Sequence

from .contracts import JobStatus, ReceiptType, canonical_json, content_sha
from .queue import JobQueue
from .runner import WorkflowRunner

IAR_HEALTH_HOST = "127.0.0.1"
IAR_HEALTH_PORT = 8090
IAR_HEALTH_PATHS = ("/health", "/v1/health")

# Existing organ call sites. Unreachable → error from the attempt.
GOVERNANCE_EVALUATE_URL = "http://127.0.0.1:8848/v1/evaluate"
GOVERNANCE_CONSUME_URL = "http://127.0.0.1:8848/v1/consume"
AXON_EXECUTE_URL = "http://127.0.0.1:8300/execute"
AXON_PROVEN_ROUTES = (
    "/health",
    "/execute",
    "/api/tasks/execute",
    "/api/tools/call",
    "/tools",
)

RF_IMPORT_PATHS = (
    "revenue_factory.authorization",
    "revenue_factory",
)
SWITCHBOARD_MODULE = "neuruh_workers.switchboard"
EVENTS_MODULE = "neuruh_workers.events"
GIT_CUSTODIAN_MODULE = "neuruh_workers.git_custodian"

GSA_AUTHORITY_PATH = "governance->sentinel->axon"
PARENT_REQUESTED_BY_PREFIX = "jgi:"

# PR #3 nrh.event.v1 contract (consume the shape; do not vendor events.py).
NRH_EVENT_V1 = "nrh.event.v1"
NRH_ENVELOPE_FIELDS = (
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
EVENT_RECEIPT_SEALED = "receipt.sealed"
EVENT_COURT_REQUESTED = "court.requested"
RECEIPT_SEALED_INVARIANTS = (
    "envelope.valid",
    "hash.matches_if_provided",
    "source_identity.coherent",
    "mission_stage.unambiguous",
    "authority.present",
    "loopback_only",
    "sentinel_closure_in_receipt_is_not_pass",
)

HttpOpen = Callable[..., Any]


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


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def http_post_json(
    url: str,
    payload: Mapping[str, Any],
    *,
    timeout_s: float = 0.25,
    opener: HttpOpen | None = None,
) -> dict[str, Any]:
    """Dial a loopback organ. Failure is the exception from the attempt."""
    body = canonical_json(dict(payload)).encode("utf-8")
    request = urllib.request.Request(
        url,
        data=body,
        method="POST",
        headers={"Content-Type": "application/json", "Accept": "application/json"},
    )
    open_fn = opener or urllib.request.urlopen
    try:
        with open_fn(request, timeout=timeout_s) as resp:
            raw = resp.read().decode("utf-8")
            status = int(getattr(resp, "status", 200))
            data = json.loads(raw) if raw else {}
            if not isinstance(data, dict):
                data = {}
            return {
                "attempted": True,
                "url": url,
                "ok": 200 <= status < 300,
                "status": status,
                "error": None,
                "decision": data.get("decision") or data.get("status") or data.get("verdict"),
            }
    except Exception as exc:  # noqa: BLE001 — fail closed from the attempt
        return {
            "attempted": True,
            "url": url,
            "ok": False,
            "status": None,
            "error": f"{type(exc).__name__}",
            "decision": None,
        }


def default_iar_health_probe(
    timeout_s: float = 0.25,
    opener: HttpOpen | None = None,
) -> dict[str, Any]:
    """Loopback IAR health JSON only. Not a model router or provider client."""
    last_reason = "unreachable"
    open_fn = opener or urllib.request.urlopen
    for path in IAR_HEALTH_PATHS:
        url = f"http://{IAR_HEALTH_HOST}:{IAR_HEALTH_PORT}{path}"
        try:
            request = urllib.request.Request(url, method="GET")
            with open_fn(request, timeout=timeout_s) as resp:
                raw = resp.read().decode("utf-8")
            data = json.loads(raw)
            if not isinstance(data, dict):
                return {
                    "present": False,
                    "url": url,
                    "reason": "health_not_json_object",
                    "authorized": False,
                    "attempted": True,
                }
            allowed = ("status", "ok", "healthy", "service", "name", "version")
            return {
                "present": True,
                "url": url,
                "health": {key: data[key] for key in allowed if key in data},
                "authorized": False,
                "attempted": True,
            }
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError, OSError) as exc:
            last_reason = type(exc).__name__
            continue
    return {
        "present": False,
        "url": f"http://{IAR_HEALTH_HOST}:{IAR_HEALTH_PORT}/health",
        "reason": last_reason,
        "authorized": False,
        "attempted": True,
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
    opener: HttpOpen | None = None,
) -> dict[str, Any]:
    """Call the IAR health contract. Never issue or consume authorization."""
    if health_probe is not None:
        iar = dict(health_probe())
    else:
        iar = default_iar_health_probe(opener=opener)
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
        "attempted": bool(iar.get("attempted", True)),
        "gap": None if organs_present else (
            "IAR loopback health and/or Revenue Factory InferenceAuthorization absent; "
            "L2/L3/L4 fail closed"
        ),
    }


def call_gsa(
    *,
    reasons: tuple[str, ...],
    opener: HttpOpen | None = None,
) -> dict[str, Any]:
    """CALL Governance → Sentinel → AXON. Fail from the attempt, never self-approve."""
    action = {
        "kind": "consequential",
        "reasons": list(reasons),
        "path": GSA_AUTHORITY_PATH,
        "caller_approved": False,
    }
    calls: list[dict[str, Any]] = []

    evaluate = http_post_json(GOVERNANCE_EVALUATE_URL, action, opener=opener)
    evaluate["organ"] = "governance"
    evaluate["method"] = "evaluate"
    calls.append(evaluate)

    consume = {
        "organ": "governance",
        "method": "consume",
        "attempted": False,
        "url": GOVERNANCE_CONSUME_URL,
        "ok": False,
        "error": "skipped_evaluate_did_not_allow",
        "decision": None,
    }
    organ_allow = bool(evaluate.get("ok") and evaluate.get("decision") in {"ALLOW", "ALLOWED", "PASS"})
    if organ_allow:
        consume = http_post_json(GOVERNANCE_CONSUME_URL, action, opener=opener)
        consume["organ"] = "governance"
        consume["method"] = "consume"
    calls.append(consume)

    # Sentinel is an independent agent jobbed by SWITCHAROO after
    # receipt.sealed → court.requested. Do not invent a Sentinel HTTP stub.
    switchboard = try_import(SWITCHBOARD_MODULE)
    if switchboard is None:
        sentinel = {
            "organ": "sentinel",
            "method": "switchboard_court_job",
            "attempted": True,
            "url": None,
            "ok": False,
            "status": None,
            "error": "SWITCHBOARD_ABSENT",
            "decision": None,
            "bind_status": "SWITCHBOARD_ABSENT",
        }
    else:
        sentinel = {
            "organ": "sentinel",
            "method": "switchboard_court_job",
            "attempted": True,
            "url": None,
            "ok": False,
            "status": None,
            "error": "NO_RECEIPT_SEALED_FOR_CONSEQUENTIAL",
            "decision": None,
            "bind_status": "SWITCHBOARD_PRESENT_NO_SEALED_RECEIPT",
        }
    calls.append(sentinel)

    axon = http_post_json(
        AXON_EXECUTE_URL,
        {"action": action, "after": "governance.evaluate", "route": "/execute"},
        opener=opener,
    )
    axon["organ"] = "axon"
    axon["method"] = "execute"
    axon["proven_routes"] = list(AXON_PROVEN_ROUTES)
    calls.append(axon)

    may_execute = (
        organ_allow
        and bool(consume.get("ok"))
        and sentinel.get("decision") == "PASS"
        and bool(axon.get("ok"))
    )
    errors = [c.get("error") for c in calls if c.get("attempted") and c.get("error")]
    return {
        "path": GSA_AUTHORITY_PATH,
        "calls": calls,
        "governance": {
            "owner": "NeuruhAI/autonomous-governance-core",
            "evaluate": evaluate,
            "consume": consume,
            "caller_approved": False,
            "authorized": False,
        },
        "sentinel": {
            "owner": "Sentinel independent court organ",
            "court": sentinel,
            "ci_job_named_court_is_not_sentinel": True,
            "verdict": sentinel.get("decision"),
            "authorized": False,
            "bind_status": sentinel.get("bind_status"),
        },
        "axon": {
            "owner": "NeuruhAI/neuruh-axon",
            "execute": axon,
            "activated": False,
            "authorized": False,
            "executed": False,
        },
        "reasons": list(reasons),
        "authorized": False,
        "executed": False,
        "enqueued": False,
        "caller_approved": False,
        "may_execute": bool(may_execute),
        "attempted": True,
        "error": errors[0] if errors else None,
        "gap": None if may_execute else (
            "GSA call failed closed from the attempt; consequential work not enqueued"
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


def event_id_for(*, event_type: str, idempotency_key: str, causation_id: str = "") -> str:
    return "evt-" + content_sha({
        "event_type": event_type,
        "idempotency_key": idempotency_key,
        "causation_id": causation_id,
    })[:24]


def build_nrh_event_v1(
    *,
    event_type: str,
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
    payload: Mapping[str, Any],
    idempotency_key: str,
    attempt: int = 1,
    max_attempts: int = 3,
    ack_state: str = "PENDING",
) -> dict[str, Any]:
    """Consumer of PR #3 EventType envelope. Field set must match nrh.event.v1."""
    body = {
        "schema_version": NRH_EVENT_V1,
        "event_id": event_id_for(
            event_type=event_type,
            idempotency_key=idempotency_key,
            causation_id=causation_id,
        ),
        "event_type": event_type,
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
        "payload_hash": content_sha(dict(payload)),
        "payload_ref_or_payload": dict(payload),
        "idempotency_key": idempotency_key,
        "attempt": attempt,
        "max_attempts": max_attempts,
        "ack_state": ack_state,
    }
    if tuple(body) != NRH_ENVELOPE_FIELDS:
        raise ValueError("nrh.event.v1 field set drifted from PR #3 contract")
    if body["payload_hash"] != content_sha(body["payload_ref_or_payload"]):
        raise ValueError("payload_hash does not match payload")
    return body


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


def attempt_switchboard_emit(
    envelope: Mapping[str, Any],
    *,
    receipts_dir: str | None = None,
    switchboard_root: str | None = None,
) -> dict[str, Any]:
    """Attempt to hand the envelope to Switchboard. Absence is a failed call."""
    sb_mod = try_import(SWITCHBOARD_MODULE)
    events_mod = try_import(EVENTS_MODULE)
    if events_mod is not None and hasattr(events_mod, "validate_envelope"):
        events_mod.validate_envelope(dict(envelope))
    if sb_mod is None:
        return {
            "attempted": True,
            "switchboard_present": False,
            "consumed": False,
            "error": "ModuleNotFoundError",
            "bind_status": "SWITCHBOARD_ABSENT",
        }
    if not switchboard_root or not receipts_dir:
        return {
            "attempted": True,
            "switchboard_present": True,
            "consumed": False,
            "error": "SwitchboardPresentUnwired",
            "bind_status": "SWITCHBOARD_PRESENT_UNWIRED_FAIL_CLOSED",
        }
    switchboard_cls = getattr(sb_mod, "Switchboard", None)
    if switchboard_cls is None:
        return {
            "attempted": True,
            "switchboard_present": True,
            "consumed": False,
            "error": "SwitchboardClassMissing",
            "bind_status": "SWITCHBOARD_PRESENT_UNWIRED_FAIL_CLOSED",
        }
    try:
        board = switchboard_cls(
            switchboard_root,
            receipts_dir=receipts_dir,
            known_missions=frozenset({envelope["mission_id"]}),
        )
        if hasattr(board, "ingest_raw"):
            board.ingest_raw(dict(envelope))
        else:
            board.tick()
        return {
            "attempted": True,
            "switchboard_present": True,
            "consumed": True,
            "error": None,
            "bind_status": "SWITCHBOARD_CONSUMED",
        }
    except Exception as exc:  # noqa: BLE001 — fail closed from the attempt
        return {
            "attempted": True,
            "switchboard_present": True,
            "consumed": False,
            "error": type(exc).__name__,
            "bind_status": "SWITCHBOARD_EMIT_REFUSED",
        }


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
    """Emit receipt.sealed + court.requested in the PR #3 nrh.event.v1 shape."""
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

    created_at = utc_now()
    correlation_id = "corr-" + content_sha({
        "mission": mission_id,
        "stage": "jgi-founder-runtime",
        "head": head,
    })[:24]
    sealed_payload = {
        "receipt_ref": receipt_ref or f"ledger:{job_id}",
        "receipt_sha": job_succeeded_sha or ledger_tail_sha,
        "mission": mission_id,
        "stage": "jgi-founder-runtime",
        "claimer": "JGI_FOUNDER_RUNTIME",
        "repo": "NeuruhAI/neuruh-workers",
        "worktree": "",
        "branch": "feature/jgi-founder-runtime-v0-20260901",
        "head": head,
        "remote": "origin",
        "expected_pr": "5",
        "expected_pr_url": "https://github.com/NeuruhAI/neuruh-workers/pull/5",
        "expected_pr_draft": "YES",
        "bind": "loopback_only",
        "sentinel_closure_in_receipt": "NOT_JUDGED",
        "clipboard_defect": None,
        "required_invariants": list(RECEIPT_SEALED_INVARIANTS),
        "ledger_tail_sha": ledger_tail_sha,
        "context_digest": context_digest,
        "evidence_refs": evidence,
        "job_id": job_id,
        "job_succeeded_sha": job_succeeded_sha,
    }
    sealed = build_nrh_event_v1(
        event_type=EVENT_RECEIPT_SEALED,
        mission_id=mission_id,
        plan_id="jgi-founder-runtime",
        task_id=job_id,
        correlation_id=correlation_id,
        causation_id="",
        source_role="JGI_FOUNDER_RUNTIME",
        source_component="founder-runtime",
        source_repo="NeuruhAI/neuruh-workers",
        source_head=head,
        created_at=created_at,
        authority_ref="jgi-founder-runtime-l0-readonly",
        payload=sealed_payload,
        idempotency_key=f"{EVENT_RECEIPT_SEALED}|{job_succeeded_sha or ledger_tail_sha}",
    )
    emit = attempt_switchboard_emit(
        sealed,
        receipts_dir=receipts_dir,
        switchboard_root=switchboard_root,
    )
    # court.requested exists only when Switchboard actually consumed
    # receipt.sealed. Do not forge source_role=SWITCHAROO on a local envelope.
    court = None
    if emit.get("consumed"):
        court_payload = {
            "court_job_id": None,
            "mission": mission_id,
            "stage": "jgi-founder-runtime",
            "claimer": "JGI_FOUNDER_RUNTIME",
            "receipt_ref": sealed_payload["receipt_ref"],
            "source_head": head,
            "required_invariants": list(RECEIPT_SEALED_INVARIANTS),
            "authority_ref": "jgi-founder-runtime-l0-readonly",
            "sentinel_model_execution": "BLOCKED",
            "verdict": None,
            "ledger_tail_sha": ledger_tail_sha,
            "context_digest": context_digest,
            "evidence_refs": evidence,
            "job_id": job_id,
            "job_succeeded_sha": job_succeeded_sha,
        }
        court = build_nrh_event_v1(
            event_type=EVENT_COURT_REQUESTED,
            mission_id=mission_id,
            plan_id="jgi-founder-runtime",
            task_id=job_id,
            correlation_id=correlation_id,
            causation_id=sealed["event_id"],
            source_role="SWITCHAROO",
            source_component="switchboard-first-seam",
            source_repo="NeuruhAI/neuruh-workers",
            source_head=head,
            created_at=utc_now(),
            authority_ref="jgi-founder-runtime-l0-readonly",
            payload=court_payload,
            idempotency_key=f"{EVENT_COURT_REQUESTED}|{job_succeeded_sha or ledger_tail_sha}",
        )
    honey = honey_pot_bind()
    return {
        "sealed": sealed,
        "court_request": court,
        "emit": emit,
        "honey_pot": honey,
    }


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
