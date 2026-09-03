"""Thin consumers for organs this PR must not rebuild.

Calls existing seams. Does not vendor Switchboard, Git Custodian, IAR,
Revenue Factory, Governance, Sentinel, AXON, or Founder Runtime.
Missing organs fail from the attempted call, not from a pre-filled
authorized=False struct. Callers never set approved=true.

Axon writes are banned. This module never POSTs AXON /execute.
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

from .contracts import JobStatus, ReceiptType, canonical_json
from .queue import JobQueue
from .runner import WorkflowRunner

IAR_HEALTH_HOST = "127.0.0.1"
IAR_HEALTH_PORT = 8090
IAR_HEALTH_PATHS = ("/health", "/v1/health")

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

FR_API_HOST = "127.0.0.1"
FR_API_PORT = 19090
FR_API_VERSION = "fr-api-v1"
FR_HEALTH_URL = f"http://{FR_API_HOST}:{FR_API_PORT}/v1/health"
FR_MISSIONS_URL = f"http://{FR_API_HOST}:{FR_API_PORT}/v1/missions"
FR_DISPATCHABLE_STATES = frozenset({"QUEUED"})

RF_IMPORT_PATHS = (
    "revenue_factory.authorization",
    "revenue_factory",
)
SWITCHBOARD_MODULE = "neuruh_workers.switchboard"
EVENTS_MODULE = "neuruh_workers.events"
GIT_CUSTODIAN_MODULE = "neuruh_workers.git_custodian"

GSA_AUTHORITY_PATH = "governance->sentinel->axon"
PARENT_REQUESTED_BY_PREFIX = "jgi:"
DISPATCH_SOURCE_ROLE = "DISPATCH_BRIDGE"
DISPATCH_SOURCE_COMPONENT = "fr-to-workers-dispatch-bridge"
DISPATCH_AUTHORITY_REF = "fr-api-v1:approved-queued-dispatch"
REQUIRED_SEALED_INVARIANTS = (
    "envelope.valid",
    "hash.matches_if_provided",
    "source_identity.coherent",
    "mission_stage.unambiguous",
    "authority.present",
    "loopback_only",
    "sentinel_closure_in_receipt_is_not_pass",
    "ci_job_named_court_is_not_sentinel",
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


def http_get_json(
    url: str,
    *,
    timeout_s: float = 0.25,
    opener: HttpOpen | None = None,
    headers: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    request = urllib.request.Request(url, method="GET", headers=dict(headers or {}))
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
                "data": data,
            }
    except Exception as exc:  # noqa: BLE001 — fail closed from the attempt
        return {
            "attempted": True,
            "url": url,
            "ok": False,
            "status": None,
            "error": type(exc).__name__,
            "data": {},
        }


def http_post_json(
    url: str,
    payload: Mapping[str, Any],
    *,
    timeout_s: float = 0.25,
    opener: HttpOpen | None = None,
    headers: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    """Dial a loopback organ. Failure is the exception from the attempt."""
    body = canonical_json(dict(payload)).encode("utf-8")
    merged = {"Content-Type": "application/json", "Accept": "application/json"}
    merged.update(dict(headers or {}))
    request = urllib.request.Request(
        url,
        data=body,
        method="POST",
        headers=merged,
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
                "data": data,
            }
    except Exception as exc:  # noqa: BLE001 — fail closed from the attempt
        return {
            "attempted": True,
            "url": url,
            "ok": False,
            "status": None,
            "error": f"{type(exc).__name__}",
            "decision": None,
            "data": {},
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


def bind_founder_runtime(
    *,
    opener: HttpOpen | None = None,
    timeout_s: float = 0.25,
) -> dict[str, Any]:
    """Consume recipe-engine FR health. This PR is not a second Founder Runtime."""
    probe = http_get_json(FR_HEALTH_URL, timeout_s=timeout_s, opener=opener)
    data = probe.get("data") or {}
    api_version = data.get("api_version")
    identity_ok = (
        bool(probe.get("ok"))
        and api_version == FR_API_VERSION
        and data.get("status") == "ok"
    )
    return {
        "owner": "NeuruhAI/recipe-engine Founder Runtime",
        "role": "CONSUME",
        "this_pr_duplicates_founder_runtime": False,
        "url": FR_HEALTH_URL,
        "attempted": True,
        "present": identity_ok,
        "api_version": api_version,
        "required_api_version": FR_API_VERSION,
        "auth_configured": data.get("auth_configured"),
        "bind_status": "FR_API_CONSUMED" if identity_ok else "FR_API_ABSENT_FAIL_CLOSED",
        "error": None if identity_ok else (probe.get("error") or "FR_API_IDENTITY_MISMATCH"),
        "mission_persistence_local": False,
        "approval_state_local": False,
        "decision_plan_local": False,
    }


def pull_dispatchable_tickets(
    *,
    opener: HttpOpen | None = None,
    token: str | None = None,
    timeout_s: float = 0.25,
) -> dict[str, Any]:
    """List FR APPROVED/QUEUED tickets. Identity and approval stay on FR."""
    health = bind_founder_runtime(opener=opener, timeout_s=timeout_s)
    if not health["present"]:
        return {
            "attempted": True,
            "ok": False,
            "tickets": [],
            "bind_status": health["bind_status"],
            "error": health["error"],
            "health": health,
        }
    headers = {}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    listed = http_get_json(
        FR_MISSIONS_URL, timeout_s=timeout_s, opener=opener, headers=headers,
    )
    missions = []
    if listed.get("ok"):
        raw = listed.get("data") or {}
        missions = list(raw.get("missions") or [])
    tickets = []
    for item in missions:
        state = str(item.get("state") or "")
        if state in FR_DISPATCHABLE_STATES:
            # Preserve the authoritative FR envelope verbatim. The Workers
            # boundary factory validates its consumed gate, work-spec hash,
            # workflow and layer before constructing execution types.
            tickets.append(dict(item))
    return {
        "attempted": True,
        "ok": bool(listed.get("ok")),
        "tickets": tickets,
        "bind_status": "FR_TICKETS_CONSUMED" if listed.get("ok") else "FR_TICKETS_ABSENT_FAIL_CLOSED",
        "error": listed.get("error"),
        "health": health,
        "this_pr_persists_missions": False,
    }


def update_fr_evidence(
    mission_id: str,
    evidence: Mapping[str, Any],
    *,
    opener: HttpOpen | None = None,
    token: str | None = None,
    timeout_s: float = 0.25,
) -> dict[str, Any]:
    """Attempt FR state/evidence update. FR owns persistence; write fail-closed.

    fr-api-v1 exposes GET /v1/missions/{id}/receipts and has no workers evidence
    write. This call attempts the update and records the organ's refusal.
    """
    health = bind_founder_runtime(opener=opener, timeout_s=timeout_s)
    headers = {}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    read = http_get_json(
        f"{FR_MISSIONS_URL}/{mission_id}",
        timeout_s=timeout_s,
        opener=opener,
        headers=headers,
    )
    write = http_post_json(
        f"{FR_MISSIONS_URL}/{mission_id}/evidence",
        {
            "api_version": FR_API_VERSION,
            "mission_id": mission_id,
            "evidence": dict(evidence),
            "note": "workers dispatch-bridge evidence; FR remains mission-state authority",
        },
        timeout_s=timeout_s,
        opener=opener,
        headers=headers,
    )
    return {
        "attempted": True,
        "mission_id": mission_id,
        "fr_health": health,
        "fr_read": {
            "attempted": read["attempted"],
            "ok": read["ok"],
            "error": read.get("error"),
        },
        "fr_write": {
            "attempted": write["attempted"],
            "ok": write["ok"],
            "error": write.get("error"),
            "url": write["url"],
        },
        "persisted_locally": False,
        "bind_status": (
            "FR_EVIDENCE_UPDATED" if write.get("ok")
            else "FR_EVIDENCE_UPDATE_FAIL_CLOSED"
        ),
        "owner": "NeuruhAI/recipe-engine Founder Runtime",
    }


def call_gsa(
    *,
    reasons: tuple[str, ...],
    opener: HttpOpen | None = None,
) -> dict[str, Any]:
    """CALL Governance. Sentinel is independent via SWITCHAROO. AXON write-banned."""
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

    axon = {
        "organ": "axon",
        "method": "execute",
        "attempted": False,
        "url": AXON_EXECUTE_URL,
        "ok": False,
        "status": None,
        "error": "AXON_WRITE_BANNED",
        "decision": None,
    }
    calls.append(axon)

    may_execute = False
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
            "write_banned": True,
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
    write_ban = ()
    if module is not None:
        constants = try_import(f"{GIT_CUSTODIAN_MODULE}.constants")
        if constants is not None:
            write_ban = tuple(sorted(getattr(constants, "WRITE_BAN_REPOS", ())))
    return {
        "owner": "NeuruhAI/neuruh-workers main neuruh_workers.git_custodian",
        "module": GIT_CUSTODIAN_MODULE,
        "present": module is not None,
        "role": "CONSUME",
        "custody": "HONEY_POT",
        "this_pr_copies_custodian": False,
        "axon_write_banned": "NeuruhAI/neuruh-axon" in write_ban,
        "write_ban_repos": list(write_ban),
        "bind_status": (
            "GIT_CUSTODIAN_CONSUMED" if module is not None
            else "GIT_CUSTODIAN_ABSENT_FAIL_CLOSED"
        ),
        "gap": None if module is not None else (
            "git_custodian must be importable from main; this PR does not vendor a second custodian"
        ),
    }


def switchboard_present() -> bool:
    return try_import(SWITCHBOARD_MODULE) is not None


def events_present() -> bool:
    return try_import(EVENTS_MODULE) is not None


def _prime_receipt_text(
    *,
    mission_id: str,
    job_id: str,
    ledger_tail_sha: str,
    context_digest: str,
    evidence_refs: Sequence[str],
    job_succeeded_sha: str | None,
    source_head: str,
    code_custody_required: bool,
) -> str:
    refs = ",".join(evidence_refs)
    lines = [
        f"MISSION_ID={mission_id}",
        "STAGE_ID=fr-to-workers-dispatch",
        f"TASK_ID={job_id}",
        f"SOURCE_HEAD={source_head}",
        f"AUTHORITY_REFERENCE={DISPATCH_AUTHORITY_REF}",
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
        f"SOURCE_ROLE={DISPATCH_SOURCE_ROLE}",
        f"SOURCE_COMPONENT={DISPATCH_SOURCE_COMPONENT}",
        f"CLAIMER={DISPATCH_SOURCE_ROLE}",
        f"CODE_CUSTODY_REQUIRED={'YES' if code_custody_required else 'NO'}",
    ]
    return "\n".join(lines) + "\n"


def emit_receipt_sealed_and_switcharoo(
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
    code_custody_required: bool = False,
) -> dict[str, Any]:
    """Hand a receipted result to live SWITCHAROO. Do not clone nrh.event.v1."""
    events = try_import(EVENTS_MODULE)
    switchboard = try_import(SWITCHBOARD_MODULE)
    honey = honey_pot_bind()
    absent = {
        "bind_status": "SWITCHBOARD_ABSENT",
        "attempted": True,
        "consumed": False,
        "events_present": events is not None,
        "switchboard_present": switchboard is not None,
        "schema_owner": EVENTS_MODULE,
        "home_grown_envelope": False,
        "job_id": job_id,
        "ledger_tail_sha": ledger_tail_sha,
        "job_succeeded_sha": job_succeeded_sha,
        "context_digest": context_digest,
        "evidence_refs": list(evidence_refs),
    }
    if events is None or switchboard is None:
        emit = {
            "attempted": True,
            "switchboard_present": switchboard is not None,
            "consumed": False,
            "error": "ModuleNotFoundError",
            "bind_status": "SWITCHBOARD_ABSENT",
        }
        return {
            "sealed": absent,
            "court_request": None,
            "emit": emit,
            "honey_pot": honey,
            "sentinel": {
                "independent": True,
                "judged": False,
                "verdict": None,
                "model_execution": "BLOCKED",
                "ci_job_named_court_is_not_sentinel": True,
                "bind_status": "SWITCHBOARD_ABSENT",
            },
            "board": None,
        }

    if not switchboard_root or not receipts_dir:
        emit = {
            "attempted": True,
            "switchboard_present": True,
            "consumed": False,
            "error": "SwitchboardPresentUnwired",
            "bind_status": "SWITCHBOARD_PRESENT_UNWIRED_FAIL_CLOSED",
        }
        return {
            "sealed": {**absent, "events_present": True, "bind_status": emit["bind_status"]},
            "court_request": None,
            "emit": emit,
            "honey_pot": honey,
            "sentinel": {
                "independent": True,
                "judged": False,
                "verdict": None,
                "model_execution": "BLOCKED",
                "ci_job_named_court_is_not_sentinel": True,
                "bind_status": emit["bind_status"],
            },
            "board": None,
        }

    switchboard_cls = getattr(switchboard, "Switchboard", None)
    if switchboard_cls is None:
        emit = {
            "attempted": True,
            "switchboard_present": True,
            "consumed": False,
            "error": "SwitchboardClassMissing",
            "bind_status": "SWITCHBOARD_PRESENT_UNWIRED_FAIL_CLOSED",
        }
        return {
            "sealed": {**absent, "events_present": True},
            "court_request": None,
            "emit": emit,
            "honey_pot": honey,
            "sentinel": {
                "independent": True,
                "judged": False,
                "verdict": None,
                "model_execution": "BLOCKED",
                "ci_job_named_court_is_not_sentinel": True,
                "bind_status": emit["bind_status"],
            },
            "board": None,
        }

    head = resolve_source_head(source_head)
    os.makedirs(receipts_dir, exist_ok=True)
    os.makedirs(switchboard_root, exist_ok=True)
    receipt_ref = os.path.abspath(os.path.join(receipts_dir, f"{job_id}.txt"))
    with open(receipt_ref, "w", encoding="utf-8") as fh:
        fh.write(_prime_receipt_text(
            mission_id=mission_id,
            job_id=job_id,
            ledger_tail_sha=ledger_tail_sha,
            context_digest=context_digest,
            evidence_refs=evidence_refs,
            job_succeeded_sha=job_succeeded_sha,
            source_head=head,
            code_custody_required=code_custody_required,
        ))

    payload = {
        "receipt_ref": receipt_ref,
        "receipt_sha": job_succeeded_sha or ledger_tail_sha,
        "mission": mission_id,
        "stage": "fr-to-workers-dispatch",
        "claimer": DISPATCH_SOURCE_ROLE,
        "repo": "NeuruhAI/neuruh-workers",
        "worktree": os.getcwd(),
        "branch": "feature/jgi-founder-runtime-v0-20260901",
        "head": head,
        "remote": "origin",
        "job_id": job_id,
        "ledger_tail_sha": ledger_tail_sha,
        "context_digest": context_digest,
        "evidence_refs": list(evidence_refs),
        "job_succeeded_sha": job_succeeded_sha,
        "sentinel_closure_in_receipt": "NOT_JUDGED",
        "required_invariants": list(REQUIRED_SEALED_INVARIANTS),
        "code_custody_required": code_custody_required,
    }
    sealed = events.build_envelope(
        event_type=events.EventType.RECEIPT_SEALED,
        mission_id=mission_id,
        plan_id="fr-to-workers-dispatch",
        task_id=job_id,
        correlation_id="corr-" + (job_succeeded_sha or ledger_tail_sha)[:24],
        causation_id="",
        source_role=DISPATCH_SOURCE_ROLE,
        source_component=DISPATCH_SOURCE_COMPONENT,
        source_repo="NeuruhAI/neuruh-workers",
        source_head=head,
        created_at=utc_now(),
        authority_ref=DISPATCH_AUTHORITY_REF,
        payload=payload,
        idempotency_key=f"receipt.sealed|{job_succeeded_sha or ledger_tail_sha}",
    )

    try:
        board = switchboard_cls(
            switchboard_root,
            receipts_dir=receipts_dir,
            known_missions=frozenset({mission_id}),
        )
        stored = board.ingest_raw(dict(sealed))
        report = board.tick()
        court_events = board.events(events.EventType.COURT_REQUESTED)
        passed = board.events(events.EventType.COURT_PASSED)
        jobs_mod = try_import("neuruh_workers.switchboard_jobs")
        sentinel_workflow = getattr(jobs_mod, "SENTINEL_WORKFLOW", "sentinel-court-artifact")
        honey_workflow = getattr(jobs_mod, "HONEY_POT_WORKFLOW", "honey-pot-candidate-push")
        model_exec = getattr(jobs_mod, "SENTINEL_MODEL_EXECUTION", "BLOCKED")
        sentinel_jobs = board.jobs_for(sentinel_workflow)
        honey_jobs = board.jobs_for(honey_workflow)
        court_request = court_events[-1] if court_events else None
        emit = {
            "attempted": True,
            "switchboard_present": True,
            "consumed": True,
            "error": None,
            "bind_status": "SWITCHBOARD_CONSUMED",
            "stored_event_id": stored.get("event_id") if isinstance(stored, dict) else None,
            "jobs_created": list(getattr(report, "jobs_created", [])),
        }
        if passed:
            # Dispatch must never be the source of a court PASS.
            emit["consumed"] = False
            emit["error"] = "FAKE_SENTINEL_PASS_REFUSED"
            emit["bind_status"] = "SENTINEL_PASS_FORGED_FAIL_CLOSED"
        sentinel = {
            "independent": True,
            "judged": False,
            "verdict": None,
            "model_execution": model_exec,
            "ci_job_named_court_is_not_sentinel": True,
            "court_job_ids": list(sentinel_jobs),
            "bind_status": (
                "SENTINEL_COURT_JOBBED_INDEPENDENT"
                if court_request is not None
                else "SENTINEL_NOT_JOBBED"
            ),
        }
        if code_custody_required:
            honey = {
                **honey,
                "required": True,
                "jobs": list(honey_jobs),
                "activated": False,
                "note": "HONEY POT waits for independent Sentinel court.passed; this PR does not PASS",
            }
        return {
            "sealed": sealed,
            "court_request": court_request,
            "emit": emit,
            "honey_pot": honey,
            "sentinel": sentinel,
            "board": board,
        }
    except Exception as exc:  # noqa: BLE001 — fail closed from the attempt
        return {
            "sealed": {**absent, "events_present": True},
            "court_request": None,
            "emit": {
                "attempted": True,
                "switchboard_present": True,
                "consumed": False,
                "error": type(exc).__name__,
                "bind_status": "SWITCHBOARD_EMIT_REFUSED",
            },
            "honey_pot": honey,
            "sentinel": {
                "independent": True,
                "judged": False,
                "verdict": None,
                "model_execution": "BLOCKED",
                "ci_job_named_court_is_not_sentinel": True,
                "bind_status": "SWITCHBOARD_EMIT_REFUSED",
            },
            "board": None,
        }


# Back-compat name used by older call sites on this same PR.
emit_receipt_sealed_and_court_request = emit_receipt_sealed_and_switcharoo


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
            "proven": False,
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

    proven = automatic and any(a.get("action") in {"JOB_RESUMED", "JOB_RECOVERED"} for a in actions)
    if proven:
        status = "PARENT_RESUME=PROVEN"
    elif automatic:
        status = "PARENT_RESUMED_FROM_RECEIPTS"
    else:
        status = "PARENT_NOT_RESUMABLE"
    return {
        "parent_mission_id": parent_mission_id,
        "automatic": automatic,
        "proven": proven,
        "status": status,
        "jobs": actions,
        "owner": "neuruh-workers main JobQueue.resume + WorkflowRunner job.recovered",
        "note": "existing job-level resume/recovery; no new mission watcher",
    }
