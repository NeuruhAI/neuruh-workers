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
import stat
import subprocess
import tempfile
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Mapping, Sequence

from .contracts import JobStatus, ReceiptType, canonical_json
from .prime_receipts import (
    FR_DISPATCH_RECEIPT_SCHEMA,
    parse_prime_receipt,
    seal_receipt_text,
)
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


@dataclass(frozen=True)
class DispatchSourceIdentity:
    head: str
    branch: str
    worktree: str
    remote: str

HttpOpen = Callable[..., Any]


def try_import(module_name: str) -> Any | None:
    try:
        return importlib.import_module(module_name)
    except ImportError:
        return None


def _canonical_observer_for(switchboard_module: Any, injected: Any) -> Any:
    """The canonical observer the bridge's board carries.

    An injected observer wins -- a court hands in one that reads an offline bare
    repository, so no tick inside a test resolves a URL a receipt names. With
    nothing injected this is a production composition root, and the board gets
    the production observer: read-only git against the executed worktree's own
    remote. A board with no observer names the canonical_base gap in its request
    and can never certify a PASS against it.
    """
    if injected is not None:
        return injected
    return getattr(switchboard_module, "observe_canonical_base_via_git", None)


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


def resolve_dispatch_source_identity(
    source_root: str,
    explicit_head: str | None,
) -> DispatchSourceIdentity:
    root = os.path.realpath(source_root)
    try:
        top = os.path.realpath(subprocess.check_output(
            ["git", "-C", root, "rev-parse", "--show-toplevel"],
            text=True,
            stderr=subprocess.DEVNULL,
        ).strip())
        head = subprocess.check_output(
            ["git", "-C", root, "rev-parse", "HEAD"],
            text=True,
            stderr=subprocess.DEVNULL,
        ).strip().lower()
        branch = subprocess.check_output(
            ["git", "-C", root, "rev-parse", "--abbrev-ref", "HEAD"],
            text=True,
            stderr=subprocess.DEVNULL,
        ).strip()
        dirty = subprocess.check_output(
            ["git", "-C", root, "status", "--porcelain"],
            text=True,
            stderr=subprocess.DEVNULL,
        ).strip()
        remote = subprocess.check_output(
            ["git", "-C", root, "remote", "get-url", "origin"],
            text=True,
            stderr=subprocess.DEVNULL,
        ).strip()
    except (OSError, subprocess.CalledProcessError) as exc:
        raise RuntimeError("Workers source identity is unavailable") from exc
    if top != root:
        raise RuntimeError("Workers source root is not the git worktree root")
    if explicit_head is not None and explicit_head.lower() != head:
        raise RuntimeError("Workers explicit source head differs from checkout HEAD")
    if dirty:
        raise RuntimeError("Workers source checkout is dirty")
    if branch != "main":
        raise RuntimeError("Workers source checkout must be canonical main")
    approved_remotes = {
        "https://github.com/NeuruhAI/neuruh-workers.git",
        "git@github.com:NeuruhAI/neuruh-workers.git",
    }
    if remote not in approved_remotes:
        raise RuntimeError("Workers source origin is not NeuruhAI/neuruh-workers")
    try:
        origin_main = subprocess.check_output(
            ["git", "-C", root, "rev-parse", "origin/main"],
            text=True,
            stderr=subprocess.DEVNULL,
        ).strip().lower()
        remote_main = _remote_main_head(root)
    except (
        OSError, subprocess.CalledProcessError, subprocess.TimeoutExpired, TimeoutError,
    ) as exc:
        raise RuntimeError("Workers canonical remote main is unavailable") from exc
    if head != origin_main or head != remote_main:
        raise RuntimeError("Workers source HEAD differs from fetched origin/main")
    return DispatchSourceIdentity(
        head=head,
        branch=branch,
        worktree=root,
        remote=remote,
    )


def _remote_main_head(root: str) -> str:
    output = subprocess.check_output(
        ["git", "-C", root, "ls-remote", "--exit-code", "origin", "refs/heads/main"],
        text=True,
        stderr=subprocess.DEVNULL,
        timeout=10,
    ).strip()
    fields = output.split()
    if len(fields) != 2 or fields[1] != "refs/heads/main" or not _looks_head(fields[0]):
        raise RuntimeError("Workers canonical remote returned an invalid main ref")
    return fields[0].lower()


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
        and data.get("auth_configured") is True
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


def pull_authoritative_mission(
    mission_id: str,
    *,
    opener: HttpOpen | None = None,
    token: str | None = None,
    timeout_s: float = 0.25,
) -> dict[str, Any]:
    if not token:
        return {
            "attempted": False,
            "ok": False,
            "error": "FR_API_OPERATOR_TOKEN_REQUIRED",
            "envelope": None,
        }
    quoted = urllib.parse.quote(mission_id, safe="")
    result = http_get_json(
        f"{FR_MISSIONS_URL}/{quoted}",
        timeout_s=timeout_s,
        opener=opener,
        headers={"Authorization": f"Bearer {token}"},
    )
    data = result.get("data")
    envelope = data.get("envelope") if isinstance(data, Mapping) else None
    return {
        "attempted": True,
        "ok": bool(result.get("ok")) and isinstance(envelope, Mapping),
        "error": result.get("error"),
        "envelope": dict(envelope) if isinstance(envelope, Mapping) else None,
    }


# --------------------------------------------------------------------------- #
# R02D: Workers -> Founder Runtime execution-outcome write-back.
# FR owns mission state; the bridge reports sealed evidence and never raises on
# a refused write (fail-closed = record the bind status, keep the dispatch).
# Route + envelope: recipe-engine `POST /v1/missions/{id}/execution-outcomes`
# (fr.execution_outcome.v1). Replay is safe by construction on the FR side.
# --------------------------------------------------------------------------- #
FR_EXECUTION_OUTCOME_SCHEMA = "fr.execution_outcome.v1"
FR_OUTCOME_RECORDED = "FR_OUTCOME_RECORDED"
FR_OUTCOME_ALREADY_KNOWN = "FR_OUTCOME_ALREADY_KNOWN"
FR_OUTCOME_WRITEBACK_FAIL_CLOSED = "FR_OUTCOME_WRITEBACK_FAIL_CLOSED"
FR_OUTCOME_WRITEBACK_NOT_CONFIGURED = "FR_OUTCOME_WRITEBACK_NOT_CONFIGURED"
# FR 409 reason codes that mean "this evidence, or a later one, is already
# known": nothing to retry, nothing lost.
FR_OUTCOME_REPLAY_CODES = frozenset({
    "OUTCOME_STALE", "IDEMPOTENCY_CONFLICT", "OUTCOME_OUT_OF_ORDER",
})
# The write path may wait on the FR service lock; never the 0.25 s probe budget.
FR_OUTCOME_WRITE_TIMEOUT_S = 8.0


def execution_outcome_idempotency_key(job_id: str, outcome: str,
                                      evidence_sha: str | None = None) -> str:
    return f"execution_outcome:{job_id}:{outcome}:{evidence_sha or '-'}"


def build_execution_outcome_envelope(
    *,
    mission_id: str,
    mission_version: int,
    work_spec_sha256: str,
    job_id: str,
    outcome: str,
    dispatch_approval_id: str,
    source_head: str,
    source_repo: str = "NeuruhAI/neuruh-workers",
    observed_at: str | None = None,
    receipt_ref: str | None = None,
    failure: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Build one fr.execution_outcome.v1 envelope from bridge facts.

    SUCCEEDED binds the sealed dispatch receipt on disk verbatim
    (`receipt_sha` = its PAYLOAD_HASH = content sha of the fields minus
    PAYLOAD_HASH); FR re-derives and refuses any mismatch.
    """
    envelope: dict[str, Any] = {
        "schema": FR_EXECUTION_OUTCOME_SCHEMA,
        "api_version": FR_API_VERSION,
        "mission_id": mission_id,
        "mission_version": int(mission_version),
        "work_spec_sha256": work_spec_sha256,
        "execution_id": job_id,
        "outcome": outcome,
        "authority_reference": (
            f"{FR_API_VERSION}:approval:{dispatch_approval_id}"
            f":work-spec:{work_spec_sha256}"
        ),
        "observed_at": observed_at or datetime.now(timezone.utc).strftime(
            "%Y-%m-%dT%H:%M:%SZ"),
        "source": {
            "role": DISPATCH_SOURCE_ROLE,
            "component": DISPATCH_SOURCE_COMPONENT,
            "repo": source_repo,
            "head": source_head,
        },
    }
    if outcome == "SUCCEEDED":
        if not receipt_ref:
            raise ValueError("a SUCCEEDED outcome requires the sealed receipt path")
        parsed = parse_prime_receipt(receipt_ref)
        envelope["receipt"] = {
            "schema": FR_DISPATCH_RECEIPT_SCHEMA,
            "fields": dict(parsed.fields),
            "receipt_sha": parsed.fields.get("PAYLOAD_HASH") or parsed.content_sha,
            "receipt_ref": parsed.path,
        }
    elif outcome == "FAILED" and failure:
        envelope["failure"] = {
            "error_class": str(failure.get("error_class") or "")[:128],
            "detail": str(failure.get("detail") or "")[:2000],
        }
    return envelope


def _http_post_json_status(
    url: str,
    payload: Mapping[str, Any],
    *,
    timeout_s: float,
    opener: HttpOpen | None,
    headers: Mapping[str, str],
) -> dict[str, Any]:
    """POST and keep the HTTP status/body of a refusal (4xx/5xx) instead of
    collapsing it into an exception name — the FR reason code is the evidence."""
    body = canonical_json(dict(payload)).encode("utf-8")
    merged = {"Content-Type": "application/json", "Accept": "application/json"}
    merged.update(dict(headers or {}))
    request = urllib.request.Request(url, data=body, method="POST", headers=merged)
    open_fn = opener or urllib.request.urlopen
    try:
        with open_fn(request, timeout=timeout_s) as resp:
            raw = resp.read().decode("utf-8")
            status = int(getattr(resp, "status", 200))
    except urllib.error.HTTPError as exc:
        status = int(exc.code)
        try:
            raw = exc.read().decode("utf-8")
        except Exception:  # noqa: BLE001
            raw = ""
    except Exception as exc:  # noqa: BLE001 — fail closed from the attempt
        return {"attempted": True, "url": url, "ok": False, "status": None,
                "error": type(exc).__name__, "data": {}}
    try:
        data = json.loads(raw) if raw else {}
    except ValueError:
        data = {}
    if not isinstance(data, dict):
        data = {}
    return {"attempted": True, "url": url, "ok": 200 <= status < 300,
            "status": status, "error": None, "data": data}


def _fr_error_code(data: Mapping[str, Any]) -> str | None:
    for key in ("code", "reason_code", "error_code"):
        value = data.get(key)
        if isinstance(value, str) and value:
            return value
    error = data.get("error")
    if isinstance(error, Mapping):
        for key in ("code", "reason_code"):
            value = error.get(key)
            if isinstance(value, str) and value:
                return value
    detail = data.get("detail")
    if isinstance(detail, Mapping):
        for key in ("code", "reason_code"):
            value = detail.get(key)
            if isinstance(value, str) and value:
                return value
    return None


def report_execution_outcome(
    mission_id: str,
    body: Mapping[str, Any],
    *,
    opener: HttpOpen | None = None,
    token: str | None = None,
    timeout_s: float = FR_OUTCOME_WRITE_TIMEOUT_S,
) -> dict[str, Any]:
    """Report one sealed execution outcome to the Founder Runtime.

    Returns ``{attempted, ok, status, code, bind_status, outcome, execution_id,
    idempotency_key, url, data}``. Never raises: a refused write is recorded as
    FR_OUTCOME_WRITEBACK_FAIL_CLOSED and the caller keeps its dispatch.
    """
    outcome = str(body.get("outcome") or "")
    job_id = str(body.get("execution_id") or "")
    receipt = body.get("receipt") or {}
    court = body.get("court") or {}
    evidence_sha = (
        (receipt.get("receipt_sha") if isinstance(receipt, Mapping) else None)
        or (court.get("record_sha") if isinstance(court, Mapping) else None)
    )
    idem = execution_outcome_idempotency_key(job_id, outcome, evidence_sha)
    quoted = urllib.parse.quote(mission_id, safe="")
    url = f"{FR_MISSIONS_URL}/{quoted}/execution-outcomes"
    base = {
        "attempted": False, "ok": False, "status": None, "code": None,
        "outcome": outcome, "execution_id": job_id,
        "idempotency_key": idem, "url": url, "data": {},
        "owner": "NeuruhAI/recipe-engine Founder Runtime",
    }
    if not token:
        return {**base, "bind_status": FR_OUTCOME_WRITEBACK_NOT_CONFIGURED,
                "error": "FR_API_OPERATOR_TOKEN_REQUIRED"}
    write = _http_post_json_status(
        url, body, timeout_s=timeout_s, opener=opener,
        headers={"Authorization": f"Bearer {token}", "Idempotency-Key": idem},
    )
    data = write.get("data") or {}
    code = _fr_error_code(data)
    if write.get("ok"):
        replay = data.get("idempotent_replay") is True or data.get("applied") is False
        bind_status = FR_OUTCOME_ALREADY_KNOWN if replay else FR_OUTCOME_RECORDED
    elif write.get("status") == 409 and code in FR_OUTCOME_REPLAY_CODES:
        bind_status = FR_OUTCOME_ALREADY_KNOWN
    else:
        bind_status = FR_OUTCOME_WRITEBACK_FAIL_CLOSED
    return {
        **base,
        "attempted": True,
        "ok": bool(write.get("ok")),
        "status": write.get("status"),
        "code": code,
        "error": write.get("error"),
        "bind_status": bind_status,
        "data": data,
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


def _fr_dispatch_receipt_text(
    *,
    mission_id: str,
    mission_version: int,
    work_spec_sha256: str,
    idempotency_key: str,
    dispatch_result: str,
    job_id: str,
    ledger_tail_sha: str,
    context_digest: str,
    evidence_refs: Sequence[str],
    job_succeeded_sha: str | None,
    source_head: str,
    source_branch: str,
    source_worktree: str,
    authority_ref: str,
    code_custody_required: bool,
) -> str:
    values = [
        ("RECEIPT_SCHEMA", FR_DISPATCH_RECEIPT_SCHEMA),
        ("MISSION_ID", mission_id),
        ("MISSION_VERSION", str(mission_version)),
        ("WORK_SPEC_SHA256", work_spec_sha256),
        ("IDEMPOTENCY_KEY", idempotency_key),
        ("DISPATCH_RESULT", dispatch_result),
        ("STAGE_ID", "fr-to-workers-dispatch"),
        ("TASK_ID", job_id),
        ("SOURCE_HEAD", source_head),
        ("AUTHORITY_REFERENCE", authority_ref),
        ("EVENT_TARGET", "receipt.sealed"),
        ("EVENT_IMPLEMENTATION_STATUS", "EMITTED"),
        ("REPO", "NeuruhAI/neuruh-workers"),
        ("WORKTREE", source_worktree),
        ("BRANCH", source_branch),
        ("REMOTE", "origin"),
        ("LEDGER_TAIL_SHA", ledger_tail_sha),
        ("JOB_ID", job_id),
        ("CONTEXT_DIGEST", context_digest),
        ("EVIDENCE_REFS", ",".join(evidence_refs)),
        ("JOB_SUCCEEDED_SHA", job_succeeded_sha or ""),
        ("SENTINEL_MODEL_EXECUTION", "BLOCKED"),
        ("SENTINEL_CLOSURE", "NOT_JUDGED"),
        ("PRIME_VERDICT", "NOT_JUDGED"),
        ("NETWORK_POLICY", "127.0.0.1:local-only"),
        ("SOURCE_ROLE", DISPATCH_SOURCE_ROLE),
        ("SOURCE_COMPONENT", DISPATCH_SOURCE_COMPONENT),
        ("CODE_CUSTODY_REQUIRED", "YES" if code_custody_required else "NO"),
    ]
    lines = []
    for key, value in values:
        if "\r" in value or "\n" in value or "\x00" in value:
            raise ValueError(f"{key} must be a single-line receipt scalar")
        lines.append(f"{key}={value}")
    return seal_receipt_text("\n".join(lines) + "\n")


def _write_immutable(path: str, text: str) -> None:
    directory = os.path.dirname(path)
    os.makedirs(directory, exist_ok=True)
    descriptor, temporary = tempfile.mkstemp(
        prefix=f".{os.path.basename(path)}.", suffix=".tmp", dir=directory)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            handle.write(text)
            handle.flush()
            os.fsync(handle.fileno())
            os.fchmod(handle.fileno(), 0o444)
        try:
            os.link(temporary, path)
        except FileExistsError:
            existing = os.lstat(path)
            if (
                not stat.S_ISREG(existing.st_mode)
                or existing.st_mode & 0o222
            ):
                raise RuntimeError(
                    "existing sealed receipt is not an immutable regular file")
            flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
            existing_fd = os.open(path, flags)
            with os.fdopen(existing_fd, encoding="utf-8") as handle:
                if handle.read() != text:
                    raise RuntimeError(
                        "immutable sealed receipt already exists with other bytes")
        directory_fd = os.open(directory, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def emit_receipt_sealed_and_switcharoo(
    *,
    mission_id: str,
    job_id: str,
    ledger_tail_sha: str,
    context_digest: str,
    evidence_refs: Sequence[str],
    job_succeeded_sha: str | None,
    dispatch_approval_id: str,
    work_spec_sha256: str,
    mission_version: int,
    idempotency_key: str,
    dispatch_result: str,
    source_head: str | None = None,
    source_root: str | None = None,
    source_identity: DispatchSourceIdentity | None = None,
    receipts_dir: str | None = None,
    switchboard_root: str | None = None,
    code_custody_required: bool = False,
    canonical_observer: Callable[[str, str, str], tuple[str, str]] | None = None,
) -> dict[str, Any]:
    """Hand a receipted result to live SWITCHAROO. Do not clone nrh.event.v1.

    ``canonical_observer`` is what the board reads the canonical base with; see
    ``_canonical_observer_for`` for the production default and why a court must
    inject its own.
    """
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

    try:
        identity = source_identity or resolve_dispatch_source_identity(
            source_root or os.getcwd(), source_head)
        current_identity = resolve_dispatch_source_identity(
            identity.worktree, identity.head)
        if current_identity != identity:
            raise RuntimeError("Workers source identity changed before receipt sealing")
        authority_ref = (
            f"{FR_API_VERSION}:approval:{dispatch_approval_id}:"
            f"work-spec:{work_spec_sha256}"
        )
        stable_tail = job_succeeded_sha or ledger_tail_sha
        receipt_text = _fr_dispatch_receipt_text(
            mission_id=mission_id,
            mission_version=mission_version,
            work_spec_sha256=work_spec_sha256,
            idempotency_key=idempotency_key,
            dispatch_result=dispatch_result,
            job_id=job_id,
            ledger_tail_sha=stable_tail,
            context_digest=context_digest,
            evidence_refs=evidence_refs,
            job_succeeded_sha=job_succeeded_sha,
            source_head=identity.head,
            source_branch=identity.branch,
            source_worktree=identity.worktree,
            authority_ref=authority_ref,
            code_custody_required=code_custody_required,
        )
        os.makedirs(switchboard_root, exist_ok=True)
        receipt_ref = os.path.abspath(os.path.join(receipts_dir, f"{job_id}.txt"))
        _write_immutable(receipt_ref, receipt_text)
        expected_receipt_sha = parse_prime_receipt(receipt_ref).content_sha
        board = switchboard_cls(
            switchboard_root,
            receipts_dir=receipts_dir,
            known_missions=frozenset({mission_id}),
            canonical_observer=_canonical_observer_for(switchboard, canonical_observer),
        )
        report = board.tick()
        sealed_events = [
            item for item in board.events(events.EventType.RECEIPT_SEALED)
            if (
                item.get("mission_id") == mission_id
                and item.get("task_id") == job_id
                and (item.get("payload_ref_or_payload") or {}).get("receipt_sha")
                == expected_receipt_sha
            )
        ]
        court_events = [
            item for item in board.events(events.EventType.COURT_REQUESTED)
            if (
                len(sealed_events) == 1
                and item.get("mission_id") == mission_id
                and item.get("task_id") == job_id
                and item.get("causation_id") == sealed_events[0].get("event_id")
            )
        ]
        passed = [
            item for item in board.events(events.EventType.COURT_PASSED)
            if (
                len(court_events) == 1
                and item.get("mission_id") == mission_id
                and item.get("correlation_id") == court_events[0].get("correlation_id")
                and item.get("causation_id") == court_events[0].get("event_id")
            )
        ]
        failed = [
            item for item in board.events(events.EventType.COURT_FAILED)
            if (
                len(court_events) == 1
                and item.get("mission_id") == mission_id
                and item.get("correlation_id") == court_events[0].get("correlation_id")
                and item.get("causation_id") == court_events[0].get("event_id")
            )
        ]
        jobs_mod = try_import("neuruh_workers.switchboard_jobs")
        honey_workflow = getattr(jobs_mod, "HONEY_POT_WORKFLOW", "honey-pot-candidate-push")
        model_exec = getattr(jobs_mod, "SENTINEL_MODEL_EXECUTION", "BLOCKED")
        honey_jobs = board.jobs_for(honey_workflow)
        sealed = sealed_events[0] if len(sealed_events) == 1 else None
        court_request = court_events[0] if len(court_events) == 1 else None
        deadlettered = list(getattr(report, "deadlettered", []))
        consumed = (
            len(sealed_events) == 1
            and len(court_events) == 1
            and len(passed) <= 1
            and len(failed) <= 1
            and not (passed and failed)
            and not deadlettered
        )
        emit = {
            "attempted": True,
            "switchboard_present": True,
            "consumed": consumed,
            "error": None if consumed else "SWITCHBOARD_RECEIPT_NOT_CONSUMED",
            "bind_status": (
                "SWITCHBOARD_CONSUMED"
                if consumed
                else "SWITCHBOARD_RECEIPT_REJECTED_FAIL_CLOSED"
            ),
            "stored_event_id": (
                sealed.get("event_id") if isinstance(sealed, dict) else None
            ),
            "jobs_created": list(getattr(report, "jobs_created", [])),
            "deadlettered": deadlettered,
            "matching_sealed_count": len(sealed_events),
            "matching_court_count": len(court_events),
        }
        prior_verdict = "PASS" if passed else ("FAIL" if failed else None)
        sentinel = {
            "independent": True,
            "judged": prior_verdict is not None,
            "verdict": prior_verdict,
            "model_execution": model_exec,
            "ci_job_named_court_is_not_sentinel": True,
            "court_job_ids": (
                [court_request["payload_ref_or_payload"]["court_job_id"]]
                if court_request is not None else []
            ),
            "bind_status": (
                f"SENTINEL_ALREADY_COURTED_{prior_verdict}"
                if prior_verdict is not None
                else "SENTINEL_COURT_JOBBED_INDEPENDENT"
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
            "sealed": sealed or {**absent, "events_present": True},
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
