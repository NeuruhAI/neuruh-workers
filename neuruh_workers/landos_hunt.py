"""WRAP: LandOS POST /hunt onto existing WorkerRegistry / JobQueue.

Consume-only HTTP against the existing LandOS organ (loopback :8410).
Does not invent a second orchestrator, scheduler, or Hunt engine.
Mint stays human (python -m app.hunt_mint); this WRAP returns job_id only.
"""
from __future__ import annotations

import json
import os
import socket
import urllib.error
import urllib.request
from typing import Any, Mapping
from urllib.parse import urlparse, urlunparse

from .definitions import RetryPolicy, StepDefinition, WorkflowCatalog, WorkflowDefinition
from .registry import Capability, StepContext, WorkerDeclaration, WorkerRegistry

CAPABILITY_ID = "landos_hunt"
#: JGI/PRIME proposed grammar alias — same WRAP; registered id is CAPABILITY_ID.
CAPABILITY_ID_ALIAS_PROPOSED = "landos.hunt.create"
WORKFLOW_NAME = "landos-hunt"
WORKFLOW_VERSION = "1.0.0"
WORKER_ID = "landos-hunt-wrap-v1"

DEFAULT_LANDOS_URL = "http://127.0.0.1:8410"
HUNT_PATH = "/hunt"
LANDOS_URL_ENV = "LANDOS_ENGINE_URL"
TOKEN_ENVS = ("NVE_LAND_TOKEN", "LANDOS_ENGINE_TOKEN")
LOOPBACK_HOSTS = frozenset({"127.0.0.1", "localhost", "::1"})
DEFAULT_TIMEOUT_S = 30.0
_MAX_BODY_BYTES = 1_048_576

_RETRY = RetryPolicy(max_attempts=2, base_delay_s=0.0)

#: Receipt / FR execution-outcome field carrying the LandOS hunt job_id.
LANDOS_HUNT_JOB_ID_FIELD = "LANDOS_HUNT_JOB_ID"


def landos_base_url() -> str:
    return (os.environ.get(LANDOS_URL_ENV) or DEFAULT_LANDOS_URL).rstrip("/")


def _hunt_token() -> str:
    for name in TOKEN_ENVS:
        value = (os.environ.get(name) or "").strip()
        if value:
            return value
    raise ValueError(
        "LandOS hunt token missing: set NVE_LAND_TOKEN or LANDOS_ENGINE_TOKEN"
    )


def _require_loopback(base_url: str) -> str:
    parsed = urlparse(base_url)
    if parsed.scheme not in ("http", "https"):
        raise ValueError(f"LandOS URL must be http(s), got {base_url!r}")
    host = (parsed.hostname or "").strip("[]")
    if host not in LOOPBACK_HOSTS:
        raise ValueError(f"LandOS URL must be loopback, got {host!r}")
    return base_url


def _endpoint(base_url: str, path: str) -> str:
    parsed = urlparse(base_url)
    return urlunparse(parsed._replace(path=path, params="", query="", fragment=""))


def _normalize_territory(raw: Any) -> list[dict[str, str]]:
    if not isinstance(raw, list) or not raw:
        raise ValueError("territory must be a non-empty list")
    out: list[dict[str, str]] = []
    for i, entry in enumerate(raw):
        if not isinstance(entry, Mapping):
            raise ValueError(f"territory[{i}] must be an object")
        fips = entry.get("fips")
        if isinstance(fips, str) and fips.strip():
            out.append({"fips": fips.strip()})
            continue
        state = entry.get("state")
        county = entry.get("county")
        if isinstance(state, str) and state.strip() and isinstance(county, str) and county.strip():
            out.append({"state": state.strip(), "county": county.strip()})
            continue
        raise ValueError(
            f"territory[{i}] must provide fips or (state, county)"
        )
    return out


def _normalize_recipe(raw: Any) -> dict[str, Any]:
    if raw is None:
        return {"signal_types": ["taxsale"]}
    if not isinstance(raw, Mapping):
        raise ValueError("recipe must be an object")
    recipe = dict(raw)
    signal_types = recipe.get("signal_types")
    if signal_types is None:
        recipe["signal_types"] = ["taxsale"]
    elif not isinstance(signal_types, list) or not signal_types:
        raise ValueError("recipe.signal_types must be a non-empty list")
    elif not all(isinstance(s, str) and s.strip() for s in signal_types):
        raise ValueError("recipe.signal_types must be non-empty strings")
    else:
        recipe["signal_types"] = [s.strip() for s in signal_types]
    return recipe


def post_hunt(
    *,
    territory: list[dict[str, str]],
    recipe: dict[str, Any],
    requested_by: str | None = None,
    base_url: str | None = None,
    token: str | None = None,
    timeout_s: float = DEFAULT_TIMEOUT_S,
    opener: Any = None,
) -> dict[str, Any]:
    """POST LandOS /hunt. Returns the organ JSON (must include job_id)."""
    url = _endpoint(_require_loopback((base_url or landos_base_url()).rstrip("/")), HUNT_PATH)
    bearer = token if token is not None else _hunt_token()
    payload: dict[str, Any] = {"territory": territory, "recipe": recipe}
    if requested_by:
        payload["requested_by"] = requested_by
    body = json.dumps(payload).encode("utf-8")
    if len(body) > _MAX_BODY_BYTES:
        raise ValueError(f"hunt body is {len(body)} bytes; max {_MAX_BODY_BYTES}")
    request = urllib.request.Request(url, data=body, method="POST")
    request.add_header("Accept", "application/json")
    request.add_header("Content-Type", "application/json")
    request.add_header("Authorization", f"Bearer {bearer}")
    open_fn = opener or urllib.request.urlopen
    try:
        with open_fn(request, timeout=timeout_s) as response:
            raw = response.read(_MAX_BODY_BYTES + 1)
            status = getattr(response, "status", None) or response.getcode()
    except urllib.error.HTTPError as exc:
        detail = ""
        try:
            detail = exc.read(_MAX_BODY_BYTES).decode("utf-8", errors="replace")[:500]
        except Exception:  # noqa: BLE001
            detail = str(exc)
        raise RuntimeError(
            f"LandOS /hunt refused HTTP {exc.code}: {detail}"
        ) from exc
    except (urllib.error.URLError, socket.timeout, TimeoutError, OSError) as exc:
        raise RuntimeError(
            f"LandOS /hunt unreachable at {url}: {type(exc).__name__}: {exc}"
        ) from exc
    try:
        data = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RuntimeError("LandOS /hunt returned non-JSON body") from exc
    if not isinstance(data, Mapping):
        raise RuntimeError("LandOS /hunt returned a non-object JSON body")
    job_id = data.get("job_id")
    if not isinstance(job_id, str) or not job_id.strip():
        raise RuntimeError("LandOS /hunt response missing job_id")
    if status not in (200, 201, 202) and status is not None:
        raise RuntimeError(f"LandOS /hunt unexpected status {status}")
    return dict(data)


def create_hunt(ctx: StepContext, params: Mapping[str, Any]) -> dict[str, Any]:
    """Capability handler: POST /hunt and return job_id into JobQueue step result."""
    territory = _normalize_territory(params.get("territory"))
    recipe = _normalize_recipe(params.get("recipe"))
    requested_by = params.get("requested_by")
    if requested_by is not None and not isinstance(requested_by, str):
        raise ValueError("requested_by must be a string when provided")
    if not requested_by:
        requested_by = f"workers:{ctx.job_id}"
    organ = post_hunt(
        territory=territory,
        recipe=recipe,
        requested_by=requested_by,
    )
    job_id = str(organ["job_id"]).strip()
    status = str(organ.get("status") or "queued")
    return {
        "job_id": job_id,
        "status": status,
        LANDOS_HUNT_JOB_ID_FIELD: job_id,
        "organ": "landos-hunt",
        "capability": CAPABILITY_ID,
        "consume": "python -m app.hunt_worker --job " + job_id,
        "mint": "python -m app.hunt_mint " + job_id,
        "note": "WRAP only — hunt_worker/mint unchanged; no parallel orchestrator",
    }


def landos_hunt_job_id_from_step_results(
    step_results: Mapping[str, Any] | None,
) -> str | None:
    """Pull LandOS hunt job_id from JobQueue step_results (FR receipt path)."""
    if not isinstance(step_results, Mapping):
        return None
    for result in step_results.values():
        if not isinstance(result, Mapping):
            continue
        if result.get("capability") == CAPABILITY_ID or result.get("organ") == "landos-hunt":
            for key in (LANDOS_HUNT_JOB_ID_FIELD, "job_id"):
                value = result.get(key)
                if isinstance(value, str) and value.strip():
                    return value.strip()
        value = result.get(LANDOS_HUNT_JOB_ID_FIELD)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


def bind_landos_hunt(registry: WorkerRegistry, catalog: WorkflowCatalog) -> None:
    """Register the smallest LandOS Hunt WRAP onto an existing stack."""
    registry.register_worker(WorkerDeclaration(
        worker_id=WORKER_ID,
        description="WRAP LandOS POST /hunt onto JobQueue; returns hunt job_id. "
                    "Loopback organ consume-only; mint stays human.",
        capabilities=(
            Capability(
                name=CAPABILITY_ID,
                description="POST LandOS /hunt and return job_id into FR/JobQueue vocabulary",
                read_only=True,
                external=False,
                execute=create_hunt,
            ),
        ),
    ))
    catalog.register(WorkflowDefinition(
        name=WORKFLOW_NAME,
        version=WORKFLOW_VERSION,
        description="Create a LandOS Hunt job via existing /hunt HTTP (WRAP).",
        read_only=True,
        steps=(
            StepDefinition(
                step_id="create_hunt",
                capability=CAPABILITY_ID,
                retry=_RETRY,
                timeout_s=60.0,
            ),
        ),
    ))
