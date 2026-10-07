"""LandOS Hunt WRAP — FR JobQueue capability for POST /hunt.

CAPABILITY_ID = landos.hunt.create
WORKFLOW_NAME = landos-hunt

This is a WRAP of the existing LandOS loopback organ (POST /hunt), not a new
scheduler. POST /hunt creates a hunt job in LandOS: it mutates organ state, so
the capability and the workflow are declared read_only=False. FR DispatchBridge
auto-executes only read_only workflows; this one stops at
GOVERNANCE_SENTINEL_AXON_REQUIRED (NON_READ_ONLY_WORKFLOW), so every hunt needs
founder approval, and a continuation child can never bind it. external=False
stays true: the organ is loopback, not an external lane.

ECONOMIC_LINK: organism-driven national Hunt
efxow_APPLY=NO; mint stays human GO; organism proof stops at hunt_job_id.
"""
from __future__ import annotations

import os
from typing import Any, Callable, Mapping

from .definitions import RetryPolicy, StepDefinition, WorkflowCatalog, WorkflowDefinition
from .organ_consumers import http_post_json
from .registry import Capability, StepContext, WorkerDeclaration, WorkerRegistry

CAPABILITY_ID = "landos.hunt.create"
WORKFLOW_NAME = "landos-hunt"
WORKER_ID = "landos-hunt-organ-v1"
WORKFLOW_VERSION = "1.0.0"

DEFAULT_LANDOS_ENGINE_URL = "http://127.0.0.1:8410"

_RETRY = RetryPolicy(max_attempts=1, base_delay_s=0.0)

HttpPost = Callable[..., dict[str, Any]]


def _engine_base_url() -> str:
    return (os.environ.get("LANDOS_ENGINE_URL") or DEFAULT_LANDOS_ENGINE_URL).rstrip("/")


def _hunt_auth_headers() -> dict[str, str]:
    """auth_hunt pattern: Bearer LANDOS_HUNT_TOKEN|NVE_LAND_TOKEN; optional X-LANDOS-SECRET."""
    headers: dict[str, str] = {}
    token = (
        os.environ.get("LANDOS_HUNT_TOKEN")
        or os.environ.get("NVE_LAND_TOKEN")
        or ""
    ).strip()
    if token:
        headers["Authorization"] = f"Bearer {token}"
    secret = (
        os.environ.get("LANDOS_SECRET")
        or os.environ.get("X_LANDOS_SECRET")
        or ""
    ).strip()
    if secret:
        headers["X-LANDOS-SECRET"] = secret
    return headers


def _territory_from_params(params: Mapping[str, Any]) -> list[Any]:
    territory = params.get("territory")
    if territory is None and isinstance(params.get("work_spec"), Mapping):
        territory = params["work_spec"].get("territory")
    if not isinstance(territory, list) or not territory:
        raise ValueError("territory must be a non-empty list (fips/state/county entries)")
    return list(territory)


def _recipe_from_params(params: Mapping[str, Any]) -> dict[str, Any] | None:
    work_spec = params.get("work_spec") if isinstance(params.get("work_spec"), Mapping) else {}
    recipe = params.get("recipe")
    if recipe is None:
        recipe = work_spec.get("recipe") if isinstance(work_spec, Mapping) else None
    if isinstance(recipe, dict):
        return dict(recipe)

    signal_types = params.get("signal_types")
    if signal_types is None and isinstance(work_spec, Mapping):
        signal_types = work_spec.get("signal_types")
    source = params.get("source")
    if source is None and isinstance(work_spec, Mapping):
        source = work_spec.get("source")

    built: dict[str, Any] = {}
    if isinstance(signal_types, list) and signal_types:
        built["signal_types"] = list(signal_types)
    if source is not None:
        built["source"] = source
    return built or None


def create_hunt(
    ctx: StepContext,
    params: Mapping[str, Any],
    *,
    http_post: HttpPost | None = None,
) -> dict[str, Any]:
    """POST LandOS /hunt; return hunt_job_id + status for FR execution-outcome."""
    territory = _territory_from_params(params)
    recipe = _recipe_from_params(params)
    requested_by = params.get("requested_by")
    if requested_by is None and isinstance(params.get("work_spec"), Mapping):
        requested_by = params["work_spec"].get("requested_by")
    if not isinstance(requested_by, str) or not requested_by.strip():
        requested_by = f"fr-workers:{ctx.job_id}"

    payload: dict[str, Any] = {
        "territory": territory,
        "requested_by": requested_by,
    }
    if recipe is not None:
        payload["recipe"] = recipe

    url = f"{_engine_base_url()}/hunt"
    post = http_post or http_post_json
    result = post(url, payload, timeout_s=5.0, headers=_hunt_auth_headers())
    if not result.get("ok"):
        raise RuntimeError(
            f"landos.hunt.create failed: status={result.get('status')!r} "
            f"error={result.get('error')!r} url={url}"
        )
    data = result.get("data") or {}
    hunt_job_id = data.get("job_id") or data.get("id")
    if not hunt_job_id:
        raise RuntimeError(
            f"landos.hunt.create response missing job_id: keys={sorted(data)}"
        )
    status = data.get("status") or result.get("decision") or "queued"
    return {
        "hunt_job_id": str(hunt_job_id),
        "status": status,
        "landos_status_code": result.get("status"),
        "capability_id": CAPABILITY_ID,
        "workflow": WORKFLOW_NAME,
    }


def bind_landos_hunt_workers(
    registry: WorkerRegistry,
    catalog: WorkflowCatalog,
) -> None:
    """Register landos.hunt.create + landos-hunt onto an existing stack."""
    registry.register_worker(WorkerDeclaration(
        worker_id=WORKER_ID,
        description="WRAP LandOS POST /hunt (local organ). Returns hunt_job_id "
                    "into FR execution-outcome. Mutating, so read_only=False: "
                    "DispatchBridge never auto-executes it.",
        capabilities=(
            Capability(
                name=CAPABILITY_ID,
                description="Create a LandOS Hunt job via POST /hunt (auth_hunt)",
                read_only=False,
                external=False,
                execute=create_hunt,
            ),
        ),
    ))
    catalog.register(WorkflowDefinition(
        name=WORKFLOW_NAME,
        version=WORKFLOW_VERSION,
        description="FR→JobQueue WRAP that POSTs LandOS /hunt and yields hunt_job_id.",
        read_only=False,
        steps=(
            StepDefinition(
                step_id="create_hunt",
                capability=CAPABILITY_ID,
                retry=_RETRY,
                timeout_s=15.0,
            ),
        ),
    ))
