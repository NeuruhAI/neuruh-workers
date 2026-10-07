"""Sample read-only workflows.

Every capability here is a pure function of its inputs: no network, no file
writes, no clocks, no randomness.  "Research" and "refresh" steps therefore
produce PLANS and PREVIEWS — structured descriptions of work a future
authorized lane could execute — never the side effects themselves.

Invalid inputs raise; nothing is silently defaulted.
"""
from __future__ import annotations

from typing import Any, Mapping

from ..definitions import RetryPolicy, StepDefinition, WorkflowCatalog, WorkflowDefinition
from ..registry import Capability, StepContext, WorkerDeclaration, WorkerRegistry

_RETRY = RetryPolicy(max_attempts=2, base_delay_s=0.0)


def _require(params: Mapping[str, Any], key: str, kind: type) -> Any:
    if key not in params:
        raise ValueError(f"required parameter {key!r} is missing")
    value = params[key]
    if not isinstance(value, kind):
        raise ValueError(f"parameter {key!r} must be {kind.__name__}")
    return value


# --------------------------------------------------------------------------- #
# 1. Property research


def normalize_subject(ctx: StepContext, params: Mapping[str, Any]) -> dict:
    address = _require(params, "address", str).strip()
    if not address:
        raise ValueError("address must be non-empty")
    county = str(params.get("county", "")).strip()
    normalized = " ".join(address.upper().replace(",", " ").split())
    return {
        "subject": {
            "address_normalized": normalized,
            "county": county.upper() or "UNKNOWN",
            "tokens": normalized.split(" "),
        }
    }


def assemble_public_record_queries(ctx: StepContext, params: Mapping[str, Any]) -> dict:
    subject = ctx.results["normalize_subject"]["subject"]
    sources = ["county_assessor", "county_recorder", "gis_parcel_viewer",
               "tax_collector", "flood_map"]
    return {
        "queries": [
            {"source": source,
             "query": subject["address_normalized"],
             "county": subject["county"],
             "mode": "read_only_plan"}
            for source in sources
        ]
    }


def compile_research_dossier(ctx: StepContext, params: Mapping[str, Any]) -> dict:
    subject = ctx.results["normalize_subject"]["subject"]
    queries = ctx.results["assemble_public_record_queries"]["queries"]
    return {
        "dossier": {
            "subject": subject,
            "planned_queries": queries,
            "query_count": len(queries),
            "status": "RESEARCH_PLAN_READY",
            "external_execution": "NOT_AUTHORIZED_IN_V1",
        }
    }


# --------------------------------------------------------------------------- #
# 2. Field-pin enrichment


def validate_pins(ctx: StepContext, params: Mapping[str, Any]) -> dict:
    pins = _require(params, "pins", list)
    if not pins:
        raise ValueError("pins must be a non-empty list")
    validated = []
    for i, pin in enumerate(pins):
        if not isinstance(pin, dict):
            raise ValueError(f"pin[{i}] must be an object")
        pin_id = pin.get("pin_id")
        lat, lon = pin.get("lat"), pin.get("lon")
        if not isinstance(pin_id, str) or not pin_id:
            raise ValueError(f"pin[{i}].pin_id must be a non-empty string")
        if not isinstance(lat, (int, float)) or not -90 <= lat <= 90:
            raise ValueError(f"pin[{i}].lat out of range")
        if not isinstance(lon, (int, float)) or not -180 <= lon <= 180:
            raise ValueError(f"pin[{i}].lon out of range")
        validated.append({"pin_id": pin_id, "lat": float(lat), "lon": float(lon),
                          "label": str(pin.get("label", ""))})
    return {"pins": sorted(validated, key=lambda p: p["pin_id"])}


def derive_geo_context(ctx: StepContext, params: Mapping[str, Any]) -> dict:
    pins = ctx.results["validate_pins"]["pins"]
    contexts = []
    for pin in pins:
        contexts.append({
            "pin_id": pin["pin_id"],
            "hemisphere_ns": "N" if pin["lat"] >= 0 else "S",
            "hemisphere_ew": "E" if pin["lon"] >= 0 else "W",
            "grid_cell": f"{int(pin['lat'] // 1)}:{int(pin['lon'] // 1)}",
        })
    return {"geo_contexts": contexts}


def enrich_pins(ctx: StepContext, params: Mapping[str, Any]) -> dict:
    pins = {p["pin_id"]: p for p in ctx.results["validate_pins"]["pins"]}
    for geo in ctx.results["derive_geo_context"]["geo_contexts"]:
        pins[geo["pin_id"]] = {**pins[geo["pin_id"]], **geo}
    return {"enriched_pins": [pins[k] for k in sorted(pins)],
            "count": len(pins)}


# --------------------------------------------------------------------------- #
# 3. Parcel intelligence assembly


def collect_parcel_inputs(ctx: StepContext, params: Mapping[str, Any]) -> dict:
    parcel = _require(params, "parcel", dict)
    apn = parcel.get("apn")
    if not isinstance(apn, str) or not apn:
        raise ValueError("parcel.apn must be a non-empty string")
    signals = params.get("signals", [])
    if not isinstance(signals, list):
        raise ValueError("signals must be a list")
    return {
        "parcel": {"apn": apn,
                   "county": str(parcel.get("county", "UNKNOWN")).upper(),
                   "acreage": parcel.get("acreage"),
                   "zoning": parcel.get("zoning")},
        "signals": signals,
    }


def cross_reference(ctx: StepContext, params: Mapping[str, Any]) -> dict:
    collected = ctx.results["collect_parcel_inputs"]
    apn = collected["parcel"]["apn"]
    matched, unmatched = [], []
    for signal in collected["signals"]:
        target = signal.get("apn") if isinstance(signal, dict) else None
        (matched if target == apn else unmatched).append(signal)
    return {"matched_signals": matched, "unmatched_signals": unmatched}


def assemble_parcel_report(ctx: StepContext, params: Mapping[str, Any]) -> dict:
    collected = ctx.results["collect_parcel_inputs"]
    xref = ctx.results["cross_reference"]
    matched = len(xref["matched_signals"])
    total = matched + len(xref["unmatched_signals"])
    return {
        "parcel_report": {
            "parcel": collected["parcel"],
            "signal_match_ratio": (matched / total) if total else None,
            "matched_signals": xref["matched_signals"],
            "flags": (["UNMATCHED_SIGNALS_PRESENT"]
                      if xref["unmatched_signals"] else []),
            "status": "ASSEMBLED_READ_ONLY",
        }
    }


# --------------------------------------------------------------------------- #
# 4. Daily opportunity brief (final step gated on human approval)


def gather_opportunities(ctx: StepContext, params: Mapping[str, Any]) -> dict:
    brief_date = _require(params, "date", str)
    raw = _require(params, "opportunities", list)
    seen: dict[str, dict] = {}
    for i, opp in enumerate(raw):
        if not isinstance(opp, dict) or not isinstance(opp.get("id"), str):
            raise ValueError(f"opportunity[{i}] must be an object with a string id")
        if opp["id"] not in seen:
            seen[opp["id"]] = {"id": opp["id"],
                               "title": str(opp.get("title", "")),
                               "vertical": str(opp.get("vertical", "general")),
                               "score": float(opp.get("score", 0.0))}
    return {"date": brief_date, "opportunities": [seen[k] for k in sorted(seen)]}


def rank_opportunities(ctx: StepContext, params: Mapping[str, Any]) -> dict:
    opps = ctx.results["gather_opportunities"]["opportunities"]
    ranked = sorted(opps, key=lambda o: (-o["score"], o["id"]))
    return {"ranked": [{**o, "rank": i + 1} for i, o in enumerate(ranked)]}


def draft_brief(ctx: StepContext, params: Mapping[str, Any]) -> dict:
    date = ctx.results["gather_opportunities"]["date"]
    ranked = ctx.results["rank_opportunities"]["ranked"]
    lines = [f"# Daily Opportunity Brief — {date}", ""]
    for opp in ranked:
        lines.append(f"{opp['rank']}. [{opp['vertical']}] {opp['title']} "
                     f"(score {opp['score']:g})")
    return {"draft": {"date": date, "body": "\n".join(lines),
                      "item_count": len(ranked)}}


def finalize_brief(ctx: StepContext, params: Mapping[str, Any]) -> dict:
    draft = ctx.results["draft_brief"]["draft"]
    return {"brief": {**draft, "status": "FINAL_APPROVED",
                      "distribution": "NONE — external delivery not authorized"}}


# --------------------------------------------------------------------------- #
# 5. World Emitter refresh preview (preview only — never writes)


def load_emitter_state(ctx: StepContext, params: Mapping[str, Any]) -> dict:
    current = _require(params, "current_worlds", list)
    proposed = _require(params, "proposed_worlds", list)
    for name, worlds in (("current_worlds", current), ("proposed_worlds", proposed)):
        if not all(isinstance(w, str) and w for w in worlds):
            raise ValueError(f"{name} must contain non-empty strings")
    return {"current": sorted(set(current)), "proposed": sorted(set(proposed))}


def compute_refresh_plan(ctx: StepContext, params: Mapping[str, Any]) -> dict:
    state = ctx.results["load_emitter_state"]
    current, proposed = set(state["current"]), set(state["proposed"])
    return {"plan": {"add": sorted(proposed - current),
                     "remove": sorted(current - proposed),
                     "keep": sorted(current & proposed)}}


def render_preview(ctx: StepContext, params: Mapping[str, Any]) -> dict:
    plan = ctx.results["compute_refresh_plan"]["plan"]
    return {
        "preview": {
            "summary": (f"add {len(plan['add'])}, remove {len(plan['remove'])}, "
                        f"keep {len(plan['keep'])}"),
            "plan": plan,
            "no_write": True,
            "note": "PREVIEW ONLY — applying this plan requires a separate "
                    "authorized mutation lane",
        }
    }


# --------------------------------------------------------------------------- #
# 6. Cockpit health snapshot


_HEALTH_ORDER = {"down": 0, "unknown": 1, "degraded": 2, "healthy": 3}


def collect_component_statuses(ctx: StepContext, params: Mapping[str, Any]) -> dict:
    components = _require(params, "components", list)
    if not components:
        raise ValueError("components must be a non-empty list")
    validated = []
    for i, comp in enumerate(components):
        if not isinstance(comp, dict) or not isinstance(comp.get("name"), str):
            raise ValueError(f"component[{i}] must be an object with a name")
        status = comp.get("status")
        if status not in _HEALTH_ORDER:
            raise ValueError(
                f"component[{i}].status {status!r} is not in "
                f"{sorted(_HEALTH_ORDER)} — refusing to guess")
        validated.append({"name": comp["name"], "status": status,
                          "detail": str(comp.get("detail", ""))})
    return {"components": sorted(validated, key=lambda c: c["name"])}


def evaluate_health(ctx: StepContext, params: Mapping[str, Any]) -> dict:
    components = ctx.results["collect_component_statuses"]["components"]
    worst = min(components, key=lambda c: _HEALTH_ORDER[c["status"]])
    counts: dict[str, int] = {}
    for comp in components:
        counts[comp["status"]] = counts.get(comp["status"], 0) + 1
    return {"overall": worst["status"], "worst_component": worst["name"],
            "counts": counts}


def render_snapshot(ctx: StepContext, params: Mapping[str, Any]) -> dict:
    health = ctx.results["evaluate_health"]
    components = ctx.results["collect_component_statuses"]["components"]
    return {
        "snapshot": {
            "overall": health["overall"],
            "worst_component": health["worst_component"],
            "counts": health["counts"],
            "components": components,
            "read_only": True,
        }
    }


# --------------------------------------------------------------------------- #
# Registration


def _cap(name: str, fn, description: str) -> Capability:
    return Capability(name=name, description=description,
                      read_only=True, external=False, execute=fn)


def build_registry() -> WorkerRegistry:
    registry = WorkerRegistry()
    registry.register_worker(WorkerDeclaration(
        worker_id="local-readonly-worker-v1",
        description="Pure, deterministic read-only computations over "
                    "explicitly provided inputs.  No network, no writes.",
        capabilities=(
            _cap("research.normalize_subject", normalize_subject,
                 "Normalize a property research subject"),
            _cap("research.assemble_queries", assemble_public_record_queries,
                 "Assemble a public-record query plan (plan only)"),
            _cap("research.compile_dossier", compile_research_dossier,
                 "Compile the research dossier from prior steps"),
            _cap("pins.validate", validate_pins, "Validate field pins"),
            _cap("pins.derive_geo", derive_geo_context, "Derive geo context"),
            _cap("pins.enrich", enrich_pins, "Merge enrichment into pins"),
            _cap("parcel.collect", collect_parcel_inputs, "Validate parcel inputs"),
            _cap("parcel.cross_reference", cross_reference,
                 "Cross-reference signals to the parcel"),
            _cap("parcel.assemble_report", assemble_parcel_report,
                 "Assemble the parcel intelligence report"),
            _cap("brief.gather", gather_opportunities, "Gather opportunities"),
            _cap("brief.rank", rank_opportunities, "Rank opportunities"),
            _cap("brief.draft", draft_brief, "Draft the daily brief"),
            _cap("brief.finalize", finalize_brief,
                 "Finalize the brief (behind a human approval gate)"),
            _cap("emitter.load_state", load_emitter_state,
                 "Load World Emitter state from provided inputs"),
            _cap("emitter.compute_plan", compute_refresh_plan,
                 "Compute the refresh plan (preview)"),
            _cap("emitter.render_preview", render_preview,
                 "Render the refresh preview (no write)"),
            _cap("cockpit.collect", collect_component_statuses,
                 "Collect cockpit component statuses"),
            _cap("cockpit.evaluate", evaluate_health, "Evaluate overall health"),
            _cap("cockpit.render", render_snapshot, "Render the health snapshot"),
        ),
    ))
    return registry


def _steps(*specs) -> tuple[StepDefinition, ...]:
    return tuple(
        StepDefinition(step_id=step_id, capability=capability, retry=_RETRY,
                       timeout_s=10.0, **extra)
        for step_id, capability, extra in specs
    )


def build_catalog() -> WorkflowCatalog:
    catalog = WorkflowCatalog()
    catalog.register(WorkflowDefinition(
        name="property-research", version="1.0.0",
        description="Normalize a subject property and assemble a read-only "
                    "public-record research plan.",
        steps=_steps(
            ("normalize_subject", "research.normalize_subject", {}),
            ("assemble_public_record_queries", "research.assemble_queries", {}),
            ("compile_research_dossier", "research.compile_dossier", {}),
        )))
    catalog.register(WorkflowDefinition(
        name="field-pin-enrichment", version="1.0.0",
        description="Validate and enrich field pins with derived geo context.",
        steps=_steps(
            ("validate_pins", "pins.validate", {}),
            ("derive_geo_context", "pins.derive_geo", {}),
            ("enrich_pins", "pins.enrich", {}),
        )))
    catalog.register(WorkflowDefinition(
        name="parcel-intelligence-assembly", version="1.0.0",
        description="Cross-reference provided signals against a parcel and "
                    "assemble an intelligence report.",
        steps=_steps(
            ("collect_parcel_inputs", "parcel.collect", {}),
            ("cross_reference", "parcel.cross_reference", {}),
            ("assemble_parcel_report", "parcel.assemble_report", {}),
        )))
    catalog.register(WorkflowDefinition(
        name="daily-opportunity-brief", version="1.0.0",
        description="Rank opportunities and draft the daily brief; finalization "
                    "is behind a human approval gate.",
        steps=_steps(
            ("gather_opportunities", "brief.gather", {}),
            ("rank_opportunities", "brief.rank", {}),
            ("draft_brief", "brief.draft", {}),
            ("finalize_brief", "brief.finalize",
             {"requires_approval": True, "approvers": ("founder",)}),
        )))
    catalog.register(WorkflowDefinition(
        name="world-emitter-refresh-preview", version="1.0.0",
        description="Compute and render a World Emitter refresh plan preview. "
                    "Never writes.",
        steps=_steps(
            ("load_emitter_state", "emitter.load_state", {}),
            ("compute_refresh_plan", "emitter.compute_plan", {}),
            ("render_preview", "emitter.render_preview", {}),
        )))
    catalog.register(WorkflowDefinition(
        name="cockpit-health-snapshot", version="1.0.0",
        description="Summarize cockpit component statuses into a health "
                    "snapshot.",
        steps=_steps(
            ("collect_component_statuses", "cockpit.collect", {}),
            ("evaluate_health", "cockpit.evaluate", {}),
            ("render_snapshot", "cockpit.render", {}),
        )))
    return catalog
