"""``legion-mission@1.0.0`` - the legion as an ordinary governed Workers job.

The mission lives on the normal ExecutionLedger (deterministic job id, sealed
spec, retries, crash recovery, receipts the Cockpit SWARM lens already reads).
The thousands of agent receipts live in the legion journal, which the
``execute_legion`` step binds into the ledger by path + tail sha + Merkle root.

Steps:
  plan_legion      verify the staged unit manifest (sha + count), compute the
                   deterministic legion_id.  Drift is a typed refusal.
  execute_legion   spawn and supervise the legion; reduce to one output.  A
                   re-run after a crash resumes from the journal.
  cognition_pass   optional: the top-K results get a short advisory narrative
                   from the LOCAL model through IAR (loopback, privacy=true).
                   K=0 (default) is receipted SKIPPED.
  seal_final_output  write FINAL_OUTPUT.md + the sealed JSON; return its sha.

Units are staged content-addressed under ``<legion_root>/units/<sha>.jsonl``
so the ledger seals a 64-hex reference, never 10,000 records.
"""
from __future__ import annotations

import json
import os
import urllib.request
from typing import Any, Mapping

from ..contracts import content_sha
from ..definitions import RetryPolicy, StepDefinition, WorkflowCatalog, WorkflowDefinition
from ..registry import Capability, StepContext, WorkerDeclaration, WorkerRegistry
from .profiles import PROFILES
from .runtime import (
    LEGION_MAX_AGENTS_DEFAULT,
    Legion,
    LegionRefused,
    LegionSpec,
    LegionSpecDrift,
    unit_manifest_sha,
)

DEFAULT_LEGION_ROOT = os.path.expanduser("~/.neuruh/legion")
IAR_URL = "http://127.0.0.1:8090/route"
LOOPBACK_PREFIXES = ("http://127.0.0.1:", "http://localhost:")


# --------------------------------------------------------------------------- #
# Unit staging (operator side, before enqueue)


def units_path(root: str, sha: str) -> str:
    return os.path.join(root, "units", f"{sha}.jsonl")


def stage_units(units: list[Mapping[str, Any]], root: str = DEFAULT_LEGION_ROOT) -> dict:
    ordered = sorted(units, key=lambda u: u["unit_id"])
    sha = unit_manifest_sha(ordered)
    path = units_path(root, sha)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    if not os.path.exists(path):
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as fh:
            for unit in ordered:
                fh.write(json.dumps(unit, sort_keys=True, separators=(",", ":"),
                                    ensure_ascii=False) + "\n")
        os.replace(tmp, path)
    return {"unit_manifest_sha": sha, "unit_count": len(ordered), "path": path}


def load_units(root: str, sha: str) -> list[dict[str, Any]]:
    path = units_path(root, sha)
    if not os.path.exists(path):
        raise LegionRefused("staged unit manifest not found", {"path": path})
    with open(path, encoding="utf-8") as fh:
        units = [json.loads(line) for line in fh if line.strip()]
    if unit_manifest_sha(units) != sha:
        raise LegionSpecDrift("staged unit manifest does not hash to its name",
                              {"path": path})
    return units


def _spec_from_params(params: Mapping[str, Any]) -> LegionSpec:
    profile = PROFILES.get(params.get("profile", ""))
    if profile is None:
        raise LegionRefused("unknown legion profile", {"profile": params.get("profile")})
    return LegionSpec(
        mission=params["mission"], profile=profile.name,
        profile_version=profile.version,
        unit_manifest_sha=params["unit_manifest_sha"],
        unit_count=int(params["unit_count"]),
        max_agents=int(params.get("max_agents", LEGION_MAX_AGENTS_DEFAULT)),
        max_parallel=int(params.get("max_parallel", 32)),
        squad_size=int(params.get("squad_size", 50)),
        cohort_size=int(params.get("cohort_size", 20)),
        agent_timeout_s=float(params.get("agent_timeout_s", 30.0)),
        max_attempts=int(params.get("max_attempts", 2)))


def _root(params: Mapping[str, Any]) -> str:
    return params.get("legion_root") or DEFAULT_LEGION_ROOT


# --------------------------------------------------------------------------- #
# Capabilities


def plan_legion(ctx: StepContext, params: Mapping[str, Any]) -> dict:
    p = ctx.params
    spec = _spec_from_params(p)
    spec.validate()
    units = load_units(_root(p), spec.unit_manifest_sha)
    if len(units) != spec.unit_count:
        raise LegionSpecDrift("staged unit count differs from the job params",
                              {"staged": len(units), "params": spec.unit_count})
    squads = -(-spec.unit_count // spec.squad_size)
    return {"legion_id": spec.legion_id, "spec": spec.to_dict(),
            "agents": spec.unit_count, "squads": squads,
            "cohorts": -(-squads // spec.cohort_size)}


def execute_legion(ctx: StepContext, params: Mapping[str, Any]) -> dict:
    p = ctx.params
    spec = _spec_from_params(p)
    planned = ctx.results["plan_legion"]["legion_id"]
    if spec.legion_id != planned:
        raise LegionSpecDrift("legion_id changed between plan and execute")
    legion = Legion(spec, PROFILES[spec.profile], load_units(_root(p), spec.unit_manifest_sha),
                    _root(p), halt_path=p.get("halt_path"))
    result = legion.run()
    if result.state != "SEALED":
        raise LegionRefused(f"legion ended {result.state}; resume by re-running the job",
                            {"legion_id": result.legion_id, "counts": dict(result.counts)})
    return {"legion_id": result.legion_id, "state": result.state,
            "final_sha": result.final_sha, "merkle_root": result.merkle_root,
            "counts": dict(result.counts), "journal": dict(result.journal),
            "final_path": result.final_path}


def _iar_narrative(row: Mapping[str, Any], url: str, timeout_s: float) -> dict:
    facts = {k: row.get(k) for k in ("county", "tier", "distress_groups", "assessed_value",
                                     "tax_owed", "recipes_qualified", "why")}
    prompt = ("You are a real-estate acquisitions analyst. Using ONLY these facts, "
              "write two sentences on why this property may be a motivated-seller "
              "opportunity and the single next piece of evidence to verify. Do not "
              "invent equity, prices, names, or contact details.\nFACTS: "
              + json.dumps(facts, sort_keys=True))
    body = json.dumps({"prompt": prompt, "privacy": True, "max_tokens": 160,
                       "temperature": 0.2}).encode()
    req = urllib.request.Request(url, data=body, headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout_s) as resp:  # noqa: S310 loopback only
        out = json.loads(resp.read())
    text = str(out.get("output") or "").strip()
    lowered = text.lower()
    banned = ("@", "phone", "call them", "equity of $", "% equity")
    if not text or len(text) > 900 or any(b in lowered for b in banned):
        return {"opportunity_id": row["opportunity_id"], "status": "WITHHELD",
                "reason": "validator refused model output", "model": out.get("model")}
    return {"opportunity_id": row["opportunity_id"], "status": "ADVISORY",
            "narrative": text, "model": out.get("model"), "backend": out.get("backend")}


def cognition_pass(ctx: StepContext, params: Mapping[str, Any]) -> dict:
    p = ctx.params
    top_k = int(p.get("cognition_top_k", 0))
    if top_k <= 0:
        return {"status": "SKIPPED", "reason": "cognition_top_k=0"}
    url = p.get("iar_url", IAR_URL)
    if not url.startswith(LOOPBACK_PREFIXES):
        raise LegionRefused("cognition is loopback-only", {"iar_url": url})
    final_path = ctx.results["execute_legion"]["final_path"]
    with open(final_path, encoding="utf-8") as fh:
        final = json.load(fh)
    rows = (final.get("output") or {}).get("top_opportunities") or []
    notes = []
    for row in rows[:top_k]:
        try:
            notes.append(_iar_narrative(row, url, float(p.get("iar_timeout_s", 90))))
        except Exception as exc:  # noqa: BLE001 - receipted, never fatal
            notes.append({"opportunity_id": row.get("opportunity_id"),
                          "status": "UNAVAILABLE", "reason": f"{type(exc).__name__}: {exc}"[:200]})
    ok = sum(1 for n in notes if n["status"] == "ADVISORY")
    return {"status": "DONE" if ok else "UNAVAILABLE", "advisory": ok,
            "requested": min(top_k, len(rows)), "notes": notes,
            "label": "advisory text from the local model; facts above it are authoritative"}


def _render_md(final: Mapping[str, Any], cognition: Mapping[str, Any], mission_job: str) -> str:
    out = final.get("output") or {}
    c = final["counts"]
    lines = [f"# LEGION FINAL OUTPUT - {final['mission']}", "",
             f"- legion `{final['legion_id']}` | profile `{final['profile']}` | job `{mission_job}`",
             f"- agents spawned {c['agents_spawned']:,} | succeeded {c['agents_succeeded']:,} | "
             f"failed {c['agents_failed']:,} | squads {c['squads']} | cohorts {c['cohorts']}",
             f"- merkle root `{final['merkle_root']}`",
             f"- unit manifest `{final['unit_manifest_sha']}`", ""]
    if out.get("kind") == "OPPORTUNITY_BRIEF":
        notes = {n["opportunity_id"]: n for n in cognition.get("notes", [])
                 if n.get("status") == "ADVISORY"}
        lines += ["## Tier distribution", "",
                  " | ".join(f"{k}: {v:,}" for k, v in out["tier_distribution"].items()), "",
                  "## Top opportunities", "",
                  "| # | tier | priority | county | address | assessed | tax owed | distress |",
                  "|---|---|---|---|---|---|---|---|"]
        for i, r in enumerate(out["top_opportunities"], 1):
            av = f"${r['assessed_value']:,.0f}" if isinstance(r.get("assessed_value"), (int, float)) else "?"
            ow = f"${r['tax_owed']:,.2f}" if isinstance(r.get("tax_owed"), (int, float)) else "?"
            lines.append(f"| {i} | {r['tier']} | {r['legion_priority']} | {r['county']} | "
                         f"{r.get('address') or '?'} | {av} | {ow} | {', '.join(r['distress_groups'])} |")
        if notes:
            lines += ["", "## Advisory notes (local model, validated)", ""]
            for r in out["top_opportunities"]:
                n = notes.get(r["opportunity_id"])
                if n:
                    lines.append(f"- **{r.get('address')}** - {n['narrative']}")
        lines += ["", "## Evidence unlock map (one source -> opportunities it would move)", ""]
        lines += [f"- `{e['source_id']}` - {e['opportunities_blocked']:,}"
                  for e in out["evidence_unlock_map"]]
        lines += ["", "## Laws", ""] + [f"- {law}" for law in out["laws"]]
    else:
        lines += ["## Output", "", "```json", json.dumps(out, indent=1, sort_keys=True), "```"]
    if final.get("failures_total"):
        lines += ["", f"## Failures ({final['failures_total']})", ""]
        lines += [f"- {f['unit_id']}: {f['error']}" for f in final["failures"]]
    return "\n".join(lines) + "\n"


def seal_final_output(ctx: StepContext, params: Mapping[str, Any]) -> dict:
    executed = ctx.results["execute_legion"]
    cognition = ctx.results.get("cognition_pass") or {}
    with open(executed["final_path"], encoding="utf-8") as fh:
        final = json.load(fh)
    if content_sha(final) != executed["final_sha"]:
        raise LegionSpecDrift("FINAL_OUTPUT.json changed after the legion sealed it")
    md_path = os.path.join(os.path.dirname(executed["final_path"]), "FINAL_OUTPUT.md")
    md = _render_md(final, cognition, ctx.job_id)
    with open(md_path + ".tmp", "w", encoding="utf-8") as fh:
        fh.write(md)
    os.replace(md_path + ".tmp", md_path)
    return {"final_output": {"json": executed["final_path"], "md": md_path,
                             "final_sha": executed["final_sha"],
                             "md_sha": content_sha(md),
                             "merkle_root": executed["merkle_root"]},
            "counts": executed["counts"], "cognition": cognition.get("status", "SKIPPED"),
            "published": False, "contact_authorized": False}


# --------------------------------------------------------------------------- #
# Registration


def legion_worker() -> WorkerDeclaration:
    def cap(name, fn, description):
        return Capability(name=name, description=description, read_only=True,
                          external=False, execute=fn)
    return WorkerDeclaration(
        worker_id="legion-supervisor-v1",
        description="Spawns and supervises read-only agent legions over staged, "
                    "content-addressed work units; reduces them to one output. "
                    "No writes outside the legion root, no contact, no spend.",
        capabilities=(
            cap("legion.plan", plan_legion, "Verify the staged units; derive legion_id"),
            cap("legion.execute", execute_legion, "Spawn, supervise and reduce the legion"),
            cap("legion.cognition", cognition_pass,
                "Optional local-model advisory notes for the top-K (loopback IAR)"),
            cap("legion.seal", seal_final_output, "Seal the one final output"),
        ))


def legion_workflow() -> WorkflowDefinition:
    once = RetryPolicy(max_attempts=1)
    resume = RetryPolicy(max_attempts=3, base_delay_s=1.0)
    return WorkflowDefinition(
        name="legion-mission", version="1.0.0",
        description="Spawn a governed legion of read-only agents over staged work "
                    "units and reduce it to ONE sealed final output.",
        steps=(
            StepDefinition("plan_legion", "legion.plan", retry=once, timeout_s=120.0),
            StepDefinition("execute_legion", "legion.execute", retry=resume,
                           timeout_s=6 * 3600.0),
            StepDefinition("cognition_pass", "legion.cognition", retry=once,
                           timeout_s=1800.0),
            StepDefinition("seal_final_output", "legion.seal", retry=once, timeout_s=120.0),
        ))


def register_legion(registry: WorkerRegistry, catalog: WorkflowCatalog) -> None:
    registry.register_worker(legion_worker())
    catalog.register(legion_workflow())
