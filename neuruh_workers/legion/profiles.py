"""Agent profiles a legion can run.

Every profile is read-only: an agent computes over the one unit it was
leased.  It never writes, contacts, spends, fetches or spawns.

- ``scale-probe@1.0.0``          synthetic work with injectable faults; proves
                                 spawn/supervision/recovery at 10,000 agents.
- ``opportunity-analyst@1.0.0``  one agent per Lead Recipe OS opportunity (v2
                                 record, compacted).  Scores distress stacking
                                 from the record's OWN signals, keeps UNKNOWN
                                 as UNKNOWN, names the evidence gap that blocks
                                 each recipe, and never authorizes contact.
"""
from __future__ import annotations

import hashlib
import time
from collections import Counter
from typing import Any, Mapping

from .runtime import AgentContext, AgentProfile

# --------------------------------------------------------------------------- #
# scale-probe


class ProbeFault(RuntimeError):
    pass


def _probe_run(ctx: AgentContext, unit: Mapping[str, Any]) -> dict[str, Any]:
    fault = unit.get("fault")
    if fault == "raise_always" or (fault == "raise_once" and ctx.attempt == 1):
        raise ProbeFault(f"injected fault {fault} on attempt {ctx.attempt}")
    if fault == "hang_once" and ctx.attempt == 1:
        time.sleep(float(unit.get("hang_s", 2.0)))
    digest = unit["unit_id"].encode()
    for _ in range(int(unit.get("work", 200))):
        digest = hashlib.sha256(digest).digest()
    return {"unit_id": unit["unit_id"], "digest": digest.hex()[:16],
            "value": int(digest.hex()[:6], 16) % 1000}


def _probe_reduce(results: list[Mapping[str, Any]]) -> dict[str, Any]:
    return {"n": len(results), "value_sum": sum(r["value"] for r in results),
            "digest": hashlib.sha256("".join(r["digest"] for r in results).encode())
            .hexdigest()[:16]}


def _probe_reduce_cohort(summaries: list[Mapping[str, Any]]) -> dict[str, Any]:
    return {"n": sum(s["n"] for s in summaries),
            "value_sum": sum(s["value_sum"] for s in summaries),
            "digest": hashlib.sha256("".join(s["digest"] for s in summaries).encode())
            .hexdigest()[:16]}


def _probe_finalize(summaries, meta) -> dict[str, Any]:
    total = _probe_reduce_cohort(summaries)
    return {"kind": "SCALE_PROOF", "agents_reduced": total["n"],
            "value_sum": total["value_sum"], "tree_digest": total["digest"]}


SCALE_PROBE = AgentProfile(
    name="scale-probe", version="1.0.0",
    description="Synthetic read-only work with injectable faults (scale court).",
    run=_probe_run, reduce_squad=_probe_reduce, reduce_cohort=_probe_reduce_cohort,
    finalize=_probe_finalize)


# --------------------------------------------------------------------------- #
# opportunity-analyst

NC_FIPS = {"37183": "Wake", "37063": "Durham", "37179": "Union",
           "37101": "Johnston", "37025": "Cabarrus"}

# signal_id -> (distress group, weight).  Weights rank; they never qualify.
DISTRESS = {
    "tax.delinquent": ("tax_event", 1.0),
    "tax.foreclosed": ("tax_foreclosure", 1.6),
    "tax_sale.sale_date": ("tax_sale", 1.4),
    "heirship.unresolved": ("estate_event", 1.3),
    "probate.opened": ("estate_event", 1.4),
    "estate.open": ("estate_event", 1.2),
    "ownership.trust_or_estate": ("ownership", 0.6),
    "absentee.out_of_state": ("absentee", 0.8),
    "code.violation": ("code_event", 0.9),
    "vacancy.vacant": ("vacancy", 1.0),
}
TOP_KEEP = 25
COUNTY_KEEP = 5


def _present(signals: Mapping[str, Any], sid: str) -> Mapping[str, Any] | None:
    s = signals.get(sid)
    return s if s and s.get("state") == "PRESENT" else None


def _value(signals: Mapping[str, Any], sid: str):
    s = _present(signals, sid)
    return s.get("value") if s else None


def _analyst_run(ctx: AgentContext, unit: Mapping[str, Any]) -> dict[str, Any]:
    signals = unit.get("signals") or {}
    subject = unit.get("subject", "")
    parts = subject.split(":")
    fips = parts[1] if len(parts) > 2 else "UNKNOWN"
    county = NC_FIPS.get(fips, fips)
    why: list[str] = []
    groups: set[str] = set()
    score = 0.0

    if _present(signals, "ownership.government"):
        tier, score = "EXCLUDED_GOVERNMENT", 0.0
        why.append("government owner: not a private-seller opportunity")
    else:
        for sid, (group, weight) in DISTRESS.items():
            s = _present(signals, sid)
            if not s:
                continue
            support = float(s.get("support") or 0.0)
            score += weight * max(support, 0.25)
            groups.add(group)
            why.append(f"{sid} PRESENT (support {support:.2f}, {s.get('basis') or 'basis?'})")
        for sid, s in signals.items():
            if s.get("state") == "CONTRADICTED" or float(s.get("contradiction") or 0) > 0.5:
                score -= 0.5
                why.append(f"{sid} contradicted")
        tenure = _value(signals, "transfer.tenure_years")
        if isinstance(tenure, (int, float)) and tenure >= 15:
            score += 0.5
            groups.add("long_tenure")
            why.append(f"owned {tenure:g} years")
        assessed = _value(signals, "valuation.assessed_value")
        owed = _value(signals, "tax.delinquent_amount")
        if isinstance(assessed, (int, float)) and assessed >= 100_000:
            score += 0.4
            why.append(f"assessed ${assessed:,.0f}")
        if (isinstance(assessed, (int, float)) and assessed > 0
                and isinstance(owed, (int, float))):
            ratio = owed / assessed
            if ratio < 0.02:
                score += 0.3
                why.append(f"tax owed is {ratio:.2%} of assessed value")
        known_value = isinstance(assessed, (int, float)) and assessed >= 100_000
        if len(groups) >= 3 and score >= 2.5 and known_value:
            tier = "A"
        elif len(groups) >= 2 and score >= 1.5:
            tier = "B"
        elif groups:
            tier = "C"
        else:
            tier = "D"

    matched = sorted(r["recipe_id"] for r in unit.get("recipes", [])
                     if r.get("decision") == "MATCH")
    qualified = sorted(r["recipe_id"] for r in unit.get("recipes", [])
                       if r.get("qualification") == "QUALIFIED")
    gaps = []
    for r in unit.get("recipes", []):
        top = r.get("nbe_top") or {}
        if r.get("decision") != "MATCH" and top.get("signal_id"):
            gaps.append({"recipe_id": r["recipe_id"], "signal_id": top["signal_id"],
                         "source_id": top.get("source_id") or "UNSOURCED",
                         "blocks": top.get("blocks")})
    gaps.sort(key=lambda g: (g["source_id"], g["signal_id"], g["recipe_id"]))
    return {
        "opportunity_id": unit["unit_id"], "run_id": unit.get("run_id"),
        "subject": subject, "county": county,
        "address": (unit.get("identity") or {}).get("address"),
        "owner_pattern": (unit.get("identity") or {}).get("owner_pattern"),
        "tier": tier, "legion_priority": round(score, 4),
        "distress_groups": sorted(groups),
        "assessed_value": _value(signals, "valuation.assessed_value"),
        "tax_owed": _value(signals, "tax.delinquent_amount"),
        "equity": "UNKNOWN (no mortgage/ROD source)",
        "recipes_matched": matched, "recipes_qualified": qualified,
        "lro_eligibility": unit.get("eligibility"),
        "evidence_gaps": gaps[:12],
        "why": why[:10],
        "contact": "NOT_AUTHORIZED",
    }


TIER_ORDER = {"A": 0, "B": 1, "C": 2, "D": 3, "EXCLUDED_GOVERNMENT": 4}
# Sources that cost money per call: shown in the unlock map, never called here.
PAID_SOURCES = {"batchdata", "regrid", "reportall", "tracerfy", "attom", "propstream"}


def _rank_key(r: Mapping[str, Any]):
    # Tier first: a $1,650 scrap lot with many flags must not outrank a
    # corroborated, valued tier-A property.
    return (TIER_ORDER.get(r["tier"], 9), -r["legion_priority"], r["opportunity_id"])


def _slim(r: Mapping[str, Any]) -> dict[str, Any]:
    keep = ("opportunity_id", "run_id", "subject", "county", "address", "owner_pattern",
            "tier", "legion_priority", "distress_groups", "assessed_value", "tax_owed",
            "equity", "recipes_matched", "recipes_qualified", "why", "contact")
    return {k: r.get(k) for k in keep}


def _merge(summaries: list[Mapping[str, Any]]) -> dict[str, Any]:
    tiers, counties, groups, gaps, gap_recipes, qualified = (
        Counter(), Counter(), Counter(), Counter(), Counter(), Counter())
    top: list[Mapping[str, Any]] = []
    by_county: dict[str, list] = {}
    n = 0
    for s in summaries:
        n += s["n"]
        tiers.update(s["tiers"]); counties.update(s["counties"])
        groups.update(s["distress_groups"]); qualified.update(s["recipes_qualified"])
        gaps.update(s["evidence_sources"]); gap_recipes.update(s["evidence_signals"])
        top.extend(s["top"])
        for county, rows in s["by_county"].items():
            by_county.setdefault(county, []).extend(rows)
    top.sort(key=_rank_key)
    return {
        "n": n, "tiers": dict(sorted(tiers.items())),
        "counties": dict(sorted(counties.items())),
        "distress_groups": dict(sorted(groups.items())),
        "recipes_qualified": dict(sorted(qualified.items())),
        "evidence_sources": dict(sorted(gaps.items())),
        "evidence_signals": dict(sorted(gap_recipes.items())),
        "top": top[:TOP_KEEP],
        "by_county": {c: sorted(rows, key=_rank_key)[:COUNTY_KEEP]
                      for c, rows in sorted(by_county.items())},
    }


def _analyst_reduce_squad(results: list[Mapping[str, Any]]) -> dict[str, Any]:
    leaf = []
    for r in results:
        opportunity_gaps_by_source = Counter({
            g["source_id"] if g["source_id"] != "UNSOURCED"
            else f"UNSOURCED:{g['signal_id']}" for g in r["evidence_gaps"]})
        leaf.append({
            "n": 1, "tiers": {r["tier"]: 1}, "counties": {r["county"]: 1},
            "distress_groups": dict(Counter(r["distress_groups"])),
            "recipes_qualified": dict(Counter(r["recipes_qualified"])),
            # one count per opportunity per source: "how many opportunities
            # would this source move", not how many recipe rows mention it.
            "evidence_sources": dict(opportunity_gaps_by_source),
            "evidence_signals": dict(Counter({g["signal_id"] for g in r["evidence_gaps"]})),
            "top": [_slim(r)] if r["tier"] != "EXCLUDED_GOVERNMENT" else [],
            "by_county": ({r["county"]: [_slim(r)]}
                          if r["tier"] != "EXCLUDED_GOVERNMENT" else {}),
        })
    return _merge(leaf)


def _analyst_finalize(summaries, meta) -> dict[str, Any]:
    total = _merge(summaries)
    unlock = sorted(total["evidence_sources"].items(), key=lambda kv: (-kv[1], kv[0]))
    return {
        "kind": "OPPORTUNITY_BRIEF",
        "opportunities_analysed": total["n"],
        "tier_distribution": total["tiers"],
        "county_distribution": total["counties"],
        "distress_group_prevalence": total["distress_groups"],
        "recipes_qualified": total["recipes_qualified"],
        "top_opportunities": total["top"],
        "top_by_county": total["by_county"],
        "evidence_unlock_map": [
            {"source_id": s, "opportunities_blocked": c,
             "access": ("NO_SOURCE_REGISTERED" if s.startswith("UNSOURCED:")
                        else "PAID_FOUNDER_GATE" if s.split(".")[0] in PAID_SOURCES
                        else "FREE_OR_PUBLIC")}
            for s, c in unlock[:20]],
        "evidence_signals_most_missing": sorted(
            total["evidence_signals"].items(), key=lambda kv: (-kv[1], kv[0]))[:15],
        "laws": [
            "legion_priority is an analyst ordering, not the LRO ranking_key; "
            "ranking_key stays None unless a recipe is SATISFIED",
            "UNKNOWN stays UNKNOWN: equity is never inferred without a mortgage/ROD source",
            "contact is NOT_AUTHORIZED for every opportunity: no DNC scrub, "
            "outbound is a founder gate",
        ],
    }


OPPORTUNITY_ANALYST = AgentProfile(
    name="opportunity-analyst", version="1.1.0",
    description="One read-only analyst agent per Lead Recipe OS opportunity.",
    run=_analyst_run, reduce_squad=_analyst_reduce_squad,
    reduce_cohort=_merge, finalize=_analyst_finalize)

PROFILES = {p.name: p for p in (SCALE_PROBE, OPPORTUNITY_ANALYST)}


def compact_lro_opportunity(opp: Mapping[str, Any]) -> dict[str, Any]:
    """Project a v2 LRO opportunity to exactly what an analyst agent reads."""
    signals = {}
    for sid, s in (opp.get("signals") or {}).items():
        signals[sid] = {"state": s.get("state"), "support": s.get("support"),
                        "contradiction": s.get("contradiction"),
                        "value": s.get("best_value"), "confidence": s.get("best_confidence"),
                        "basis": s.get("best_basis")}
    recipes = []
    for m in opp.get("recipe_matches") or []:
        top = ((m.get("nbe") or {}).get("top") or {})
        recipes.append({"recipe_id": m.get("recipe_id"), "decision": m.get("decision"),
                        "qualification": m.get("qualification"),
                        "nbe_top": {"signal_id": top.get("signal_id"),
                                    "source_id": top.get("source_id"),
                                    "blocks": top.get("blocks")} if top else None})
    identity = opp.get("identity") or {}
    return {"unit_id": opp["opportunity_id"], "run_id": opp.get("run_id") or opp.get("universe_id"),
            "subject": opp.get("subject"),
            "identity": {"address": identity.get("address"),
                         "owner_pattern": identity.get("owner_pattern")},
            "eligibility": opp.get("eligibility"), "signals": signals, "recipes": recipes}
