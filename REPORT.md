# LEGION / LRO read-only report — 2026-10-02

Worktree at `8719d8d`. `origin/main` differs in `neuruh_workers/legion/` by one
render-only line (`workflow.py:219`, #27), so the citations below hold on main.
No code changed. This file is the only write.

## 1. Merge check — MERGED

- PR #26 MERGED 2026-09-30T12:32:45Z, merge commit `dbd9ef6`, head `8719d8d`.
- `git log origin/main --oneline -5`: `341612c` (#27) · `012d2c0` · `dbd9ef6` (#26) · `8719d8d` · `b41516b`.
- `fd45791`, `b41516b` and `8719d8d` are all ancestors of `origin/main`. `git branch --merged origin/main | grep legion` returns `fable/legion-swarm-20260928`.
- The literal `git log main` / `git branch --merged main` shows nothing. The local `main` ref
  (main checkout) is stale at `60fd442`, before #24/#26. It was not moved.

## 2. Roll-up code read

**a. Strictly feed-forward. No return path.**
- An agent sees only its context and its own unit (`runtime.py:418-421`). Context is ids + attempt + squad
  (`runtime.py:92-98`). `_analyst_run` never reads ctx (`profiles.py:103-182`).
- Squad reduces member results (`runtime.py:510-513`). Cohort reduces squad summaries (`runtime.py:539-540`).
  FINAL reduces cohort summaries (`runtime.py:578`). Each level seals once and never re-seals (`runtime.py:505-506`, `534-535`).
- The only loop is the agent tier's own retry. A failed or timed-out attempt re-queues the same unit, and only
  `attempt` changes (`runtime.py:413`, `455-456`, `482-483`). No reducer triggers it.
- Crash replay restores state; it does not feed back (`runtime.py:319-348`). A sealed legion is returned untouched (`runtime.py:359-360`).
- Cognition runs after the seal. Its notes go into the MD only (`workflow.py:202-219`), and the FINAL JSON sha is
  re-checked unchanged (`workflow.py:243-244`).

**b. Squad and cohort seals: outputs only. FINAL: outputs + the rule's *label* (name@version), never the rule's code.**
- Agent leaf = `agent_id, unit_id, terminal, result_sha, error` (`runtime.py:523-531`).
- Squad = Merkle root of member leaves + `summary_sha` of the reducer output (`runtime.py:514-517`).
- Cohort = Merkle root over squad roots + `summary_sha` (`runtime.py:541-546`).
- FINAL `final_sha` = `content_sha(schema + meta + output)`. `meta` carries `profile: name@version`,
  `unit_manifest_sha` and `merkle_root` (`runtime.py:571-580`).
- Indirect binding: every journal record sha covers `legion_id` (`journal.py:35-37`, `110-113`).
  `legion_id` = sha of the spec, including `profile` + `profile_version` (`runtime.py:151-153`, `162-167`). That is a label, not code.
- Two gaps (findings only, not fixed):
  1. The Merkle tree is built from agent leaves only (`runtime.py:545` uses squad `merkle_root`, not `summary_sha`).
     The root proves which agent results existed, not that the summaries were derived from them.
  2. Weights and thresholds (`profiles.py:77-90`, `129-153`) and `_merge` (`profiles.py:203-228`) are never hashed.
     If one is edited without a version bump, `legion_id` stays the same. A resume would then serve old sealed squads
     from the journal (`runtime.py:343-346`) next to new ones built under the edited rule, and no sha would change.
     The only guard is the label match (`workflow.py:87-90`, `runtime.py:265-268`).

**c. One function for all 50 agents in a squad, and for all 6,487 in the legion. No per-agent config.**
- Scoring and tiering: `_analyst_run` (`profiles.py:103-153`). Weights are the module constant `DISTRESS` (`profiles.py:77-88`).
- Bound once per legion: `PROFILES[spec.profile]` (`workflow.py:132`). Every agent runs `self.profile.run` (`runtime.py:421`).
- The only per-agent variation is the unit's own signals (`profiles.py:104`). The same holds for `reduce_squad`
  (`profiles.py:231-249`), `reduce_cohort=_merge` (`profiles.py:286`) and `finalize` (`profiles.py:252-279`).
- The proof run `legion-8b4f…` sealed `opportunity-analyst@1.1.0`: squad 50, cohort 20, 130 squads, 7 cohorts.

## 3. Labeled set scope (lead-recipe-os @ `0f6693e` = origin/main) — NONE EXIST

Three outcome-shaped schemas exist in code. All three are empty or never advanced in the live stores
(`~/.neuruh/lead-recipe-os/core` is the default `LRO_CORE_ROOT`, `api/core.py:46`, and lro-core supervision does not override it):

| Store | Outcome fields (code) | Live rows |
|---|---|---|
| `outcomes` table, `core/outcomes.sqlite` (`outcomes/learning.py:31-32`, `api/core.py:59`) | `action` (contact/offer/acquire/list/sell/…), `result` (WON/LOST/CONTACTED/NO_ANSWER/REJECTED/FALSE_POSITIVE/FALSE_NEGATIVE/PENDING/RECORDED), `failure_reason`, `score`, `at`, `actor` (`learning.py:14-17`) | **0** outcomes, **0** proposals |
| `opportunities.state` + `body.history`, `core/runs.sqlite` (`execution/runs.py:34-35`; state machine `opportunity/genome.py:11-23`: …QUALIFIED→CONTACTABLE→ACTION_READY→IN_PROGRESS→WON/LOST) | `state`, `history[]` transitions | 15,932 rows (7,931 distinct ids, 14 runs), `updated_at` 2026-09-22T13:55Z→22:38Z (one day). State: QUALIFIED 7,055 / VALIDATING 6,846 / ENRICHING 2,031. **Nothing past QUALIFIED.** Every row has exactly 1 history entry (`discovered`). |
| `OUTCOME_BOUND` in SearchMemory, `factory/search_memory.jsonl` (`factory/memory.py:31-37`) | stage: CONTACTED/CONVERSATION/OFFER/CONTRACT/CLOSED/DEAD/FALSE_POSITIVE/FALSE_NEGATIVE/NOT_INTERESTED/WRONG_PARTY | 19 entries, **0** OUTCOME_BOUND (3 LEARNING_RECORDED, all `NEXT_SEARCH`) |

These are not outcomes: `source_calls.outcome` is a source-fetch result (the table is empty), and `evaluations.decision/qualification` is recipe match.
The registry defines `listing.*` and `deed.recorded_event` signals, but no live row carries any of them.
The only market-history signal present is `transfer.tenure_years` (5,148 rows), which is a feature, not a label.

**What a backfill would need (nothing built):**
1. **Label definition + window, anchored to each opportunity's as-of date** (run `created_at`), with labels taken only from
   events after it. Today that is at most 10 days past 2026-09-22.
2. **A stable join key: `subject` (parcel), not `opportunity_id`.** Ids change per run: 15,932 rows map to 7,931 ids.
3. **A source per outcome type:**
   - Sold/transferred: deed/transfer records for the 5 NC counties after 09-22. A re-pull of the assessor's last-sale date
     would also show a change against `transfer.tenure_years`.
   - Listed/price change: a listing or MLS feed (the `listing.*` signals have no wired source on live rows).
   - Contacted/responded: CRM dispositions. LRO opportunities are `contact: NOT_AUTHORIZED`, so none should exist.
   - Status transitions: repeated runs over the same territories, diffed per `subject`. Only one day of runs exists.
4. **A writer:** `OutcomeStore.capture` (`learning.py:41-65`) is the existing path. Calibration proposals must be decided
   by a human (`learning.py:96-97`). Any paid source (deed/MLS vendors) needs founder approval under the no-paid-enrichment rule.
