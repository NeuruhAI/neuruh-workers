# CONVERGENCE COURT RECEIPT — NEURUH-JGI-CONVERGENCE-001

```text
MISSION=NEURUH-JGI-CONVERGENCE-001
ARCHITECTURE=CONVERGENCE COURT
REPO=NeuruhAI/neuruh-workers
PR=https://github.com/NeuruhAI/neuruh-workers/pull/5
PR_STATE=DRAFT
MERGED=NO
CONVERTED_FROM_DRAFT=NO
PRODUCTION_MUTATIONS=0
MODEL_CALL=NO
SPEND=NO
OUTBOUND_MESSAGE=NO
DEEDSONAR=NOT_WRITTEN
NEURUH_WEB=NOT_WRITTEN
PUBLIC_COMMONS=NOT_WRITTEN

COURTED_CANDIDATE_HEAD=09608f6b2dee7ccfdbc3b4d560f98ed40bc02df5
CONVERGENCE_SEAM_HEAD=f9c640af784bc435c466ccfc804deefb8a3ef72a
LANE_B_CORRECTIVE=YES
BASE=main@05c669afa0d922607c290d86b98142bf0aec6a3f
BRANCH=feature/jgi-founder-runtime-v0-20260901
COURT_AGENT=https://cursor.com/agents/bc-d263ce40-be40-463d-b8ec-522b63a983ba

PRIOR_COURT_VERDICT=HOLD
SENTINEL_VERDICT=NOT_JUDGED
ARCHITECTURE_COURT_PASS=NO
MERGED=NO
```

## Independent verification of reported court (do not trust chat)

Reported on PR #5: 67 tests PASS, secret scan PASS, deterministic JGI demo PASS,
GitHub Actions JGI Founder Runtime court run #5 PASS on `09608f6`.

Re-run on this court, clean checkout of exact SHA `09608f6b2dee7ccfdbc3b4d560f98ed40bc02df5`:

| Claim | Result | Evidence |
|---|---|---|
| 67 tests PASS | **CONFIRMED** | `python3 -m unittest discover -s tests -t .` → `Ran 67 tests in 0.439s OK` |
| secret scan PASS | **CONFIRMED** | `python3 scripts/secret_scan.py` → `SECRET SCAN: CLEAN (35 tracked files, 9 patterns)` |
| deterministic JGI demo PASS | **CONFIRMED** | `scripts/run_jgi_demo.py` → `status=SUCCEEDED`, `job_id=job-42027ee43e5572cf49ba68c8`, `job.succeeded` via existing `property-research`, ledger `receipts=9`, `tail_sha=ca3505fadc8c9f3f64fa326c1156e42ee09b7e787020ec468e40d5ffc625fe13` |
| GHA court on that HEAD | **CONFIRMED** | check run `court` conclusion=`success` at https://github.com/NeuruhAI/neuruh-workers/actions/runs/33570668959/job/100063709239 |

Post-seam court on `f9c640af784bc435c466ccfc804deefb8a3ef72a`:

| Court | Result |
|---|---|
| unittest | **PASS** `Ran 69 tests in 0.530s OK` (67 prior + ownership/parent-resume) |
| secret scan | **PASS** `SECRET SCAN: CLEAN (37 tracked files, 9 patterns)` |
| JGI demo | **PASS** L0 `SUCCEEDED` without founder; `job_id=job-7c5cf2d5393a5cd03d125d7c`; ledger `receipts=9` `tail_sha=98645755da9ab28d8a5f2ad8d20c327e4c88d3079abb1c49f65a386fab39d214` |

## Question the court answers

Can PR #5 **own/integrate** (not recreate) the existing organism spine:

`INTENT → CONTEXT COMPILER → CHEAPEST CAPABLE ROUTER → WORKER → COURT → RECEIPT → AUTOMATIC PARENT RESUME`

## Ownership map

| Organ | Owner (existing) | This PR | Conflict? |
|---|---|---|---|
| INTENT | `FounderRuntime.MissionIntent` (this PR) | **OWN** | no |
| CONTEXT COMPILER | `FounderRuntime.compile_context` (this PR) | **OWN** | no |
| CHEAPEST CAPABLE ROUTER (L0/L1 paths) | `FounderRuntime.rank_candidates` (this PR) | **OWN** as path ranker only | reconciled: not IAR |
| CHEAPEST CAPABLE ROUTER (L2/L3/L4 inference) | `NeuruhAI/aegis` `core/iar` + `NeuruhAI/neuruh-live-integration#1` `revenue_factory/authorization.py` (`InferenceAuthorization`, `ConsumptionLedger`) | **CONSUME** (named bind, `authorized=false`) | was unbound status-only; smallest seam now names the contract |
| WORKER | `neuruh-workers` main `WorkerRegistry` + sample workflows | **REUSE** | no |
| COURT | `neuruh-workers#3` Switchboard `court.requested` (`SENTINEL_MODEL_EXECUTION=BLOCKED`); Switchboard does not declare PASS | **CONSUME** (not copied onto this branch) | sibling unmerged PR |
| RECEIPT | `neuruh-workers` main `ExecutionLedger` | **REUSE** | no |
| AUTOMATIC PARENT RESUME | main `JobQueue.resume` + `WorkflowRunner job.recovered` | **CONSUME** — child success resumes existing parent jobs | precise gap if parent job absent |
| Switchboard / SWITCHAROO | `neuruh-workers#3` (`neuruh_workers/switchboard.py`, events.jsonl wrapper) and earlier `#2` | **CONSUME** | sibling unmerged; this PR does not copy it |
| Git Custodian / HONEY_POT | `neuruh-workers#4` `neuruh_workers.git_custodian` | **CONSUME** | sibling unmerged; this PR does not copy it |
| Revenue Factory / IAR reasoning authorization | live-integration#1 `revenue_factory/` ; IAR = Aegis `core/iar` | **CONSUME** | named, not executed |
| Governance | `NeuruhAI/autonomous-governance-core` | **CONSUME** | not wired |
| Sentinel | workers#3 sentinel-verify job artifact | **CONSUME** | not wired |
| AXON | `NeuruhAI/neuruh-axon` (PR #4 observe-only / write-banned in Git Custodian T07) | **CONSUME** | `OrganRegistrationSeam.activate()` still refuses on this branch |
| ExecutionLedger | main @ `05c669af` | **REUSE** | no second ledger |
| WorkflowRunner | main @ `05c669af` | **REUSE** | no second runner |
| Mission recovery | main `WorkflowRunner` `job.recovered` (job-level only) | **PARTIAL** | **BLOCKER** for mission-level watcher |
| Outcome learning | `NeuruhAI/neuruh-outcome-calibration-ledger` + live-integration#1 `reasoning_debt.py` (`auto_apply=False`) | **CONSUME** | **BLOCKER** — not consumed |

Machine-readable copy: `neuruh_workers/convergence.py` (`SPINE_OWNERS`).

## Spine proof

### Proven on this PR (L0/L1 without founder)

1. INTENT accepted as `MissionIntent`.
2. CONTEXT COMPILER emits a pointer packet (`jeramie_manual_handoffs_target=0`, no `chat_history` / `transcript`, bounded).
3. CHEAPEST CAPABLE **path ranker** prefers an equivalent L0 workflow over an L4 model path.
4. WORKER is the existing `WorkflowRunner` enqueue+execute of catalogued read-only work.
5. RECEIPT is the existing hash-chained `ExecutionLedger` (`job.succeeded` once; repeat dispatch → `ALREADY_SUCCEEDED`).

This is integration of the **already-on-main** workflow engine, not a second engine.

### Reconciled seam (smallest; no second router)

Pre-court conflict: consequential flags returned `FOUNDER_GATE_REQUIRED`, which made founder look like the authority. Mission requires **Governance → Sentinel → AXON**.

Reconciliation on this branch only:

- consequential flags now return `GOVERNANCE_SENTINEL_AXON_REQUIRED` with `authority_path=governance->sentinel->axon` and `authorized=false` / `executed=false`
- L2/L3/L4 now carry a bind to `revenue_factory/authorization.py` `InferenceAuthorization` + Aegis IAR; `model_call=false`; no authorization is issued or consumed here
- ownership map forbids this PR from owning Switchboard, Git Custodian, IAR, Revenue Factory, Governance, Sentinel, AXON
- `rank_candidates` remains L0/L1 path ranking of **already-valid workflow candidates**. It does not classify worlds, call Aegis `/v1/route`, or issue model authorizations

Switchboard `#2/#3` and Git Custodian `#4` were **not** merged or copied. Copying them would be a second handoff bus / second custodian.

### Not proven (exact HOLD blockers)

1. **L2/L3/L4 economic authority is named, not consumed.**  
   `InferenceAuthorization` + `ConsumptionLedger.consume` live on unmerged `neuruh-live-integration#1`. This PR cannot present or retire a single-use authorization. Invariant still holds locally: no model call.

2. **Governance → Sentinel → AXON is named, not wired.**  
   Workers `#3` Switchboard, workers `#4` Git Custodian, and `neuruh-axon` remain sibling/unmerged. `OrganRegistrationSeam.activate()` still refuses. Consequential dispatch now fails closed to GSA instead of founder-as-executor, but cannot run the GSA chain from this package.

3. **AUTOMATIC PARENT RESUME is bound to existing job recovery.**  
   Child success resumes existing parent jobs via `JobQueue.resume` / `job.recovered`. If no parent job exists on the ledger the gap is `PARENT_JOB_ABSENT_IN_LEDGER`. There is still no stale-mission watcher.

4. **Mission-level recovery watcher is absent.**  
   Job crash recovery (`job.recovered`) exists on main `WorkflowRunner`. Stale/nonterminal **missions** without an existing parent job are not auto-created.

5. **Outcome learning is not consumed.**  
   No accepted-outcome compile into cheaper L0 rules. Revenue Factory `reasoning_debt.py` stays `auto_apply=False` and is out of this repo.

6. **Switchboard is not on this branch.**  
   L0/L1 success now emits `receipt.sealed` + `court.requested` as a consumer packet (and a PRIME observe file when configured). Switchboard is imported if present; otherwise `SWITCHBOARD_ABSENT_FAIL_CLOSED`. `#3` is not copied.

## Lane B corrective (same PR #5 branch; not a new PR)

The HOLD court named the organs. This corrective **binds or fail-closes** them
without rebuilding Switchboard, IAR, AXON, Governance, Sentinel, or a second
router / bus / ledger / git custodian.

| Court defect | Binding | Still fail-closed |
|---|---|---|
| Local USD ranker (`founder_minute_value_usd=2.0`) | deleted; L0/L1 is layer/latency pre-filter only | ranker cannot authorize spend or model calls |
| L2/L3/L4 unbound | `bind_iar_rf` probes `127.0.0.1:8090` health JSON + RF `InferenceAuthorization` import | organs absent → `REASONING_BROKER_REQUIRED`, no enqueue |
| COURT / `receipt.sealed` missing | emit `receipt.sealed` + `court.requested` with evidence refs/SHAs; write PRIME observe file when `receipts_dir` set | `switchboard.py` not on this branch → `SWITCHBOARD_ABSENT_FAIL_CLOSED`; no vendored fork |
| Consequential `FOUNDER_GATE_REQUIRED` | `GOVERNANCE_SENTINEL_AXON_REQUIRED`; evaluate+consume if importable; caller never `approved=true` | G/S/A absent → not enqueued |
| `parent_mission_id` unused | child success resumes existing parent jobs via `JobQueue.resume` / `job.recovered` | parent job missing → `PARENT_JOB_ABSENT_IN_LEDGER` |
| Sibling #3/#4 not imported | import-if-present consumers only | no second `switchboard.py` or `git_custodian` |

This is an implementation bind on the existing draft PR. It is **not** Sentinel PASS,
**not** architecture court PASS, and **not** a merge.

## What this PR may own after HOLD is cleared

Candidate owner of: INTENT, CONTEXT COMPILER, L0/L1 cheapest-capable **path ranking**, and autonomous read-only dispatch into the existing WORKER + RECEIPT.

Not a candidate owner of: Switchboard, Git Custodian, IAR, Revenue Factory authorization, Governance, Sentinel, AXON, mission recovery watcher, outcome learning.

## Non-actions of this court

- did not merge PR #5
- did not convert PR #5 from draft
- did not push unrelated features
- did not write DeedSonar, neuruh-web, or public commons
- did not activate Mother/AXON seams
- did not copy Switchboard or Git Custodian onto this branch

## Next gates (in order; do not skip)

1. Consume workers `#3` Switchboard as the court/handoff spine (merge or import-by-reference; do not rewrite).
2. Bind L2/L3/L4 by **consuming** live-integration `#1` `InferenceAuthorization` (fail closed without a matching consumed auth).
3. Route consequential execution only after Governance + Sentinel PASS into canonical AXON (workers `#4` remains custody, not merge authority).
4. Add receipt-driven mission recovery + automatic parent resume as a watcher over `ExecutionLedger`, not a second runner.
5. Consume outcome-calibration / reasoning-debt after those ownership gates.

```text
NEXT=LANE_B_BOUND_OR_FAIL_CLOSED_ON_PR5
HUMAN_CLIPBOARD_REQUIRED=NO
JERAMIE_MANUAL_HANDOFFS_TARGET=0
SENTINEL_PASS=NO
READY_TO_MERGE=NO
```
