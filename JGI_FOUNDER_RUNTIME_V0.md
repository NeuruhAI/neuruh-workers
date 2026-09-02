# JGI Founder Runtime v0 — executable candidate

**Mission:** JGI-FOUNDER-RUNTIME-V0-001 / NEURUH-JGI-CONVERGENCE-001 Lane B  
**Repo:** `NeuruhAI/neuruh-workers`  
**State:** candidate PR #5 only; draft; not merged; not live; not Sentinel PASS.  
**Production mutations:** 0.

This is the first executable slice of the larger Jeramie General Intelligence control plane.
It does **not** create another workflow engine. It sits above the workflow layer already proven in
`neuruh-workers` and reuses:

- `WorkflowCatalog`
- `WorkerRegistry`
- `JobQueue`
- `WorkflowRunner`
- `ExecutionLedger`
- existing content-bound approval gates
- existing external-action lockout
- existing `job.recovered` / `JobQueue.resume` for parent resume

It consumes, and does not vendor, sibling/foreign organs:

- SWITCHAROO / Switchboard (`neuruh-workers#3`) — import-if-present
- Git Custodian / HONEY POT (`neuruh-workers#4`) — import-if-present
- IAR (`NeuruhAI/aegis` `iar_v2`, loopback `127.0.0.1:8090` health JSON)
- Revenue Factory `InferenceAuthorization`
- Governance (`NeuruhAI/autonomous-governance-core` evaluate+consume)
- Sentinel (independent court organ; not a CI job name)
- AXON (`NeuruhAI/neuruh-axon` ToolRegistry/guards/receipts)

## Bound vs fail-closed (Lane B corrective)

| Spine step | Bound now | Still fail-closed |
|---|---|---|
| INTENT | `MissionIntent` on this PR | — |
| CONTEXT COMPILER | pointer packet, no chat/transcript | — |
| CHEAPEST CAPABLE ROUTER (L0/L1) | **DELETED** USD `score_candidate` (`$2` / `$0.05` gone). Remaining sort is layer+capability only | this PR does not own spend |
| CHEAPEST CAPABLE ROUTER (L2/L3/L4) | GET IAR `127.0.0.1:8090/health` + RF import | no organ → no enqueue, no model call |
| WORKER | existing `JobQueue` + `WorkflowRunner` | material flags still fail closed before enqueue |
| COURT | Switchboard jobs `court.requested` after it consumes `receipt.sealed` | this PR does not mint `court.requested`; module absent → `SWITCHBOARD_ABSENT` |
| RECEIPT | existing `ExecutionLedger` | — |
| `receipt.sealed` | consume `neuruh_workers.events.build_envelope` when PR #3 is importable | events/switchboard absent → `SWITCHBOARD_ABSENT`, no home-grown envelope |
| AUTOMATIC PARENT RESUME | **BIND** — child success resumes existing parent jobs via `JobQueue.resume` / `job.recovered` | parent job missing on ledger → `PARENT_JOB_ABSENT_IN_LEDGER` |
| Governance → Sentinel → AXON | **CALLS** POST `8848/v1/evaluate` then `/v1/consume` on ALLOW; AXON `8300/execute`. Caller never `approved=true` | Sentinel is jobbed by SWITCHAROO after `receipt.sealed` — module absent → `SWITCHBOARD_ABSENT`; no invented `:8101` stub |
| HONEY POT | import-if-present consumer | custodian not copied; custody stays on PR #4 |

## What v0 actually does

`MissionIntent` + candidate paths enter the Founder Runtime. The runtime:

1. selects among already-valid paths by intelligence layer then capability name (USD ranker deleted);
2. prefers cheaper layers when both are valid;
3. compiles a <=4 KiB pointer-heavy context packet instead of replaying a chat transcript;
4. stops consequential gates (money, external message, production mutation, irreversible action, private-IP risk, non-read-only) as `GOVERNANCE_SENTINEL_AXON_REQUIRED` — founder is not the executor and never sets `approved=true`;
5. refuses to self-authorize L2/L3/L4 inference and returns `REASONING_BROKER_REQUIRED` bound to the IAR/RF contract (`iar_v2` health JSON + `InferenceAuthorization`);
6. for safe L0/L1 read-only work, reuses the existing queue/runner and **autonomously enqueues + executes** the workflow;
7. after a receipted worker result, emits `receipt.sealed` and a `COURT_REQUEST` packet onto the SWITCHAROO seam (or fail-closed documents the missing module);
8. if `parent_mission_id` is set, resumes existing parent jobs from the ledger;
9. repeated identical dispatch resolves to the same job and does not re-run completed work.

## Autonomy boundary

V0 is already autonomous for deterministic read-only workflows. It is intentionally **not** autonomous for
consequential actions.

```text
Jeramie intent
  -> candidate paths
  -> cheapest-capable pre-filter (no USD spend)
  -> compact context
  -> consequential? ------- yes -> Governance → Sentinel → AXON (fail-closed if absent)
  -> model required? ------ yes -> IAR / Revenue Factory (fail-closed if absent)
  -> existing read-only workflow
  -> deterministic JobQueue
  -> WorkflowRunner
  -> hash-chained receipts
  -> receipt.sealed + court.requested (SWITCHAROO consume / fail-closed)
  -> automatic parent resume from ledger
```

This gives the organism a real, court-able path toward `JERAMIE_MANUAL_HANDOFFS=0` without bypassing
existing governance.

## Cheap intelligence doctrine

The Founder Runtime uses the shared hierarchy:

- `L0_DETERMINISTIC`
- `L1_NEURUH_DETERMINISTIC`
- `L2_LOCAL_MODEL`
- `L3_CHEAP_REMOTE_MODEL`
- `L4_FRONTIER_MODEL`

Only L0/L1 execute in this package. Model inference and spend must be delegated to IAR / Revenue Factory.
This package does not embed a provider client or a second model router.

## What remains fail-closed / not claimed

1. **Switchboard is not on this branch.** `receipt.sealed` is emitted as a consumer packet + optional PRIME file. Sentinel is not jobbed until `#3` is present.
2. **IAR / Revenue Factory are not in this repo.** L2/L3/L4 probe the published contract and fail closed without issuing authorization.
3. **Governance, Sentinel, and AXON processes are not in this CI.** Consequential work does not enqueue around them.
4. **No stale-mission watcher.** Parent resume covers existing parent *jobs* on the ledger, not a new recovery daemon.
5. **No architecture-court PASS job.** The GitHub Actions job named `court` still runs unit tests, secret scan, and the JGI demo only.
6. **Not merged, not live, not Sentinel PASS.**

## Court (unit)

The tests prove:

- safe deterministic mission runs through the existing workflow engine to `job.succeeded` with zero manual handoff;
- identical dispatch is idempotent and does not execute twice;
- consequential flags do not enqueue without Governance → Sentinel → AXON;
- L2/L3/L4 fail closed without IAR/RF and never self-authorize;
- the local ranker cannot authorize spend or model calls and has no USD constants;
- after L0 success the runtime attempts SWITCHAROO emit; `court.requested` is omitted unless Switchboard consumed the sealed receipt;
- `parent_mission_id` resumes a paused or recovered parent from the existing ledger;
- this PR does not add `switchboard.py` or `git_custodian`.

Run:

```bash
python3 -m unittest discover -s tests -t .
python3 scripts/secret_scan.py
python3 scripts/run_jgi_demo.py --ledger /tmp/jgi-demo.jsonl
```

## Non-claims

- not AGI in the scientific sense
- not deployed
- not merged
- not converted from draft
- not canonical owner yet
- not Sentinel PASS
- not architecture court PASS
- no model call
- no spend
- no outbound message
- no production write
- no replacement for AXON, Governance, Mother, Recipe Engine, Switchboard, Git Custodian, Outcome Record, or Cockpit
