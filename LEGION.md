# Legion — governed mass fan-out (5,000–10,000 agents → one output)

`neuruh_workers.legion` is the elastic swarm the Research Swarm canon
(vault `01-CANON/research-swarm/`, 2026-09-08) routed to **Workers leases**:
"Historical 5,150/10,000-agent counts are lineage/capacity concepts … Legions /
LegionCard / swarm visual → Workers leases." This package turns that lineage
into a real, courted capability. It adds no new control plane. A legion is an
ordinary Workers job (`legion-mission@1.0.0`) on the normal ExecutionLedger.

## Shape

```
staged units (content-addressed, sha sealed in the job)
  → legion-mission job (ExecutionLedger: plan → execute → cognition → seal)
      → Legion supervisor (one per legion, lock file)
          → N agents  (logical leases on a bounded thread pool)
          → squads    (squad_size agents, Merkle-sealed + reduced)
          → cohorts   (cohort_size squads, Merkle-sealed + reduced)
          → prime     (ONE final output, root of the whole tree)
      → FINAL_OUTPUT.json (sha-sealed) + FINAL_OUTPUT.md
```

- **Agent**: `agt-<sha(legion_id|unit_id)>`. It has its own lease, attempt
  counter, deadline and receipt. It is not an OS process: 10,000 agents share
  `max_parallel` threads, which is why the swarm fits an 8 GB host.
- **Journal**: `<root>/<legion_id>/journal.jsonl`. It is hash-chained, verified
  once on open, and O(1) per append. (The ExecutionLedger re-verifies on every
  append; 20,000 agent receipts through it would be quadratic.) The job binds
  the journal by path, tail sha and Merkle root.
- **Determinism**: legion_id = f(spec); the final sha and Merkle root do not
  depend on parallelism or on unit order (courted at 1 vs 96 threads).

## Guarantees (tests/test_legion.py)

| Property | Court |
|---|---|
| 10,000 agents → 200 squads → 10 cohorts → 1 sealed output | `test_10000_agents_one_final_output` |
| Same output at any parallelism / unit order | `test_output_is_independent_of_parallelism`, `test_unit_order_does_not_change_identity` |
| Retry, timeout (late result discarded), terminal failure all receipted | `Supervision.*` |
| Crash mid-run → resume → byte-identical final sha, no terminal agent re-run | `test_crash_then_resume_matches_uninterrupted_run` |
| Torn tail discarded + receipted; any other tamper = hard stop | `test_torn_tail_*`, `test_tampered_journal_is_a_hard_stop` |
| Kill switch (`HALT` file) drains and halts; resumes later | `test_kill_switch_halts_then_resumes` |
| One supervisor per legion | `test_second_live_supervisor_refused` |
| Hard ceiling 25,000 (default 10,000); read-only profiles only; no agent can spawn | `Governance.*` |
| Staged manifest tamper refused before any agent runs | `test_staged_manifest_tamper_is_refused` |
| Cognition is loopback-only; model output validated or withheld | `test_cognition_*` |

## Profiles

- `scale-probe@1.0.0` — synthetic work with injectable faults (`raise_once`,
  `raise_always`, `hang_once`). Used by the scale court.
- `opportunity-analyst@1.1.0` — one agent per Lead Recipe OS opportunity. It
  scores distress stacking from the record's own signals, excludes government
  owners, keeps equity `UNKNOWN`, names the evidence gap that blocks each recipe,
  and marks every row `contact: NOT_AUTHORIZED`. The final output is an
  OPPORTUNITY_BRIEF with the top 25, the top 5 per county, the tier
  distribution, and an **evidence unlock map**: which single source would move
  the most opportunities.

`legion_priority` is an analyst ordering. It is not the LRO `ranking_key`,
which stays `None` unless a recipe is SATISFIED.

## Run it

```bash
# stage live opportunities (loopback core; bearer from Keychain, never printed)
LRO_CORE_TOKEN=$(security find-generic-password -s neuruh-lro-core-bearer -a lro-core -w) \
  python3 scripts/run_legion.py stage-lro --runs <run_id>,<run_id>
python3 scripts/run_legion.py launch --units <sha> --profile opportunity-analyst \
  --mission lro-nc5 --requested-by founder --cognition-top-k 5
python3 scripts/run_legion.py status <legion_id>
python3 scripts/run_legion.py verify <legion_id>
touch ~/.neuruh/legion/<legion_id>/HALT     # kill switch
```

Scale court: `stage-synthetic --agents 10000 --fault-every 997`, then
`launch --profile scale-probe`.

## Boundaries

- Read-only. There is no write, contact, spend, or external capability. The
  legion worker declares only `read_only=True, external=False` capabilities, and
  the registry's external lockout is unchanged.
- Cognition (`--cognition-top-k`) calls IAR on loopback with `privacy: true`
  (local llama3.2:3b). Its text is labelled advisory, validated, and withheld on
  a banned pattern. The default is off.
- Not yet wired: Founder Runtime → JGI dispatch → `legion-mission`. The job
  runs from the CLI, or from any caller holding a `WorkerRegistry` built with
  `register_legion`. The default `build_registry()` catalog is unchanged, so the
  live FR-dispatch tick is unaffected.
- A halted or stalled legion fails its job step after the retry budget. Resume
  by removing `HALT` and relaunching with a new `--dedupe-key`: the same spec
  resolves to the same legion_id and journal, so completed agents are not re-run.
