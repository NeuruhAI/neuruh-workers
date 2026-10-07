# LEGION: governed 10,000-agent swarm (leases inside Workers)

Status: **v0.1.0 — built, courted, proven on live data. Not merged. Not wired to Founder Runtime.**
Proof receipts: [`PROOF_20260928.md`](PROOF_20260928.md).

## 1. What it is

Legion spawns thousands of read-only agents over a staged set of work units,
supervises them, and reduces their work to **one sealed final output**.

- It is an ordinary Workers job, `legion-mission@1.0.0`, on the normal
  ExecutionLedger. It is **not** a new orchestrator or control plane.
- It adds no write, contact, spend or external capability.
- The default is 10,000 agents per legion. The ceiling is 25,000.

## 2. Canon lineage

| Source | Decision |
|---|---|
| Kimi *Neuruh Swarm Architecture Blueprint* (375 pp, historical donor) | Described 5,150 / 10,000-agent "Legions", a Hive Mind and a LegionCard visual. Feasibility was UNKNOWN and "uncontrolled self-replication" was REJECTED (vault `research-swarm/04_DETAILED_HANDOFF_2026-09-08.md` §2) |
| Research Swarm canon (neuruh-vault `01-CANON/research-swarm/`, merged 2026-09-08, vault #8 / #11) | "Workers/Switchboard + Capability/Atomic Registries as elastic swarm topology … Historical 5,150/10,000-agent counts are lineage/capacity concepts" (README) |
| T12 Recovered Swarm Canon (`16_RECOVERED_SWARM_CANON_T12_2026-09-15.md` §4.2, §5) | "5,150 / 10,000 agents, Legions, Hive Mind → HISTORICAL (capacity lineage only) → **Workers leases**"; "Legions / LegionCard / swarm visual → Cockpit SWARM lens" |
| This build | Implements the Workers-lease route: agents are leases inside Workers, and the mission is a Workers job |

## 3. Architecture

```
staged units   <legion_root>/units/<sha256>.jsonl   (content-addressed; the sha is sealed in the job)
      │
legion-mission@1.0.0  (ExecutionLedger, 11 records per run)
  plan_legion → execute_legion → cognition_pass → seal_final_output
      │
Legion supervisor  (exactly one per legion: supervisor.lock with pid)
  ├─ agents   agt-<sha(legion_id|unit_id)>: lease, attempt, deadline, receipt
  │           N logical agents on a bounded thread pool (max_parallel, ≤256)
  ├─ squads   squad_size agents (default 50) → Merkle-sealed + reduced
  ├─ cohorts  cohort_size squads (default 20) → Merkle-sealed + reduced
  └─ prime    all cohorts → ONE FINAL_OUTPUT.json (sha-sealed) → FINAL_OUTPUT.md
      │
per-run journal  <legion_root>/<legion_id>/journal.jsonl
  hash-chained, verified once on open, O(1) append, fsync every 256 lines + every seal
```

- **Why a per-run journal.** `ExecutionLedger.append` re-verifies the whole
  chain on every write. That is O(n) per append, so ~13,000–20,000 agent
  receipts through it would be quadratic. The journal is bound into the
  ledger by path, tail sha and Merkle root in the `execute_legion` result. The
  ledger stays the mission truth, with **11 records per run**
  (`job.enqueued`, `job.started`, 4 × `step.started` / `step.succeeded`,
  `job.succeeded`).
- **Identity.**
  - `legion_id = sha(spec)`. The spec covers mission, profile, profile
    version, unit manifest sha, shape and limits. It excludes
    `max_parallel`, which is scheduling, not content.
  - `profile_version` is a job param, so a changed profile can never dedupe
    onto an old sealed job.
- **Determinism.**
  - Merkle leaves are content-only (agent, unit, terminal state, result sha,
    error).
  - Reducers always see members in agent_id order.
  - The final sha is therefore independent of parallelism and of unit order.
- **Status.** `<legion_id>/status.json` is rewritten about once a second
  while running (queued, inflight, peak, counts, journal tail).

## 4. Limits

| Limit | Value |
|---|---|
| Agents per legion (default `max_agents`) | 10,000 |
| Absolute ceiling (`LEGION_MAX_AGENTS_ABSOLUTE`) | 25,000 (typed refusal above) |
| Physical parallelism (`max_parallel`) | 1–256 (default 32) |
| Attempts per agent | 1–5 (default 2) |
| Agent deadline | `agent_timeout_s` (default 30 s) |
| `execute_legion` step timeout | 6 h, with 3 attempts (a re-run resumes from the journal) |

## 5. Safety controls

| Control | Mechanism | Court (`tests/test_legion.py`) |
|---|---|---|
| Crash-resume parity | Every state change is journaled. Re-open replays it, terminal agents are never re-run, and sealed squads and cohorts are served from the journal. The final sha is identical to an uninterrupted run | `test_crash_then_resume_matches_uninterrupted_run`, `test_sealed_legion_is_idempotent` |
| Tamper halt | Any sequence, hash or field defect in the journal raises `LedgerCorrupt`, with no repair path. Only a torn (newline-less) physical tail from a crash is discarded, and that discard is itself receipted | `test_tampered_journal_is_a_hard_stop`, `test_torn_tail_is_discarded_and_receipted` |
| Staged-manifest tamper | The units file must hash to its name and match the job's sha and count before any agent runs | `test_staged_manifest_tamper_is_refused`, `test_unit_drift_is_refused` |
| HALT kill switch | If `<legion_root>/<legion_id>/HALT` exists, no new leases are issued, in-flight agents drain, and `legion.halted` is receipted. Removing the file and relaunching resumes the run | `test_kill_switch_halts_then_resumes` |
| Single supervisor | `supervisor.lock` holds the owner pid. A live second opener gets `LegionBusy`; a stale lock (dead pid) is reclaimed | `test_second_live_supervisor_refused` |
| Read-only, non-spawning agents | Profiles must declare `read_only`. Only the supervisor leases, and an agent receives one unit and returns data, so it has no path to spawn agents. All four capabilities are `read_only=True, external=False`, and the registry's external lockout is unchanged | `test_non_read_only_profile_refused`, `test_all_legion_capabilities_are_read_only_and_internal` |
| Retry / timeout / terminal failure | Bounded attempts. A timed-out attempt's late result is discarded (lease epoch). Terminal failures are listed in the final output | `Supervision.*` |
| Ceiling | Above `max_agents` or 25,000 → `LegionRefused` | `test_absolute_ceiling` |
| NOT_AUTHORIZED contact | Every analyst row carries `contact: NOT_AUTHORIZED` (no DNC scrub; outbound is a founder gate). Equity is always `UNKNOWN` without a mortgage/ROD source. `legion_priority` is never the LRO `ranking_key` | `test_brief_laws` |
| IAR note withholding | Cognition is loopback-only (`127.0.0.1` / `localhost`) with `privacy: true` (local llama3.2:3b). Output containing `@`, "phone", "call them" or an equity figure, or longer than 900 chars, is `WITHHELD`. Notes are labelled advisory, and the default is off | `test_cognition_pass_is_loopback_and_validated`, `test_cognition_withholds_banned_output`, `test_cognition_refuses_non_loopback` |
| Stale profile | A job `profile_version` that differs from the installed profile raises `LEGION_SPEC_DRIFT` | `test_stale_profile_version_is_refused` |

## 6. Profiles

- `scale-probe@1.0.0`: synthetic work with injectable faults (`raise_once`,
  `raise_always`, `hang_once`).
- `opportunity-analyst@1.1.0`: one agent per Lead Recipe OS opportunity (v2
  record, compacted; owner names are not staged).
  - Ranks tier first (A/B/C/D, government owners excluded), then by
    `legion_priority`.
  - The OPPORTUNITY_BRIEF holds the top 25, the top 5 per county (parcel id
    where there is no address), the tier and distress prevalence, and the
    evidence unlock map. Each unlock row is labelled
    `NO_SOURCE_REGISTERED` / `PAID_FOUNDER_GATE` / `FREE_OR_PUBLIC`.

## 7. Run commands

```bash
cd ~/neuruh-worktrees/legion-swarm-20260928

# Stage live LRO opportunities (loopback core; bearer from Keychain, never printed).
# lro-core pages at ~45 s / 200 rows on the 8 GB host: ~25 min for 6,487.
LRO_CORE_TOKEN=$(security find-generic-password -s neuruh-lro-core-bearer -a lro-core -w) \
  python3 scripts/run_legion.py stage-lro --runs <run_id>,<run_id>,...

# Launch (reuse a staged manifest by sha; no LRO reload)
python3 scripts/run_legion.py launch --units <manifest_sha> --profile opportunity-analyst \
  --mission <mission> --requested-by founder --max-parallel 32 --cognition-top-k 5

# Scale court
python3 scripts/run_legion.py stage-synthetic --agents 10000 --fault-every 997 --hang-s 3
python3 scripts/run_legion.py launch --units <sha> --profile scale-probe \
  --mission <mission> --requested-by founder --max-parallel 64 --agent-timeout-s 2

python3 scripts/run_legion.py status <legion_id>
python3 scripts/run_legion.py verify <legion_id>
touch ~/.neuruh/legion/<legion_id>/HALT                         # kill switch
python3 -m unittest tests.test_legion                             # court
```

Paths:
- Legion root: `~/.neuruh/legion/`
- Workers ledger: `~/.neuruh/workers/legion/execution-ledger.jsonl`

Row-level output (addresses, parcels) stays under `~/.neuruh/legion/` and is
never committed.

## 8. Open items

- [ ] Founder review and merge of this branch (merge commit, exact SHA).
- [ ] Wire Founder Runtime mission → JGI dispatch → `legion-mission`. Held by
      founder directive; the default `build_registry()` catalog and the live
      FR-dispatch tick are unchanged.
- [ ] Cockpit SWARM lens: add `~/.neuruh/workers/legion/execution-ledger.jsonl`
      to `NEURUH_WORKERS_LEDGER_PATHS`. It is a deploy step and has not been
      done.
- [ ] lro-core opportunity paging is offset-scanned (~45 s / 200 rows). Staging
      is the slow step, not the legion (~2.4–3.8 s for 6,487 agents).
- [ ] Evidence unlock: the top gaps are signals with **no registered source**
      (`absentee.long_hold`, `assemblage.*`, `clock.*`). `batchdata` is
      `PAID_FOUNDER_GATE` and stays off under the no-paid-enrichment rule.
- [ ] Host headroom: swap was ~8.5–8.9 / 9–10 GB during the proof runs.
- [ ] Legion capabilities are loopback-only for cognition. A cloud cognition
      tier would need Governance plus a founder spend grant and is not built.
