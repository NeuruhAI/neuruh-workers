# FOUNDER_RUNTIME_TO_WORKERS_DISPATCH_BRIDGE

**Mission:** NEURUH-AUTONOMY-ACTIVATION-001  
**Repo:** `NeuruhAI/neuruh-workers`  
**PR:** #5  
**Role:** consume-only dispatch bridge. Not a second Founder Runtime.

recipe-engine Founder Runtime (`FR_API=:19090`, `API_VERSION=fr-api-v1`) remains
mission-state authority. This package does **not** own mission persistence,
mission identity, approval state, decision-plan authority, another scheduler,
or another model router. Ranker stays deleted (no `score_candidate`, no `$2/min`).

## Bind path

```text
Founder Runtime APPROVED/QUEUED
  → Workers JobQueue
  → ExecutionLedger
  → receipt.sealed
  → SWITCHAROO
  → independent Sentinel
  → HONEY POT when code custody is required
  → child completion
  → JobQueue.resume(parent)
  → Founder Runtime state/evidence update
```

`PARENT_RESUME` is proven by `JobQueue.resume` / `job.recovered` on an existing
parent job. This PR does not add a mission watcher.

## What this consumes from main

- live `neuruh_workers.switchboard` / `neuruh_workers.events` (fail closed if missing)
- live `neuruh_workers.git_custodian` / HONEY POT (fail closed if missing)
- canonical CI (Python 3.11/3.12/3.13, unittest discovery, `scripts/secret_scan.py`)

This PR does not vendor or copy `switchboard.py`, `events.py`, or `git_custodian/`.

## What this refuses

- a second Founder Runtime
- Ether
- AXON writes (`AXON_WRITE_BANNED`; `NeuruhAI/neuruh-axon` stays write-banned)
- minting `court.requested` as if this PR were Switchboard
- fake Sentinel PASS (CI job named `court` is not Sentinel; `SENTINEL_MODEL_EXECUTION=BLOCKED`)
- local USD ranker / model router

## Court

```bash
python3 -m unittest discover -s tests -t .
python3 scripts/secret_scan.py
python3 scripts/run_jgi_demo.py --ledger /tmp/jgi-demo.jsonl
```
