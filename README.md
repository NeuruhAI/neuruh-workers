# Neuruh Workers 1-3

Notion API automation scripts for the Neuruh IP Registry.

## Workers

- **Worker 1 — IP Logger** (`worker_1_ip_logger.py`): Upsert assets into IP Registry
- **Worker 2 — Build Status** (`worker_2_build_status.py`): Update build status + append build log
- **Worker 3 — Context Loader** (`worker_3_context_loader.py`): Load active session context at session start

## Setup

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
# Edit .env with your Notion token
```

## Usage

```bash
# Get session context
python worker_3_context_loader.py

# Log new IP
python worker_1_ip_logger.py --asset-id AI-CB-010 --title "New Framework" --vertical "AI Platform Core" --status "In Progress"

# Update build status
python worker_2_build_status.py --asset-id AI-CB-010 --status "Built" --log "Phase 1 complete"
```

## Autonomous Workflows v1

The `neuruh_workers` package adds governed multi-step workflows: declarative
definitions, a hash-chained append-only receipt ledger, deterministic job IDs,
idempotent steps, retry/backoff, timeouts, pause/resume, human approval gates,
dry-run, and six read-only sample workflows. External actions are structurally
disabled. See `WORKFLOWS.md`.

## Integration with AXON

See `axon_integration.ts` for registering workers as AXON Phase 2 tools.

## IP Registry DB ID

`3c48ca3132574937a3f3d729a93eb578`

---

*Neuruh LLC — Sovereign AI Execution Infrastructure*
