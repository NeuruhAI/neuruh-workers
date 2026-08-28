# Neuruh Autonomous Workflows v1

The `neuruh_workers` package is the worker and workflow layer that lets Neuruh
perform **governed multi-step work**: declarative workflows executed step by
step against a hash-chained, append-only receipt ledger.

## Architecture

```
WorkflowDefinition (declarative: steps, retry, timeout, approval gates)
        │  sealed into the enqueue receipt (spec + spec_sha)
        ▼
JobQueue ── deterministic job IDs ──► ExecutionLedger (append-only JSONL,
        │                              hash-chained, verified on every open
        ▼                              and every append; state is REPLAYED,
WorkflowRunner ── receipts per         never stored)
        │         transition
        ▼
WorkerRegistry (capability declarations: read_only / external)
```

- **Deterministic job IDs** — `job-<sha256(workflow|version|params|dedupe|dry_run)>`;
  enqueueing identical work is a receipted no-op.
- **Idempotent steps** — a step that has a `step.succeeded` receipt is never
  re-executed; its result is served from the ledger.
- **Retry + backoff** — per-step policy, deterministic exponential delays;
  the budget counts *receipted failures*, so a crashed attempt (started, no
  outcome) is recoverable rather than budget-burning.
- **Timeout** — per-step wall-clock limit, receipted as `step.timeout`.
- **Pause/resume** — cooperative at step boundaries; paused jobs refuse to run
  until resumed. Crash recovery uses the same replay path (`job.recovered`).
- **Human approval gates** — a gated step blocks the job
  (`BLOCKED_ON_APPROVAL`) and emits a challenge bound to the exact
  job+step+params content. Approval requires the right approver (never the
  requester), an unused nonce, the exact challenge and the exact params hash.
  Every rejected attempt is receipted. Denial fails the job terminally.
  **Dry-run does not bypass gates.**
- **Dry-run** — executes `plan()` instead of `execute()`; results are stamped
  `dry_run`. A dry-run is a distinct job from the real run.
- **States** — `QUEUED, RUNNING, PAUSED, BLOCKED_ON_APPROVAL, SUCCEEDED,
  FAILED, CANCELLED`. Closed vocabulary; unknown receipt types fail closed.
- **No silent fallback** — corrupt ledger, unknown capability, spec drift
  between enqueue and execution, invalid input: all are typed refusals.
- **External actions are structurally disabled** — an `external` capability
  cannot be registered without an operator authorization sentinel, and the v1
  runner refuses to execute external capabilities unconditionally (receipted
  `external.action_denied`). No email, SMS, contracts, purchases or
  production writes exist in this layer.
- **Mother/AXON seam** — `OrganRegistrationSeam` accepts typed declarations
  (loopback endpoint + exact 40-hex source SHA) and receipts them
  `PENDING_AUTHORIZATION`; activation refuses unconditionally in v1.
- **Switchboard V1** — a loopback wrap of the same ledger + queue. It polls
  the JSONL file with an idempotent cursor and, when a sealed Prime receipt
  (`prime.receipt_sealed`, the V1 stand-in for the `receipt.sealed` target
  contract) appears, enqueues a `sentinel-verify` job. A Sentinel PASS
  (`sentinel.verification_pass`, or `job.succeeded` on that sentinel job)
  enqueues a payload-only `git-custody` job. Switchboard writes its own
  `switchboard.job_created` / `switchboard.job_already_existed` receipts.
  It does not activate organ seams, does not stand up Ether, and does not
  push git. See `neuruh_workers/switchboard.py` (`EVENT_IMPLEMENTATION_STATUS`).

## Sample read-only workflows

| Workflow | Steps | Notes |
|---|---|---|
| `property-research` | normalize → assemble queries → dossier | produces a research *plan*; no external fetches |
| `field-pin-enrichment` | validate → derive geo → enrich | pure geo derivation |
| `parcel-intelligence-assembly` | collect → cross-reference → report | joins provided signals to a parcel |
| `daily-opportunity-brief` | gather → rank → draft → **finalize (approval gate)** | distribution explicitly not authorized |
| `world-emitter-refresh-preview` | load → plan → preview | `no_write: true`; preview only |
| `cockpit-health-snapshot` | collect → evaluate → render | worst-of health rollup |

## CLI

```bash
python3 scripts/run_workflow.py list
python3 scripts/run_workflow.py enqueue cockpit-health-snapshot \
    --params '{"components":[{"name":"qdrant","status":"healthy"}]}' \
    --requested-by operator
python3 scripts/run_workflow.py run <job-id>
python3 scripts/run_workflow.py approve <job-id> finalize_brief \
    --approver founder --nonce <unique>
python3 scripts/run_workflow.py verify-ledger
```

## Tests

```bash
python3 -m unittest discover -s tests -t .
python3 scripts/secret_scan.py
```

Covers: unit (definitions/ledger/queue/runner/switchboard), replay +
idempotency, crash recovery, adversarial approvals, external-action lockout,
seam refusal, Switchboard V1 (Prime sealed receipt → Sentinel job, no
double-enqueue, unsealed ignore, replay cursor, path refusal), and a secret
scan of every tracked file.
