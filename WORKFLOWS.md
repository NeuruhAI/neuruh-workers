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
- **Git Custodian V1** — a separate governed worker (`neuruh_workers.git_custodian`)
  that may perform only `FEATURE_BRANCH_PUSH`, `PR_CREATE`, `PR_UPDATE`, and
  `CI_READ` after a structured independent court (`INDEPENDENT_COURT=PASS` with
  mission/stage/source HEAD/candidate evidence) and founder-candidate authority.
  It is a custody bridge to a reviewable candidate (remote feature branch + PR +
  CI state + promotion-ready *evidence*). It is not a merge bot, deploy bot,
  release approver, or Sentinel. Force push, direct main, auto-merge, tags,
  production deploys, and history rewrite are structurally forbidden. The global
  external-action lockout is unchanged.
- **Mother/AXON seam** — `OrganRegistrationSeam` accepts typed declarations
  (loopback endpoint + exact 40-hex source SHA) and receipts them
  `PENDING_AUTHORIZATION`; activation refuses unconditionally in v1.

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

Covers: unit (definitions/ledger/queue/runner), replay + idempotency,
crash recovery, adversarial approvals, external-action lockout, seam
refusal, Switchboard first seam (observe → court job; independent court
evidence → honey-pot artifact; no fake Sentinel PASS), Git Custodian V1
(consumes Switchboard `candidate.push.requested` / `nrh.event.v1`; T07
golden idempotency; bare-remote fail-closed matrix; mock PR adapter), and
a secret scan of every tracked file.

Canonical CI: `.github/workflows/ci.yml` (`workers-ci`) runs unittest
discovery + `secret_scan.py` on Python 3.11/3.12/3.13 against the exact
HEAD. `.github/workflows/switchboard-first-seam.yml` is the first-seam
court retained from SWITCHAROO.

## Switchboard first seam (SWITCHAROO)

A thin wrap around the existing ledger + job queue.  A sealed PRIME receipt
file is observed once, the envelope is validated (Switchboard does **not**
declare PASS), and exactly one Sentinel `court.requested` **job artifact** is
enqueued.  Sentinel court is independent: `SENTINEL_CLOSURE` / `PRIME_VERDICT`
on the PRIME receipt are not PASS, and reconstruction fixtures are refused.
`court.passed` is accepted only from independent Sentinel evidence.  That
event then materialises exactly one Honey Pot / Git Custodian
`candidate.push.requested` artifact (`nrh.event.v1`).  Switchboard itself
does not git-push (artifact `git_push=BLOCKED`).  Sentinel model execution
is `BLOCKED`.  `OrganRegistrationSeam.activate()` still refuses.
Loopback/file observation only.  `HUMAN_CLIPBOARD_REQUIRED=NO`.

The live T07 pair is checked in at `tests/fixtures/t07/`: `T07_RECEIPT.txt`
(operator receipt, including the clipboard defect
`EVENT_IMPLEMENTATION_STATUS=NOT_PROVEN`) is observed through
`court.requested` only.  `T07_OBSERVER_COURT.json` is a reconstruction and is
**refused** as court.passed.  Honey Pot expected identity (existing axon PR
#4) is proven only after independent court evidence.  This wrap does not
mutate `neuruh-axon` and does not start Ether.

```bash
python3 scripts/run_switchboard.py tick --receipts <dir> --root .neuruh/switchboard
```

## Git Custodian V1 (REL-GIT-001 / HONEY POT)

Custody worker. It **consumes** Switchboard `candidate.push.requested`
(`nrh.event.v1` via `neuruh_workers.events.validate_envelope`); it does not
copy or reimplement the envelope and does not mint a second event bus.
`SWITCHAROO_INTEGRATED=YES`. `STRUCTURED_SENTINEL_PASS_REQUIRED=YES` — a
bare `"PASS"` string is not a court.

Responsibility ends at: remote feature branch + PR + CI state +
promotion-ready **evidence**. `promotion.ready` is not merge authority.
Writes to `NeuruhAI/neuruh-axon` remain write-banned (T07 observe-only).

```bash
python3 -m unittest discover -s tests -t . -p 'test_git_custodian*.py'
```

Allowed after structured independent court + founder-candidate authority:
`FEATURE_BRANCH_PUSH`, `PR_CREATE` / `PR_UPDATE`, `CI_READ`.
Structurally forbidden: direct main, force push, auto-merge, tags,
production deploy, history rewrite.
