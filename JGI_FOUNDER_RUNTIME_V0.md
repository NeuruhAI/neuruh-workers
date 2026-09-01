# JGI Founder Runtime v0 — executable candidate

**Mission:** JGI-FOUNDER-RUNTIME-V0-001  
**Repo:** `NeuruhAI/neuruh-workers`  
**State:** candidate PR only; not canonical/live until convergence + court.  
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

## What v0 actually does

`MissionIntent` + candidate paths enter the Founder Runtime. The runtime:

1. ranks candidate execution paths by expected economic utility;
2. prefers cheaper paths when value/success are equivalent;
3. compiles a <=4 KiB pointer-heavy context packet instead of replaying a chat transcript;
4. stops true founder-only gates (money, external message, production mutation, irreversible action, private-IP risk);
5. refuses to self-authorize L2/L3/L4 inference and returns `REASONING_BROKER_REQUIRED`;
6. for safe L0/L1 read-only work, reuses the existing queue/runner and **autonomously enqueues + executes** the workflow;
7. relies on the existing hash-chained ledger for execution receipts and idempotent replay;
8. repeated identical dispatch resolves to the same job and does not re-run completed work.

## Autonomy boundary

V0 is already autonomous for deterministic read-only workflows. It is intentionally **not** autonomous for
consequential actions.

```text
Jeramie intent
  -> candidate paths
  -> cheapest/value-aware route
  -> compact context
  -> founder gate? -------- yes -> stop with exact reasons
  -> model required? ------ yes -> reasoning broker required
  -> existing read-only workflow
  -> deterministic JobQueue
  -> WorkflowRunner
  -> hash-chained receipts
  -> success / approval gate / typed failure
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

Only L0/L1 execute in this package. Model inference must be delegated to a separately authorized reasoning
broker. The Revenue Factory candidate already implements an economic model-call seam with single-use authorization,
cache, no-inference receipts and reasoning-debt compilation; JGI should consume that seam after architecture
convergence rather than rebuild it.

## What remains before true continuous autonomy

1. **Architecture binding:** court which repo owns mission supervision; this PR is a candidate because `neuruh-workers`
   already owns governed multi-step work.
2. **Switchboard integration:** consume the pending receipt->court seams instead of manual clipboard handoffs.
3. **Reasoning broker binding:** route L2/L3/L4 to IAR/Revenue Factory under single-use authorization and budget.
4. **Canonical AXON binding:** only after Governance/Sentinel allows an action; never invent a second executor.
5. **Mission recovery watcher:** stale/nonterminal missions are resumed from receipts automatically.
6. **Execution Genome:** accepted outcomes compile repeated expensive reasoning into cheaper deterministic rules.
7. **Offer Foundry adapter:** customer problem -> public-safe capability search -> Recipe Engine -> bounded offer -> governed workflow.
8. **Cockpit projection:** only `NOW / MONEY / BLOCKED / LEARNED` founder surfaces.

## Court

The new tests prove:

- safe deterministic mission runs through the existing workflow engine to `job.succeeded` with zero manual handoff;
- identical dispatch is idempotent and does not execute twice;
- production mutation is founder-gated before enqueue;
- frontier reasoning is routed but not self-authorized;
- an equivalent deterministic path beats a more expensive model path;
- context packets are bounded and contain no transcript/chat field.

Run:

```bash
python3 -m unittest discover -s tests -t .
python3 scripts/secret_scan.py
```

## Non-claims

- not AGI in the scientific sense
- not deployed
- not merged
- not canonical owner yet
- no model call
- no spend
- no outbound message
- no production write
- no replacement for AXON, Governance, Mother, Recipe Engine, Switchboard, Git Custodian, Outcome Record, or Cockpit
