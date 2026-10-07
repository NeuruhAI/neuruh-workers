# PROMOTION EXECUTOR (nrh.git.promotion.v1)

Mission `NEURUH-APPROVAL-AND-PROMOTION-SEAMS-20260902`. Closes the parking
defect where a passed candidate stayed DRAFT/COURTED_CANDIDATE because
`promotion.ready` had no mechanical consumer.

## Contract — all four, or refuse

```
Sentinel PASS (sealed independent_sentinel_court, candidate_sha == head, hash-bound)
+ Governance ALLOW (fresh /v2/authorize: action_class merge, target pull_request:<repo>#<n>,
                    branch = base, precondition_hash over heads, nonce spent once; never cached)
+ Honey Pot custody (promotion.ready record for repo/branch/head/PR; PR OPEN, head == courted head)
+ head unchanged (remote branch == head, remote base == base-at-court; re-checked after
                  authorization; enforced again by gh pr merge --match-head-commit)
→ PROMOTION_MERGE (true merge commit; draft made ready under PR_READY)
→ verify: base after contains head, contains base-before, equals/contains merge sha; PR merged
→ deploy.requested child event when deploy_required (no deploy performed here)
```

Ownership unchanged: Honey Pot = custody (this lives in `git_custodian/`),
Sentinel = court (verified, never produced), Bob = orchestration (emits
`promotion.requested`; never decides the merge), Governance = authority (asked
every time). `AUTO_MERGE` stays forbidden; `PROMOTION_MERGE` is granted only
by `founder.promotion.v1` and does not exist without the four gates.

## Surfaces

- `neuruh_workers/git_custodian/promotion.py` — contract, verifiers, `PromotionExecutor`,
  `LoopbackGovernanceAuthorize` (127.0.0.1:8848 only).
- `neuruh_workers/git_custodian/gh_backend.py` — `GhPRAdapter` (`gh pr view/ready/merge --merge
  --match-head-commit`), `GhCIAdapter` (`gh api` check-runs + status). No squash/rebase/admin/force.
- `neuruh_workers/switchboard.py` — `promotion_executor=` consumer for `promotion.requested`;
  without an executor the event is unroutable and nothing merges.
- `scripts/run_promotion.py` — one packet end to end (`--dry-run` stops before Governance).
- Receipts: `promotion.requested/authorized/executed/verified/refused`, `deploy.requested`.

## Court (`tests/test_promotion_executor.py`, 49 cases; full suite 115/115)

Golden path (mocks) · deploy child · idempotent replay (asks nothing, merges
nothing) · secret redaction · HEAD_DRIFT / BASE_DRIFT before Governance ·
head moved after authorization (last check) · PR head moved (match-head) ·
every Sentinel refusal (FAIL never inverted, open exceptions, bare string,
head/mission/hash mismatch, non-independent) · custody refusals · CI not PASS ·
Governance founder_required (prompt surfaced) / deny / unreachable / forged
nonce / different action / claims execution / no receipt · authority refusals
(founder.candidate.v1 cannot promote; AUTO_MERGE in `allowed` is invalid) ·
contract refusals (base not canonical, protected candidate, write-ban repo,
`caller_approved` unknown field, head == base) · MERGED_UNVERIFIED reported and
never retried · **real git bare-remote court** (real merge commit, real
ancestry verify, replay PROVEN, real head drift refused) · Switchboard hop
(routes, acks, deadletters unknown mission, never merges without executor) ·
`gh` adapter argv contract.

## Live status

Governance live (`:8848`) knows no `git_custodian` component and refuses all
non-R0 mutation (`MUTATION_NOT_RATIFIED` — the trust validator forbids
ratification). Live promotion is therefore BLOCKED_EXTERNAL on the founder's
Governance changes listed in autonomous-governance-core
`GOVERNANCE_V2_PROMOTION_SEAMS.md`. The executor's behaviour under those
answers (refuse, surface, merge nothing) is courted above.
