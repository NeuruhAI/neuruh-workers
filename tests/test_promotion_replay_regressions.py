"""RED regressions for the promotion replay defect the adversarial review confirmed.

C. A promotion must never report success merely because the pull request is
   already merged. Before an already-merged PR can be receipted as a proven
   promotion, every coordinate of the original authorization must still match:
   repo, PR number, candidate SHA, canonical base, court hash, Honey Pot custody,
   and the resulting merge ancestry. Anything else is MERGED_UNVERIFIED.
"""
from __future__ import annotations

import unittest

from neuruh_workers.git_custodian import GitCustodianRefused
from tests.test_promotion_executor import (
    BASE, BRANCH, HEAD, MERGE, MISSION, OTHER, REPO,
    RecordedGovernance, _events, _stack, custody, promotion, sentinel_record,
)


class PromotionReplayRegressions(unittest.TestCase):
    def _refuses(self, executor, body, reason):
        with self.assertRaises(GitCustodianRefused) as ctx:
            executor.run(body)
        self.assertEqual(ctx.exception.reason, reason, ctx.exception)
        return ctx.exception

    # --- the head nobody courted ---------------------------------------- #
    def test_merged_pr_whose_head_is_not_the_courted_head_is_refused(self):
        """Someone pushed past the court and merged by hand. Not a promotion."""
        executor, git, pr, gov, stack, body = _stack(
            self, merged=True, pr_head=OTHER,
            ancestry={(HEAD, MERGE), (BASE, MERGE), (OTHER, MERGE)},
        )
        exc = self._refuses(executor, body, "HEAD_DRIFT")
        self.assertEqual(gov.calls, [], "Governance is never asked to bless a merge already done")
        self.assertEqual(pr.promote_calls, [])
        self.assertEqual(_events(executor)[-1], "promotion.refused")
        self.assertIn(OTHER, str(exc.detail))

    def test_merged_pr_on_a_different_branch_or_base_is_refused(self):
        for field, value, reason in (("head_branch", "feat/someone-else", "PR_MISMATCH"),
                                     ("base_branch", "develop", "PR_MISMATCH")):
            with self.subTest(field=field):
                executor, git, pr, gov, stack, body = _stack(self, merged=True)
                setattr(pr.prs[0], field, value)
                self._refuses(executor, body, reason)
                self.assertEqual(gov.calls, [])

    def test_merged_pr_belonging_to_another_mission_is_refused(self):
        executor, git, pr, gov, stack, body = _stack(self, merged=True)
        pr.prs[0].mission_id = "SOME_OTHER_MISSION"
        self._refuses(executor, body, "PR_MISMATCH")
        self.assertEqual(gov.calls, [])

    # --- the merge must actually be in the canonical base ---------------- #
    def test_merged_pr_whose_merge_is_not_in_the_base_is_merged_unverified(self):
        executor, git, pr, gov, stack, body = _stack(self, merged=True, ancestry=set())
        exc = self._refuses(executor, body, "PR_ALREADY_MERGED_UNVERIFIED")
        self.assertIn("MERGED_UNVERIFIED", str(exc).upper().replace("_", "_"))

    def test_merged_pr_with_no_merge_commit_recorded_is_refused(self):
        """A merged PR that cannot name its merge commit proves nothing."""
        executor, git, pr, gov, stack, body = _stack(self, merged=True)
        pr.prs[0].merge_commit_sha = ""
        self._refuses(executor, body, "PR_ALREADY_MERGED_UNVERIFIED")

    def test_merged_pr_whose_base_moved_off_the_authorized_base_is_refused(self):
        """The canonical base must still descend from the base the court saw."""
        executor, git, pr, gov, stack, body = _stack(
            self, merged=True, remote_heads={"main": OTHER},
            ancestry={(HEAD, OTHER)},   # head is in, but the authorized base is not
        )
        self._refuses(executor, body, "PR_ALREADY_MERGED_UNVERIFIED")

    # --- the gates still run on the replay path -------------------------- #
    def test_replay_still_requires_a_clean_sentinel_and_matching_custody(self):
        failed = sentinel_record(verdict="FAIL")
        executor, git, pr, gov, stack, body = _stack(
            self, merged=True, sentinel=failed,
            promotion_body=promotion(sentinel=failed))
        self._refuses(executor, body, "SENTINEL_NOT_PASS")

        rec = custody(head=OTHER)
        executor2, git2, pr2, gov2, stack2, body2 = _stack(
            self, merged=True, custody_rec=rec,
            promotion_body=promotion(custody_rec=rec))
        self._refuses(executor2, body2, "CUSTODY_MISMATCH")

    # --- a legitimate replay still proves itself ------------------------- #
    def test_legitimate_replay_after_a_governed_merge_is_still_proven(self):
        executor, git, pr, gov, stack, body = _stack(self)
        first = executor.run(body)
        second = executor.run(body)
        self.assertEqual(first["IDEMPOTENCY"], "APPLIED")
        self.assertEqual(second["IDEMPOTENCY"], "PROVEN")
        self.assertEqual(second["MERGE_SHA"], MERGE)
        self.assertEqual(len(gov.calls), 1)
        self.assertEqual(len(pr.promote_calls), 1)

    def test_replay_of_a_deploy_required_promotion_still_requests_the_deploy(self):
        """A crash between the merge and the deploy child must not lose the child."""
        executor, git, pr, gov, stack, body = _stack(self)
        body = dict(body, deploy_required=True, deploy_target="vercel:fixture-preview")
        executor.run(body)
        replay = executor.run(body)
        self.assertEqual(replay["IDEMPOTENCY"], "PROVEN")
        self.assertEqual(replay["DEPLOY_CHILD_REQUESTED"], "YES")
        self.assertEqual(_events(executor)[-1], "deploy.requested")


if __name__ == "__main__":
    unittest.main()
