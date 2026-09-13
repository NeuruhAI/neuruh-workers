"""W1 composition root: the EXISTING custodian + executor bound in run_switchboard.

Default OFF: with the env unset the production board keeps its first-seam
shape (three consumers + the adjudicator).  With the switches on, the board
registers the two Honey Pot consumers and routes candidate.push.requested to
GitCustodian (real bare remote, mock PR) and promotion.requested to
PromotionExecutor (recorded git, recorded Governance).  No network, no real
push, no real merge.
"""
from __future__ import annotations

import importlib.util
import json
import os
import shutil
import tempfile
import unittest
from argparse import Namespace
from unittest import mock

from neuruh_workers.contracts import content_sha
from neuruh_workers.events import EventType
from neuruh_workers.git_custodian import (
    FOUNDER_CANDIDATE_V1,
    FOUNDER_PROMOTION_V1,
    GitCustodian,
    MockCIAdapter,
    MockPRAdapter,
    PromotionExecutor,
    build_promotion_request_event,
)
from neuruh_workers.git_custodian.court import verify_court
from neuruh_workers.switchboard import ADJUDICATE_CONSUMER_ID, Switchboard
from tests.canonical_fixture import OfflineCanonical
from tests.git_custodian_fixtures import local_candidate, local_court, make_seeded_pair, sha
from tests.helpers import fixed_clock, make_stack
from tests.test_git_custodian_switchboard_bind import MISSION as BIND_MISSION, _receipt_body
from tests.test_promotion_executor import (
    MISSION as PROMO_MISSION,
    RecordedGovernance,
    custody,
    promotion,
    sentinel_record,
)
from tests.test_switchboard_first_seam import _independent_evidence, _write_receipt

_SCRIPT = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "scripts", "run_switchboard.py")
_spec = importlib.util.spec_from_file_location("run_switchboard_under_test", _SCRIPT)
rs = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(rs)

FIRST_SEAM_CONSUMERS = {
    "switchboard.court-on-sealed.v1",
    "switchboard.push-on-court-passed.v1",
    "switchboard.remediate-on-court-failed.v1",
    ADJUDICATE_CONSUMER_ID,
}
CUSTODY_CONSUMER = "git-custodian.consume-push-requested.v1"
PROMOTION_CONSUMER = "git-custodian.consume-promotion-requested.v1"
ENV_KEYS = (rs.GIT_CUSTODIAN_ENV, rs.PROMOTION_EXECUTOR_ENV,
            rs.GOVERNANCE_URL_ENV, rs.RECORD_DIRS_ENV)


def _clean_env(**on):
    env = {k: v for k, v in os.environ.items() if k not in ENV_KEYS}
    env.update(on)
    return mock.patch.dict(os.environ, env, clear=True)


class CompositionShapeTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="neuruh-w1-")
        self.addCleanup(lambda: shutil.rmtree(self.tmp, ignore_errors=True))
        self.root = os.path.join(self.tmp, "state")
        self.receipts = os.path.join(self.tmp, "receipts")
        os.makedirs(self.receipts, exist_ok=True)
        self.args = Namespace(root=self.root, receipts=self.receipts)

    def _ids(self, board):
        return {c.consumer_id for c in board.consumers}

    def test_env_unset_keeps_the_first_seam_shape(self):
        with _clean_env():
            board = rs._board(self.args)
        self.assertEqual(self._ids(board), FIRST_SEAM_CONSUMERS)
        self.assertIsNone(board.git_custodian)
        self.assertIsNone(board.promotion_executor)

    def test_env_off_values_do_not_bind(self):
        with _clean_env(**{rs.GIT_CUSTODIAN_ENV: "0", rs.PROMOTION_EXECUTOR_ENV: "no"}):
            board = rs._board(self.args)
        self.assertEqual(self._ids(board), FIRST_SEAM_CONSUMERS)

    def test_env_on_binds_the_existing_organs(self):
        with _clean_env(**{rs.GIT_CUSTODIAN_ENV: "1", rs.PROMOTION_EXECUTOR_ENV: "1",
                           "NSV_INVOKED_SHA": "a" * 40}):
            board = rs._board(self.args)
        self.assertEqual(self._ids(board), FIRST_SEAM_CONSUMERS | {CUSTODY_CONSUMER, PROMOTION_CONSUMER})
        self.assertIsInstance(board.git_custodian, GitCustodian)
        self.assertIsInstance(board.promotion_executor, PromotionExecutor)
        # Bound to local records + the one founder policy each, like run_promotion.py.
        self.assertIsInstance(board.git_custodian.court_store, rs.FileRecordStore)
        self.assertEqual(board.git_custodian.authority_store.get("any"), FOUNDER_CANDIDATE_V1)
        self.assertIsInstance(board.promotion_executor.court_store, rs.FileRecordStore)
        self.assertEqual(board.promotion_executor.authority_store.get("x"), FOUNDER_PROMOTION_V1)
        self.assertEqual(board.promotion_executor.source_sha, "a" * 40)
        self.assertEqual(board.promotion_executor.governance.url, rs.GOVERNANCE_AUTHORIZE_URL)
        self.assertEqual(rs.record_dirs(self.root), [os.path.join(os.path.abspath(self.root), "courts")])

    def test_custodian_alone_binds_only_custody(self):
        with _clean_env(**{rs.GIT_CUSTODIAN_ENV: "1"}):
            board = rs._board(self.args)
        self.assertEqual(self._ids(board), FIRST_SEAM_CONSUMERS | {CUSTODY_CONSUMER})
        self.assertIsNone(board.promotion_executor)

    def test_governance_url_env_is_loopback_only_and_honoured(self):
        with _clean_env(**{rs.PROMOTION_EXECUTOR_ENV: "1",
                           rs.GOVERNANCE_URL_ENV: "http://127.0.0.1:8848/v2/authorize",
                           "NSV_INVOKED_SHA": "b" * 40}):
            board = rs._board(self.args)
        self.assertEqual(board.promotion_executor.governance.url, "http://127.0.0.1:8848/v2/authorize")


class FileRecordStoreTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="neuruh-w1-store-")
        self.addCleanup(lambda: shutil.rmtree(self.tmp, ignore_errors=True))
        self.courts = os.path.join(self.tmp, "courts")
        os.makedirs(self.courts)
        self.store = rs.FileRecordStore([self.courts])

    def _write(self, name, obj):
        path = os.path.join(self.courts, name)
        with open(path, "w", encoding="utf-8") as fh:
            json.dump(obj, fh)
        return path

    def test_resolves_bare_name_absolute_and_file_uri(self):
        path = self._write("court-1.json", {"independent_court": "PASS"})
        for ref in ("court-1", "court-1.json", path, "file://" + path):
            self.assertEqual(self.store.get(ref), {"independent_court": "PASS"}, ref)

    def test_missing_traversal_and_non_object_resolve_to_nothing(self):
        self._write("list.json", [1, 2])
        self._write("bad.json", {"a": 1})
        with open(os.path.join(self.courts, "broken.json"), "w") as fh:
            fh.write("{not json")
        for ref in ("", "nope", "../bad", "./bad", "sub/bad", "list", "broken",
                    "court://local/bare-remote", "sentinel://court/synthetic-1"):
            self.assertIsNone(self.store.get(ref), ref)

    def test_founder_policy_store_presents_one_policy(self):
        store = rs.FounderPolicyStore(FOUNDER_PROMOTION_V1)
        self.assertEqual(store.get("authority://anything"), FOUNDER_PROMOTION_V1)
        self.assertIsNone(store.get(""))
        self.assertIsNone(store.get(None))


class CustodyRouteTests(unittest.TestCase):
    """candidate.push.requested -> the composed GitCustodian pushes to a bare remote."""

    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="neuruh-w1-custody-")
        self.addCleanup(lambda: shutil.rmtree(self.tmp, ignore_errors=True))
        self.pair = make_seeded_pair(self.tmp)
        self.court = local_court(self.pair, mission_id=BIND_MISSION)
        state = os.path.join(self.tmp, "switchboard")
        courts = os.path.join(state, "courts")
        os.makedirs(courts, exist_ok=True)
        # The court record lives where the board seals courts; the candidate
        # names it by bare name and binds it by hash, exactly as verify_court expects.
        self.court_path = os.path.join(courts, "bare-remote-court.json")
        with open(self.court_path, "w", encoding="utf-8") as fh:
            json.dump(self.court, fh, sort_keys=True)
        self.candidate = local_candidate(
            self.pair, court=self.court, mission_id=BIND_MISSION,
            court_receipt_ref="bare-remote-court", court_receipt_hash=content_sha(self.court))
        self.pr = MockPRAdapter()
        stack = make_stack(self)
        self.worker = rs.compose_git_custodian(
            state, pr=self.pr, ci=MockCIAdapter(), ledger=stack.ledger)
        receipts = os.path.join(self.tmp, "receipts")
        os.makedirs(receipts, exist_ok=True)
        self.canonical = OfflineCanonical(self.pair["work"], self.pair["remote"])
        self.board = Switchboard(
            state, receipts_dir=receipts, known_missions=frozenset({BIND_MISSION}),
            git_custodian=self.worker, canonical_observer=self.canonical.observe)

    def test_composed_custodian_consumes_and_pushes(self):
        self.assertEqual(verify_court(self.worker.court_store.get("bare-remote-court"), self.candidate),
                         self.court)
        _write_receipt(self.board.receipts_dir, "BIND_RECEIPT.txt",
                       _receipt_body(self.pair, self.candidate))
        self.board.tick()
        sealed = self.board.events(EventType.RECEIPT_SEALED)[-1]
        court = self.board.events(EventType.COURT_REQUESTED)[-1]
        self.board.record_court_outcome(
            passed=True,
            court_job_id=court["payload_ref_or_payload"]["court_job_id"],
            mission_id=sealed["mission_id"], plan_id=sealed["plan_id"], task_id=sealed["task_id"],
            correlation_id=sealed["correlation_id"], causation_id=court["event_id"],
            source_head=sealed["source_head"], authority_ref=sealed["authority_ref"],
            source_repo=sealed["source_repo"],
            receipt_ref=sealed["payload_ref_or_payload"]["receipt_ref"],
            evidence=_independent_evidence(self.board, sealed, court, passed=True))
        self.board.tick()  # court.passed -> candidate.push.requested
        self.assertEqual(len(self.board.events(EventType.CANDIDATE_PUSH_REQUESTED)), 1)
        self.board.tick()  # composed git-custodian consumer
        self.assertEqual(sha(self.pair["remote"], f"refs/heads/{self.pair['feature_branch']}"),
                         self.pair["feature_sha"])
        self.assertEqual(self.pr.creates, 1)
        self.assertTrue(self.pr.prs[0].draft)
        self.assertFalse(self.worker.git.force_used)
        self.board.tick()  # idempotent
        self.assertEqual(self.pr.creates, 1)


class PromotionRouteTests(unittest.TestCase):
    """promotion.requested -> the composed PromotionExecutor merges only on ALLOW."""

    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="neuruh-w1-promotion-")
        self.addCleanup(lambda: shutil.rmtree(self.tmp, ignore_errors=True))
        self.state = os.path.join(self.tmp, "state")
        self.courts = os.path.join(self.state, "courts")
        os.makedirs(self.courts, exist_ok=True)
        receipts = os.path.join(self.tmp, "receipts")
        os.makedirs(receipts, exist_ok=True)
        self.receipts = receipts

    def _records(self):
        from tests.test_promotion_executor import HEAD
        sentinel = sentinel_record(head=HEAD)
        custody_rec = custody(head=HEAD)
        for name, obj in (("sentinel-synthetic-1.json", sentinel), ("custody-synthetic-1.json", custody_rec)):
            with open(os.path.join(self.courts, name), "w", encoding="utf-8") as fh:
                json.dump(obj, fh, sort_keys=True)
        body = promotion(sentinel=sentinel, custody_rec=custody_rec,
                         sentinel_receipt_ref="sentinel-synthetic-1",
                         custody_receipt_ref="custody-synthetic-1")
        return body

    def _executor(self, gov):
        from tests.test_promotion_executor import _stack
        # Recorded git/PR/CI from the executor's own suite; only the stores,
        # policy and governance binding come from the composition root.
        stub, git, pr, _gov, stack, _body = _stack(self)
        executor = rs.compose_promotion_executor(
            self.state, pr=pr, ci=stub.ci, governance=gov, ledger=stack.ledger, source_sha="c" * 40)
        executor.git = git
        executor.clock = fixed_clock()
        return executor, pr

    def test_composed_executor_routes_and_merges_on_allow(self):
        gov = RecordedGovernance()
        executor, pr = self._executor(gov)
        board = Switchboard(self.state, receipts_dir=self.receipts,
                            known_missions=frozenset({PROMO_MISSION}), clock=fixed_clock(),
                            promotion_executor=executor)
        body = self._records()
        event = build_promotion_request_event(body)
        board.ingest_raw(event)
        report = board.tick()
        self.assertIn(event["event_id"], report.acked)
        self.assertTrue(pr.prs[0].merged)
        self.assertEqual(len(gov.calls), 1)
        self.assertEqual(report.deadlettered, [])
        self.assertEqual(board.tick().acked, [])

    def test_composed_executor_refuses_when_governance_denies(self):
        gov = RecordedGovernance(decision="deny")
        executor, pr = self._executor(gov)
        board = Switchboard(self.state, receipts_dir=self.receipts,
                            known_missions=frozenset({PROMO_MISSION}), clock=fixed_clock(),
                            promotion_executor=executor)
        board.ingest_raw(build_promotion_request_event(self._records()))
        for _ in range(4):
            board.tick()
        self.assertFalse(pr.prs[0].merged)
        self.assertEqual(len(pr.promote_calls), 0)

    def test_unresolvable_record_ref_is_a_refusal_not_a_merge(self):
        gov = RecordedGovernance()
        executor, pr = self._executor(gov)
        board = Switchboard(self.state, receipts_dir=self.receipts,
                            known_missions=frozenset({PROMO_MISSION}), clock=fixed_clock(),
                            promotion_executor=executor)
        body = self._records()
        body["sentinel_receipt_ref"] = "sentinel://court/not-on-disk"
        board.ingest_raw(build_promotion_request_event(body))
        for _ in range(4):
            board.tick()
        self.assertFalse(pr.prs[0].merged)
        self.assertEqual(gov.calls, [])


if __name__ == "__main__":
    unittest.main()
