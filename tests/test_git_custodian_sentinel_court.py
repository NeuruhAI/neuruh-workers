"""Git Custodian consumes the court format the live Sentinel organ actually seals.

Git Custodian V1 verified nrh.court.receipt.v1 -- a shape no live court emits --
so a real independent_sentinel_court PASS could never unlock custody. The record
below mirrors the fields of the organ's sealed record (court-de881ba5, 2026-09-14)
that the verifier binds; the candidate is bound to it by digest.
"""
from __future__ import annotations

import unittest

from neuruh_workers.contracts import content_sha
from neuruh_workers.git_custodian.court import SENTINEL_COURT_KIND, verify_court
from neuruh_workers.git_custodian.errors import GitCustodianRefused

HEAD = "817d67c43b15cc0e3a2df6629e9d37cd07bdec74"


def sealed_record(**overrides):
    record = {
        "kind": SENTINEL_COURT_KIND,
        "independent": True,
        "source_role": "SENTINEL",
        "verdict": "PASS",
        "court_id": "court-de881ba5dee391a20aca5172",
        "mission_id": "msn_custody_candidate",
        "task_id": "job-908734b84b206b0427de1ce9",
        "source_head": HEAD,
        "candidate_sha": HEAD,
        "canonical_sha": HEAD,
        "source_repo": "NeuruhAI/neuruh-receipts",
        "base_relation": "candidate_is_canonical_base",
        "findings": [],
        "exceptions": [],
        "required_repairs": [],
        "sealed_at": "2026-09-14T22:06:44Z",
    }
    record.update(overrides)
    return record


def candidate_for(record, **overrides):
    candidate = {
        "mission_id": "msn_custody_candidate",
        "task_id": "job-908734b84b206b0427de1ce9",
        "repo": "NeuruhAI/neuruh-receipts",
        "branch": "mission/msn_custody_candidate",
        "expected_local_head": HEAD,
        "court_receipt_ref": "/tmp/courts/court-de881ba5dee391a20aca5172.json",
        "court_receipt_hash": content_sha(record),
    }
    candidate.update(overrides)
    return candidate


class SealedSentinelCourtTests(unittest.TestCase):
    def test_clean_sealed_pass_bound_to_the_candidate_is_accepted(self):
        record = sealed_record()
        court = verify_court(record, candidate_for(record))
        self.assertEqual(court["independent_court"], "PASS")
        self.assertEqual(court["stage"], record["task_id"])
        self.assertEqual(court["candidate"], {"repo": "NeuruhAI/neuruh-receipts",
                                              "branch": "mission/msn_custody_candidate", "head": HEAD})
        self.assertEqual(court["court_id"], record["court_id"])

    def test_every_binding_fails_closed(self):
        cases = [
            ({"verdict": "FAIL"}, {}, "COURT_FAILED"),
            ({"findings": ["drift"]}, {}, "COURT_FAILED"),
            ({"required_repairs": ["fix"]}, {}, "COURT_FAILED"),
            ({"independent": False}, {}, "COURT_INVALID"),
            ({"source_role": "PRIME"}, {}, "COURT_INVALID"),
            ({"base_relation": "unverified"}, {}, "COURT_INVALID"),
            ({"mission_id": "msn_other"}, {}, "COURT_MISMATCH"),
            ({"task_id": "job-other"}, {}, "COURT_MISMATCH"),
            ({"candidate_sha": "a" * 40}, {}, "COURT_MISMATCH"),
            ({"source_head": "a" * 40}, {}, "COURT_MISMATCH"),
            ({"source_repo": "NeuruhAI/neuruh-workers"}, {}, "COURT_MISMATCH"),
            ({}, {"court_receipt_hash": "0" * 64}, "COURT_HASH_MISMATCH"),
            ({}, {"expected_local_head": "b" * 40}, "COURT_MISMATCH"),
        ]
        for record_overrides, candidate_overrides, reason in cases:
            with self.subTest(record=record_overrides, candidate=candidate_overrides):
                record = sealed_record(**record_overrides)
                with self.assertRaises(GitCustodianRefused) as caught:
                    verify_court(record, candidate_for(record, **candidate_overrides))
                self.assertEqual(caught.exception.reason, reason)

    def test_a_bare_pass_string_is_still_not_a_court(self):
        with self.assertRaises(GitCustodianRefused) as caught:
            verify_court("PASS", candidate_for(sealed_record()))
        self.assertEqual(caught.exception.reason, "COURT_INVALID")


if __name__ == "__main__":
    unittest.main()
