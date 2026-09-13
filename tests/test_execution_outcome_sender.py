"""R02D: Workers -> Founder Runtime execution-outcome write-back (sender side).

The Founder Runtime owns mission state and exposes
``POST /v1/missions/{id}/execution-outcomes`` (fr.execution_outcome.v1). The
bridge reports sealed evidence; it never raises on a refused write and never
dispatches twice because of one. These courts stub the FR opener and check the
exact headers, body shape, replay semantics and fail-closed classes.
"""
from __future__ import annotations

import io
import json
import os
import tempfile
import unittest
import urllib.error

from neuruh_workers.organ_consumers import (
    FR_OUTCOME_ALREADY_KNOWN,
    FR_OUTCOME_RECORDED,
    FR_OUTCOME_WRITEBACK_FAIL_CLOSED,
    FR_OUTCOME_WRITEBACK_NOT_CONFIGURED,
    build_execution_outcome_envelope,
    execution_outcome_idempotency_key,
    report_execution_outcome,
)
from neuruh_workers.prime_receipts import (
    FR_DISPATCH_RECEIPT_SCHEMA,
    canonical_receipt_sha,
    parse_prime_receipt,
    seal_receipt_text,
)
# Imported as a module so the loader does not re-collect the parent court here.
from tests import test_founder_runtime as _fr_court

RuntimeStatus = _fr_court.RuntimeStatus

HEAD = "0" * 39 + "a"
WORK_SPEC = "1" * 64


class _Resp:
    def __init__(self, status: int, body: dict):
        self.status = status
        self._body = json.dumps(body).encode("utf-8")

    def read(self):
        return self._body

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False


def _http_error(status: int, body: dict) -> urllib.error.HTTPError:
    return urllib.error.HTTPError(
        "http://127.0.0.1:19090/x", status, "refused", hdrs=None,
        fp=io.BytesIO(json.dumps(body).encode("utf-8")))


def _capture(responses):
    """Opener that records every request and answers from ``responses`` in order."""
    seen = []

    def opener(request, timeout=None):
        seen.append({
            "method": request.get_method(),
            "url": request.full_url,
            "headers": {k.lower(): v for k, v in request.header_items()},
            "body": json.loads(request.data.decode("utf-8")) if request.data else None,
            "timeout": timeout,
        })
        answer = responses.pop(0)
        if isinstance(answer, Exception):
            raise answer
        return answer

    return opener, seen


def _dispatched_envelope(job_id="job-abc", outcome="DISPATCHED"):
    return build_execution_outcome_envelope(
        mission_id="msn_test", mission_version=3, work_spec_sha256=WORK_SPEC,
        job_id=job_id, outcome=outcome, dispatch_approval_id="appr_1",
        source_head=HEAD, observed_at="2026-09-13T20:00:00Z")


class EnvelopeCourt(unittest.TestCase):
    def test_progress_envelope_shape_and_authority_reference(self):
        env = _dispatched_envelope()
        self.assertEqual(env["schema"], "fr.execution_outcome.v1")
        self.assertEqual(env["api_version"], "fr-api-v1")
        self.assertEqual(env["execution_id"], "job-abc")
        self.assertEqual(env["mission_version"], 3)
        self.assertEqual(env["authority_reference"],
                         f"fr-api-v1:approval:appr_1:work-spec:{WORK_SPEC}")
        self.assertEqual(env["source"], {
            "role": "DISPATCH_BRIDGE", "component": "fr-to-workers-dispatch-bridge",
            "repo": "NeuruhAI/neuruh-workers", "head": HEAD})
        self.assertNotIn("receipt", env)
        self.assertNotIn("failure", env)

    def test_succeeded_envelope_binds_the_sealed_receipt_verbatim(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "job-abc.txt")
            text = seal_receipt_text(
                f"RECEIPT_SCHEMA={FR_DISPATCH_RECEIPT_SCHEMA}\nMISSION_ID=msn_test\n"
                "JOB_ID=job-abc\nTASK_ID=job-abc\n")
            with open(path, "w", encoding="utf-8") as fh:
                fh.write(text)
            env = build_execution_outcome_envelope(
                mission_id="msn_test", mission_version=3, work_spec_sha256=WORK_SPEC,
                job_id="job-abc", outcome="SUCCEEDED", dispatch_approval_id="appr_1",
                source_head=HEAD, receipt_ref=path)
            parsed = parse_prime_receipt(path)
            self.assertEqual(env["receipt"]["schema"], FR_DISPATCH_RECEIPT_SCHEMA)
            self.assertEqual(env["receipt"]["fields"], parsed.fields)
            self.assertEqual(env["receipt"]["receipt_sha"], parsed.fields["PAYLOAD_HASH"])
            self.assertEqual(env["receipt"]["receipt_sha"], canonical_receipt_sha(parsed.fields))
            self.assertEqual(env["receipt"]["receipt_ref"], os.path.abspath(path))

    def test_succeeded_without_receipt_is_refused(self):
        with self.assertRaises(ValueError):
            build_execution_outcome_envelope(
                mission_id="m", mission_version=1, work_spec_sha256=WORK_SPEC,
                job_id="j", outcome="SUCCEEDED", dispatch_approval_id="a", source_head=HEAD)

    def test_failed_envelope_carries_bounded_failure(self):
        env = build_execution_outcome_envelope(
            mission_id="m", mission_version=1, work_spec_sha256=WORK_SPEC,
            job_id="j", outcome="FAILED", dispatch_approval_id="a", source_head=HEAD,
            failure={"error_class": "X", "detail": "d" * 5000})
        self.assertEqual(env["failure"]["error_class"], "X")
        self.assertEqual(len(env["failure"]["detail"]), 2000)


class SenderCourt(unittest.TestCase):
    def test_headers_body_and_recorded(self):
        opener, seen = _capture([_Resp(200, {"applied": True, "idempotent_replay": False,
                                             "events_appended": ["e1"]})])
        report = report_execution_outcome(
            "msn_test", _dispatched_envelope(), opener=opener, token="tok-1")
        self.assertEqual(len(seen), 1)
        req = seen[0]
        self.assertEqual(req["method"], "POST")
        self.assertTrue(req["url"].endswith("/v1/missions/msn_test/execution-outcomes"))
        self.assertEqual(req["headers"]["authorization"], "Bearer tok-1")
        self.assertEqual(req["headers"]["idempotency-key"],
                         "execution_outcome:job-abc:DISPATCHED:-")
        self.assertEqual(req["headers"]["content-type"], "application/json")
        self.assertEqual(req["body"]["schema"], "fr.execution_outcome.v1")
        self.assertGreaterEqual(req["timeout"], 8.0)
        self.assertTrue(report["ok"])
        self.assertEqual(report["status"], 200)
        self.assertEqual(report["bind_status"], FR_OUTCOME_RECORDED)

    def test_idempotency_key_carries_receipt_sha_for_succeeded(self):
        sha = "f" * 64
        env = {**_dispatched_envelope(), "outcome": "SUCCEEDED",
               "receipt": {"schema": FR_DISPATCH_RECEIPT_SCHEMA, "fields": {},
                           "receipt_sha": sha, "receipt_ref": "/r"}}
        opener, seen = _capture([_Resp(200, {"applied": True})])
        report = report_execution_outcome("msn_test", env, opener=opener, token="t")
        self.assertEqual(seen[0]["headers"]["idempotency-key"],
                         execution_outcome_idempotency_key("job-abc", "SUCCEEDED", sha))
        self.assertEqual(report["bind_status"], FR_OUTCOME_RECORDED)

    def test_same_evidence_twice_is_already_known(self):
        opener, seen = _capture([
            _Resp(200, {"applied": True, "idempotent_replay": False}),
            _Resp(200, {"applied": False, "idempotent_replay": True, "events_appended": []}),
        ])
        first = report_execution_outcome("m", _dispatched_envelope(), opener=opener, token="t")
        second = report_execution_outcome("m", _dispatched_envelope(), opener=opener, token="t")
        self.assertEqual(first["bind_status"], FR_OUTCOME_RECORDED)
        self.assertEqual(second["bind_status"], FR_OUTCOME_ALREADY_KNOWN)
        self.assertEqual(seen[0]["headers"]["idempotency-key"],
                         seen[1]["headers"]["idempotency-key"])

    def test_409_replay_class_codes_are_already_known(self):
        for code in ("OUTCOME_STALE", "IDEMPOTENCY_CONFLICT", "OUTCOME_OUT_OF_ORDER"):
            opener, _seen = _capture([_http_error(409, {"code": code, "message": "x"})])
            report = report_execution_outcome("m", _dispatched_envelope(), opener=opener, token="t")
            self.assertEqual(report["status"], 409)
            self.assertEqual(report["code"], code)
            self.assertEqual(report["bind_status"], FR_OUTCOME_ALREADY_KNOWN, code)

    def test_409_binding_refusals_404_and_timeout_fail_closed(self):
        cases = [
            _http_error(409, {"code": "RECEIPT_HASH_MISMATCH"}),
            _http_error(409, {"error": {"code": "WORK_SPEC_MISMATCH"}}),
            _http_error(404, {"code": "MISSION_NOT_FOUND"}),
            _http_error(401, {"code": "UNAUTHORIZED"}),
            TimeoutError("timed out"),
            OSError("connection refused"),
        ]
        for case in cases:
            opener, _seen = _capture([case])
            report = report_execution_outcome("m", _dispatched_envelope(), opener=opener, token="t")
            self.assertTrue(report["attempted"])
            self.assertFalse(report["ok"])
            self.assertEqual(report["bind_status"], FR_OUTCOME_WRITEBACK_FAIL_CLOSED, repr(case))
        self.assertEqual(
            report_execution_outcome("m", _dispatched_envelope(),
                                     opener=_capture([_http_error(409, {"code": "RECEIPT_HASH_MISMATCH"})])[0],
                                     token="t")["code"],
            "RECEIPT_HASH_MISMATCH")

    def test_no_token_is_not_attempted(self):
        opener, seen = _capture([])
        report = report_execution_outcome("m", _dispatched_envelope(), opener=opener, token=None)
        self.assertFalse(report["attempted"])
        self.assertEqual(report["bind_status"], FR_OUTCOME_WRITEBACK_NOT_CONFIGURED)
        self.assertEqual(seen, [])


class BridgeWritebackCourt(_fr_court.FounderRuntimeCourt):
    """The dispatch bridge against an FR stub that accepts outcomes."""

    # Reuse the parent's fixture only; its own courts run in their own module.
    for _name in [n for n in dir(_fr_court.FounderRuntimeCourt) if n.startswith("test_")]:
        locals()[_name] = None
    del _name

    def setUp(self):
        super().setUp()
        base_opener = self.health_opener
        self.posts = []
        self.post_responses = []

        def opener(request, timeout=None):
            if request.get_method() == "POST" and request.full_url.endswith("/execution-outcomes"):
                self.posts.append({
                    "url": request.full_url,
                    "headers": {k.lower(): v for k, v in request.header_items()},
                    "body": json.loads(request.data.decode("utf-8")),
                })
                answer = self.post_responses.pop(0) if self.post_responses else _Resp(
                    200, {"applied": True, "idempotent_replay": False})
                if isinstance(answer, Exception):
                    raise answer
                return answer
            return base_opener(request, timeout=timeout)

        self.runtime.http_open = opener

    def test_dispatch_reports_dispatched_then_succeeded_bound_to_the_sealed_receipt(self):
        result = self.runtime.dispatch(self.mission(), [self.property_candidate()])
        self.assertEqual(result.status, RuntimeStatus.SUCCEEDED)
        self.assertEqual([p["body"]["outcome"] for p in self.posts], ["DISPATCHED", "SUCCEEDED"])
        for post in self.posts:
            self.assertEqual(post["headers"]["authorization"], "Bearer court-token")
            body = post["body"]
            self.assertEqual(body["mission_id"], "JGI-COURT-001")
            self.assertEqual(body["mission_version"], 1)
            self.assertEqual(body["work_spec_sha256"], self.default_work_spec_sha)
            self.assertEqual(body["execution_id"], result.job_id)
            self.assertEqual(body["authority_reference"],
                             f"fr-api-v1:approval:appr-jgi-court-001:work-spec:{self.default_work_spec_sha}")
            self.assertEqual(body["source"]["head"], self.source_sha)
            self.assertEqual(body["source"]["role"], "DISPATCH_BRIDGE")
        succeeded = self.posts[1]["body"]
        receipt = succeeded["receipt"]
        sealed = result.sealed_receipt["payload_ref_or_payload"]
        self.assertEqual(receipt["schema"], FR_DISPATCH_RECEIPT_SCHEMA)
        self.assertEqual(receipt["receipt_sha"], sealed["receipt_sha"])
        self.assertEqual(receipt["fields"]["PAYLOAD_HASH"], sealed["receipt_sha"])
        self.assertEqual(canonical_receipt_sha(receipt["fields"]), sealed["receipt_sha"])
        self.assertEqual(os.path.realpath(receipt["receipt_ref"]),
                         os.path.realpath(sealed["receipt_ref"]))
        self.assertEqual(receipt["fields"]["MISSION_ID"], "JGI-COURT-001")
        self.assertEqual(receipt["fields"]["JOB_ID"], result.job_id)
        self.assertEqual(receipt["fields"]["DISPATCH_RESULT"], "SUCCEEDED")
        self.assertEqual(receipt["fields"]["SENTINEL_CLOSURE"], "NOT_JUDGED")
        self.assertEqual(self.posts[1]["headers"]["idempotency-key"],
                         f"execution_outcome:{result.job_id}:SUCCEEDED:{sealed['receipt_sha']}")
        wb = result.fr_outcome_writeback
        self.assertEqual(wb["DISPATCHED"]["bind_status"], FR_OUTCOME_RECORDED)
        self.assertEqual(wb["SUCCEEDED"]["bind_status"], FR_OUTCOME_RECORDED)

    def test_refused_writeback_does_not_touch_the_dispatch(self):
        self.post_responses = [
            _http_error(409, {"code": "NOT_DISPATCHED_STATE"}),
            TimeoutError("fr busy"),
        ]
        result = self.runtime.dispatch(self.mission(), [self.property_candidate()])
        self.assertEqual(result.status, RuntimeStatus.SUCCEEDED)
        self.assertTrue(result.sealed_receipt)
        wb = result.fr_outcome_writeback
        self.assertEqual(wb["DISPATCHED"]["bind_status"], FR_OUTCOME_WRITEBACK_FAIL_CLOSED)
        self.assertEqual(wb["DISPATCHED"]["code"], "NOT_DISPATCHED_STATE")
        self.assertEqual(wb["SUCCEEDED"]["bind_status"], FR_OUTCOME_WRITEBACK_FAIL_CLOSED)
        # exactly one job, one receipt: the refusal changed nothing on the workers side
        self.assertEqual(len(os.listdir(self.receipts_dir)), 1)

    def test_replayed_dispatch_resends_only_succeeded_with_the_same_key(self):
        first = self.runtime.dispatch(self.mission(), [self.property_candidate()])
        self.record_outcome(first)
        self.post_responses = [_Resp(200, {"applied": False, "idempotent_replay": True})]
        replay = self.runtime.dispatch(self.mission(), [self.property_candidate()])
        self.assertEqual(replay.status, RuntimeStatus.ALREADY_SUCCEEDED)
        self.assertEqual([p["body"]["outcome"] for p in self.posts],
                         ["DISPATCHED", "SUCCEEDED", "SUCCEEDED"])
        self.assertEqual(self.posts[1]["headers"]["idempotency-key"],
                         self.posts[2]["headers"]["idempotency-key"])
        self.assertEqual(replay.fr_outcome_writeback["SUCCEEDED"]["bind_status"],
                         FR_OUTCOME_ALREADY_KNOWN)
        self.assertEqual(len(os.listdir(self.receipts_dir)), 1)


if __name__ == "__main__":
    unittest.main()
