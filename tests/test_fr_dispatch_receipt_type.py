"""Typed FR dispatch receipt: the contract, and the bug it replaced.

The FR to Workers dispatch acknowledgement is a transport receipt, not a PRIME worker
receipt. Before this contract existed it was pushed through the PRIME validator, where it
could only pass if PRIME itself were widened to accept the literal claimer string
DISPATCH_BRIDGE. This module proves the original defect, proves the repair, and pins the
PRIME invariant so it cannot be loosened again to make a court green.
"""
import os
import tempfile
import unittest

from neuruh_workers.organ_consumers import _fr_dispatch_receipt_text
from neuruh_workers.prime_receipts import (
    FR_DISPATCH_RECEIPT_SCHEMA,
    parse_prime_receipt,
    seal_receipt_text,
    validate_receipt,
)
from neuruh_workers.events import DeadletterReason

MISSION = "msn_typed_dispatch_court"
HEAD = "a" * 40
WORK_SPEC = "b" * 64
IDEMPOTENCY = "c" * 64
LEDGER_TAIL = "d" * 64
CONTEXT_DIGEST = "e" * 64
JOB_SUCCEEDED = "f" * 64
JOB_ID = "job_typed_court"
WORKTREE = "/tmp/worktree"
APPROVAL = "appr_typed_court"
AUTHORITY = f"fr-api-v1:approval:{APPROVAL}:work-spec:{WORK_SPEC}"
KNOWN = frozenset({MISSION})


def _emitted_dispatch_fields() -> dict[str, str]:
    """The dispatch receipt the bridge actually emits, read back field by field.

    The fixture is derived from `_fr_dispatch_receipt_text` rather than hand-copied,
    so the emitter and this type court cannot drift apart again: a field added to the
    emitted receipt shows up here, and a field the validator starts requiring is
    exercised against the real emission instead of a stale hand-written sample.
    PAYLOAD_HASH is dropped because `_write` reseals whatever fields it is given.
    """
    text = _fr_dispatch_receipt_text(
        mission_id=MISSION,
        mission_version=1,
        work_spec_sha256=WORK_SPEC,
        idempotency_key=IDEMPOTENCY,
        dispatch_result="SUCCEEDED",
        job_id=JOB_ID,
        ledger_tail_sha=LEDGER_TAIL,
        context_digest=CONTEXT_DIGEST,
        evidence_refs=["ev-typed-court"],
        job_succeeded_sha=JOB_SUCCEEDED,
        source_head=HEAD,
        source_branch="main",
        source_worktree=WORKTREE,
        authority_ref=AUTHORITY,
        code_custody_required=True,
    )
    fields: dict[str, str] = {}
    for line in text.splitlines():
        if "=" in line:
            key, value = line.split("=", 1)
            fields[key] = value
    fields.pop("PAYLOAD_HASH", None)
    return fields


_EMITTED_DISPATCH_FIELDS = _emitted_dispatch_fields()


def _fields(**overrides):
    """The canonical dispatch receipt this repair emits."""
    base = dict(_EMITTED_DISPATCH_FIELDS)
    for key, value in overrides.items():
        if value is None:
            base.pop(key, None)
        else:
            base[key] = value
    return base


def _write(fields, *, seal=True, raw=None):
    body = raw if raw is not None else "\n".join(f"{k}={v}" for k, v in fields.items()) + "\n"
    if seal:
        body = seal_receipt_text(body)
    handle = tempfile.NamedTemporaryFile("w", suffix=".txt", delete=False, encoding="utf-8")
    handle.write(body)
    handle.close()
    return handle.name


def _validate(fields, **kwargs):
    path = _write(fields, **kwargs)
    try:
        return validate_receipt(parse_prime_receipt(path), known_missions=KNOWN)
    finally:
        os.unlink(path)


class FrDispatchReceiptTypeCourt(unittest.TestCase):
    # ---- the original defect, reproduced -------------------------------------------

    def test_undeclared_dispatch_shaped_receipt_is_refused_as_it_was_on_main(self):
        """The exact origin/main shape: CLAIMANT=DISPATCH_BRIDGE, no declared type.

        This is what the bridge used to emit and what the Switchboard deadlettered. It
        must still be refused: the repair is a new type, not a widened PRIME.
        """
        fields = _fields(RECEIPT_SCHEMA=None, CLAIMANT="DISPATCH_BRIDGE")
        verdict = _validate(fields)
        self.assertFalse(verdict.ok)
        self.assertEqual(verdict.reason, DeadletterReason.CONTRADICTION)
        self.assertIn("PRIME", verdict.detail)

    def test_dispatch_bridge_is_never_accepted_as_prime(self):
        """DISPATCH_BRIDGE_ACCEPTED_AS_PRIME=NO."""
        for claimer_field in ("CLAIMER", "CLAIMANT", "OWNER_ROLE"):
            fields = _fields(RECEIPT_SCHEMA=None, **{claimer_field: "DISPATCH_BRIDGE"})
            verdict = _validate(fields)
            self.assertFalse(verdict.ok, f"{claimer_field} must not buy PRIME acceptance")

    def test_prime_self_court_invariant_unchanged(self):
        """PRIME_SELF_COURT_INVARIANT=UNCHANGED.

        A genuine PRIME receipt still passes on exactly the original rule, and the
        original refusal wording is still the one a non-PRIME claimer receives.
        """
        prime = _fields(
            RECEIPT_SCHEMA=None, MISSION_VERSION=None, WORK_SPEC_SHA256=None,
            IDEMPOTENCY_KEY=None, DISPATCH_RESULT=None, SOURCE_COMPONENT=None,
            SOURCE_ROLE="PRIME", CLAIMER="PRIME",
        )
        self.assertTrue(_validate(prime).ok)

        worker = _fields(
            RECEIPT_SCHEMA=None, MISSION_VERSION=None, WORK_SPEC_SHA256=None,
            IDEMPOTENCY_KEY=None, DISPATCH_RESULT=None, SOURCE_COMPONENT=None,
            SOURCE_ROLE="WORKER", CLAIMER="WORKER",
        )
        verdict = _validate(worker)
        self.assertFalse(verdict.ok)
        self.assertIn("a worker may not court", verdict.detail)

    # ---- the typed contract --------------------------------------------------------

    def test_declared_dispatch_receipt_is_valid(self):
        """FR_DISPATCH_RECEIPT_TYPE=VALID."""
        self.assertTrue(_validate(_fields()).ok)

    def test_the_emitted_receipt_is_the_receipt_this_court_validates(self):
        """EMITTER_VALIDATOR_AGREEMENT=PROVEN.

        The emitter writes its own seal, so this validates the exact bytes the bridge
        would hand the Switchboard, with no test-side resealing.  A required field the
        emitter stops writing, or a field the validator starts requiring that the
        emitter never writes, fails here instead of only in a live dispatch.
        """
        text = _fr_dispatch_receipt_text(
            mission_id=MISSION,
            mission_version=1,
            work_spec_sha256=WORK_SPEC,
            idempotency_key=IDEMPOTENCY,
            dispatch_result="SUCCEEDED",
            job_id=JOB_ID,
            ledger_tail_sha=LEDGER_TAIL,
            context_digest=CONTEXT_DIGEST,
            evidence_refs=["ev-typed-court"],
            job_succeeded_sha=JOB_SUCCEEDED,
            source_head=HEAD,
            source_branch="main",
            source_worktree=WORKTREE,
            authority_ref=AUTHORITY,
            code_custody_required=True,
        )
        path = _write({}, seal=False, raw=text)
        try:
            receipt = parse_prime_receipt(path)
            self.assertEqual(receipt.receipt_schema, FR_DISPATCH_RECEIPT_SCHEMA)
            verdict = validate_receipt(receipt, known_missions=KNOWN)
            self.assertTrue(verdict.ok, verdict.detail)
            self.assertEqual(
                set(receipt.fields) - {"PAYLOAD_HASH"},
                set(_EMITTED_DISPATCH_FIELDS),
                "the fixture no longer mirrors the emitted receipt",
            )
        finally:
            os.unlink(path)

    def test_unknown_receipt_type_fails_closed(self):
        verdict = _validate(_fields(RECEIPT_SCHEMA="nrh.receipt.something.v9"))
        self.assertFalse(verdict.ok)
        self.assertEqual(verdict.reason, DeadletterReason.INVALID_SCHEMA)
        self.assertIn("unknown receipt type", verdict.detail)

    def test_dispatch_receipt_may_not_carry_prime_ownership_fields(self):
        """A transport receipt does not own PRIME work, so it may not say it does."""
        for field in ("CLAIMER", "CLAIMANT", "OWNER_ROLE"):
            verdict = _validate(_fields(**{field: "DISPATCH_BRIDGE"}))
            self.assertFalse(verdict.ok, f"{field} must be refused on a dispatch receipt")
            self.assertIn("PRIME ownership", verdict.detail)

    def test_dispatch_receipt_may_not_present_itself_as_prime(self):
        verdict = _validate(_fields(SOURCE_ROLE="PRIME_DISPATCH"))
        self.assertFalse(verdict.ok)

    def test_every_required_binding_is_required(self):
        for field in (
            "MISSION_ID", "MISSION_VERSION", "WORK_SPEC_SHA256", "IDEMPOTENCY_KEY",
            "DISPATCH_RESULT", "AUTHORITY_REFERENCE", "SOURCE_ROLE",
            "SOURCE_COMPONENT", "TASK_ID",
        ):
            verdict = _validate(_fields(**{field: None}))
            self.assertFalse(verdict.ok, f"{field} must be required")

    def test_work_spec_digest_must_agree_with_the_authority_reference(self):
        """The bridge may not name one work spec and be authorised for another."""
        verdict = _validate(_fields(WORK_SPEC_SHA256="d" * 64))
        self.assertFalse(verdict.ok)
        self.assertEqual(verdict.reason, DeadletterReason.CONTRADICTION)
        self.assertIn("disagrees", verdict.detail)

    def test_authority_reference_must_name_an_fr_approval(self):
        verdict = _validate(_fields(AUTHORITY_REFERENCE="self-signed"))
        self.assertFalse(verdict.ok)

    def test_malformed_scalars_fail_closed(self):
        self.assertFalse(_validate(_fields(MISSION_VERSION="0")).ok)
        self.assertFalse(_validate(_fields(MISSION_VERSION="one")).ok)
        self.assertFalse(_validate(_fields(IDEMPOTENCY_KEY="short")).ok)
        self.assertFalse(_validate(_fields(WORK_SPEC_SHA256="NOTHEX" * 10)).ok)
        self.assertFalse(_validate(_fields(DISPATCH_RESULT="DISPATCHED")).ok)

    def test_unknown_mission_and_bad_head_still_fail_closed_for_the_new_type(self):
        self.assertFalse(_validate(_fields(MISSION_ID="msn_not_known")).ok)
        self.assertFalse(_validate(_fields(SOURCE_HEAD="not-a-sha")).ok)

    def test_broken_seal_fails_closed_for_the_new_type(self):
        fields = _fields()
        path = _write(fields)
        try:
            with open(path, encoding="utf-8") as handle:
                text = handle.read()
            tampered = text.replace(f"MISSION_VERSION={fields['MISSION_VERSION']}",
                                    "MISSION_VERSION=99")
            with open(path, "w", encoding="utf-8") as handle:
                handle.write(tampered)
            verdict = validate_receipt(parse_prime_receipt(path), known_missions=KNOWN)
            self.assertFalse(verdict.ok)
            self.assertEqual(verdict.reason, DeadletterReason.BROKEN_HASH)
        finally:
            os.unlink(path)

    def test_duplicate_receipt_fields_deadletter(self):
        """DUPLICATE_RECEIPT_FIELDS=DEADLETTER, for both receipt types."""
        raw = "\n".join(f"{k}={v}" for k, v in _fields().items())
        raw += f"\nMISSION_VERSION=99\n"
        path = _write({}, seal=False, raw=raw)
        try:
            verdict = validate_receipt(parse_prime_receipt(path), known_missions=KNOWN)
            self.assertFalse(verdict.ok)
            self.assertEqual(verdict.reason, DeadletterReason.INVALID_SCHEMA)
            self.assertIn("duplicate receipt fields", verdict.detail)
        finally:
            os.unlink(path)

    def test_sealing_refuses_to_hash_a_duplicated_field(self):
        with self.assertRaises(ValueError):
            seal_receipt_text("MISSION_ID=a\nMISSION_ID=b\n")

    def test_non_loopback_bind_fails_closed(self):
        self.assertFalse(_validate(_fields(NETWORK_POLICY="0.0.0.0:8080")).ok)


if __name__ == "__main__":
    unittest.main()
