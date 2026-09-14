"""Append-only, hash-chained execution ledger.

The ledger is the single source of truth: job state is never stored, it is
always replayed from receipts.  That is what makes pause/resume, idempotent
replay and crash recovery the same mechanism instead of three features.

A broken chain is a hard stop (LedgerCorrupt) — the layer refuses to operate
on a ledger it cannot verify.  There is no repair path and no fallback.
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from typing import Any, Callable, Iterator, Mapping

from .contracts import (
    GENESIS_SHA,
    LedgerCorrupt,
    Receipt,
    canonical_json,
    content_sha as canonical_json_sha,
)

Clock = Callable[[], str]


def utc_clock() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def split_lines_exact(body: bytes) -> list[bytes]:
    """Lines as the producers wrote them: split on b"\\n" ONLY, terminators kept.

    Byte-for-byte what the independent court hashes when it retrieves a line
    range of a ledger (sentinel_court.retrieve.split_lines_exact). bytes.splitlines
    also splits on U+2028/U+2029 and \\x1c-\\x1e, which a Governance line may carry
    raw; the two sides would then hash different bytes for the same range.
    """
    parts = body.split(b"\n")
    lines = [part + b"\n" for part in parts[:-1]]
    if parts[-1]:
        lines.append(parts[-1])
    return lines


class ExecutionLedger:
    """One JSONL file, append-only, verified on every open and every append."""

    FIELDS = {"seq", "receipt_type", "job_id", "payload", "observed_at",
              "prev_sha", "receipt_sha"}

    def __init__(self, path: str, *, clock: Clock | None = None):
        self._path = path
        self._clock = clock or utc_clock
        parent = os.path.dirname(os.path.abspath(path))
        os.makedirs(parent, exist_ok=True)
        # Verify the existing chain immediately: a corrupt ledger must never
        # accept new receipts.
        self._tail_seq, self._tail_sha = self._verify_all()

    @property
    def path(self) -> str:
        """The JSONL file this ledger is: what a court is told to retrieve."""
        return self._path

    # -- reading ----------------------------------------------------------- #

    def _iter_lines(self) -> Iterator[str]:
        if not os.path.exists(self._path):
            return
        with open(self._path, "r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if line:
                    yield line

    def _parse(self, line: str, lineno: int) -> Receipt:
        try:
            record = json.loads(line)
        except json.JSONDecodeError as exc:
            raise LedgerCorrupt(f"line {lineno} is not valid JSON", {"line": lineno}) from exc
        if not isinstance(record, dict) or set(record) != self.FIELDS:
            raise LedgerCorrupt(f"line {lineno} has an invalid field set", {"line": lineno})
        return Receipt(
            seq=record["seq"], receipt_type=record["receipt_type"],
            job_id=record["job_id"], payload=record["payload"],
            observed_at=record["observed_at"], prev_sha=record["prev_sha"],
            receipt_sha=record["receipt_sha"],
        )

    def _verify_all(self) -> tuple[int, str]:
        seq, prev = 0, GENESIS_SHA
        for lineno, line in enumerate(self._iter_lines(), start=1):
            receipt = self._parse(line, lineno)
            if receipt.seq != seq + 1:
                raise LedgerCorrupt(
                    f"sequence break at line {lineno}: expected {seq + 1}, found {receipt.seq}",
                    {"line": lineno})
            if receipt.prev_sha != prev:
                raise LedgerCorrupt(
                    f"hash chain break at line {lineno}", {"line": lineno})
            if receipt.compute_sha() != receipt.receipt_sha:
                raise LedgerCorrupt(
                    f"receipt hash mismatch at line {lineno}", {"line": lineno})
            seq, prev = receipt.seq, receipt.receipt_sha
        return seq, prev

    def verify(self) -> dict[str, Any]:
        """Full re-verification; returns chain facts for audits."""
        seq, sha = self._verify_all()
        return {"receipts": seq, "tail_sha": sha, "genesis_sha": GENESIS_SHA}

    def receipts(self, job_id: str | None = None) -> list[Receipt]:
        out = []
        for lineno, line in enumerate(self._iter_lines(), start=1):
            receipt = self._parse(line, lineno)
            if job_id is None or receipt.job_id == job_id:
                out.append(receipt)
        return out

    def canonical_chain(self, job_id: str | None = None) -> list[str]:
        """Timestamp-free re-chained hashes: two runs of the same logical work
        produce identical canonical chains even though their wall clocks (and
        therefore their on-disk receipt_shas) differ."""
        chain: list[str] = []
        prev = GENESIS_SHA
        for receipt in self.receipts(job_id):
            body = receipt.canonical_body()
            body["canonical_prev"] = prev
            prev = canonical_json_sha(body)
            chain.append(prev)
        return chain

    # -- writing ----------------------------------------------------------- #

    def append(self, receipt_type: str, job_id: str,
               payload: Mapping[str, Any]) -> Receipt:
        # Re-verify tail against disk so concurrent/external tampering is
        # caught before we extend the chain.
        self._tail_seq, self._tail_sha = self._verify_all()
        json.loads(canonical_json(payload))  # payload must be pure JSON
        receipt = Receipt(
            seq=self._tail_seq + 1,
            receipt_type=receipt_type,
            job_id=job_id,
            payload=dict(payload),
            observed_at=self._clock(),
            prev_sha=self._tail_sha,
        )
        sealed = Receipt(**{**receipt.__dict__, "receipt_sha": receipt.compute_sha()})
        with open(self._path, "a", encoding="utf-8") as fh:
            fh.write(sealed.to_line() + "\n")
            fh.flush()
            os.fsync(fh.fileno())
        self._tail_seq, self._tail_sha = sealed.seq, sealed.receipt_sha
        return sealed
