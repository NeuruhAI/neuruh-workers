"""Legion journal: the per-legion, append-only, hash-chained agent record.

Why a second file and not the ExecutionLedger?  ``ExecutionLedger.append``
re-verifies the whole chain on every append (O(n) per write).  That is right
for a job with a dozen receipts and quadratic for a legion: 10,000 agents x
~2 receipts each would re-hash the file ~20,000 times.  The legion journal
verifies the chain ONCE on open and then extends it from the in-memory tail,
so an append is O(1).  It is bound back into the ExecutionLedger by the
legion step result (journal path + tail sha + Merkle root), so the Workers
ledger stays the single source of mission truth and this file is its
evidence sidecar - not a second job store.

Durability: lines are flushed on every append and fsync'd every
``fsync_every`` lines and on every seal.  A crash can therefore leave at most
one torn (newline-less) final line.  That line is the only thing ever
discarded, and only when it is the physical tail without a terminator; any
other defect (bad JSON on a terminated line, sequence or hash break) is a
hard ``LedgerCorrupt`` stop - no repair path.
"""
from __future__ import annotations

import json
import os
import threading
from typing import Any, Iterator, Mapping

from ..contracts import LedgerCorrupt, canonical_json, content_sha, sha256_hex
from ..ledger import utc_clock

JOURNAL_GENESIS_SHA = sha256_hex("neuruh-legion-journal-genesis-v1")

FIELDS = {"seq", "kind", "legion_id", "body", "at", "prev_sha", "sha"}


def _record_sha(record: Mapping[str, Any]) -> str:
    return content_sha({k: record[k] for k in
                        ("seq", "kind", "legion_id", "body", "at", "prev_sha")})


class LegionJournal:
    def __init__(self, path: str, legion_id: str, *, clock=None,
                 fsync_every: int = 256):
        self.path = path
        self.legion_id = legion_id
        self._clock = clock or utc_clock
        self._fsync_every = max(1, int(fsync_every))
        self._lock = threading.Lock()
        self._since_sync = 0
        self.torn_tail_discarded = False
        os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
        self._records: list[dict[str, Any]] = []
        self._tail_seq, self._tail_sha = self._load()
        self._fh = open(self.path, "a", encoding="utf-8")

    # -- reading ----------------------------------------------------------- #

    def _load(self) -> tuple[int, str]:
        if not os.path.exists(self.path):
            return 0, JOURNAL_GENESIS_SHA
        with open(self.path, "rb") as fh:
            raw = fh.read()
        if raw and not raw.endswith(b"\n"):
            # Torn write from a crash: only the physical, unterminated tail.
            cut = raw.rfind(b"\n") + 1
            with open(self.path, "r+b") as fh:
                fh.truncate(cut)
            raw = raw[:cut]
            self.torn_tail_discarded = True
        seq, prev = 0, JOURNAL_GENESIS_SHA
        for lineno, line in enumerate(raw.split(b"\n")[:-1], start=1):
            try:
                record = json.loads(line)
            except json.JSONDecodeError as exc:
                raise LedgerCorrupt(f"legion journal line {lineno} is not JSON",
                                    {"line": lineno, "path": self.path}) from exc
            if not isinstance(record, dict) or set(record) != FIELDS:
                raise LedgerCorrupt(f"legion journal line {lineno} has an invalid field set",
                                    {"line": lineno, "path": self.path})
            if record["legion_id"] != self.legion_id:
                raise LedgerCorrupt(f"legion journal line {lineno} belongs to another legion",
                                    {"line": lineno, "found": record["legion_id"]})
            if record["seq"] != seq + 1 or record["prev_sha"] != prev:
                raise LedgerCorrupt(f"legion journal chain break at line {lineno}",
                                    {"line": lineno, "path": self.path})
            if _record_sha(record) != record["sha"]:
                raise LedgerCorrupt(f"legion journal hash mismatch at line {lineno}",
                                    {"line": lineno, "path": self.path})
            seq, prev = record["seq"], record["sha"]
            self._records.append(record)
        return seq, prev

    def records(self, kind: str | None = None) -> Iterator[dict[str, Any]]:
        with self._lock:
            snapshot = list(self._records)
        for record in snapshot:
            if kind is None or record["kind"] == kind:
                yield record

    @property
    def tail(self) -> dict[str, Any]:
        return {"receipts": self._tail_seq, "tail_sha": self._tail_sha,
                "genesis_sha": JOURNAL_GENESIS_SHA, "path": self.path}

    # -- writing ----------------------------------------------------------- #

    def append(self, kind: str, body: Mapping[str, Any], *,
               sync: bool = False) -> dict[str, Any]:
        json.loads(canonical_json(body))  # pure JSON or refuse
        with self._lock:
            record = {"seq": self._tail_seq + 1, "kind": kind,
                      "legion_id": self.legion_id, "body": dict(body),
                      "at": self._clock(), "prev_sha": self._tail_sha}
            record["sha"] = _record_sha(record)
            self._fh.write(canonical_json(record) + "\n")
            self._fh.flush()
            self._since_sync += 1
            if sync or self._since_sync >= self._fsync_every:
                os.fsync(self._fh.fileno())
                self._since_sync = 0
            self._tail_seq, self._tail_sha = record["seq"], record["sha"]
            self._records.append(record)
            return record

    def sync(self) -> None:
        with self._lock:
            self._fh.flush()
            os.fsync(self._fh.fileno())
            self._since_sync = 0

    def close(self) -> None:
        with self._lock:
            if not self._fh.closed:
                self._fh.flush()
                os.fsync(self._fh.fileno())
                self._fh.close()


def verify_journal(path: str, legion_id: str) -> dict[str, Any]:
    """Independent full verification (opens read-only semantics: a torn tail
    is reported, not repaired)."""
    with open(path, "rb") as fh:
        raw = fh.read()
    torn = bool(raw) and not raw.endswith(b"\n")
    seq, prev = 0, JOURNAL_GENESIS_SHA
    lines = raw.split(b"\n")
    for lineno, line in enumerate(lines[:-1], start=1):
        record = json.loads(line)
        if (set(record) != FIELDS or record["legion_id"] != legion_id
                or record["seq"] != seq + 1 or record["prev_sha"] != prev
                or _record_sha(record) != record["sha"]):
            raise LedgerCorrupt(f"legion journal invalid at line {lineno}",
                                {"line": lineno, "path": path})
        seq, prev = record["seq"], record["sha"]
    return {"receipts": seq, "tail_sha": prev, "torn_tail": torn}
