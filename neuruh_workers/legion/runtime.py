"""Legion runtime: spawn N governed agents, supervise them, reduce to ONE output.

Vocabulary (what the words mean here - nothing more):

- **agent**   one leased unit of work with its own identity
              (``agt-<sha(legion_id|unit_id)>``), attempt counter, deadline and
              sealed receipt.  An agent is a logical worker, not an OS process:
              thousands of agents share a bounded physical thread pool
              (``max_parallel``).  That is what lets 10,000 agents run on an
              8 GB host.
- **squad**   ``squad_size`` agents (in unit_id order).  When every member is
              terminal the squad lead reduces their results and seals the Merkle
              root of their terminal receipts.
- **cohort**  ``cohort_size`` squads, reduced and sealed the same way.
- **legion**  every cohort; the prime reducer produces the ONE final output and
              seals the root of the whole tree.

Guarantees:

- Deterministic identity: legion_id is a pure function of the sealed spec
  (profile + version + unit manifest sha + shape); agent_id of legion + unit.
- The final output is independent of scheduling: reducers always see members
  in agent_id order, and wall-clock facts are kept OUT of the content sha.
- Crash-safe: every state change is a journal receipt.  Re-opening replays the
  journal; terminal agents are never re-run, sealed squads/cohorts are served
  from the journal, and a sealed legion returns its recorded result untouched.
- Supervised: per-agent deadline, bounded retries, typed terminal failure.  A
  timed-out attempt's late result is discarded (lease epoch mismatch).
- Exclusive: one live supervisor per legion (lock file with pid); a second
  opener refuses instead of interleaving the chain.
- Kill switch: if ``halt_path`` exists, no new leases are issued; in-flight
  agents drain and the legion is receipted HALTED (resumable later).
- Hard ceiling: ``LEGION_MAX_AGENTS_ABSOLUTE``.  No agent can spawn agents;
  only the supervisor leases, so there is no self-replication path.
"""
from __future__ import annotations

import errno
import json
import os
import threading
import time
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass, field
from typing import Any, Callable, Mapping, Sequence

from ..contracts import WorkflowError, canonical_json, content_sha, sha256_hex
from .journal import LegionJournal

LEGION_SCHEMA = "neuruh.legion.v1"
LEGION_MAX_AGENTS_DEFAULT = 10_000
LEGION_MAX_AGENTS_ABSOLUTE = 25_000
LEGION_MAX_PARALLEL_ABSOLUTE = 256


class LegionRefused(WorkflowError):
    code = "LEGION_REFUSED"


class LegionSpecDrift(WorkflowError):
    code = "LEGION_SPEC_DRIFT"


class LegionBusy(WorkflowError):
    code = "LEGION_BUSY"


class LegionCrashInjected(Exception):
    """Test hook only: simulates the supervisor process dying."""


# --------------------------------------------------------------------------- #
# Merkle


def merkle_root(leaves: Sequence[str]) -> str:
    if not leaves:
        return sha256_hex("neuruh-legion-empty-tree")
    level = [sha256_hex("leaf:" + leaf) for leaf in leaves]
    while len(level) > 1:
        if len(level) % 2:
            level.append(level[-1])
        level = [sha256_hex("node:" + level[i] + level[i + 1])
                 for i in range(0, len(level), 2)]
    return level[0]


# --------------------------------------------------------------------------- #
# Profiles and spec


@dataclass(frozen=True)
class AgentContext:
    legion_id: str
    agent_id: str
    unit_id: str
    attempt: int
    squad_id: str


@dataclass(frozen=True)
class AgentProfile:
    """What an agent does and how its peers' work is reduced.

    ``run`` must be read-only with respect to the world: it may compute over
    its unit, it may not write, contact, spend or spawn.  Reducers must be
    pure functions of member results given in agent_id order."""
    name: str
    version: str
    description: str
    run: Callable[[AgentContext, Mapping[str, Any]], Mapping[str, Any]]
    reduce_squad: Callable[[list[Mapping[str, Any]]], Mapping[str, Any]]
    reduce_cohort: Callable[[list[Mapping[str, Any]]], Mapping[str, Any]]
    finalize: Callable[[list[Mapping[str, Any]], Mapping[str, Any]], Mapping[str, Any]]
    read_only: bool = True


@dataclass(frozen=True)
class LegionSpec:
    mission: str
    profile: str
    profile_version: str
    unit_manifest_sha: str
    unit_count: int
    max_agents: int = LEGION_MAX_AGENTS_DEFAULT
    max_parallel: int = 32
    squad_size: int = 50
    cohort_size: int = 20
    agent_timeout_s: float = 30.0
    max_attempts: int = 2

    def validate(self) -> None:
        if not self.mission or not isinstance(self.mission, str):
            raise LegionRefused("mission is required")
        if self.max_agents > LEGION_MAX_AGENTS_ABSOLUTE:
            raise LegionRefused(
                f"max_agents {self.max_agents} exceeds the absolute ceiling "
                f"{LEGION_MAX_AGENTS_ABSOLUTE}", {"max_agents": self.max_agents})
        if not 1 <= self.unit_count <= self.max_agents:
            raise LegionRefused(
                f"unit_count {self.unit_count} outside 1..{self.max_agents}",
                {"unit_count": self.unit_count, "max_agents": self.max_agents})
        if not 1 <= self.max_parallel <= LEGION_MAX_PARALLEL_ABSOLUTE:
            raise LegionRefused("max_parallel outside 1..%d" % LEGION_MAX_PARALLEL_ABSOLUTE)
        if self.squad_size < 1 or self.cohort_size < 1:
            raise LegionRefused("squad_size and cohort_size must be >= 1")
        if self.agent_timeout_s <= 0 or not 1 <= self.max_attempts <= 5:
            raise LegionRefused("agent_timeout_s must be > 0 and max_attempts 1..5")

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": LEGION_SCHEMA, "mission": self.mission,
            "profile": self.profile, "profile_version": self.profile_version,
            "unit_manifest_sha": self.unit_manifest_sha,
            "unit_count": self.unit_count, "max_agents": self.max_agents,
            "max_parallel": self.max_parallel, "squad_size": self.squad_size,
            "cohort_size": self.cohort_size,
            "agent_timeout_s": float(self.agent_timeout_s),
            "max_attempts": self.max_attempts,
        }

    @property
    def legion_id(self) -> str:
        # max_parallel is physical scheduling, not content: the same work at a
        # different parallelism is the same legion (and resumes as one).
        identity = {k: v for k, v in self.to_dict().items() if k != "max_parallel"}
        return "legion-" + content_sha(identity)[:24]


def unit_manifest_sha(units: Sequence[Mapping[str, Any]]) -> str:
    return content_sha(sorted(units, key=lambda u: u["unit_id"]))


# --------------------------------------------------------------------------- #
# Lock


class _LegionLock:
    def __init__(self, path: str):
        self.path = path
        self._held = False

    def acquire(self) -> None:
        for _ in range(2):
            try:
                fd = os.open(self.path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
            except FileExistsError:
                if self._stale():
                    os.unlink(self.path)
                    continue
                raise LegionBusy("another supervisor holds this legion",
                                 {"lock": self.path})
            with os.fdopen(fd, "w") as fh:
                fh.write(str(os.getpid()))
            self._held = True
            return
        raise LegionBusy("could not acquire legion lock", {"lock": self.path})

    def _stale(self) -> bool:
        try:
            with open(self.path) as fh:
                pid = int(fh.read().strip() or "0")
        except (OSError, ValueError):
            return True
        if pid == os.getpid():
            return False
        try:
            os.kill(pid, 0)
        except OSError as exc:
            return exc.errno == errno.ESRCH
        return False

    def release(self) -> None:
        if self._held:
            try:
                os.unlink(self.path)
            except FileNotFoundError:
                pass
            self._held = False


# --------------------------------------------------------------------------- #
# Supervisor


@dataclass
class _AgentState:
    agent_id: str
    unit_id: str
    index: int
    squad_id: str
    attempts: int = 0
    terminal: str | None = None          # SUCCEEDED | FAILED
    terminal_sha: str | None = None
    result: Mapping[str, Any] | None = None
    error: str | None = None


@dataclass
class LegionResult:
    legion_id: str
    state: str                              # SEALED | HALTED
    final_output: Mapping[str, Any] | None
    final_sha: str | None
    merkle_root: str | None
    counts: Mapping[str, Any]
    journal: Mapping[str, Any]
    final_path: str | None
    run_facts: Mapping[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {"legion_id": self.legion_id, "state": self.state,
                "final_sha": self.final_sha, "merkle_root": self.merkle_root,
                "counts": dict(self.counts), "journal": dict(self.journal),
                "final_path": self.final_path, "run_facts": dict(self.run_facts)}


class Legion:
    def __init__(self, spec: LegionSpec, profile: AgentProfile,
                 units: Sequence[Mapping[str, Any]], root: str, *,
                 halt_path: str | None = None, clock=None,
                 crash_after_terminal: int | None = None,
                 status_interval_s: float = 1.0):
        spec.validate()
        if profile.name != spec.profile or profile.version != spec.profile_version:
            raise LegionSpecDrift("profile does not match the sealed spec",
                                  {"spec": [spec.profile, spec.profile_version],
                                   "profile": [profile.name, profile.version]})
        if not profile.read_only:
            raise LegionRefused("legion profiles must be read-only",
                                {"profile": profile.name})
        ids = [u.get("unit_id") for u in units]
        if any(not isinstance(i, str) or not i for i in ids):
            raise LegionRefused("every unit needs a non-empty string unit_id")
        if len(set(ids)) != len(ids):
            raise LegionRefused("unit_ids must be unique")
        if len(units) != spec.unit_count:
            raise LegionSpecDrift("unit count differs from the sealed spec",
                                  {"spec": spec.unit_count, "units": len(units)})
        if unit_manifest_sha(units) != spec.unit_manifest_sha:
            raise LegionSpecDrift("unit manifest sha differs from the sealed spec",
                                  {"spec": spec.unit_manifest_sha})
        self.spec = spec
        self.profile = profile
        self.legion_id = spec.legion_id
        self.units = {u["unit_id"]: u for u in units}
        self.root = root
        self.dir = os.path.join(root, self.legion_id)
        os.makedirs(self.dir, exist_ok=True)
        self.journal_path = os.path.join(self.dir, "journal.jsonl")
        self.final_path = os.path.join(self.dir, "FINAL_OUTPUT.json")
        self.status_path = os.path.join(self.dir, "status.json")
        self.halt_path = halt_path or os.path.join(self.dir, "HALT")
        self._clock = clock
        self._crash_after = crash_after_terminal
        self._status_interval = status_interval_s
        self._lock = _LegionLock(os.path.join(self.dir, "supervisor.lock"))

        ordered = sorted(self.units)
        self.agents: dict[str, _AgentState] = {}
        self.squads: dict[str, list[str]] = {}
        for index, unit_id in enumerate(ordered):
            agent_id = "agt-" + sha256_hex(self.legion_id + "|" + unit_id)[:20]
            squad_id = f"squad-{index // spec.squad_size:05d}"
            self.agents[agent_id] = _AgentState(agent_id, unit_id, index, squad_id)
            self.squads.setdefault(squad_id, []).append(agent_id)
        for members in self.squads.values():
            members.sort()
        squad_ids = sorted(self.squads)
        self.cohorts: dict[str, list[str]] = {}
        for index, squad_id in enumerate(squad_ids):
            self.cohorts.setdefault(f"cohort-{index // spec.cohort_size:04d}",
                                    []).append(squad_id)
        self.sealed_squads: dict[str, dict[str, Any]] = {}
        self.sealed_cohorts: dict[str, dict[str, Any]] = {}

    # -- replay ------------------------------------------------------------ #

    def _replay(self, journal: LegionJournal) -> dict[str, Any] | None:
        opened = next(journal.records("legion.opened"), None)
        if opened is None:
            journal.append("legion.opened", {
                "spec": self.spec.to_dict(), "legion_id": self.legion_id,
                "agents": len(self.agents), "squads": len(self.squads),
                "cohorts": len(self.cohorts)}, sync=True)
        elif opened["body"]["spec"] != self.spec.to_dict():
            recorded = {k: v for k, v in opened["body"]["spec"].items() if k != "max_parallel"}
            current = {k: v for k, v in self.spec.to_dict().items() if k != "max_parallel"}
            if recorded != current:
                raise LegionSpecDrift("journal was opened with a different spec",
                                      {"legion_id": self.legion_id})
        for record in journal.records():
            kind, body = record["kind"], record["body"]
            agent = self.agents.get(body.get("agent_id", ""))
            if kind == "agent.leased" and agent:
                agent.attempts = max(agent.attempts, body["attempt"])
            elif kind == "agent.succeeded" and agent:
                agent.terminal, agent.terminal_sha = "SUCCEEDED", record["sha"]
                agent.result = body["result"]
            elif kind == "agent.failed" and agent and body.get("final"):
                agent.terminal, agent.terminal_sha = "FAILED", record["sha"]
                agent.error = body.get("error")
            elif kind == "squad.sealed":
                self.sealed_squads[body["squad_id"]] = body
            elif kind == "cohort.sealed":
                self.sealed_cohorts[body["cohort_id"]] = body
        sealed = next(journal.records("legion.sealed"), None)
        return sealed["body"] if sealed else None

    # -- run --------------------------------------------------------------- #

    def run(self) -> LegionResult:
        self._lock.acquire()
        journal = LegionJournal(self.journal_path, self.legion_id, clock=self._clock)
        try:
            if journal.torn_tail_discarded:
                journal.append("journal.torn_tail_discarded", {"path": self.journal_path})
            sealed = self._replay(journal)
            if sealed is not None:
                return self._result_from_sealed(sealed, journal)
            resumed = any(a.attempts for a in self.agents.values())
            if resumed:
                journal.append("legion.resumed", {
                    "terminal": sum(1 for a in self.agents.values() if a.terminal),
                    "sealed_squads": len(self.sealed_squads)}, sync=True)
            return self._supervise(journal)
        finally:
            journal.close()
            self._lock.release()

    def _supervise(self, journal: LegionJournal) -> LegionResult:
        spec = self.spec
        started = time.monotonic()
        queue = [a.agent_id for a in sorted(self.agents.values(), key=lambda a: a.index)
                 if not a.terminal]
        # Seal anything that was complete before a crash but not yet sealed.
        for squad_id in sorted(self.squads):
            self._maybe_seal_squad(journal, squad_id)
        inflight: dict[Future, tuple[str, int, float]] = {}
        zombies: list[Future] = []
        terminal_count = sum(1 for a in self.agents.values() if a.terminal)
        last_status = 0.0
        peak_inflight = 0
        halted = False

        pool = ThreadPoolExecutor(max_workers=spec.max_parallel,
                                  thread_name_prefix=self.legion_id[:15])
        try:
            while queue or inflight:
                if not halted and os.path.exists(self.halt_path):
                    halted = True
                    journal.append("legion.halt_requested",
                                   {"halt_path": self.halt_path,
                                    "queued": len(queue), "inflight": len(inflight)},
                                   sync=True)
                zombies = [z for z in zombies if not z.done()]
                capacity = spec.max_parallel - len(inflight) - len(zombies)
                if capacity <= 0 and not inflight and queue and not halted:
                    # Every slot is held by a timed-out thread: give them one
                    # more deadline to return before declaring the legion stalled.
                    wait(zombies, timeout=spec.agent_timeout_s, return_when=FIRST_COMPLETED)
                    zombies = [z for z in zombies if not z.done()]
                    capacity = spec.max_parallel - len(zombies)
                if capacity <= 0 and not inflight and queue and not halted:
                    journal.append("legion.stalled", {
                        "abandoned_threads": len(zombies),
                        "reason": "every pool slot is held by a timed-out agent"},
                        sync=True)
                    halted = True
                    break
                while queue and capacity > 0 and not halted:
                    agent = self.agents[queue.pop(0)]
                    agent.attempts += 1
                    journal.append("agent.leased", {
                        "agent_id": agent.agent_id, "unit_id": agent.unit_id,
                        "attempt": agent.attempts, "squad_id": agent.squad_id,
                        "deadline_s": float(spec.agent_timeout_s)})
                    ctx = AgentContext(self.legion_id, agent.agent_id, agent.unit_id,
                                       agent.attempts, agent.squad_id)
                    unit = self.units[agent.unit_id]
                    future = pool.submit(self.profile.run, ctx, unit)
                    inflight[future] = (agent.agent_id, agent.attempts, time.monotonic())
                    capacity -= 1
                peak_inflight = max(peak_inflight, len(inflight))
                if halted and not inflight:
                    break
                done, _ = wait(list(inflight), timeout=0.05,
                               return_when=FIRST_COMPLETED)
                now = time.monotonic()
                for future in done:
                    agent_id, attempt, _t0 = inflight.pop(future)
                    agent = self.agents[agent_id]
                    exc = future.exception()
                    if exc is None:
                        result = future.result()
                        try:
                            json.loads(canonical_json(result))
                        except (TypeError, ValueError) as bad:
                            exc = bad
                    if exc is None:
                        record = journal.append("agent.succeeded", {
                            "agent_id": agent_id, "attempt": attempt,
                            "result": result, "result_sha": content_sha(result)})
                        agent.terminal, agent.terminal_sha = "SUCCEEDED", record["sha"]
                        agent.result = result
                    else:
                        final = attempt >= spec.max_attempts
                        error = f"{type(exc).__name__}: {str(exc)[:300]}"
                        record = journal.append("agent.failed", {
                            "agent_id": agent_id, "attempt": attempt,
                            "error": error, "final": final})
                        if final:
                            agent.terminal, agent.terminal_sha = "FAILED", record["sha"]
                            agent.error = error
                        else:
                            queue.append(agent_id)
                    if agent.terminal:
                        terminal_count += 1
                        self._maybe_seal_squad(journal, agent.squad_id)
                        if self._crash_after is not None and terminal_count >= self._crash_after:
                            journal.sync()
                            raise LegionCrashInjected(terminal_count)
                for future, (agent_id, attempt, t0) in list(inflight.items()):
                    if now - t0 <= spec.agent_timeout_s:
                        continue
                    inflight.pop(future)
                    if not future.cancel():
                        zombies.append(future)   # slot is freed when it returns
                    agent = self.agents[agent_id]
                    final = attempt >= spec.max_attempts
                    record = journal.append("agent.timed_out", {
                        "agent_id": agent_id, "attempt": attempt,
                        "deadline_s": float(spec.agent_timeout_s), "final": final})
                    if final:
                        agent.terminal, agent.terminal_sha = "FAILED", record["sha"]
                        agent.error = f"TIMEOUT after {spec.agent_timeout_s}s"
                        journal.append("agent.failed", {
                            "agent_id": agent_id, "attempt": attempt,
                            "error": agent.error, "final": True})
                        terminal_count += 1
                        self._maybe_seal_squad(journal, agent.squad_id)
                    else:
                        queue.append(agent_id)
                if now - last_status >= self._status_interval:
                    last_status = now
                    self._write_status(journal, "RUNNING", len(queue), len(inflight),
                                       started, peak_inflight)
        finally:
            pool.shutdown(wait=False, cancel_futures=True)

        elapsed = round(time.monotonic() - started, 3)
        if halted and any(not a.terminal for a in self.agents.values()):
            journal.append("legion.halted", {
                "terminal": terminal_count, "remaining": len(self.agents) - terminal_count},
                sync=True)
            self._write_status(journal, "HALTED", len(queue), 0, started, peak_inflight)
            return LegionResult(self.legion_id, "HALTED", None, None, None,
                                self._counts(), journal.tail, None,
                                {"elapsed_s": elapsed, "peak_inflight": peak_inflight})
        return self._seal(journal, elapsed, peak_inflight, started)

    # -- reduce tree ------------------------------------------------------- #

    def _maybe_seal_squad(self, journal: LegionJournal, squad_id: str) -> None:
        if squad_id in self.sealed_squads:
            return
        members = [self.agents[a] for a in self.squads[squad_id]]
        if any(not m.terminal for m in members):
            return
        ok = [m.result for m in members if m.terminal == "SUCCEEDED"]
        failed = [{"agent_id": m.agent_id, "unit_id": m.unit_id, "error": m.error}
                  for m in members if m.terminal == "FAILED"]
        summary = dict(self.profile.reduce_squad(ok))
        body = {"squad_id": squad_id, "members": len(members),
                "succeeded": len(ok), "failed": failed,
                "merkle_root": merkle_root([self._leaf(m) for m in members]),
                "summary": summary, "summary_sha": content_sha(summary)}
        journal.append("squad.sealed", body)
        self.sealed_squads[squad_id] = body
        cohort_id = next(c for c, squads in self.cohorts.items() if squad_id in squads)
        self._maybe_seal_cohort(journal, cohort_id)

    @staticmethod
    def _leaf(agent: _AgentState) -> str:
        # Content only: the journal receipt sha carries wall-clock time and chain
        # position, which would make the tree depend on scheduling.
        return content_sha({"agent_id": agent.agent_id, "unit_id": agent.unit_id,
                            "terminal": agent.terminal,
                            "result_sha": content_sha(agent.result)
                            if agent.terminal == "SUCCEEDED" else None,
                            "error": agent.error})

    def _maybe_seal_cohort(self, journal: LegionJournal, cohort_id: str) -> None:
        if cohort_id in self.sealed_cohorts:
            return
        squad_ids = self.cohorts[cohort_id]
        if any(s not in self.sealed_squads for s in squad_ids):
            return
        squads = [self.sealed_squads[s] for s in squad_ids]
        summary = dict(self.profile.reduce_cohort([s["summary"] for s in squads]))
        body = {"cohort_id": cohort_id, "squads": len(squads),
                "agents": sum(s["members"] for s in squads),
                "succeeded": sum(s["succeeded"] for s in squads),
                "failed": sum(len(s["failed"]) for s in squads),
                "merkle_root": merkle_root([s["merkle_root"] for s in squads]),
                "summary": summary, "summary_sha": content_sha(summary)}
        journal.append("cohort.sealed", body, sync=True)
        self.sealed_cohorts[cohort_id] = body

    def _counts(self) -> dict[str, Any]:
        agents = list(self.agents.values())
        return {
            "agents_spawned": len(agents),
            "agents_succeeded": sum(1 for a in agents if a.terminal == "SUCCEEDED"),
            "agents_failed": sum(1 for a in agents if a.terminal == "FAILED"),
            "agents_pending": sum(1 for a in agents if not a.terminal),
            "attempts_total": sum(a.attempts for a in agents),
            "squads": len(self.squads), "squads_sealed": len(self.sealed_squads),
            "cohorts": len(self.cohorts), "cohorts_sealed": len(self.sealed_cohorts),
        }

    def _seal(self, journal: LegionJournal, elapsed: float, peak_inflight: int,
              started: float) -> LegionResult:
        for cohort_id in sorted(self.cohorts):
            self._maybe_seal_cohort(journal, cohort_id)
        cohorts = [self.sealed_cohorts[c] for c in sorted(self.cohorts)]
        root = merkle_root([c["merkle_root"] for c in cohorts])
        counts = self._counts()
        failures = [f for s in sorted(self.sealed_squads)
                    for f in self.sealed_squads[s]["failed"]]
        meta = {"legion_id": self.legion_id, "mission": self.spec.mission,
                "profile": f"{self.profile.name}@{self.profile.version}",
                "unit_manifest_sha": self.spec.unit_manifest_sha,
                "merkle_root": root, "counts": {k: counts[k] for k in (
                    "agents_spawned", "agents_succeeded", "agents_failed",
                    "squads", "cohorts")},
                "failures": failures[:50], "failures_total": len(failures)}
        final = dict(self.profile.finalize([c["summary"] for c in cohorts], meta))
        final = {"schema": "neuruh.legion.final_output.v1", **meta, "output": final}
        final_sha = content_sha(final)
        run_facts = {"elapsed_s": elapsed, "peak_inflight": peak_inflight,
                     "max_parallel": self.spec.max_parallel,
                     "attempts_total": counts["attempts_total"],
                     "agents_per_second": round(len(self.agents) / elapsed, 1) if elapsed else None}
        journal.append("legion.sealed", {
            "final_sha": final_sha, "merkle_root": root, "counts": counts,
            "run_facts": run_facts}, sync=True)
        tmp = self.final_path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as fh:
            fh.write(json.dumps(final, indent=1, sort_keys=True, ensure_ascii=False))
        os.replace(tmp, self.final_path)
        self._write_status(journal, "SEALED", 0, 0, started, peak_inflight)
        return LegionResult(self.legion_id, "SEALED", final, final_sha, root, counts,
                            journal.tail, self.final_path, run_facts)

    def _result_from_sealed(self, sealed: Mapping[str, Any],
                            journal: LegionJournal) -> LegionResult:
        final = None
        if os.path.exists(self.final_path):
            with open(self.final_path, encoding="utf-8") as fh:
                final = json.load(fh)
            if content_sha(final) != sealed["final_sha"]:
                raise LegionSpecDrift("FINAL_OUTPUT.json does not match the sealed sha",
                                      {"legion_id": self.legion_id})
        return LegionResult(self.legion_id, "SEALED", final, sealed["final_sha"],
                            sealed["merkle_root"], sealed["counts"], journal.tail,
                            self.final_path if final else None, sealed["run_facts"])

    def _write_status(self, journal: LegionJournal, state: str, queued: int,
                      inflight: int, started: float, peak: int) -> None:
        status = {"legion_id": self.legion_id, "mission": self.spec.mission,
                  "state": state, "queued": queued, "inflight": inflight,
                  "peak_inflight": peak, **self._counts(),
                  "elapsed_s": round(time.monotonic() - started, 1),
                  "journal": journal.tail}
        tmp = self.status_path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(status, fh, sort_keys=True)
        os.replace(tmp, self.status_path)
