"""Resumable interval rebuilds with explicit affected boundaries.

Why intervals
-------------
Keeping full per-event funnel state costs one checkpoint per event; rebuilding
from scratch on every ingest costs a full replay.  This engine stores a
checkpoint every ``interval_events`` ordered events (default 256).  A
checkpoint row holds only the *delta*: the funnel state of the users that
interval actually touched (the rest is inherited by unioning earlier rows, so
a row stays proportional to one interval rather than to the whole log).  The
consumed event signatures live on the job and let a later run find the first
position where the log no longer matches what was checkpointed.

Appending new events only replays the open tail, so ingest stays cheap.  A
late event or a correction invalidates much more: session derivation is
global, so inserting an event *between* two already-processed events can merge
two sessions and change matches that happened long before the insertion point.
This engine therefore compares the current signature prefix with the
checkpointed prefix, finds the first differing position, and restarts at the
last checkpoint **at or before** that position.  The report says exactly where
that was (``affected_from_ms`` / ``affected_from_event_id``), how many
intervals were reused and how many recomputed.  It never "patches forward from
the late timestamp".

Resumability
------------
Each interval checkpoint is committed to the ``rebuild_checkpoints`` table as
it finishes, and the job row records ``next_interval``.  A job paused (or a
process killed) therefore resumes from the exact interval boundary it had
reached, in the same process or a new one.  Events imported during a pass are detected by comparing the
input digest before and after; the job runs another incremental pass instead
of freezing numbers that do not correspond to any single input state.
"""

from __future__ import annotations

import base64
import json
import os
import threading
import time
import uuid
import zlib

from .engine import FunnelResult, StepCount
from .errors import Conflict, NotFound
from .models import Attempt, FunnelRules, UserState, new_user_state
from .sessions import derive_sessions
from .state_machine import feed_event

DEFAULT_INTERVAL_EVENTS = 256


def _get(row, key, default=None):
    try:
        v = row[key]
        return default if v is None else v
    except (KeyError, IndexError):
        return getattr(row, key, default)


def _pack(obj) -> str:
    raw = json.dumps(obj, separators=(",", ":"), sort_keys=True).encode("utf-8")
    return base64.b64encode(zlib.compress(raw, 6)).decode("ascii")


def _unpack(text: str):
    return json.loads(zlib.decompress(base64.b64decode(text.encode("ascii"))))


def _sig(row) -> str:
    """Compact, stable signature of everything that affects a computation."""

    props = _get(row, "properties") or ""
    return "|".join(
        [
            str(_get(row, "event_id")),
            str(int(_get(row, "ts_ms"))),
            str(_get(row, "name")),
            str(_get(row, "user_id")),
            str(_get(row, "kind") or ""),
            str(_get(row, "session_id") or ""),
            str(int(_get(row, "revision") or 0)),
            props,
        ]
    )


def _state_to_dict(s: UserState) -> dict:
    # Compact keys: these blobs are written after every interval.
    return {
        "a": [
            {"i": x.idx, "s": x.started_at, "t": x.ts, "ss": x.sess}
            for x in s.attempts
        ],
        "bt": s.best_ts,
        "bs": s.best_sess,
        "m": s.max_step,
        "c": s.completed,
        "e": s.entered,
        "f": s.first_entry_at,
    }


def _state_from_dict(d: dict) -> UserState:
    return UserState(
        attempts=[
            Attempt(
                idx=x["i"],
                started_at=x["s"],
                ts=list(x["t"]),
                sess=list(x["ss"]),
            )
            for x in d["a"]
        ],
        best_ts=list(d["bt"]),
        best_sess=list(d["bs"]),
        max_step=d["m"],
        completed=d["c"],
        entered=d["e"],
        first_entry_at=d["f"],
    )


def _pid_alive(pid: int) -> bool:
    if not pid:
        return False
    try:
        os.kill(int(pid), 0)
    except ProcessLookupError:
        return False
    except PermissionError:  # exists, owned by someone else
        return True
    return True


def first_difference(old: list[str], new: list[str]) -> int:
    limit = min(len(old), len(new))
    for i in range(limit):
        if old[i] != new[i]:
            return i
    return limit


def describe_difference(old: list[str], new: list[str], pos: int) -> str:
    """Say what changed at the affected boundary, not just that it changed."""

    if not old:
        return "initial build"
    if pos >= len(new):
        if len(old) == len(new):
            return "no event changed; only the open tail interval was replayed"
        return "events removed"
    if pos >= len(old):
        return "events appended after the last checkpoint"
    old_id = old[pos].split("|", 1)[0]
    new_id = new[pos].split("|", 1)[0]
    if old_id == new_id:
        return (
            "event corrected in place (timestamp/name/user/session/revision "
            "changed); session derivation may reach back before this event"
        )
    old_index = {sig.split("|", 1)[0]: i for i, sig in enumerate(old)}
    new_index = {sig.split("|", 1)[0]: i for i, sig in enumerate(new)}
    if new_id in old_index and old_index[new_id] > pos:
        return (
            "event corrected: an event that used to sit here moved later in "
            "the timeline, shifting this region; sessions are re-derived"
        )
    if old_id in new_index and new_index[old_id] > pos:
        return (
            "late event inserted before this position; sessions spanning the "
            "insertion point are re-derived"
        )
    return "event order changed at this boundary; region re-derived"


class RebuildJob:
    FIELDS = (
        "job_id", "funnel_id", "rules_version", "rules_hash", "status",
        "input_digest", "event_count", "affected_from_ms",
        "affected_from_event_id", "affected_reason", "reused_intervals",
        "recomputed_intervals", "total_intervals", "passes", "report_id",
        "error", "started_at_ms", "updated_at_ms", "next_interval",
        "signatures", "checkpoints", "input_changed_during_rebuild",
        "interval_events", "session_gap_ms", "owner_pid",
    )

    def __init__(self, **kw):
        self.job_id = kw.get("job_id") or str(uuid.uuid4())
        self.funnel_id = kw["funnel_id"]
        self.rules_version = kw["rules_version"]
        self.rules_hash = kw["rules_hash"]
        self.status = kw.get("status", "queued")
        self.input_digest = kw.get("input_digest", "")
        self.event_count = kw.get("event_count", 0)
        self.affected_from_ms = kw.get("affected_from_ms")
        self.affected_from_event_id = kw.get("affected_from_event_id")
        self.affected_reason = kw.get("affected_reason", "")
        self.reused_intervals = kw.get("reused_intervals", 0)
        self.recomputed_intervals = kw.get("recomputed_intervals", 0)
        self.total_intervals = kw.get("total_intervals", 0)
        self.passes = kw.get("passes", 0)
        self.report_id = kw.get("report_id", "")
        self.error = kw.get("error", "")
        self.started_at_ms = kw.get("started_at_ms", int(time.time() * 1000))
        self.updated_at_ms = kw.get("updated_at_ms", 0)
        self.next_interval = kw.get("next_interval", 0)
        # The consumed-prefix is a compact list of event ids; funnel state
        # lives in the checkpoint table, one row per interval.
        self.signatures = kw.get("signatures") or _pack([])
        self.input_changed_during_rebuild = kw.get(
            "input_changed_during_rebuild", False
        )
        self.interval_events = kw.get("interval_events", DEFAULT_INTERVAL_EVENTS)
        self.session_gap_ms = kw.get("session_gap_ms", 30 * 60 * 1000)
        # Public metadata only; checkpoint data lives in signatures/checkpoints.
        self.inherited_from_job = kw.get("inherited_from_job", "")
        # The pid that owns the worker thread.  Another process uses it to
        # decide whether a "running" row is genuinely running or left behind.
        self.owner_pid = kw.get("owner_pid", 0)

    def to_dict(self, include_checkpoint: bool = False) -> dict:
        body = {
            "job_id": self.job_id,
            "funnel_id": self.funnel_id,
            "rules_version": self.rules_version,
            "rules_hash": self.rules_hash,
            "status": self.status,
            "input_digest": self.input_digest,
            "event_count": self.event_count,
            "affected_from_ms": self.affected_from_ms,
            "affected_from_event_id": self.affected_from_event_id,
            "affected_reason": self.affected_reason,
            "reused_intervals": self.reused_intervals,
            "recomputed_intervals": self.recomputed_intervals,
            "total_intervals": self.total_intervals,
            "passes": self.passes,
            "report_id": self.report_id,
            "error": self.error,
            "started_at_ms": self.started_at_ms,
            "updated_at_ms": self.updated_at_ms,
            "input_changed_during_rebuild": self.input_changed_during_rebuild,
            "inherited_from_job": self.inherited_from_job,
            "interval_events": self.interval_events,
            "owner_pid": self.owner_pid,
            "progress": {
                "intervals_done": self.next_interval,
                "intervals_total": self.total_intervals,
                "events_done": min(self.event_count, self.next_interval * self.interval_events),
                "events_total": self.event_count,
            },
        }
        if include_checkpoint:
            body["signatures"] = self.signatures
            body["next_interval"] = self.next_interval
        return body


class RebuildManager:
    """Owns rebuild jobs; one worker thread per job.

    Job rows live in the database, not only in memory: the compute service
    writes them, the API process reads them, and a restart can find the
    checkpoint it left behind.  Only the process that *spawned* a job holds
    its worker thread, so ``stop`` in another process pauses the row and the
    owning worker notices on its next interval.
    """

    def __init__(self, store, reports, session_gap_ms: int = 30 * 60 * 1000,
                 interval_events: int = DEFAULT_INTERVAL_EVENTS):
        self.store = store
        self.reports = reports
        self.session_gap_ms = session_gap_ms
        self.interval_events = interval_events
        self._jobs: dict[str, RebuildJob] = {}
        self._threads: dict[str, threading.Thread] = {}
        self._stops: dict[str, threading.Event] = {}
        self._lock = threading.RLock()
        for body in store.list_jobs():
            job = RebuildJob(**{k: v for k, v in body.items() if k in RebuildJob.FIELDS})
            # A job whose owner process is gone has no live worker.  Say so
            # instead of letting a dead job look like it is still progressing.
            # A live owner in another process is left alone.
            if job.status in ("running", "queued") and not _pid_alive(job.owner_pid):
                job.status = "paused"
                job.error = job.error or "interrupted by process restart"
                self._persist(job)
            self._jobs[job.job_id] = job

    # -- persistence ------------------------------------------------------

    def _persist(self, job: RebuildJob) -> None:
        self.store.upsert_job(job.to_dict(include_checkpoint=True))

    def _save(self, job: RebuildJob) -> None:
        job.updated_at_ms = int(time.time() * 1000)
        self._persist(job)

    def reload(self, job_id: str) -> RebuildJob:
        """Refresh in-memory state from the DB (another process may write)."""

        body = self.store.get_job(job_id)
        if body is None:
            raise NotFound(f"rebuild job '{job_id}' not found")
        job = RebuildJob(**{k: v for k, v in body.items() if k in RebuildJob.FIELDS})
        self._jobs[job.job_id] = job
        return job

    # -- queries ----------------------------------------------------------

    def get(self, job_id: str) -> RebuildJob:
        if job_id not in self._jobs:
            return self.reload(job_id)
        return self._jobs[job_id]

    def list(self, funnel_id: str | None = None) -> list[dict]:
        return [
            {k: v for k, v in body.items() if k not in ("signatures", "checkpoints")}
            for body in self.store.list_jobs(funnel_id)
        ]

    def active_for(self, funnel_id: str) -> RebuildJob | None:
        body = self.store.active_job(funnel_id)
        if body is None:
            return None
        job = RebuildJob(**{k: v for k, v in body.items() if k in RebuildJob.FIELDS})
        if job.job_id in self._jobs:
            return self._jobs[job.job_id]
        return job

    # -- control ----------------------------------------------------------

    def start(self, funnel_id: str, rules: FunnelRules, rules_meta: dict,
              reason: str = "") -> RebuildJob:
        with self._lock:
            current = self.active_for(funnel_id)
            if current is not None:
                if current.rules_hash == rules.rules_hash:
                    if current.status == "paused":
                        self.resume(current.job_id)
                    return current
                raise Conflict(
                    f"a rebuild for '{funnel_id}' is already {current.status} under "
                    f"rules v{current.rules_version}; stop it before starting another",
                    details={"job_id": current.job_id},
                )
            digest, count = self.store.input_digest()
            job = RebuildJob(
                funnel_id=funnel_id,
                rules_version=rules.version,
                rules_hash=rules.rules_hash,
                status="queued",
                input_digest=digest,
                event_count=count,
                affected_reason=reason or "initial build",
                interval_events=self.interval_events,
                session_gap_ms=self.session_gap_ms,
            )
            self._inherit_checkpoints(job)
            self._jobs[job.job_id] = job
            self._save(job)
        self.reports.mark_stale(funnel_id, f"rebuild {job.job_id} in progress")
        self._spawn(job)
        return job

    def _inherit_checkpoints(self, job: RebuildJob) -> None:
        """Seed a fresh job with the last completed chain for the same rules.

        This is what makes an append cheap: the new run starts from the
        previous run's checkpoints and only replays the open tail.  It is only
        done when the rule identity and interval geometry match — a different
        rule set means every stored funnel state is meaningless.
        """

        candidates = []
        for body in self.store.list_jobs(job.funnel_id):
            if (
                body.get("rules_hash") == job.rules_hash
                and body.get("status") == "complete"
                and int(body.get("interval_events") or 0) == job.interval_events
                and body.get("signatures")
            ):
                candidates.append(body)
        if not candidates:
            return
        previous = max(candidates, key=lambda b: b.get("updated_at_ms") or 0)
        if not self.store.list_checkpoints(previous["job_id"]):
            return
        job.signatures = previous["signatures"]
        job.next_interval = previous.get("next_interval", 0)
        job.total_intervals = previous.get("total_intervals", 0)
        job.inherited_from_job = previous["job_id"]
        self.store.copy_checkpoints(previous["job_id"], job.job_id)

    def resume(self, job_id: str) -> RebuildJob:
        with self._lock:
            job = self.get(job_id)
            if job_id in self._threads and self._threads[job_id].is_alive():
                return job
            if job.status not in ("paused", "failed"):
                raise Conflict(f"job is {job.status}; only paused or failed jobs resume")
            job.status = "queued"
            job.error = ""
            self._save(job)
            self._spawn(job)
            return job

    def stop(self, job_id: str) -> RebuildJob:
        """Request a stop and pause the row immediately.

        Pausing the row first makes the request visible to a worker in another
        process too; the owner then notices ``status != running`` while
        writing its next checkpoint and exits.
        """

        with self._lock:
            job = self.get(job_id)
            stop = self._stops.get(job_id)
            thread = self._threads.get(job_id)
            if job.status in ("queued", "running"):
                job.status = "paused"
                self._save(job)
        if stop is not None:
            stop.set()
        if thread is not None and thread.is_alive():
            thread.join(timeout=15)
        return self.reload(job_id)

    def wait(self, job_id: str, timeout: float = 30.0) -> RebuildJob:
        thread = self._threads.get(job_id)
        if thread is not None:
            thread.join(timeout=timeout)
        return self.reload(job_id)

    def _spawn(self, job: RebuildJob) -> None:
        stop = threading.Event()
        self._stops[job.job_id] = stop
        job.owner_pid = os.getpid()

        def target():
            try:
                self._run(job, stop)
            except Exception as exc:  # keep the job resumable and inspectable
                with self._lock:
                    job.status = "failed"
                    job.error = f"{type(exc).__name__}: {exc}"
                    self._save(job)

        thread = threading.Thread(target=target, name=f"rebuild-{job.job_id[:8]}", daemon=True)
        self._threads[job.job_id] = thread
        thread.start()

    # -- engine -----------------------------------------------------------

    def _run(self, job: RebuildJob, stop: threading.Event) -> None:
        rules = self.store.get_rules(job.funnel_id, job.rules_version)
        with self._lock:
            job.status = "running"
            job.error = ""
            self._save(job)

        while True:
            if stop.is_set():
                self._finish_paused(job)
                return

            rows = self.store.all_events()
            sessions = derive_sessions(rows, job.session_gap_ms)
            sigs = [_sig(r) for r in rows]
            old_sigs = _unpack(job.signatures) if job.signatures else []
            checkpoints = self.store.list_checkpoints(job.job_id)

            if job.passes == 0 and not old_sigs:
                dirty = 0
            else:
                dirty = first_difference(old_sigs, sigs)

            start_pos, start_index, states = self._choose_restart(
                checkpoints, dirty, job.interval_events
            )
            with self._lock:
                job.passes += 1
                job.event_count = len(rows)
                job.total_intervals = max(1, (len(rows) + job.interval_events - 1) // job.interval_events)
                job.reused_intervals = start_index
                job.recomputed_intervals = 0
                job.next_interval = start_index
                job.signatures = _pack(sigs)
                if dirty < len(rows):
                    job.affected_from_ms = int(_get(rows[dirty], "ts_ms"))
                    job.affected_from_event_id = _get(rows[dirty], "event_id")
                else:
                    job.affected_from_ms = None
                    job.affected_from_event_id = None
                if job.passes == 1 and not old_sigs:
                    job.affected_reason = job.affected_reason or "initial build"
                else:
                    job.affected_reason = describe_difference(old_sigs, sigs, dirty)
                job.input_changed_during_rebuild = False
                self._save(job)

            # Everything at or after the restart point is stale; drop those
            # checkpoints before writing new ones so a resumed job can never
            # read a row from an older session derivation.
            self.store.delete_checkpoints_from(job.job_id, start_index)
            digest_before = self.store.input_digest()[0]
            n_intervals = job.total_intervals
            touched: set[str] = set()

            for index in range(start_index, n_intervals):
                if stop.is_set() or self._paused_externally(job):
                    self._finish_paused(job)
                    return
                from_pos = index * job.interval_events
                to_pos = min(len(rows), from_pos + job.interval_events)
                touched.clear()
                for pos in range(from_pos, to_pos):
                    row = rows[pos]
                    user_id = _get(row, "user_id")
                    state = states.get(user_id)
                    if state is None:
                        state = new_user_state(len(rules.steps))
                        states[user_id] = state
                    touched.add(user_id)
                    feed_event(
                        state,
                        rules,
                        int(_get(row, "ts_ms")),
                        _get(row, "name"),
                        sessions.by_seq.get(int(_get(row, "seq"))),
                    )
                # Only users touched by this interval are new state; the rest
                # are inherited from the previous checkpoint.  That keeps each
                # row proportional to the interval, not to the whole log.
                self.store.put_checkpoint(
                    job.job_id,
                    index,
                    to_pos,
                    _pack({u: _state_to_dict(states[u]) for u in touched}),
                )
                with self._lock:
                    job.next_interval = index + 1
                    job.recomputed_intervals += 1
                    self._save(job)

            # Ingest during the pass invalidates the numbers we just produced.
            digest_after = self.store.input_digest()[0]
            if digest_after != digest_before:
                with self._lock:
                    job.input_changed_during_rebuild = True
                    job.affected_reason = (
                        "events were imported during the rebuild; running another "
                        "incremental pass from the affected boundary"
                    )
                    self._save(job)
                if stop.is_set():
                    self._finish_paused(job)
                    return
                continue

            result = self._aggregate(states, rules, rows)
            meta = self.store.get_rules_version(job.funnel_id, job.rules_version)
            report = self.reports.create(
                job.funnel_id,
                rules,
                {"sha256": meta["sha256"], "prev_sha256": self._prev_sha(meta)},
                result,
                job.session_gap_ms,
                input_digest=digest_after,
                extra_coverage={
                    "rebuild_job": job.job_id,
                    "affected_from_ms": job.affected_from_ms,
                    "affected_from_event_id": job.affected_from_event_id,
                    "affected_reason": job.affected_reason,
                    "reused_intervals": job.reused_intervals,
                    "recomputed_intervals": job.recomputed_intervals,
                    "interval_events": job.interval_events,
                    "input_changed_during_rebuild": job.input_changed_during_rebuild,
                },
            )
            with self._lock:
                job.report_id = report["report_id"]
                job.status = "complete"
                job.input_digest = digest_after
                self._save(job)
            return

    def _paused_externally(self, job: RebuildJob) -> bool:
        """True if another process paused this job while we were working."""

        body = self.store.get_job(job.job_id)
        return bool(body and body.get("status") not in ("running", "queued"))

    def _finish_paused(self, job: RebuildJob) -> None:
        with self._lock:
            if job.status == "running":
                job.status = "paused"
            self._save(job)

    def _prev_sha(self, meta: dict) -> str:
        rows = self.store.list_rules_versions(meta["funnel_id"])
        for i, row in enumerate(rows):
            if row["version"] == meta["version"]:
                return rows[i - 1]["sha256"] if i > 0 else "genesis"
        return "genesis"

    @staticmethod
    def _choose_restart(
        checkpoints: list[dict], dirty: int, interval_events: int
    ) -> tuple[int, int, dict[str, UserState]]:
        """Restart at the last checkpoint ending at or before ``dirty``.

        Checkpoints store only the users an interval touched, so the state at
        a boundary is the union of that row and every earlier row.  Returns
        ``(event_position, interval_index, per_user_state)``.
        """

        start_pos, start_index = 0, 0
        states: dict[str, UserState] = {}
        for ck in checkpoints:
            if ck["to_pos"] > dirty:
                break
            from_pos = ck["interval_index"] * interval_events
            if ck["to_pos"] - from_pos != interval_events:
                # A short interval is the tail of the stream; once more events
                # arrive it has to grow, so it can never be reused as-is.
                break
            states.update(
                {u: _state_from_dict(d) for u, d in _unpack(ck["body"]).items()}
            )
            start_pos = ck["to_pos"]
            start_index = ck["interval_index"] + 1
        return start_pos, start_index, states

    def _aggregate(self, states: dict[str, UserState], rules: FunnelRules,
                   rows: list) -> FunnelResult:
        result = FunnelResult(
            rules_hash=rules.rules_hash,
            rules_version=rules.version,
            step_counts=[StepCount(s.name) for s in rules.steps],
            latest_event_ts=max((int(_get(r, "ts_ms")) for r in rows), default=0),
        )
        for user_id, state in states.items():
            if not state.entered:
                continue
            result.entries += 1
            for i in range(state.max_step):
                result.step_counts[i].reached += 1
            if state.completed:
                result.conversions += 1
                for sc in result.step_counts:
                    sc.converted += 1
            elif (
                state.first_entry_at is not None
                and result.latest_event_ts - state.first_entry_at
                < rules.conversion_window_ms
            ):
                result.unresolved += 1
        return result
