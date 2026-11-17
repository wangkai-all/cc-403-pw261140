# Pointwise

A funnel analysis service with a rules editor, a resumable interval rebuild
engine, and an auditable report trail.  It is built from an empty directory:
synthetic-but-real event data goes in, and every number on screen is computed
from it by the engines in this repository.

```
python -m pointwise seed  --db data/pointwise.db --users 400 --days 14
python -m pointwise serve --db data/pointwise.db --port 8787
# optional second process, same DB:
python -m pointwise collect --db data/pointwise.db --file events.jsonl
```

Open <http://127.0.0.1:8787/>.

No third-party dependencies: Python 3.11, SQLite, and vanilla JS.

## What it does

* **Editable funnel steps.** Steps are added, removed, reordered and renamed in
  the web console.  Wait bounds, same-session constraints and repeated steps
  are ordinary per-step rules, not special cases.  A repeated step is two
  adjacent steps naming the same event; by default it needs two distinct
  occurrences, and `count_repeats` on the later step collapses the run into
  one occurrence.  Cross-session waits are the default; `same_session` opts in
  to the stricter reading.
* **Preview before saving.** `POST /api/funnels/{id}/preview` evaluates a
  candidate rule set against the live log and returns per-step numbers, the
  canonical rules hash, and a human diff against the current frozen version.
  Freezing requires the previewed hash, so what is stored is what was seen.
* **Frozen rule versions.** Every freeze writes an immutable snapshot with an
  analytics hash (content only) and a hash-chain link
  `sha256(prev_sha256 + "\n" + payload)`.  The chain is verified on every
  save, and reports carry both the rules hash and the chain link.
* **Reports state their inputs.** Each report stores its rule version and
  hash, the snapshot chain link, the event-log digest, the event count, the
  session-gap setting and the engine version.  `GET /api/funnels/{id}/explain`
  re-runs the frozen rules over today's log and says whether the report is
  still reproducible.
* **Unfinished work never looks like 0 %.** A conversion rate is `null` when
  there are no settled entries; otherwise it is flagged `provisional` while
  some entrants are still inside their conversion window, with a separate
  `settled_conversion_rate`.  While a rebuild is running or paused, the last
  report is marked `stale` and the status endpoint reports `rebuilding` /
  `rebuild_paused` instead of presenting old or partial numbers as current.
* **Resumable rebuilds.** A checkpoint row is committed for every interval,
  holding only the delta of user states that interval touched (see below).
  Stop pauses at an interval boundary; a SIGKILLed process resumes from its
  persisted checkpoints in a new process.  Events imported mid-rebuild trigger
  another incremental pass rather than freezing numbers that match no single
  input state.  Jobs, reports and checkpoints live in SQLite, so the
  collector, the rebuild and the web process all see the same queue.
* **Affected boundaries, not "patch forward".** A late event or correction
  changes session derivation *globally*, so the engine compares the current
  event-signature prefix with the checkpointed one, restarts at the last
  checkpoint at or before the first difference, and reports
  `affected_from_ms`, `affected_from_event_id`, `affected_reason`,
  `reused_intervals` and `recomputed_intervals`.
* **Per-user path view.** Events, derived sessions (including merges and the
  alias ids they absorbed), and which step(s) each event satisfied — the path
  marks the exact event that filled each step, including a repeated step
  collapsed by `count_repeats`.

## The two engines

`pointwise/state_machine.py` is the one implementation of funnel matching.
Both engines call it; they differ only in where state is checkpointed.

| | per-event state | per-interval state (this repo) |
|---|---|---|
| cost per append | one state write per event, O(users × steps) bytes | one row per 256 events, only the users that interval touched |
| cost after a late/corrected event | supposedly one event — but session derivation is global, so state before the late timestamp may already be wrong | replay from the last interval boundary at or before the first differing event signature |
| resume granularity | one event | one interval (default 256 events) |
| session merges | a merge can retroactively invalidate very old state with no way to tell how far back | same, and the boundary is computed and reported explicitly |

Keeping state per event is not wrong *in principle*, but it only pays off if
the state at event *N* is independent of events after *N*.  Funnel matching
and session derivation are not: a late `session_end` can split a session that
a `same_session` step already relied on, and an `identity_merge` can join two
users' histories retroactively.  So the per-event scheme still has to find a
recompute boundary, and it pays a checkpoint write on every event to get one.
Intervals trade a coarser resume granularity (at most 255 events of rework)
for a much smaller checkpoint footprint.

Checkpoint bodies are zlib+base64 JSON of the users touched in that interval,
one row per interval in `rebuild_checkpoints`.  Restoring the state at a
boundary unions every row up to it; `_choose_restart` also refuses to reuse a
short tail interval, because the next append would make it grow.

Measured on 3,200 simulated users / 14 days / 39,171 events including the
late correction batch (Python 3.11, one core, default 256-event intervals): a
full rebuild takes ≈2.6 s, and a rebuild with nothing new to do takes ≈1.3 s
while reusing 153 of 154 intervals.  Both results match the reference engine
exactly.

`pointwise/engine.py` is the single-pass reference implementation used for
previews and as the oracle in tests.  `tests/test_rebuild.py` and the fuzz
runs assert that the interval engine converges to exactly the reference
numbers.

## Session derivation

Sessions are derived, never trusted:

1. first event opens a session;
2. `session_start` opens one;
3. a gap larger than the session gap opens one;
4. the event after a `session_end` opens one;
5. `identity_merge` events union the session named in `props.merge_session`
   with the session the merge event itself belongs to (and its own payload
   `session_id`), which is how a pre-login anonymous session is joined to a
   later identified one, and how a late back-filled event re-joins history.

Rules 1–4 are applied **per user**, over that user's own events in time order.
Events from different users interleave in the log; another user's event
between two of yours must not bridge your gap or end your session.

Derived ids prefer the payload `session_id` of the session's `session_start`.
If a payload id is reused across a boundary that had to be split, the later
window gets an `#n` suffix so `same_session` can still tell them apart.
Session ids are namespaced per user: two people sending the same payload id
are never merged, and a merge only affects its own user's windows.

## Layout

```
pointwise/
  collector.py     JSON/JSONL parsing, validation, correction semantics
  store.py         SQLite: events, revisions, rule snapshots, reports,
                   rebuild jobs and interval checkpoints
  sessions.py      deterministic session derivation and merges
  state_machine.py the funnel state machine (single source of truth)
  engine.py        single-pass reference engine, previews, user paths
  rebuild.py       resumable interval engine, delta checkpoints, boundaries
  report.py        report records, staleness, reproducibility
  rules.py         validation, canonical hashing, diffs
  rules_service.py preview/freeze orchestration
  API + console    api.py, static/index.html, static/app.js, static/app.css
  seed.py          synthetic data: repeats, cross-session waits, merges;
                   followups() delivers corrections and a late event separately
tests/             semantics, rebuild, HTTP API, multi-process
```

## API sketch

```
POST /api/collect                         array or JSONL (idempotent, corrections)
POST /api/seed                            generate + ingest sample data
POST /api/funnels/{id}/preview            live numbers + hash + diff
POST /api/funnels/{id}/versions           freeze (requires previewed hash)
GET  /api/funnels/{id}/versions[/{v}]
POST /api/funnels/{id}/reports            start/resume a rebuild
GET  /api/funnels/{id}/status             state: ready|stale|rebuilding|rebuild_paused|no_report
GET  /api/funnels/{id}/explain            reproducibility audit
GET  /api/users/{uid}/path?funnel_id=     events + derived sessions + step matches
GET  /api/events?limit=  /api/events/{id}/revisions
POST /api/rebuilds/{job}/stop|resume|wait
```

## Tests

```
python -m unittest discover -s tests -v
```

Notable cases: idempotent re-import, in-place correction with revision
history, repeated steps, same-session vs cross-session, late `session_end`
splitting a session, identity merges joining split sessions, conversion-window
expiry and retries, checkpoint reuse on append, affected-boundary reporting on
late insert/correction, stop/resume, SIGKILL then resume in a new process,
separate collector process, rebuilding while events are still arriving,
per-user session derivation with interleaved users, and the interval engine
matching the reference engine exactly.

`python -m pointwise seed` ingests the base log and then a second delivery of
corrections plus one back-filled event, so a demo database already contains
revisions and an input digest different from its first batch.
