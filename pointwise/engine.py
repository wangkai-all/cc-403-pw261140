"""Reference pointwise engine.

Used for two things:

1. The ``/api/funnels/{id}/preview`` endpoint evaluates a candidate rule set
   against the live log with this exact, single-pass semantics.
2. Tests use it as an oracle against the interval/rebuild engine.  The rebuild
   engine is *supposed* to produce the same numbers; if it does not, one of
   them has a bug.

Everything is ordered by ``(ts_ms, seq)``; ties are resolved by ingestion order,
never by float comparison.  Session derivation always uses the *full*,
corrected log, so a late event can move an earlier event into another session.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from .models import FunnelRules, UserState, new_user_state
from .sessions import SessionMap, derive_sessions
from .state_machine import feed_event


def _get(row, key, default=None):
    try:
        v = row[key]
        return default if v is None else v
    except (KeyError, IndexError):
        return getattr(row, key, default)


@dataclass
class StepCount:
    name: str
    reached: int = 0
    converted: int = 0


@dataclass
class UserOutcome:
    user_id: str
    max_step: int = 0
    completed: bool = False
    first_entry_at: int | None = None
    step_ts: list[int | None] = field(default_factory=list)
    step_sessions: list[str | None] = field(default_factory=list)


@dataclass
class FunnelResult:
    rules_hash: str = ""
    rules_version: int = 0
    entries: int = 0
    conversions: int = 0
    unresolved: int = 0
    step_counts: list[StepCount] = field(default_factory=list)
    users: dict[str, UserOutcome] = field(default_factory=dict)
    intervals_used: int = 0
    reused_intervals: int = 0
    latest_event_ts: int = 0

    @property
    def settled_entries(self) -> int:
        return self.entries - self.unresolved

    @property
    def conversion_rate(self) -> float | None:
        """Classic rate over all entries; null only when there are no entries.

        Users still inside their conversion window are not zeroes: the rate is
        flagged provisional, and ``settled_conversion_rate`` gives the
        conservative view over entries whose window has elapsed.
        """

        if self.entries == 0 or self.settled_entries <= 0:
            # Every entry is still inside its window: no rate exists yet.
            # This is the "don't dress an unfinished rebuild up as 0 %" case.
            return None
        return self.conversions / self.entries

    @property
    def provisional(self) -> bool:
        return self.unresolved > 0

    @property
    def settled_conversion_rate(self) -> float | None:
        if self.settled_entries <= 0:
            return None
        return self.conversions / self.settled_entries

    def to_dict(self) -> dict:
        return {
            "rules_hash": self.rules_hash,
            "rules_version": self.rules_version,
            "entries": self.entries,
            "conversions": self.conversions,
            "unresolved_entries": self.unresolved,
            "settled_entries": self.settled_entries,
            "conversion_rate": self.conversion_rate,
            "conversion_rate_available": self.conversion_rate is not None,
            "conversion_rate_provisional": self.provisional,
            "settled_conversion_rate": self.settled_conversion_rate,
            "steps": [
                {"name": s.name, "reached": s.reached, "converted": s.converted}
                for s in self.step_counts
            ],
            "latest_event_ts": self.latest_event_ts,
        }


def outcome_from_state(user_id: str, state: UserState) -> UserOutcome:
    return UserOutcome(
        user_id=user_id,
        max_step=state.max_step,
        completed=state.completed,
        first_entry_at=state.first_entry_at,
        step_ts=list(state.best_ts),
        step_sessions=list(state.best_sess),
    )


def evaluate(store, rules: FunnelRules, session_gap_ms: int = 30 * 60 * 1000) -> FunnelResult:
    rows = store.all_events()
    sessions = derive_sessions(rows, session_gap_ms)
    result = FunnelResult(
        rules_hash=rules.rules_hash,
        rules_version=rules.version,
        step_counts=[StepCount(s.name) for s in rules.steps],
        latest_event_ts=max((int(_get(r, "ts_ms")) for r in rows), default=0),
    )
    states: dict[str, UserState] = {}
    for row in rows:
        user_id = _get(row, "user_id")
        state = states.get(user_id)
        if state is None:
            state = new_user_state(len(rules.steps))
            states[user_id] = state
        feed_event(
            state,
            rules,
            int(_get(row, "ts_ms")),
            _get(row, "name"),
            sessions.by_seq.get(int(_get(row, "seq"))),
        )

    for user_id, state in states.items():
        outcome = outcome_from_state(user_id, state)
        result.users[user_id] = outcome
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
            and result.latest_event_ts - state.first_entry_at < rules.conversion_window_ms
        ):
            # The user's window has not elapsed yet: they are not a zero, they
            # are unknown.  Excluded from the denominator.
            result.unresolved += 1
    return result


def evaluate_user(store, user_id: str, rules: FunnelRules,
                  session_gap_ms: int = 30 * 60 * 1000) -> dict:
    from .errors import NotFound

    rows = store.user_events(user_id)
    if not rows:
        raise NotFound(f"no events for user '{user_id}'")
    sessions = derive_sessions(store.all_events(), session_gap_ms)
    state = new_user_state(len(rules.steps))
    for row in rows:
        feed_event(
            state,
            rules,
            int(_get(row, "ts_ms")),
            _get(row, "name"),
            sessions.by_seq.get(int(_get(row, "seq"))),
        )
    # Which step(s) each event actually satisfied: the first event that reached
    # a step is exactly ``best_ts[i]``.  Two steps can share a timestamp when
    # ``count_repeats`` collapses a run into one occurrence.
    by_ts: dict[int, list[int]] = {}
    for i, matched_at in enumerate(state.best_ts):
        if matched_at is not None:
            by_ts.setdefault(int(matched_at), []).append(i)
    steps = []
    for i, step in enumerate(rules.steps):
        steps.append(
            {
                "index": i,
                "name": step.name,
                "event_name": step.event_name,
                "reached": i < state.max_step,
                "matched_at_ms": state.best_ts[i],
                "session_id": state.best_sess[i],
                "on_current_attempt": any(
                    a.idx > i and a.ts[i] is not None for a in state.attempts
                ),
            }
        )
    return {
        "user_id": user_id,
        "rules_hash": rules.rules_hash,
        "rules_version": rules.version,
        "completed": state.completed,
        "max_step": state.max_step,
        "entered": state.entered,
        "first_entry_at": state.first_entry_at,
        "steps": steps,
        "sessions": [
            {
                "session_id": s.session_id,
                "from_ms": s.from_ms,
                "to_ms": s.to_ms,
                "events": len(s.event_seqs),
                "ends_with_session_end": s.ends_with_session_end,
                "merged_aliases": s.merge_aliases,
            }
            for s in sessions.sessions
            if any(s.session_id == sessions.by_seq.get(int(r["seq"])) for r in rows)
        ],
        "path": [
            {
                "seq": int(r["seq"]),
                "event_id": r["event_id"],
                "name": r["name"],
                "kind": r["kind"],
                "ts_ms": int(r["ts_ms"]),
                "session_id": sessions.by_seq.get(int(r["seq"])),
                "properties": r["properties"],
                "revision": r["revision"],
                "matched_steps": by_ts.get(int(r["ts_ms"]), []),
            }
            for r in rows
        ],
    }
