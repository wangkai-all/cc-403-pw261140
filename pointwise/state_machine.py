"""The single authoritative funnel state machine.

Both the pointwise reference engine and the interval/rebuild engine drive
:func:`feed_event`; they differ only in where a :class:`UserState` is
checkpointed.  If the resumable engine used a second implementation of the
matching rules, the two could quietly disagree and a report would depend on
which path produced it.

Matching, in order, for the next expected step:

1. Event name must equal ``step.event_name`` (``None`` = any event).
2. The wait since the previously matched step must be inside
   ``[min_wait_ms, max_wait_ms]``.
3. The total time since the attempt's entry must be inside
   ``conversion_window_ms``.  The window is inclusive at both ends, so a step
   exactly at the edge counts.
4. ``same_session`` (scope ``session``) requires the derived session id of this
   event to equal the derived session id of the previous matched event in the
   same attempt.  Leaving it off is what permits cross-session waits.

Repeat steps (two adjacent steps naming the same event):

* ``count_repeats=False`` -> two distinct occurrences are needed.
* ``count_repeats=True`` on the later step -> one occurrence satisfies both;
  the run is collapsed onto the previous step's timestamp.

Attempts: every matching entry event opens a candidate walk.  A candidate is
dropped when the next event is already past its conversion-window end (it can
never complete), or when the in-flight list exceeds ``MAX_ATTEMPTS`` (oldest
first).  A user converts when any candidate finishes; ``max_step`` keeps the
best progress seen.  Late events therefore can revive an attempt that a
naive "current state" machine would have discarded on the spot.
"""

from __future__ import annotations

from .models import MAX_ATTEMPTS, Attempt, FunnelRules, Step, UserState


def _wait_ok(step: Step, prev_ts: int, ts: int) -> bool:
    wait = ts - prev_ts
    if step.min_wait_ms is not None and wait < step.min_wait_ms:
        return False
    if step.max_wait_ms is not None and wait > step.max_wait_ms:
        return False
    return True


def _consume(state: UserState, attempt: Attempt, rules: FunnelRules, ts: int,
             name: str, session_id) -> bool:
    """Advance one attempt by one event.  Returns True if anything matched."""

    consumed = False
    steps = rules.steps
    while attempt.idx < len(steps):
        idx = attempt.idx
        step = steps[idx]
        if step.event_name is not None and name != step.event_name:
            break
        if idx > 0:
            prev_ts = attempt.ts[idx - 1]
            if prev_ts is None:  # defensive; a consistent attempt always has it
                break
            repeat_slot = (
                step.count_repeats and steps[idx - 1].event_name == step.event_name
            )
            if not repeat_slot and not _wait_ok(step, prev_ts, ts):
                break
            if ts - attempt.started_at > rules.conversion_window_ms:
                break
            if step.same_session and step.same_session_scope == "session":
                prev_session = attempt.sess[idx - 1]
                if (
                    prev_session is not None
                    and session_id is not None
                    and prev_session != session_id
                ):
                    break

        if idx == 0:
            attempt.ts[0] = ts
            attempt.sess[0] = session_id
            attempt.started_at = ts
        elif step.count_repeats and steps[idx - 1].event_name == step.event_name:
            attempt.ts[idx] = attempt.ts[idx - 1]
            attempt.sess[idx] = attempt.sess[idx - 1]
        else:
            attempt.ts[idx] = ts
            attempt.sess[idx] = session_id

        attempt.idx = idx + 1
        if state.best_ts[idx] is None:
            state.best_ts[idx] = attempt.ts[idx]
            state.best_sess[idx] = attempt.sess[idx]
        state.max_step = max(state.max_step, attempt.idx)
        consumed = True

        if attempt.idx >= len(steps):
            state.completed = True
            break
        # A single event may only satisfy the next step too when the rule says
        # the same event counts twice (count_repeats); otherwise stop here.
        if not (
            steps[attempt.idx].count_repeats
            and steps[idx].event_name == steps[attempt.idx].event_name
        ):
            break
    return consumed


def feed_event(state: UserState, rules: FunnelRules, ts: int, name: str,
               session_id) -> None:
    steps = rules.steps
    if not steps:
        return

    # 1. Retire candidates that provably cannot complete any more.
    state.attempts = [
        a
        for a in state.attempts
        if a.idx >= len(steps) or ts - a.started_at <= rules.conversion_window_ms
    ]
    state.attempts = [a for a in state.attempts if a.idx < len(steps)]

    # 2. Offer the event to each open candidate (order does not matter: the
    #    candidates are independent, and an event can advance several of them).
    matched_any = False
    for attempt in state.attempts:
        if _consume(state, attempt, rules, ts, name, session_id):
            matched_any = True

    # 3. Open a new candidate when this event can start one.  Every matching
    #    occurrence is a genuine new entry; an attempt never lingers at step 0
    #    (it is consumed immediately), so this cannot duplicate a candidate.
    first = steps[0]
    if first.event_name is None or name == first.event_name:
        attempt = Attempt(
            idx=0,
            started_at=None,
            ts=[None] * len(steps),
            sess=[None] * len(steps),
        )
        if _consume(state, attempt, rules, ts, name, session_id):
            state.entered = True
            if state.first_entry_at is None:
                state.first_entry_at = ts
            if attempt.idx < len(steps):
                state.attempts.append(attempt)
            matched_any = True

    if not matched_any:
        return

    # Evict the least-progressed attempt first: an entry event that never got
    # past step 1 is the cheapest thing to forget.
    while len(state.attempts) > MAX_ATTEMPTS:
        victim = min(
            range(len(state.attempts)),
            key=lambda i: (state.attempts[i].idx, state.attempts[i].started_at or 0),
        )
        state.attempts.pop(victim)
