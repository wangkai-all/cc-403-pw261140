"""Data model for Pointwise.

All timestamps are integer milliseconds since the Unix epoch (UTC).  Event and
session ids are opaque strings; clients should treat them as such.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional


@dataclass(frozen=True)
class Step:
    """One configurable funnel step.

    Matching is sequential over the user's events (ordered by timestamp, then
    ingestion order):

    * ``event_name`` names the event that satisfies this step.  Only the first
      step may use ``None`` ("any event").
    * ``min_wait_ms`` / ``max_wait_ms`` bound the distance from the previous
      matched step.
    * ``same_session`` (with ``same_session_scope='session'``) requires the
      step to happen in the same *derived* session as the previous step.
      Leaving it off is what enables cross-session waits.
    * ``count_repeats``: when False (default) a repeated step (two adjacent
      steps naming the same event) needs two distinct occurrences.  When True,
      one occurrence may satisfy a run of adjacent identical steps.
    * ``description`` is free text stored with the frozen rule version.
    """

    name: str
    event_name: Optional[str]
    min_wait_ms: Optional[int] = None
    max_wait_ms: Optional[int] = None
    same_session: bool = False
    same_session_scope: str = "session"
    count_repeats: bool = False
    description: str = ""


@dataclass
class FunnelRules:
    funnel_id: str
    version: int
    conversion_window_ms: int
    steps: list[Step]
    created_at_ms: int = 0
    note: str = ""
    rules_hash: str = ""


@dataclass(frozen=True)
class SnapshotBoundary:
    version: int
    created_at_ms: int
    sha256: str
    rules_json: str


@dataclass
class Attempt:
    """One in-flight walk through the steps.

    A user can have several: every entry event starts a candidate, and the
    user converts if *any* candidate completes.  This is what makes a retry
    after a stalled attempt count, and it keeps the measurement honest when a
    user enters the funnel repeatedly over a long conversion window.
    """

    idx: int = 0
    started_at: Optional[int] = None
    ts: list[Optional[int]] = field(default_factory=list)
    sess: list[Optional[str]] = field(default_factory=list)

    def clone(self) -> "Attempt":
        return Attempt(idx=self.idx, started_at=self.started_at,
                       ts=list(self.ts), sess=list(self.sess))


@dataclass
class UserState:
    """Per-user funnel state, small enough to checkpoint at interval edges.

    ``attempts`` are the open candidates.  ``best_ts`` / ``best_sess`` remember
    the first time the user reached each step across all attempts, which is
    what reports count.  ``completed`` and ``entered`` are sticky.
    """

    attempts: list[Attempt] = field(default_factory=list)
    best_ts: list[Optional[int]] = field(default_factory=list)
    best_sess: list[Optional[str]] = field(default_factory=list)
    max_step: int = 0
    completed: bool = False
    entered: bool = False
    first_entry_at: Optional[int] = None

    def clone(self) -> "UserState":
        return UserState(
            attempts=[a.clone() for a in self.attempts],
            best_ts=list(self.best_ts),
            best_sess=list(self.best_sess),
            max_step=self.max_step,
            completed=self.completed,
            entered=self.entered,
            first_entry_at=self.first_entry_at,
        )


# More concurrent attempts than this adds checkpoint weight without changing
# any realistic result; the least-progressed candidate is dropped first
# (ties broken by the older start).
MAX_ATTEMPTS = 12


def new_user_state(step_count: int) -> UserState:
    return UserState(best_ts=[None] * step_count, best_sess=[None] * step_count)
