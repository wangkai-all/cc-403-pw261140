"""Synthetic-but-real behaviour data.

Everything here is computed from the generated events by the real engines.
There are no hard-coded conversion numbers: the seed script emits a log, the
collector ingests it, and every figure the UI shows comes back out of the
pointwise engine.

The generator deliberately creates the messy cases the task calls out:

* repeat steps (``add_to_cart`` twice),
* cross-session waits (a checkout completed the next day),
* identity merges that join a pre-login anonymous session to a later one,
* late events that land before the end of an earlier session,
* and corrections (the same ``event_id`` re-emitted with a fixed timestamp).

The base log is :func:`generate`; :func:`followups` supplies the records that
arrive *after* it (a correction and a back-filled late event).  They are
ingested separately so the store really records revisions and a changed input
digest, instead of the collector silently collapsing a same-batch duplicate.
"""

from __future__ import annotations

import random

ADJECTIVES = ["amber", "blue", "coral", "dusk", "ember", "frost", "green", "hazel"]
NOUNS = ["otter", "heron", "maple", "quartz", "raven", "sable", "tide", "wren"]

DAY_MS = 24 * 3600 * 1000
MINUTE_MS = 60 * 1000


def _user_id(rng: random.Random) -> str:
    return f"{rng.choice(ADJECTIVES)}-{rng.choice(NOUNS)}-{rng.randrange(1000, 9999)}"


def generate(users: int = 400, days: int = 14, seed: int = 7) -> list[dict]:
    rng = random.Random(seed)
    now = int(days * DAY_MS)
    start = 1_726_000_000_000  # arbitrary fixed epoch base
    events: list[dict] = []
    counter = 0

    def emit(user, ts, name, kind="event", session=None, props=None, event_id=None):
        nonlocal counter
        counter += 1
        return {
            "event_id": event_id or f"e-{counter:07d}",
            "user_id": user,
            "ts": int(ts),
            "event": name,
            "kind": kind,
            "session_id": session,
            "props": props or {},
        }

    for u in range(users):
        user = _user_id(rng)
        # Each user has a few visits across the period.
        visits = rng.randint(1, 5)
        t = start + rng.randrange(0, now)
        for v in range(visits):
            session = f"s-{user}-{v}"
            t += rng.randrange(30 * MINUTE_MS, 3 * DAY_MS)
            if t > start + now:
                break
            events.append(emit(user, t, "app_open", "session_start", session))
            events.append(emit(user, t + 500, "view_home", "event", session))
            events.append(emit(user, t + 2000, "view_product", "event", session,
                               {"sku": rng.randrange(100, 999)}))

            if rng.random() < 0.55:
                at = t + 3000
                events.append(emit(user, at, "add_to_cart", "event", session))
                if rng.random() < 0.35:  # repeat step: same event again
                    events.append(emit(user, at + 1500, "add_to_cart", "event", session))
                if rng.random() < 0.6:
                    events.append(emit(user, at + 4000, "checkout_started", "event", session))
                    if rng.random() < 0.3:
                        # Cross-session purchase: comes back much later.
                        purchase_at = t + rng.randrange(MINUTE_MS, 2 * DAY_MS)
                        new_session = f"s-{user}-{v}-late"
                        events.append(
                            emit(user, purchase_at, "app_open", "session_start", new_session)
                        )
                        events.append(
                            emit(user, purchase_at + 1000, "purchase", "event", new_session,
                                 {"value": rng.randrange(10, 400)})
                        )
                    elif rng.random() < 0.75:
                        events.append(
                            emit(user, at + 9000, "purchase", "event", session,
                                 {"value": rng.randrange(10, 400)})
                        )
            events.append(emit(user, t + 20000 + rng.randrange(0, 5000), "app_close",
                               "session_end", session))

    # Identity merges: some visitors' first (anonymous) session is joined to
    # the last session they were seen in, as when a pre-login session is
    # linked to an identified account later.
    by_user: dict[str, list[dict]] = {}
    for ev in events:
        by_user.setdefault(ev["user_id"], []).append(ev)
    for user, evs in by_user.items():
        starts = [e for e in evs if e["kind"] == "session_start"]
        if len(starts) < 2 or rng.random() >= 0.25:
            continue
        anonymous, identified = starts[0], starts[-1]
        events.append(
            emit(
                user,
                identified["ts"],
                "identity_merge",
                "identity_merge",
                identified["session_id"],
                {"merge_session": anonymous["session_id"]},
                event_id=f"merge-{identified['event_id']}",
            )
        )

    events.sort(key=lambda e: (e["ts"], e["event_id"]))
    return events


def followups(events: list[dict], seed: int = 7) -> list[dict]:
    """Records delivered after the base log: corrections and one late event.

    A correction keeps its ``event_id`` but moves the timestamp earlier, which
    is exactly what forces a rebuild to restart before the original position.
    The late event is back-filled into a window that was already closed.
    """

    rng = random.Random(seed ^ 0x5EED)
    out: list[dict] = []
    sample = rng.sample(events, min(6, len(events)))
    for original in sample:
        if original["kind"] == "session_end":
            continue
        corrected = dict(original)
        corrected["ts"] = original["ts"] - rng.randrange(1, 600) * 1000
        corrected["props"] = {**original["props"], "corrected": True}
        out.append(corrected)
    starts = [e for e in events if e["kind"] == "session_start"]
    if starts:
        pick = rng.choice(starts)
        out.append(
            {
                "event_id": f"late-{pick['event_id']}",
                "user_id": pick["user_id"],
                "ts": pick["ts"] + 1200,
                "event": "view_home",
                "kind": "event",
                "session_id": pick["session_id"],
                "props": {"late": True},
            }
        )
    out.sort(key=lambda e: (e["ts"], e["event_id"]))
    return out
