"""Diagnostics: is a frozen report still reproducible from today's inputs?

``explain`` answers three questions:

1. Does the stored rule snapshot still hash to its committed chain link?
   (Tamper evidence for the rules contract.)
2. Does re-running the *same* frozen rules against today's event log produce
   the same numbers as the stored report?  If not, the report is stale.
3. Which users changed, and why (new events, corrections, late inserts)?

It never mutates anything.
"""

from __future__ import annotations

import json

from .engine import evaluate
from .errors import Conflict, NotFound
from .store import Store


def _row_equal(rec: dict, result) -> bool:
    return (
        rec.get("entries") == result.entries
        and rec.get("conversions") == result.conversions
        and rec.get("unresolved_entries") == result.unresolved
    )


def explain(store: Store, reports, funnel_id: str, report_id: str | None = None,
            session_gap_ms: int = 30 * 60 * 1000) -> dict:
    rec = reports.get(report_id) if report_id else reports.latest_complete(funnel_id)
    if rec is None:
        raise NotFound(f"no report for '{funnel_id}'")
    if rec["funnel_id"] != funnel_id:
        raise Conflict("report belongs to a different funnel")

    chain_ok = True
    chain_error = None
    try:
        store.verify_chain(funnel_id)
    except Conflict as exc:
        chain_ok = False
        chain_error = exc.message

    rules = store.get_rules(funnel_id, rec["rules_version"])
    result = evaluate(store, rules, session_gap_ms)
    digest_now, count_now = store.input_digest()
    reproducible = _row_equal(rec, result) and digest_now == rec["input_digest"]

    return {
        "funnel_id": funnel_id,
        "report_id": rec["report_id"],
        "report": {
            "entries": rec["entries"],
            "conversions": rec["conversions"],
            "conversion_rate": rec["conversion_rate"],
            "rules_version": rec["rules_version"],
            "rules_hash": rec["rules_hash"],
            "input_digest": rec["input_digest"],
            "event_count": rec["event_count"],
            "status": rec["status"],
        },
        "rules_snapshot": {
            "version": rec["rules_version"],
            "sha256": rec.get("rules_sha256"),
            "chain_ok": chain_ok,
            "chain_error": chain_error,
        },
        "now": {
            "input_digest": digest_now,
            "event_count": count_now,
            "entries": result.entries,
            "conversions": result.conversions,
            "conversion_rate": result.conversion_rate,
            "step_counts": [
                {"name": sc.name, "reached": sc.reached, "converted": sc.converted}
                for sc in result.step_counts
            ],
        },
        "reproducible": reproducible,
        "drift": {
            "input_changed": digest_now != rec["input_digest"],
            "events_added": count_now - rec["event_count"],
            "entries_delta": result.entries - rec["entries"],
            "conversions_delta": result.conversions - rec["conversions"],
        },
    }


def event_revision_history(store: Store, event_id: str) -> dict:
    from .errors import NotFound as _NotFound

    rows = store.all_events()
    current = next((r for r in rows if r["event_id"] == event_id), None)
    revisions = store.event_revisions(event_id)
    if current is None and not revisions:
        raise _NotFound(f"event '{event_id}' not found")
    return {
        "event_id": event_id,
        "current": dict(current) if current else None,
        "revisions": [
            {
                "revision": r["revision"],
                "at_ms": r["at_ms"],
                "old": json.loads(r["old_row"]),
                "new": json.loads(r["new_row"]),
            }
            for r in revisions
        ],
    }
