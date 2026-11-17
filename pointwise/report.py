"""Reports: frozen numbers, explicit staleness, no fake zeroes.

A report row records exactly what it was computed from:

* ``rules_version`` + ``rules_hash`` + the rule snapshot's ``sha256`` chain link
* ``input_digest`` + ``event_count`` for the event log
* ``session_gap_ms`` and engine version

``compute_conversion_rate`` returns ``None`` — surfaced as JSON ``null`` —
whenever the denominator is zero or still open.  The HTTP layer renders that
as "not yet computable", and the UI never substitutes 0 %.
"""

from __future__ import annotations

import json
import time
import uuid

from .engine import FunnelResult
from .errors import Conflict, NotFound
from .store import Store

ENGINE_VERSION = "pointwise-1"


class ReportService:
    """Reports live in their own table, so every process reads the same rows.

    There is deliberately no in-memory cache: the collector, the report
    service and the web process are independent, and a cached copy is exactly
    how a stale number would leak into a fresh page.
    """

    def __init__(self, store: Store):
        self.store = store

    def create(self, funnel_id: str, rules, rules_meta: dict, result: FunnelResult,
               session_gap_ms: int, extra_coverage: dict | None = None,
               input_digest: str | None = None) -> dict:
        digest, count = self.store.input_digest()
        if input_digest is not None and digest != input_digest:
            # The log moved between aggregation and publication.  Refuse to
            # label these numbers with a digest they were not computed from.
            raise Conflict(
                "event log changed while publishing the report; run another pass",
                details={"expected": input_digest, "actual": digest},
            )
        body = result.to_dict()
        coverage = {
            "event_stream_is_complete": True,
            "unresolved_entries": result.unresolved,
            "settled_entries": result.settled_entries,
            "note": (
                "conversion_rate is null when no entry has had the full "
                "conversion window to convert"
            ),
        }
        if extra_coverage:
            coverage.update(extra_coverage)
        rec = {
            "report_id": str(uuid.uuid4()),
            "funnel_id": funnel_id,
            "status": "complete",
            "stale_reason": None,
            "rules_version": rules.version,
            "rules_hash": rules.rules_hash,
            "rules_sha256": rules_meta.get("sha256"),
            "rules_prev_sha256": rules_meta.get("prev_sha256", "genesis"),
            "input_digest": digest,
            "event_count": count,
            "session_gap_ms": session_gap_ms,
            "engine_version": ENGINE_VERSION,
            "created_at_ms": int(time.time() * 1000),
            "entries": body["entries"],
            "conversions": body["conversions"],
            "unresolved_entries": body["unresolved_entries"],
            "settled_entries": body["settled_entries"],
            "conversion_rate": body["conversion_rate"],
            "conversion_rate_available": body["conversion_rate_available"],
            "conversion_rate_provisional": body["conversion_rate_provisional"],
            "settled_conversion_rate": body["settled_conversion_rate"],
            "steps": body["steps"],
            "latest_event_ts": body["latest_event_ts"],
            "coverage": coverage,
        }
        self.store.upsert_report(rec)
        return rec

    def list(self, funnel_id: str | None = None) -> list[dict]:
        return self.store.list_reports(funnel_id)

    def get(self, report_id: str) -> dict:
        rec = self.store.get_report(report_id)
        if rec is None:
            raise NotFound(f"report '{report_id}' not found")
        return rec

    def mark_stale(self, funnel_id: str, reason: str) -> None:
        """Demote every currently-complete report for a funnel.

        Only completed rows are touched, so two rebuilds starting at the same
        time cannot un-finish each other's results, and a finished report is
        never resurrected by a later stop.
        """

        if not reason:
            return
        self.store.update_report_status(funnel_id, "stale", reason)

    def mark_stale_all(self, reason: str) -> None:
        for funnel_id in {r["funnel_id"] for r in self.store.list_reports()}:
            self.mark_stale(funnel_id, reason)

    def latest(self, funnel_id: str) -> dict | None:
        rows = self.list(funnel_id)
        return rows[0] if rows else None

    def latest_complete(self, funnel_id: str) -> dict | None:
        for rec in self.list(funnel_id):
            if rec["status"] == "complete":
                return rec
        return None

    def assert_fresh(self, funnel_id: str, rules) -> dict:
        """A report is only returned as current if rules and input both match."""

        rec = self.latest_complete(funnel_id)
        if rec is None:
            raise Conflict(f"no complete report for '{funnel_id}'")
        digest, _ = self.store.input_digest()
        if rec["rules_hash"] != rules.rules_hash:
            raise Conflict(
                "report was computed under different rules",
                details={"report_rules_hash": rec["rules_hash"],
                         "current_rules_hash": rules.rules_hash},
            )
        if rec["input_digest"] != digest:
            raise Conflict(
                "event log changed since this report was computed",
                details={"report_input_digest": rec["input_digest"],
                         "current_input_digest": digest},
            )
        return rec

    def get_by_identity(self, funnel_id: str, rules_hash: str) -> dict | None:
        for rec in self.list(funnel_id):
            if rec["rules_hash"] == rules_hash:
                return rec
        return None
