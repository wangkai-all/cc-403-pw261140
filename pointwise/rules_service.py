"""Rule versioning, preview and frozen-report assembly.

``RuleService`` is the only writer of rule snapshots.  ``preview`` evaluates a
candidate rules payload against the **live** log and returns the per-step
numbers plus a human diff; nothing is persisted, so a user can experiment
freely.  ``freeze`` persists the candidate as the next immutable version and
refuses if the payload changed since the preview the user confirmed.
"""

from __future__ import annotations

import json

from .engine import evaluate
from .errors import Conflict, NotFound
from .models import FunnelRules
from .rules import parse_rules, preview_diff
from .sessions import derive_sessions
from .store import Store


class RuleService:
    def __init__(self, store: Store, session_gap_ms: int = 30 * 60 * 1000):
        self.store = store
        self.session_gap_ms = session_gap_ms

    def list_versions(self, funnel_id: str) -> list[dict]:
        return [
            {
                "version": row["version"],
                "rules_hash": row["rules_hash"],
                "sha256": row["sha256"],
                "created_at_ms": row["created_at_ms"],
                "note": row["note"],
                "step_count": len(json.loads(row["payload"])["steps"]),
                "payload": json.loads(row["payload"]),
            }
            for row in self.store.list_rules_versions(funnel_id)
        ]

    def get(self, funnel_id: str, version: int | None = None) -> FunnelRules:
        if version is None:
            row = self.store.latest_rules_row(funnel_id)
            if row is None:
                raise NotFound(f"funnel '{funnel_id}' has no frozen rules yet")
            return parse_rules(json.loads(row["payload"]), funnel_id, row["version"])
        return self.store.get_rules(funnel_id, version)

    def preview(self, funnel_id: str, payload: dict) -> dict:
        candidate = parse_rules(payload, funnel_id, version=0)
        current = None
        try:
            current = self.get(funnel_id)
        except NotFound:
            pass
        result = evaluate(self.store, candidate, self.session_gap_ms)
        body = result.to_dict()
        body.update(
            {
                "funnel_id": funnel_id,
                "rules_hash": candidate.rules_hash,
                "current_rules_hash": current.rules_hash if current else None,
                "current_version": current.version if current else None,
                "comparable_to_current": bool(
                    current is not None and current.rules_hash == candidate.rules_hash
                ),
                "diff": preview_diff(current, candidate),
                "steps": [
                    {
                        "name": step.name,
                        "event_name": step.event_name,
                        "reached": result.step_counts[i].reached,
                        "converted": result.step_counts[i].converted,
                    }
                    for i, step in enumerate(candidate.steps)
                ],
                "input_digest": self.store.input_digest()[0],
            }
        )
        return body

    def freeze(self, funnel_id: str, payload: dict,
               expected_hash: str | None = None, note: str = "") -> dict:
        candidate = parse_rules(payload, funnel_id, version=0)
        if expected_hash and candidate.rules_hash != expected_hash:
            raise Conflict(
                "rules differ from the previewed version; preview again before saving",
                details={"expected": expected_hash, "actual": candidate.rules_hash},
            )
        if note:
            candidate.note = note
        return self.store.append_rules(candidate)
