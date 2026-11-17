"""Collector ingest: parse, validate, canonicalise, then store.

The collector is runnable as its own process (``python -m pointwise collect``)
and shares only the SQLite file with the compute and report services.  It
never computes funnels, so a slow rebuild cannot back-pressure ingestion.

Accepted JSONL record (one object per line)::

    {"event_id": "e-1", "user_id": "u-1", "ts": 1700000000000,
     "event": "checkout_started", "session_id": "s-1",
     "kind": "event", "props": {...}}

``ts`` accepts integer milliseconds or an ISO-8601 string.  ``kind`` is one of
``event``, ``session_start``, ``session_end``, ``identity_merge``.  Missing
``event_id`` values are derived deterministically from the record so a batch
retried byte-for-byte is idempotent.
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone

from .errors import ValidationError
from .rules import canonical_json

KINDS = {"event", "session_start", "session_end", "identity_merge"}


def parse_ts(value) -> int:
    if isinstance(value, bool):
        raise ValidationError("ts must be an integer ms timestamp or ISO-8601 string")
    if isinstance(value, (int, float)):
        n = int(value)
        if n < 0:
            raise ValidationError("ts must be non-negative")
        return n
    if isinstance(value, str):
        text = value.strip()
        if not text:
            raise ValidationError("ts is empty")
        if text.isdigit():
            return parse_ts(int(text))
        try:
            dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError as exc:
            raise ValidationError(f"cannot parse ts {value!r}: {exc}") from exc
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp() * 1000)
    raise ValidationError("ts is required")


def parse_record(raw: dict, line_no: int | None = None) -> dict:
    where = f"line {line_no}: " if line_no is not None else ""
    if not isinstance(raw, dict):
        raise ValidationError(where + "record must be a JSON object")

    user_id = raw.get("user_id", raw.get("user"))
    if not isinstance(user_id, str) or not user_id.strip():
        raise ValidationError(where + "user_id is required")
    user_id = user_id.strip()

    name = raw.get("event", raw.get("name"))
    if not isinstance(name, str) or not name.strip():
        raise ValidationError(where + "event name is required")
    name = name.strip()

    if "ts" not in raw and "timestamp_ms" not in raw:
        raise ValidationError(where + "ts is required")
    ts = parse_ts(raw.get("ts", raw.get("timestamp_ms")))

    kind = raw.get("kind") or "event"
    if kind not in KINDS:
        raise ValidationError(where + f"unknown kind {kind!r}; expected one of {sorted(KINDS)}")

    session_id = raw.get("session_id")
    if session_id is not None:
        if not isinstance(session_id, str) or not session_id.strip():
            raise ValidationError(where + "session_id must be a non-empty string when present")
        session_id = session_id.strip()

    props = raw.get("props", raw.get("properties")) or {}
    if not isinstance(props, dict):
        raise ValidationError(where + "props must be an object")

    event_id = raw.get("event_id")
    if event_id is not None:
        if not isinstance(event_id, str) or not event_id.strip():
            raise ValidationError(where + "event_id must be a non-empty string when present")
        event_id = event_id.strip()
    else:
        seed = canonical_json(
            {k: v for k, v in raw.items() if k not in ("event_id",)}
        )
        event_id = "auto-" + hashlib.sha256(seed.encode("utf-8")).hexdigest()[:24]

    if kind == "identity_merge":
        target = props.get("merge_session")
        if not isinstance(target, str) or not target:
            raise ValidationError(
                where + "identity_merge events need props.merge_session naming the session to join"
            )

    return {
        "event_id": event_id,
        "user_id": user_id,
        "timestamp_ms": ts,
        "name": name,
        "kind": kind,
        "session_id": session_id,
        "properties": props,
    }


def parse_jsonl(text: str) -> tuple[list[dict], list[dict]]:
    """Return (records, errors).  Invalid lines are reported, not skipped."""

    records: list[dict] = []
    errors: list[dict] = []
    positions: dict[str, int] = {}
    for line_no, line in enumerate(text.splitlines(), start=1):
        if not line.strip():
            continue
        try:
            raw = json.loads(line)
        except json.JSONDecodeError as exc:
            errors.append({"line": line_no, "error": f"invalid JSON: {exc.msg}"})
            continue
        if isinstance(raw, list):
            errors.append({"line": line_no, "error": "expected one object per line, found array"})
            continue
        try:
            rec = parse_record(raw, line_no)
        except ValidationError as exc:
            errors.append({"line": line_no, "error": exc.message})
            continue
        _put(records, positions, rec)
    return records, errors


def _put(records: list[dict], positions: dict[str, int], rec: dict) -> None:
    """Last record wins for a repeated id inside one batch.

    That is the "collector sent a correction in the same upload" case: the
    earlier copy is not an error, it is superseded.  The store still records a
    revision because the surviving record is compared with what it had.
    """

    prior = positions.get(rec["event_id"])
    if prior is None:
        positions[rec["event_id"]] = len(records)
        records.append(rec)
    else:
        records[prior] = rec


def ingest(store, payload, source: str = "api") -> dict:
    """Ingest a JSON array or a JSONL string.

    Semantics are append-only with correction support; see :mod:`pointwise.store`.
    The response states the affected user ids and time span so the caller can
    tell whether a rebuild is needed and which boundary it starts from.
    """

    if isinstance(payload, str):
        stripped = payload.lstrip()
        if stripped.startswith("["):
            try:
                return ingest(store, json.loads(stripped), source)
            except json.JSONDecodeError as exc:
                raise ValidationError(f"invalid JSON array: {exc}") from exc
        records, errors = parse_jsonl(payload)
    elif isinstance(payload, list):
        records, errors = [], []
        positions: dict[str, int] = {}
        for i, raw in enumerate(payload, start=1):
            try:
                rec = parse_record(raw, i)
            except ValidationError as exc:
                errors.append({"line": i, "error": exc.message})
                continue
            _put(records, positions, rec)
    else:
        raise ValidationError("payload must be a JSON array or a JSONL string")

    if not records:
        raise ValidationError("no valid events in payload", details={"errors": errors[:20]})

    stats = store.insert_events(records)
    stats.update(
        {
            "source": source,
            "accepted": len(records),
            "rejected": errors,
            "min_ts": min(r["timestamp_ms"] for r in records),
            "max_ts": max(r["timestamp_ms"] for r in records),
            "changed": stats["inserted"] + stats["corrected"],
            "input_digest": store.input_digest()[0],
        }
    )
    stats["rebuild_needed"] = stats["changed"] > 0
    return stats
