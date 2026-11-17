"""SQLite persistence for the append-only event log and rule snapshots.

Event log semantics
-------------------
* ``event_id`` is the collector's stable key.  Re-importing an identical event
  is a no-op (idempotent retries / overlapping batches are safe).
* Re-importing the same id with different content is a **correction**: the
  event row is updated, its revision counter is bumped, and the previous row is
  kept in ``event_revisions``.  Nothing is deleted, so any report's input
  digest is reproducible from what was stored when it ran.
* ``identity_merge`` events are ordinary rows with kind
  ``identity_merge``; they are interpreted during session derivation.

Rule snapshots
--------------
Each frozen rule version stores the exact canonical JSON, its analytics hash
(ignores the version number) and a hash-chain link
``sha256(prev_sha256 + "\\n" + payload)``.  The chain is verified on read.
"""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import threading
import time

from .errors import Conflict, NotFound
from .models import FunnelRules
from .rules import canonical_json, chain_hash, rules_from_stored, rules_hash

SCHEMA = """
CREATE TABLE IF NOT EXISTS events (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id TEXT NOT NULL UNIQUE,
    user_id TEXT NOT NULL,
    ts_ms INTEGER NOT NULL,
    name TEXT NOT NULL,
    kind TEXT NOT NULL,
    session_id TEXT,
    properties TEXT NOT NULL DEFAULT '{}',
    ingested_at_ms INTEGER NOT NULL,
    revision INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_events_user_ts ON events(user_id, ts_ms, event_id);
CREATE INDEX IF NOT EXISTS idx_events_ts ON events(ts_ms, event_id);

CREATE TABLE IF NOT EXISTS event_revisions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id TEXT NOT NULL,
    revision INTEGER NOT NULL,
    old_row TEXT NOT NULL,
    new_row TEXT NOT NULL,
    at_ms INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS rules_versions (
    funnel_id TEXT NOT NULL,
    version INTEGER NOT NULL,
    payload TEXT NOT NULL,
    rules_hash TEXT NOT NULL,
    created_at_ms INTEGER NOT NULL,
    sha256 TEXT NOT NULL,
    note TEXT NOT NULL DEFAULT '',
    PRIMARY KEY (funnel_id, version)
);

CREATE TABLE IF NOT EXISTS reports (
    report_id TEXT PRIMARY KEY,
    funnel_id TEXT NOT NULL,
    status TEXT NOT NULL,
    created_at_ms INTEGER NOT NULL,
    body TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_reports_funnel ON reports(funnel_id, created_at_ms);

CREATE TABLE IF NOT EXISTS rebuild_jobs (
    job_id TEXT PRIMARY KEY,
    funnel_id TEXT NOT NULL,
    status TEXT NOT NULL,
    updated_at_ms INTEGER NOT NULL,
    body TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_jobs_funnel ON rebuild_jobs(funnel_id, status);

CREATE TABLE IF NOT EXISTS rebuild_checkpoints (
    job_id TEXT NOT NULL,
    interval_index INTEGER NOT NULL,
    to_pos INTEGER NOT NULL,
    body TEXT NOT NULL,
    PRIMARY KEY (job_id, interval_index)
);

CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
"""


def now_ms() -> int:
    return int(time.time() * 1000)


class Store:
    def __init__(self, path: str):
        self.path = path
        if path != ":memory:":
            os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        with self._lock:
            self._conn.executescript(SCHEMA)
            if path != ":memory:":
                self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute("PRAGMA synchronous=NORMAL")
            self._conn.commit()

    # -- events -----------------------------------------------------------

    def insert_events(self, events) -> dict:
        inserted = corrected = unchanged = 0
        affected: set[str] = set()
        with self._lock:
            cur = self._conn.cursor()
            for ev in events:
                event_id = ev["event_id"]
                row = cur.execute(
                    "SELECT * FROM events WHERE event_id=?", (event_id,)
                ).fetchone()
                props = canonical_json(ev.get("properties") or {})
                if row is None:
                    cur.execute(
                        """INSERT INTO events
                           (event_id,user_id,ts_ms,name,kind,session_id,properties,
                            ingested_at_ms,revision)
                           VALUES (?,?,?,?,?,?,?,?,0)""",
                        (
                            event_id,
                            ev["user_id"],
                            int(ev["timestamp_ms"]),
                            ev["name"],
                            ev.get("kind") or "event",
                            ev.get("session_id"),
                            props,
                            now_ms(),
                        ),
                    )
                    inserted += 1
                    affected.add(ev["user_id"])
                    continue
                same = (
                    row["user_id"] == ev["user_id"]
                    and row["ts_ms"] == int(ev["timestamp_ms"])
                    and row["name"] == ev["name"]
                    and row["kind"] == (ev.get("kind") or "event")
                    and (row["session_id"] or None) == (ev.get("session_id") or None)
                    and row["properties"] == props
                )
                if same:
                    unchanged += 1
                    continue
                cur.execute(
                    "INSERT INTO event_revisions(event_id,revision,old_row,new_row,at_ms)"
                    " VALUES (?,?,?,?,?)",
                    (
                        event_id,
                        row["revision"] + 1,
                        json.dumps(dict(row), sort_keys=True),
                        canonical_json(ev),
                        now_ms(),
                    ),
                )
                cur.execute(
                    """UPDATE events SET user_id=?,ts_ms=?,name=?,kind=?,session_id=?,
                       properties=?,ingested_at_ms=?,revision=revision+1
                       WHERE event_id=?""",
                    (
                        ev["user_id"],
                        int(ev["timestamp_ms"]),
                        ev["name"],
                        ev.get("kind") or "event",
                        ev.get("session_id"),
                        props,
                        now_ms(),
                        event_id,
                    ),
                )
                corrected += 1
                affected.add(row["user_id"])
                affected.add(ev["user_id"])
            self._conn.commit()
        return {
            "inserted": inserted,
            "corrected": corrected,
            "unchanged": unchanged,
            "affected_users": sorted(affected),
        }

    def all_events(self) -> list[sqlite3.Row]:
        with self._lock:
            return list(
                self._conn.execute(
                    "SELECT * FROM events ORDER BY ts_ms, seq, event_id"
                ).fetchall()
            )

    def user_events(self, user_id: str) -> list[sqlite3.Row]:
        with self._lock:
            return list(
                self._conn.execute(
                    "SELECT * FROM events WHERE user_id=? ORDER BY ts_ms, seq, event_id",
                    (user_id,),
                ).fetchall()
            )

    def event_count(self) -> int:
        with self._lock:
            return self._conn.execute("SELECT COUNT(*) FROM events").fetchone()[0]

    def input_digest(self) -> tuple[str, int]:
        h = hashlib.sha256()
        rows = self.all_events()
        for r in rows:
            item = [
                r["event_id"], r["user_id"], r["ts_ms"], r["name"], r["kind"],
                r["session_id"] or "", r["properties"], r["revision"],
            ]
            h.update(canonical_json(item).encode("utf-8"))
            h.update(b"\n")
        return h.hexdigest(), len(rows)

    def event_revisions(self, event_id: str) -> list[dict]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM event_revisions WHERE event_id=? ORDER BY revision",
                (event_id,),
            ).fetchall()
        return [dict(r) for r in rows]

    # -- rules ------------------------------------------------------------

    def list_rules_versions(self, funnel_id: str) -> list[dict]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM rules_versions WHERE funnel_id=? ORDER BY version",
                (funnel_id,),
            ).fetchall()
        return [dict(r) for r in rows]

    def get_rules_version(self, funnel_id: str, version: int) -> dict:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM rules_versions WHERE funnel_id=? AND version=?",
                (funnel_id, version),
            ).fetchone()
        if row is None:
            raise NotFound(f"funnel '{funnel_id}' has no rules version {version}")
        return dict(row)

    def latest_rules_row(self, funnel_id: str) -> dict | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM rules_versions WHERE funnel_id=? ORDER BY version DESC LIMIT 1",
                (funnel_id,),
            ).fetchone()
        return dict(row) if row else None

    def list_funnels(self) -> list[str]:
        """Every funnel id with at least one durable trace, sorted.

        Derived from the tables rather than a separate registry, so a funnel
        frozen by the CLI in another process shows up here without any
        cross-process bookkeeping.
        """

        with self._lock:
            rows = self._conn.execute(
                "SELECT funnel_id FROM rules_versions"
                " UNION SELECT funnel_id FROM reports"
                " UNION SELECT funnel_id FROM rebuild_jobs"
                " ORDER BY funnel_id"
            ).fetchall()
        return [r[0] for r in rows]

    def get_rules(self, funnel_id: str, version: int) -> FunnelRules:
        row = self.get_rules_version(funnel_id, version)
        return rules_from_stored(json.loads(row["payload"]), funnel_id, version)

    def append_rules(self, rules: FunnelRules) -> dict:
        with self._lock:
            self.verify_chain(rules.funnel_id)
            rows = self.list_rules_versions(rules.funnel_id)
            version = (rows[-1]["version"] + 1) if rows else 1
            rules.version = version
            rules.created_at_ms = rules.created_at_ms or now_ms()
            payload = {
                "conversion_window_ms": rules.conversion_window_ms,
                "steps": [
                    {
                        "name": s.name,
                        "event_name": s.event_name,
                        "min_wait_ms": s.min_wait_ms,
                        "max_wait_ms": s.max_wait_ms,
                        "same_session": bool(s.same_session),
                        "same_session_scope": s.same_session_scope,
                        "count_repeats": bool(s.count_repeats),
                        "description": s.description,
                    }
                    for s in rules.steps
                ],
                "note": rules.note,
            }
            payload_json = canonical_json(payload)
            prev = rows[-1]["sha256"] if rows else "genesis"
            sha = chain_hash(prev, payload_json)
            self._conn.execute(
                "INSERT INTO rules_versions"
                "(funnel_id,version,payload,rules_hash,created_at_ms,sha256,note)"
                " VALUES (?,?,?,?,?,?,?)",
                (
                    rules.funnel_id,
                    version,
                    payload_json,
                    rules_hash(rules),
                    rules.created_at_ms,
                    sha,
                    rules.note,
                ),
            )
            self._conn.commit()
        return {
            "funnel_id": rules.funnel_id,
            "version": version,
            "rules_hash": rules_hash(rules),
            "sha256": sha,
            "prev_sha256": prev,
            "created_at_ms": rules.created_at_ms,
            "payload": payload,
        }

    def verify_chain(self, funnel_id: str) -> None:
        rows = self.list_rules_versions(funnel_id)
        prev = "genesis"
        for row in rows:
            if chain_hash(prev, row["payload"]) != row["sha256"]:
                raise Conflict(
                    f"rules snapshot v{row['version']} of '{funnel_id}' failed "
                    "hash-chain verification; stored rules were altered"
                )
            prev = row["sha256"]

    def delete_all_rules(self, funnel_id: str) -> None:
        with self._lock:
            self._conn.execute("DELETE FROM rules_versions WHERE funnel_id=?", (funnel_id,))
            self._conn.commit()

    # -- reports ----------------------------------------------------------

    def upsert_report(self, rec: dict) -> None:
        with self._lock:
            self._conn.execute(
                "INSERT INTO reports(report_id,funnel_id,status,created_at_ms,body)"
                " VALUES (?,?,?,?,?)"
                " ON CONFLICT(report_id) DO UPDATE SET"
                " status=excluded.status, body=excluded.body",
                (
                    rec["report_id"],
                    rec["funnel_id"],
                    rec["status"],
                    int(rec["created_at_ms"]),
                    canonical_json(rec),
                ),
            )
            self._conn.commit()

    def get_report(self, report_id: str) -> dict | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT body FROM reports WHERE report_id=?", (report_id,)
            ).fetchone()
        return json.loads(row["body"]) if row else None

    def list_reports(self, funnel_id: str | None = None) -> list[dict]:
        with self._lock:
            if funnel_id is None:
                rows = self._conn.execute(
                    "SELECT body FROM reports ORDER BY created_at_ms DESC"
                ).fetchall()
            else:
                rows = self._conn.execute(
                    "SELECT body FROM reports WHERE funnel_id=?"
                    " ORDER BY created_at_ms DESC",
                    (funnel_id,),
                ).fetchall()
        return [json.loads(r["body"]) for r in rows]

    def update_report_status(self, funnel_id: str, status: str, reason: str) -> int:
        with self._lock:
            rows = self._conn.execute(
                "SELECT report_id, body FROM reports WHERE funnel_id=? AND status='complete'",
                (funnel_id,),
            ).fetchall()
            for row in rows:
                body = json.loads(row["body"])
                body["status"] = status
                body["stale_reason"] = reason
                self._conn.execute(
                    "UPDATE reports SET status=?, body=? WHERE report_id=?",
                    (status, canonical_json(body), row["report_id"]),
                )
            self._conn.commit()
        return len(rows)

    # -- rebuild jobs -----------------------------------------------------

    def upsert_job(self, job: dict) -> None:
        with self._lock:
            self._conn.execute(
                "INSERT INTO rebuild_jobs(job_id,funnel_id,status,updated_at_ms,body)"
                " VALUES (?,?,?,?,?)"
                " ON CONFLICT(job_id) DO UPDATE SET"
                " funnel_id=excluded.funnel_id, status=excluded.status,"
                " updated_at_ms=excluded.updated_at_ms, body=excluded.body",
                (
                    job["job_id"],
                    job["funnel_id"],
                    job["status"],
                    int(job.get("updated_at_ms") or 0),
                    canonical_json(job),
                ),
            )
            self._conn.commit()

    def get_job(self, job_id: str) -> dict | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT body FROM rebuild_jobs WHERE job_id=?", (job_id,)
            ).fetchone()
        return json.loads(row["body"]) if row else None

    def list_jobs(self, funnel_id: str | None = None) -> list[dict]:
        with self._lock:
            if funnel_id is None:
                rows = self._conn.execute(
                    "SELECT body FROM rebuild_jobs ORDER BY updated_at_ms DESC"
                ).fetchall()
            else:
                rows = self._conn.execute(
                    "SELECT body FROM rebuild_jobs WHERE funnel_id=?"
                    " ORDER BY updated_at_ms DESC",
                    (funnel_id,),
                ).fetchall()
        return [json.loads(r["body"]) for r in rows]

    def active_job(self, funnel_id: str) -> dict | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT body FROM rebuild_jobs WHERE funnel_id=?"
                " AND status IN ('queued','running','paused')"
                " ORDER BY updated_at_ms DESC LIMIT 1",
                (funnel_id,),
            ).fetchone()
        return json.loads(row["body"]) if row else None

    # -- rebuild checkpoints ----------------------------------------------

    def put_checkpoint(self, job_id: str, interval_index: int, to_pos: int,
                       body: str) -> None:
        """One row per interval: only the delta of users touched in it."""

        with self._lock:
            self._conn.execute(
                "INSERT INTO rebuild_checkpoints(job_id,interval_index,to_pos,body)"
                " VALUES(?,?,?,?)"
                " ON CONFLICT(job_id,interval_index) DO UPDATE SET"
                " to_pos=excluded.to_pos, body=excluded.body",
                (job_id, interval_index, to_pos, body),
            )
            self._conn.commit()

    def list_checkpoints(self, job_id: str) -> list[dict]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT interval_index, to_pos, body FROM rebuild_checkpoints"
                " WHERE job_id=? ORDER BY interval_index",
                (job_id,),
            ).fetchall()
        return [dict(r) for r in rows]

    def delete_checkpoints_from(self, job_id: str, interval_index: int) -> None:
        with self._lock:
            self._conn.execute(
                "DELETE FROM rebuild_checkpoints WHERE job_id=? AND interval_index>=?",
                (job_id, interval_index),
            )
            self._conn.commit()

    def copy_checkpoints(self, from_job: str, to_job: str) -> int:
        with self._lock:
            rows = self._conn.execute(
                "SELECT interval_index, to_pos, body FROM rebuild_checkpoints"
                " WHERE job_id=? ORDER BY interval_index",
                (from_job,),
            ).fetchall()
            self._conn.executemany(
                "INSERT INTO rebuild_checkpoints(job_id,interval_index,to_pos,body)"
                " VALUES(?,?,?,?)"
                " ON CONFLICT(job_id,interval_index) DO UPDATE SET"
                " to_pos=excluded.to_pos, body=excluded.body",
                [(to_job, r["interval_index"], r["to_pos"], r["body"]) for r in rows],
            )
            self._conn.commit()
        return len(rows)

    # -- meta -------------------------------------------------------------

    def meta_get(self, key: str, default=None):
        with self._lock:
            row = self._conn.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
        return row["value"] if row else default

    def meta_set(self, key: str, value: str) -> None:
        with self._lock:
            self._conn.execute(
                "INSERT INTO meta(key,value) VALUES(?,?)"
                " ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (key, value),
            )
            self._conn.commit()

    def close(self) -> None:
        with self._lock:
            self._conn.close()
