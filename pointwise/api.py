"""HTTP API and the single-page console.

The API process holds the store, the rule service, the report service and the
rebuild manager.  The collector can run in this process *or* as a separate
process writing to the same SQLite file (see ``python -m pointwise collect``);
the compute path re-reads the log on every rebuild pass, so it sees those
writes without being restarted.

Endpoints
---------
GET  /api/health
POST /api/collect                       ingest request body (array or JSONL string)
POST /api/funnels/{id}/preview          {rules: capacity...} -> live numbers + diff
POST /api/funnels/{id}/versions         freeze a rule version
GET  /api/funnels/{id}/versions
GET  /api/funnels/{id}/versions/{v}
POST /api/funnels/{id}/reports          start a rebuild using a frozen version
GET  /api/funnels/{id}/reports
GET  /api/funnels/{id}/status           rebuild state; never a fake 0 %
GET  /api/reports/{report_id}
GET  /api/users/{user_id}/path?funnel_id=&version=
POST /api/rebuilds/{job_id}/stop|resume
GET  /api/rebuilds
GET  /api/events?limit=
"""

from __future__ import annotations

import json
import mimetypes
import os
import threading
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, unquote, urlparse

from .collector import ingest
from .errors import Conflict, NotFound, PointwiseError, ValidationError
from .rebuild import RebuildManager
from .report import ReportService
from .rules import parse_rules
from .rules_service import RuleService
from .store import Store

STATIC_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static")


class App:
    def __init__(self, db_path: str, session_gap_ms: int = 30 * 60 * 1000,
                 interval_events: int = 256):
        self.store = Store(db_path)
        self.rules = RuleService(self.store, session_gap_ms)
        self.reports = ReportService(self.store)
        self.rebuilds = RebuildManager(
            self.store, self.reports, session_gap_ms, interval_events
        )
        self.session_gap_ms = session_gap_ms
        self.lock = threading.RLock()

    # -- helpers ----------------------------------------------------------

    def _rules_for(self, query: dict, body: dict | None = None) -> tuple:
        """Resolve rules for a read endpoint.

        A frozen version is preferred.  If the funnel has no frozen rules yet,
        a ``?rules_json=<urlencoded>`` candidate is accepted so the web
        console's path view works while a rule set is still being edited.
        """

        funnel_id = query["funnel_id"][0]
        version_q = query.get("version")
        if version_q:
            return funnel_id, self.rules.get(funnel_id, int(version_q[0]))
        try:
            return funnel_id, self.rules.get(funnel_id)
        except NotFound:
            candidate = query.get("rules_json")
            if candidate:
                return funnel_id, parse_rules(json.loads(candidate[0]), funnel_id, 0)
            raise

    def funnel_status(self, funnel_id: str) -> dict:
        rules = None
        rules_error = None
        try:
            rules = self.rules.get(funnel_id)
        except NotFound as exc:
            rules_error = exc.message
        digest, count = self.store.input_digest()
        latest_any = self.reports.latest(funnel_id)
        latest = self.reports.latest_complete(funnel_id)
        job = self.rebuilds.active_for(funnel_id)
        status = {
            "funnel_id": funnel_id,
            "event_count": count,
            "input_digest": digest,
            "rules": None,
            "report": None,
            "rebuild": None,
            "state": "empty",
            "message": "",
        }
        if rules is not None:
            status["rules"] = {
                "version": rules.version,
                "rules_hash": rules.rules_hash,
                "conversion_window_ms": rules.conversion_window_ms,
                "steps": [
                    {
                        "name": s.name,
                        "event_name": s.event_name,
                        "min_wait_ms": s.min_wait_ms,
                        "max_wait_ms": s.max_wait_ms,
                        "same_session": s.same_session,
                        "count_repeats": s.count_repeats,
                    }
                    for s in rules.steps
                ],
            }
        else:
            status["message"] = rules_error

        if job is not None:
            status["rebuild"] = job.to_dict()
            if job.status in ("queued", "running"):
                status["state"] = "rebuilding"
                status["message"] = (
                    "a rebuild is running; numbers below are the last frozen report, "
                    "not the current state"
                )
            elif job.status == "paused":
                status["state"] = "rebuild_paused"
                status["message"] = "rebuild is paused; it will resume from its checkpoint"

        # Show the most recent report even if it is stale, but never as current.
        if latest_any is not None:
            shown = dict(latest_any)
            status["report"] = shown
            if latest_any["status"] != "complete":
                if status["state"] == "empty":
                    status["state"] = "stale"
                stale = (
                    latest_any.get("stale_reason")
                    or "this report is stale; start a rebuild to refresh it"
                )
                # Keep the rebuild context: a paused job plus a stale report
                # should say both, not let one hide the other.
                status["message"] = (
                    f"{status['message']} — {stale}" if status["message"] else stale
                )
            elif latest_any["input_digest"] != digest:
                status["state"] = "stale"
                status["message"] = (
                    "the event log changed since this report was computed; "
                    "start a rebuild to refresh it"
                )
                shown["status"] = "stale"
                shown["stale_reason"] = "event log changed"
            elif rules is not None and latest_any["rules_hash"] != rules.rules_hash:
                status["state"] = "stale"
                status["message"] = (
                    "the report was computed under a different rule version; "
                    "freeze the current rules and rebuild"
                )
                shown["status"] = "stale"
                shown["stale_reason"] = "rules changed"
            elif status["state"] == "empty":
                status["state"] = "ready"

        if latest_any is None and status["state"] == "empty" and rules is not None:
            if job is not None and job.status in ("queued", "running"):
                status["state"] = "rebuilding"
            else:
                status["state"] = "no_report"
                status["message"] = "no report yet; start a rebuild"

        if status["state"] == "empty" and count == 0:
            status["message"] = "no events collected yet"
        return status

    # -- routing ----------------------------------------------------------

    def handle(self, method: str, path: str, query: dict, body, raw_body: bytes):
        # Paths are percent-decoded once, here: user ids and funnel ids may
        # legitimately contain spaces or slashes once encoded.
        parts = [unquote(p) for p in path.split("/") if p]

        if path in ("/", "/index.html"):
            return _static_response("index.html")
        if path.startswith("/static/"):
            return _static_response(parts[-1])

        if not parts or parts[0] != "api":
            raise NotFound(f"unknown path {path}")
        parts = parts[1:]

        if method == "GET" and parts == ["health"]:
            return 200, {
                "status": "ok",
                "event_count": self.store.event_count(),
                "input_digest": self.store.input_digest()[0],
                "engine": "pointwise-1",
            }

        if method == "GET" and parts == ["events"]:
            limit = int(query.get("limit", ["200"])[0])
            rows = self.store.all_events()
            return 200, {
                "total": len(rows),
                "events": [
                    {
                        "seq": r["seq"],
                        "event_id": r["event_id"],
                        "user_id": r["user_id"],
                        "ts_ms": r["ts_ms"],
                        "name": r["name"],
                        "kind": r["kind"],
                        "revision": r["revision"],
                    }
                    for r in rows[-limit:]
                ],
            }

        if method == "POST" and parts == ["collect"]:
            payload = body if body is not None else raw_body.decode("utf-8", "replace")
            return 200, ingest(self.store, payload, source="api")

        if method == "POST" and parts == ["seed"]:
            from .seed import followups, generate

            body = body or {}
            seed = int(body.get("seed", 7))
            events = generate(
                users=int(body.get("users", 240)),
                days=int(body.get("days", 10)),
                seed=seed,
            )
            stats = ingest(self.store, events, source="seed")
            # Corrections and a back-filled event arrive as a separate batch:
            # re-emitting them inside the first upload would be collapsed by
            # the collector's last-wins rule and never exercise revisions.
            after = ingest(self.store, followups(events, seed), source="seed-followup")
            affected = sorted(
                set(stats.get("affected_users", [])) | set(after.get("affected_users", []))
            )
            return 200, {
                "inserted": stats["inserted"] + after["inserted"],
                "corrected": stats["corrected"] + after["corrected"],
                "unchanged": stats["unchanged"] + after["unchanged"],
                "accepted": stats["accepted"] + after["accepted"],
                "rejected": stats["rejected"] + after["rejected"],
                "affected_users": affected,
                "min_ts": min(stats["min_ts"], after["min_ts"]),
                "max_ts": max(stats["max_ts"], after["max_ts"]),
                "changed": stats["changed"] + after["changed"],
                "input_digest": after["input_digest"],
                "rebuild_needed": (stats["changed"] + after["changed"]) > 0,
                "source": "seed",
            }

        if method == "GET" and parts == ["funnels"]:
            # Durable funnels come from the tables (so a CLI freeze in another
            # process appears here); ids that only exist in a preview are kept
            # in a small registry so the console can list them too.
            known = json.loads(self.store.meta_get("known_funnels", "[]"))
            return 200, {"funnels": sorted(set(self.store.list_funnels()) | set(known))}

        if len(parts) >= 3 and parts[0] == "funnels":
            funnel_id = parts[1]
            action = parts[2]
            self._remember_funnel(funnel_id)

            if method == "GET" and action == "status":
                return 200, self.funnel_status(funnel_id)

            if method == "POST" and action == "preview":
                return 200, self.rules.preview(funnel_id, body or {})

            if action == "versions":
                if method == "GET" and len(parts) == 3:
                    return 200, {"versions": self.rules.list_versions(funnel_id)}
                if method == "GET" and len(parts) == 4:
                    version = int(parts[3])
                    rules = self.store.get_rules(funnel_id, version)
                    row = self.store.get_rules_version(funnel_id, version)
                    return 200, {
                        "version": version,
                        "rules_hash": row["rules_hash"],
                        "sha256": row["sha256"],
                        "created_at_ms": row["created_at_ms"],
                        "payload": json.loads(row["payload"]),
                    }
                if method == "POST" and len(parts) == 3:
                    body = body or {}
                    return 201, self.rules.freeze(
                        funnel_id,
                        body.get("rules", body),
                        expected_hash=body.get("expected_hash"),
                        note=body.get("note", ""),
                    )

            if action == "reports":
                if method == "GET":
                    return 200, {"reports": self.reports.list(funnel_id)}
                if method == "POST":
                    body = body or {}
                    version = body.get("version")
                    if version is None:
                        rules = self.rules.get(funnel_id)
                        row = self.store.latest_rules_row(funnel_id)
                    else:
                        rules = self.store.get_rules(funnel_id, int(version))
                        row = self.store.get_rules_version(funnel_id, int(version))
                    job = self.rebuilds.start(
                        funnel_id,
                        rules,
                        {"sha256": row["sha256"]},
                        reason=body.get("reason") or "manual rebuild",
                    )
                    return 202, {"job": job.to_dict()}

        if method == "GET" and parts == ["reports"]:
            return 200, {"reports": self.reports.list()}

        if len(parts) == 2 and parts[0] == "reports" and method == "GET":
            return 200, self.reports.get(parts[1])

        if len(parts) == 3 and parts[0] == "funnels" and parts[2] == "explain" and method == "GET":
            from .diagnostics import explain

            report_id = query.get("report_id", [None])[0]
            return 200, explain(
                self.store, self.reports, parts[1], report_id, self.session_gap_ms
            )

        if len(parts) == 3 and parts[0] == "events" and parts[2] == "revisions" and method == "GET":
            from .diagnostics import event_revision_history

            return 200, event_revision_history(self.store, parts[1])

        if len(parts) == 3 and parts[0] == "users" and parts[2] == "path":
            from .engine import evaluate_user

            funnel_id, rules = self._rules_for(query)
            result = evaluate_user(self.store, parts[1], rules, self.session_gap_ms)
            digest, _ = self.store.input_digest()
            result["input_digest"] = digest
            result["report"] = self.reports.latest_complete(funnel_id)
            result["rules_stale"] = (
                self.reports.latest_complete(funnel_id) is None
                or self.reports.latest_complete(funnel_id)["rules_hash"] != rules.rules_hash
            )
            return 200, result

        if parts and parts[0] == "rebuilds":
            if method == "GET" and len(parts) == 1:
                return 200, {"jobs": self.rebuilds.list()}
            if len(parts) == 2 and method == "GET":
                return 200, self.rebuilds.get(parts[1]).to_dict()
            if len(parts) == 3 and method == "POST" and parts[2] == "stop":
                return 200, {"job": self.rebuilds.stop(parts[1]).to_dict()}
            if len(parts) == 3 and method == "POST" and parts[2] == "resume":
                return 200, {"job": self.rebuilds.resume(parts[1]).to_dict()}
            if len(parts) == 3 and method == "POST" and parts[2] == "wait":
                timeout = float((body or {}).get("timeout", 30))
                return 200, {"job": self.rebuilds.wait(parts[1], timeout).to_dict()}

        raise NotFound(f"unknown endpoint {method} {path}")

    def _remember_funnel(self, funnel_id: str) -> None:
        ids = json.loads(self.store.meta_get("known_funnels", "[]"))
        if funnel_id not in ids:
            ids.append(funnel_id)
            self.store.meta_set("known_funnels", json.dumps(sorted(ids)))

def _static_response(name: str):
    safe = os.path.basename(name)
    path = os.path.join(STATIC_DIR, safe)
    if not os.path.isfile(path):
        raise NotFound(f"static file {safe} not found")
    with open(path, "rb") as fh:
        data = fh.read()
    ctype = mimetypes.guess_type(path)[0] or "application/octet-stream"
    return 200, (ctype, data)


class Handler(BaseHTTPRequestHandler):
    app: App = None  # type: ignore[assignment]
    server_version = "pointwise/1"

    def log_message(self, fmt, *args):  # quiet, structured enough for tests
        pass

    def _read_body(self) -> tuple[object, bytes]:
        length = int(self.headers.get("Content-Length") or 0)
        raw = self.rfile.read(length) if length else b""
        ctype = (self.headers.get("Content-Type") or "").split(";")[0].strip()
        if not raw:
            return None, raw
        if ctype in ("application/x-ndjson", "text/plain"):
            return raw.decode("utf-8", "replace"), raw
        try:
            return json.loads(raw.decode("utf-8")), raw
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            raise ValidationError(f"invalid JSON body: {exc}")

    def _dispatch(self, method: str) -> None:
        parsed = urlparse(self.path)
        query = parse_qs(parsed.query)
        try:
            body, raw = self._read_body() if method in ("POST", "PUT") else (None, b"")
            status, payload = self.app.handle(method, parsed.path, query, body, raw)
            if isinstance(payload, tuple):  # raw static response
                ctype, data = payload
                self.send_response(status)
                self.send_header("Content-Type", ctype)
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)
                return
            data = json.dumps(payload, indent=2, ensure_ascii=False).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)
        except PointwiseError as exc:
            data = json.dumps(exc.as_dict(), indent=2).encode("utf-8")
            self.send_response(exc.status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)
        except Exception as exc:  # pragma: no cover - defensive
            data = json.dumps(
                {"error": f"{type(exc).__name__}: {exc}", "code": "internal"}
            ).encode("utf-8")
            self.send_response(HTTPStatus.INTERNAL_SERVER_ERROR)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

    def do_GET(self):
        self._dispatch("GET")

    def do_POST(self):
        self._dispatch("POST")


def make_server(db_path: str, host: str = "127.0.0.1", port: int = 8787,
                session_gap_ms: int = 30 * 60 * 1000,
                interval_events: int = 256) -> ThreadingHTTPServer:
    app = App(db_path, session_gap_ms, interval_events)
    handler = type("BoundHandler", (Handler,), {"app": app})
    server = ThreadingHTTPServer((host, port), handler)
    server.app = app  # type: ignore[attr-defined]
    return server
