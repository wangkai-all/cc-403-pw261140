import sys
sys.path.insert(0, "/workspace")

import json
import os
import tempfile
import threading
import time
import unittest
import urllib.error
import urllib.request

from pointwise.api import make_server

BASE = 1_700_000_000_000
DAY = 86_400_000


class ApiCase(unittest.TestCase):
    interval_events = 8

    def setUp(self):
        fd, self.db_path = tempfile.mkstemp(suffix=".db")
        os.close(fd)
        self.server = make_server(
            self.db_path, host="127.0.0.1", port=0, interval_events=self.interval_events
        )
        self.port = self.server.server_address[1]
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.server.app.store.close()
        for suffix in ("", "-wal", "-shm"):
            try:
                os.unlink(self.db_path + suffix)
            except FileNotFoundError:
                pass

    def call(self, method, path, body=None, content_type="application/json", raw=False):
        data = None
        headers = {}
        if body is not None:
            if isinstance(body, (dict, list)):
                data = json.dumps(body).encode()
            elif isinstance(body, str):
                data = body.encode()
            else:
                data = body
            headers["Content-Type"] = content_type
        req = urllib.request.Request(
            f"http://127.0.0.1:{self.port}{path}", data=data, method=method, headers=headers
        )
        try:
            with urllib.request.urlopen(req) as resp:
                payload = resp.read()
                return resp.status, payload if raw else json.loads(payload)
        except urllib.error.HTTPError as exc:
            payload = exc.read()
            return exc.code, payload if raw else json.loads(payload)

    def event(self, eid, uid, ts, name, kind="event", session=None, props=None):
        return {
            "event_id": eid,
            "user_id": uid,
            "ts": ts,
            "event": name,
            "kind": kind,
            "session_id": session,
            "props": props or {},
        }

    def seed_events(self, users=10):
        events = []
        for u in range(users):
            uid = f"u{u}"
            events.append(self.event(f"{uid}-o", uid, BASE + u * 1000, "open", "session_start", f"s{uid}"))
            events.append(self.event(f"{uid}-v", uid, BASE + u * 1000 + 10, "view", "event", f"s{uid}"))
            if u % 2 == 0:
                events.append(self.event(f"{uid}-b", uid, BASE + u * 1000 + 20, "buy", "event", f"s{uid}"))
        return events

    RULES = {
        "conversion_window_ms": 7 * DAY,
        "steps": [
            {"name": "open", "event_name": "open"},
            {"name": "view", "event_name": "view"},
            {"name": "buy", "event_name": "buy"},
        ],
    }

    def freeze_and_wait(self):
        _, preview = self.call("POST", "/api/funnels/f/preview", self.RULES)
        status, frozen = self.call(
            "POST",
            "/api/funnels/f/versions",
            {"rules": self.RULES, "expected_hash": preview["rules_hash"]},
        )
        self.assertEqual(status, 201)
        status, started = self.call("POST", "/api/funnels/f/reports", {"version": frozen["version"]})
        self.assertEqual(status, 202)
        job_id = started["job"]["job_id"]
        for _ in range(200):
            _, job = self.call("GET", f"/api/rebuilds/{job_id}")
            if job["status"] in ("complete", "failed", "paused"):
                break
            time.sleep(0.02)
        return frozen, job


class ApiTests(ApiCase):
    def test_health_and_index(self):
        status, body = self.call("GET", "/api/health")
        self.assertEqual(status, 200)
        self.assertEqual(body["event_count"], 0)
        status, page = self.call("GET", "/", raw=True)
        self.assertEqual(status, 200)
        self.assertIn(b"Pointwise", page)
        status, css = self.call("GET", "/static/app.css", raw=True)
        self.assertEqual(status, 200)

    def test_full_flow_preview_freeze_rebuild_status(self):
        status, ing = self.call("POST", "/api/collect", self.seed_events(10))
        self.assertEqual(status, 200)
        self.assertEqual(ing["inserted"], 25)

        status, preview = self.call("POST", "/api/funnels/f/preview", self.RULES)
        self.assertEqual(status, 200)
        self.assertEqual(preview["entries"], 10)
        self.assertEqual(preview["conversions"], 5)
        self.assertEqual(preview["conversion_rate"], 0.5)
        # Non-converters are still inside their 7-day window in this fixture,
        # so the rate is honest but flagged provisional; settled rate ignores them.
        self.assertTrue(preview["conversion_rate_provisional"])
        self.assertEqual(preview["settled_conversion_rate"], 1.0)
        self.assertIn("first version", preview["diff"][0])

        status, frozen = self.call(
            "POST", "/api/funnels/f/versions",
            {"rules": self.RULES, "expected_hash": preview["rules_hash"]},
        )
        self.assertEqual(status, 201)

        status, started = self.call("POST", "/api/funnels/f/reports", {"version": 1})
        self.assertEqual(status, 202)
        job_id = started["job"]["job_id"]
        for _ in range(200):
            _, job = self.call("GET", f"/api/rebuilds/{job_id}")
            if job["status"] in ("complete", "failed"):
                break
            time.sleep(0.02)
        self.assertEqual(job["status"], "complete", job.get("error"))

        status, report = self.call("GET", f"/api/reports/{job['report_id']}")
        self.assertEqual(status, 200)
        self.assertEqual(report["rules_version"], 1)
        self.assertEqual(report["rules_hash"], frozen["rules_hash"])
        self.assertTrue(report["input_digest"])
        self.assertEqual(report["conversion_rate"], 0.5)

        status, funnel_status = self.call("GET", "/api/funnels/f/status")
        self.assertEqual(status, 200)
        self.assertEqual(funnel_status["state"], "ready")
        self.assertEqual(funnel_status["report"]["report_id"], report["report_id"])

    def test_preview_hash_mismatch_rejected(self):
        self.call("POST", "/api/collect", self.seed_events(2))
        _, preview = self.call("POST", "/api/funnels/f/preview", self.RULES)
        changed = json.loads(json.dumps(self.RULES))
        changed["steps"][2]["event_name"] = "purchase"
        status, body = self.call(
            "POST", "/api/funnels/f/versions",
            {"rules": changed, "expected_hash": preview["rules_hash"]},
        )
        self.assertEqual(status, 409)
        self.assertEqual(body["code"], "conflict")

    def test_status_stale_and_no_fake_zero(self):
        self.call("POST", "/api/collect", self.seed_events(6))
        frozen, job = self.freeze_and_wait()
        self.call(
            "POST",
            "/api/collect",
            [self.event("late-1", "u5", BASE + 5005, "noise", "event", "s5")],
        )
        _, funnel_status = self.call("GET", "/api/funnels/f/status")
        self.assertEqual(funnel_status["state"], "stale")
        self.assertEqual(funnel_status["report"]["status"], "stale")
        self.assertIn("changed", funnel_status["message"])

    def test_stop_and_resume_job(self):
        self.call("POST", "/api/collect", self.seed_events(40))
        frozen, _ = self.freeze_and_wait()
        _, started = self.call("POST", "/api/funnels/f/reports", {"version": frozen["version"]})
        job_id = started["job"]["job_id"]
        status, stopped = self.call("POST", f"/api/rebuilds/{job_id}/stop", {})
        self.assertEqual(status, 200)
        self.assertIn(stopped["job"]["status"], ("paused", "complete"))
        if stopped["job"]["status"] == "paused":
            status, resumed = self.call("POST", f"/api/rebuilds/{job_id}/resume", {})
            self.assertEqual(status, 200)
            for _ in range(300):
                _, job = self.call("GET", f"/api/rebuilds/{job_id}")
                if job["status"] in ("complete", "failed"):
                    break
                time.sleep(0.02)
            self.assertEqual(job["status"], "complete", job.get("error"))

    def test_user_path_returns_derived_sessions(self):
        self.call("POST", "/api/collect", self.seed_events(3))
        self.freeze_and_wait()
        status, path = self.call("GET", "/api/users/u0/path?funnel_id=f")
        self.assertEqual(status, 200)
        self.assertEqual(path["user_id"], "u0")
        self.assertEqual(len(path["steps"]), 3)
        self.assertEqual(path["max_step"], 3)
        self.assertTrue(path["completed"])
        self.assertTrue(all(ev["session_id"] for ev in path["path"]))
        self.assertEqual(len(path["sessions"]), 1)
        self.assertEqual(path["sessions"][0]["session_id"], "su0")
        # Each matched step names the event that satisfied it.
        hits = {ev["event_id"]: ev["matched_steps"] for ev in path["path"]}
        self.assertEqual(hits["u0-o"], [0])
        self.assertEqual(hits["u0-v"], [1])
        self.assertEqual(hits["u0-b"], [2])
        # A non-converter shows how far they got rather than a zero.
        status, path2 = self.call("GET", "/api/users/u1/path?funnel_id=f")
        self.assertEqual(path2["max_step"], 2)
        self.assertFalse(path2["completed"])

    def test_user_path_with_live_rules_when_unfrozen(self):
        self.call("POST", "/api/collect", self.seed_events(3))
        import urllib.parse

        q = urllib.parse.quote(json.dumps(self.RULES))
        status, path = self.call("GET", f"/api/users/u1/path?funnel_id=f&rules_json={q}")
        self.assertEqual(status, 200)
        self.assertEqual(path["rules_version"], 0)

    def test_validation_errors_are_structured(self):
        status, body = self.call(
            "POST", "/api/funnels/f/preview",
            {"conversion_window_ms": 1000, "steps": []},
        )
        self.assertEqual(status, 422)
        self.assertEqual(body["code"], "invalid_rules")

        status, body = self.call("GET", "/api/nope")
        self.assertEqual(status, 404)

    def test_jsonl_ingest(self):
        lines = "\n".join(
            json.dumps(self.event(f"x{i}", "u", BASE + i, "open")) for i in range(3)
        )
        status, out = self.call(
            "POST", "/api/collect", lines, content_type="application/x-ndjson"
        )
        self.assertEqual(status, 200)
        self.assertEqual(out["inserted"], 3)

    def test_explain_reports_reproducibility(self):
        self.call("POST", "/api/collect", self.seed_events(8))
        frozen, job = self.freeze_and_wait()
        status, explained = self.call("GET", "/api/funnels/f/explain")
        self.assertEqual(status, 200)
        self.assertTrue(explained["reproducible"])
        self.assertTrue(explained["rules_snapshot"]["chain_ok"])
        self.assertEqual(explained["drift"]["events_added"], 0)

        self.call(
            "POST",
            "/api/collect",
            [self.event("u3-o", "u3", BASE + 3000, "open", "session_start", "s3")],
        )
        _, explained = self.call("GET", "/api/funnels/f/explain")
        self.assertFalse(explained["reproducible"])
        self.assertTrue(explained["drift"]["input_changed"])

    def test_event_revision_history(self):
        self.call("POST", "/api/collect", [self.event("e1", "u1", BASE, "open")])
        self.call("POST", "/api/collect", [self.event("e1", "u1", BASE + 5, "open")])
        status, hist = self.call("GET", "/api/events/e1/revisions")
        self.assertEqual(status, 200)
        self.assertEqual(len(hist["revisions"]), 1)
        self.assertEqual(hist["current"]["ts_ms"], BASE + 5)

    def test_funnel_list_and_versions(self):
        self.call("POST", "/api/funnels/a/preview", self.RULES)
        self.call("POST", "/api/funnels/b/preview", self.RULES)
        status, body = self.call("GET", "/api/funnels")
        self.assertEqual(status, 200)
        self.assertEqual(body["funnels"], ["a", "b"])

        self.call("POST", "/api/funnels/a/versions", {"rules": self.RULES})
        status, versions = self.call("GET", "/api/funnels/a/versions")
        self.assertEqual(status, 200)
        self.assertEqual(len(versions["versions"]), 1)
        self.assertEqual(versions["versions"][0]["version"], 1)
        status, single = self.call("GET", "/api/funnels/a/versions/1")
        self.assertEqual(status, 200)
        self.assertEqual(single["version"], 1)
        status, _ = self.call("GET", "/api/funnels/a/versions/9")
        self.assertEqual(status, 404)


if __name__ == "__main__":
    unittest.main()
