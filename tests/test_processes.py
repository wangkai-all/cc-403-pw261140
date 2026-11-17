"""End-to-end checks that the processes really are independent.

* ``collect`` runs as its own Python process writing to the shared DB.
* The API server keeps serving from that DB.
* A rebuild killed with SIGKILL resumes from its checkpoint in a new process.
"""

import json
import os
import signal
import subprocess
import sys
import tempfile
import time
import unittest
import urllib.request

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def free_port():
    import socket

    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


class ProcessTests(unittest.TestCase):
    def setUp(self):
        fd, self.db = tempfile.mkstemp(suffix=".db")
        os.close(fd)
        self.port = free_port()
        self.server = subprocess.Popen(
            [sys.executable, "-m", "pointwise", "serve", "--db", self.db,
             "--port", str(self.port), "--interval-events", "16"],
            cwd=ROOT,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        for _ in range(200):
            try:
                self.get("/api/health")
                break
            except Exception:
                time.sleep(0.05)
        else:
            self.fail("server did not start: " + (self.server.stderr.read() if self.server.stderr else ""))

    def tearDown(self):
        self.server.terminate()
        try:
            self.server.wait(timeout=10)
        except subprocess.TimeoutExpired:
            self.server.kill()
        for suffix in ("", "-wal", "-shm"):
            try:
                os.unlink(self.db + suffix)
            except FileNotFoundError:
                pass

    def get(self, path):
        with urllib.request.urlopen(f"http://127.0.0.1:{self.port}{path}", timeout=10) as r:
            return json.loads(r.read())

    def post(self, path, body):
        data = json.dumps(body).encode()
        req = urllib.request.Request(
            f"http://127.0.0.1:{self.port}{path}", data=data, method="POST",
            headers={"Content-Type": "application/json"},
        )
        with urllib.request.urlopen(req, timeout=20) as r:
            return json.loads(r.read())

    def collect_file(self, events):
        fd, path = tempfile.mkstemp(suffix=".jsonl")
        os.close(fd)
        with open(path, "w", encoding="utf-8") as fh:
            for ev in events:
                fh.write(json.dumps(ev) + "\n")
        proc = subprocess.run(
            [sys.executable, "-m", "pointwise", "collect", "--db", self.db, "--file", path],
            cwd=ROOT, capture_output=True, text=True,
        )
        os.unlink(path)
        self.assertEqual(proc.returncode, 0, proc.stderr)
        return json.loads(proc.stdout)

    def test_separate_collector_process_is_seen_by_server(self):
        base = 1_700_000_000_000
        events = []
        for u in range(6):
            uid = f"u{u}"
            events.append({"event_id": f"{uid}-o", "user_id": uid, "ts": base + u * 1000,
                           "event": "open", "kind": "session_start", "session_id": f"s{uid}"})
            events.append({"event_id": f"{uid}-v", "user_id": uid, "ts": base + u * 1000 + 10,
                           "event": "view", "session_id": f"s{uid}"})
        out = self.collect_file(events)
        self.assertEqual(out["inserted"], 12)
        health = self.get("/api/health")
        self.assertEqual(health["event_count"], 12)
        # A second import of the same file is a no-op.
        out = self.collect_file(events)
        self.assertEqual(out["unchanged"], 12)
        self.assertFalse(out["rebuild_needed"])

    def test_sigkill_then_resume_in_new_process(self):
        base = 1_700_000_000_000
        events = []
        for u in range(300):
            uid = f"u{u}"
            t = base + u * 1000
            events.append({"event_id": f"{uid}-o", "user_id": uid, "ts": t,
                           "event": "open", "kind": "session_start", "session_id": f"s{uid}"})
            events.append({"event_id": f"{uid}-v", "user_id": uid, "ts": t + 10,
                           "event": "view", "session_id": f"s{uid}"})
            events.append({"event_id": f"{uid}-b", "user_id": uid, "ts": t + 20,
                           "event": "buy", "session_id": f"s{uid}"})
        self.collect_file(events)

        rules = {
            "conversion_window_ms": 7 * 86_400_000,
            "steps": [
                {"name": "open", "event_name": "open"},
                {"name": "view", "event_name": "view"},
                {"name": "buy", "event_name": "buy"},
            ],
        }
        preview = self.post("/api/funnels/f/preview", rules)
        frozen = self.post(
            "/api/funnels/f/versions",
            {"rules": rules, "expected_hash": preview["rules_hash"]},
        )
        started = self.post("/api/funnels/f/reports", {"version": frozen["version"]})
        job_id = started["job"]["job_id"]

        # Kill the whole server process hard, mid-rebuild.
        self.server.send_signal(signal.SIGKILL)
        self.server.wait(timeout=10)

        port2 = free_port()
        server2 = subprocess.Popen(
            [sys.executable, "-m", "pointwise", "serve", "--db", self.db,
             "--port", str(port2), "--interval-events", "16"],
            cwd=ROOT, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
        )
        try:
            for _ in range(200):
                try:
                    with urllib.request.urlopen(
                        f"http://127.0.0.1:{port2}/api/rebuilds/{job_id}", timeout=10
                    ) as r:
                        job = json.loads(r.read())
                    break
                except Exception:
                    time.sleep(0.05)
            else:
                self.fail("second server did not start")
            # The interrupted job is visible as paused, never silently "complete".
            self.assertIn(job["status"], ("paused", "complete"))
            if job["status"] == "paused":
                req = urllib.request.Request(
                    f"http://127.0.0.1:{port2}/api/rebuilds/{job_id}/resume",
                    data=b"{}", method="POST",
                    headers={"Content-Type": "application/json"},
                )
                urllib.request.urlopen(req, timeout=20).read()
                for _ in range(400):
                    with urllib.request.urlopen(
                        f"http://127.0.0.1:{port2}/api/rebuilds/{job_id}", timeout=10
                    ) as r:
                        job = json.loads(r.read())
                    if job["status"] in ("complete", "failed"):
                        break
                    time.sleep(0.02)
            self.assertEqual(job["status"], "complete", job.get("error"))
            with urllib.request.urlopen(
                f"http://127.0.0.1:{port2}/api/funnels/f/status", timeout=10
            ) as r:
                status = json.loads(r.read())
            self.assertEqual(status["state"], "ready")
            self.assertEqual(status["report"]["conversions"], 300)
            self.assertEqual(status["report"]["entries"], 300)
        finally:
            server2.terminate()
            try:
                server2.wait(timeout=10)
            except subprocess.TimeoutExpired:
                server2.kill()

    def test_cli_seed_and_rebuild(self):
        proc = subprocess.run(
            [sys.executable, "-m", "pointwise", "seed", "--db", self.db,
             "--users", "40", "--days", "5", "--seed", "3"],
            cwd=ROOT, capture_output=True, text=True,
        )
        self.assertEqual(proc.returncode, 0, proc.stderr)
        preview = self.post("/api/funnels/demo/preview", {
            "conversion_window_ms": 7 * 86_400_000,
            "steps": [
                {"name": "open", "event_name": "app_open"},
                {"name": "view", "event_name": "view_product"},
                {"name": "cart", "event_name": "add_to_cart"},
                {"name": "buy", "event_name": "purchase"},
            ],
        })
        self.assertGreater(preview["entries"], 0)
        self.post("/api/funnels/demo/versions", {"rules": {
            "conversion_window_ms": 7 * 86_400_000,
            "steps": [
                {"name": "open", "event_name": "app_open"},
                {"name": "view", "event_name": "view_product"},
                {"name": "cart", "event_name": "add_to_cart"},
                {"name": "buy", "event_name": "purchase"},
            ],
        }})
        proc = subprocess.run(
            [sys.executable, "-m", "pointwise", "rebuild", "--db", self.db,
             "--funnel", "demo", "--interval-events", "32"],
            cwd=ROOT, capture_output=True, text=True,
        )
        self.assertEqual(proc.returncode, 0, proc.stderr)
        job = json.loads(proc.stdout)
        self.assertEqual(job["status"], "complete")
        report = self.get(f"/api/reports/{job['report_id']}")
        self.assertEqual(report["rules_version"], 1)
        self.assertTrue(report["input_digest"])


if __name__ == "__main__":
    unittest.main()
