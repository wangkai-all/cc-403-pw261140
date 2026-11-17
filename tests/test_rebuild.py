import sys
sys.path.insert(0, "/workspace")

import json
import os
import tempfile
import time
import unittest

from pointwise.engine import evaluate
from pointwise.report import ReportService
from pointwise.rebuild import RebuildManager
from pointwise.rules import parse_rules
from pointwise.rules_service import RuleService
from pointwise.store import Store

BASE = 1_700_000_000_000
HOUR = 3600 * 1000
DAY = 24 * HOUR


def ev(event_id, user_id, ts, name, kind="event", session=None, props=None):
    return {
        "event_id": event_id,
        "user_id": user_id,
        "ts": ts,
        "event": name,
        "kind": kind,
        "session_id": session,
        "props": props or {},
    }


def make_rules(funnel="f", session_strict=False):
    return {
        "conversion_window_ms": 7 * DAY,
        "steps": [
            {"name": "open", "event_name": "open"},
            {"name": "view", "event_name": "view", "same_session": session_strict},
            {"name": "buy", "event_name": "buy", "same_session": session_strict},
        ],
    }


class RebuildCase(unittest.TestCase):
    interval_events = 8

    def setUp(self):
        fd, self.db_path = tempfile.mkstemp(suffix=".db")
        os.close(fd)
        self.store = Store(self.db_path)
        self.rules_svc = RuleService(self.store)
        self.reports = ReportService(self.store)
        self.manager = RebuildManager(
            self.store, self.reports, interval_events=self.interval_events
        )

    def tearDown(self):
        self.store.close()
        for suffix in ("", "-wal", "-shm"):
            try:
                os.unlink(self.db_path + suffix)
            except FileNotFoundError:
                pass

    def ingest(self, events):
        from pointwise.collector import ingest

        return ingest(self.store, events)

    def freeze(self, payload=None):
        return self.rules_svc.freeze("f", payload or make_rules())

    def rebuild(self, frozen, timeout=30):
        rules = self.store.get_rules("f", frozen["version"])
        job = self.manager.start("f", rules, {"sha256": frozen["sha256"]})
        job = self.manager.wait(job.job_id, timeout)
        return job

    def assert_matches_reference(self, frozen):
        rules = self.store.get_rules("f", frozen["version"])
        ref = evaluate(self.store, rules)
        rec = self.reports.latest_complete("f")
        self.assertIsNotNone(rec, "no complete report after rebuild")
        self.assertEqual(rec["entries"], ref.entries)
        self.assertEqual(rec["conversions"], ref.conversions)
        self.assertEqual(rec["unresolved_entries"], ref.unresolved)
        self.assertEqual(
            [(s["name"], s["reached"], s["converted"]) for s in rec["steps"]],
            [(s.name, s.reached, s.converted) for s in ref.step_counts],
        )
        return rec


def cohort(n_users=12, base=BASE, converted=0.5):
    events = []
    for u in range(n_users):
        uid = f"u{u}"
        t = base + u * 60_000
        events.append(ev(f"{uid}-open", uid, t, "open", "session_start", f"s{uid}"))
        events.append(ev(f"{uid}-view", uid, t + 1000, "view", "event", f"s{uid}"))
        if u < int(n_users * converted):
            events.append(ev(f"{uid}-buy", uid, t + 2000, "buy", "event", f"s{uid}"))
    return events


class RebuildTests(RebuildCase):
    def test_initial_build_matches_reference(self):
        self.ingest(cohort(20))
        frozen = self.freeze()
        job = self.rebuild(frozen)
        self.assertEqual(job.status, "complete", job.error)
        self.assertEqual(job.affected_reason, "initial build")
        self.assert_matches_reference(frozen)

    def test_append_reuses_checkpoints(self):
        self.ingest(cohort(20))
        frozen = self.freeze()
        self.rebuild(frozen)
        first = self.reports.latest_complete("f")
        self.ingest(
            [ev("extra-open", "zz", BASE + DAY, "open", "session_start", "sz"),
             ev("extra-view", "zz", BASE + DAY + 5, "view", "event", "sz"),
             ev("extra-buy", "zz", BASE + DAY + 10, "buy", "event", "sz")]
        )
        job = self.rebuild(frozen)
        self.assertEqual(job.status, "complete", job.error)
        self.assertIn("append", job.affected_reason)
        self.assertGreater(job.reused_intervals, 0)
        rec = self.assert_matches_reference(frozen)
        self.assertGreater(rec["conversions"], first["conversions"])
        self.assertNotEqual(rec["report_id"], first["report_id"])

    def test_late_event_reports_affected_boundary_and_recomputes(self):
        events = cohort(16)
        self.ingest(events)
        frozen = self.freeze({"conversion_window_ms": 7 * DAY, "steps": [
            {"name": "open", "event_name": "open"},
            {"name": "view", "event_name": "view", "same_session": True},
            {"name": "buy", "event_name": "buy", "same_session": True},
        ]})
        first = self.rebuild(frozen)
        base_rec = self.reports.latest_complete("f")
        self.assertEqual(base_rec["conversions"], 8)

        # Late session_end inserted early in user u1's stream: it splits the
        # session, so u1's same-session funnel must now fail.
        self.ingest([ev("u1-end", "u1", BASE + 60_000 + 500, "close", "session_end", "su1")])
        job = self.rebuild(frozen)
        self.assertEqual(job.status, "complete", job.error)
        self.assertIn("late event", job.affected_reason)
        # u1 starts at BASE+60s, so the boundary is near the start, not at the
        # insertion wall-clock time.
        self.assertIsNotNone(job.affected_from_ms)
        self.assertLessEqual(job.affected_from_ms, BASE + 60_000 + 500)
        rec = self.assert_matches_reference(frozen)
        self.assertEqual(rec["conversions"], 7)
        self.assertGreaterEqual(job.recomputed_intervals, 1)
        self.assertEqual(
            rec["coverage"]["affected_from_event_id"], "u1-end"
        )

    def test_correction_changes_earlier_results(self):
        events = cohort(12)
        self.ingest(events)
        frozen = self.freeze()
        self.rebuild(frozen)
        before = self.assert_matches_reference(frozen)

        # Correct u0's entry event: it was really a page view, not the entry
        # event.  u0 must disappear from the entry count even though the
        # correction is to the very first event of the stream.
        self.ingest(
            [ev("u0-open", "u0", BASE, "nope", "event", "su0")]
        )
        job = self.rebuild(frozen)
        self.assertEqual(job.status, "complete", job.error)
        self.assertIn("corrected", job.affected_reason)
        after = self.assert_matches_reference(frozen)
        self.assertEqual(after["entries"], before["entries"] - 1)

    def test_pause_and_resume_from_checkpoint(self):
        self.ingest(cohort(120))
        frozen = self.freeze()
        rules = self.store.get_rules("f", frozen["version"])
        job = self.manager.start("f", rules, {"sha256": frozen["sha256"]})
        # Stop almost immediately; the first interval may or may not be done.
        self.manager.stop(job.job_id)
        job = self.manager.get(job.job_id)
        self.assertIn(job.status, ("paused", "complete"))
        if job.status == "paused":
            done = job.next_interval
            self.assertGreaterEqual(done, 0)
            self.manager.resume(job.job_id)
            job = self.manager.wait(job.job_id, 60)
        self.assertEqual(job.status, "complete", job.error)
        self.assert_matches_reference(frozen)

    def test_resume_after_process_restart(self):
        self.ingest(cohort(100))
        frozen = self.freeze()
        rules = self.store.get_rules("f", frozen["version"])
        job = self.manager.start("f", rules, {"sha256": frozen["sha256"]})
        self.manager.stop(job.job_id)
        job = self.manager.get(job.job_id)
        # Simulate a killed process: build a brand-new manager over the same DB.
        for thread in list(self.manager._threads.values()):
            thread.join(timeout=10)
        self.reports2 = ReportService(self.store)
        manager2 = RebuildManager(self.store, self.reports2, interval_events=self.interval_events)
        reloaded = manager2.get(job.job_id)
        if job.status == "running":  # interrupted mid-flight
            self.assertIn(reloaded.status, ("paused", "complete"))
        if reloaded.status == "paused":
            manager2.resume(reloaded.job_id)
            reloaded = manager2.wait(reloaded.job_id, 60)
        if reloaded.status == "complete":
            rules = self.store.get_rules("f", frozen["version"])
            ref = evaluate(self.store, rules)
            rec = self.reports2.latest_complete("f")
            self.assertEqual(rec["conversions"], ref.conversions)

    def test_ingest_during_rebuild_triggers_another_pass(self):
        self.ingest(cohort(80))
        frozen = self.freeze()
        rules = self.store.get_rules("f", frozen["version"])
        # Make ingestion happen from another thread while the rebuild runs.
        import threading

        def feed():
            time.sleep(0.002)
            for i in range(5):
                self.ingest(
                    [ev(f"live-{i}", f"live{i}", BASE + 2 * DAY + i, "open",
                        "session_start", f"sl{i}"),
                     ev(f"live-{i}-buy", f"live{i}", BASE + 2 * DAY + i + 5, "buy",
                        "event", f"sl{i}")]
                )

        thread = threading.Thread(target=feed)
        thread.start()
        job = self.manager.start("f", rules, {"sha256": frozen["sha256"]})
        job = self.manager.wait(job.job_id, 120)
        thread.join()
        self.assertEqual(job.status, "complete", job.error)
        if job.input_changed_during_rebuild:
            self.assertGreaterEqual(job.passes, 2)
        self.assert_matches_reference(frozen)

    def test_status_never_reports_zero_while_rebuilding(self):
        self.ingest(cohort(60))
        frozen = self.freeze()
        self.rebuild(frozen)
        self.ingest([ev("late-x", "u0", BASE + 10_500, "view", "event", "su0")])
        rules = self.store.get_rules("f", frozen["version"])
        job = self.manager.start("f", rules, {"sha256": frozen["sha256"]})
        self.manager.stop(job.job_id)
        latest = self.reports.latest("f")
        self.assertEqual(latest["status"], "stale")
        self.assertEqual(latest["stale_reason"], f"rebuild {job.job_id} in progress")
        # Nothing claims to be complete for the new input while the rebuild is
        # unfinished, so no surface can present a fresh-looking 0 %.
        self.assertIsNone(self.reports.latest_complete("f"))

    def test_rebuild_conflict_for_different_rules(self):
        from pointwise.errors import Conflict

        self.ingest(cohort(60))
        frozen = self.freeze()
        rules = self.store.get_rules("f", frozen["version"])
        job = self.manager.start("f", rules, {"sha256": frozen["sha256"]})
        self.manager.stop(job.job_id)
        if self.manager.get(job.job_id).status == "paused":
            other = self.freeze(
                {"conversion_window_ms": 7 * DAY,
                 "steps": [{"name": "open", "event_name": "open"}]}
            )
            with self.assertRaises(Conflict):
                self.manager.start(
                    "f",
                    self.store.get_rules("f", other["version"]),
                    {"sha256": other["sha256"]},
                )


if __name__ == "__main__":
    unittest.main()
