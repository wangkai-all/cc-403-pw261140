import sys
sys.path.insert(0, "/workspace")

import json
import os
import tempfile
import threading
import unittest
import urllib.error
import urllib.request

from pointwise.api import make_server
from pointwise.collector import ingest
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


def rules(step_defs, window_ms=7 * DAY, funnel_id="f"):
    return parse_rules(
        {"conversion_window_ms": window_ms, "steps": step_defs}, funnel_id, 0
    )


class StoreCase(unittest.TestCase):
    def setUp(self):
        fd, self.db_path = tempfile.mkstemp(suffix=".db")
        os.close(fd)
        self.store = Store(self.db_path)

    def tearDown(self):
        self.store.close()
        for suffix in ("", "-wal", "-shm"):
            try:
                os.unlink(self.db_path + suffix)
            except FileNotFoundError:
                pass

    def ingest(self, events):
        return ingest(self.store, events)


class IngestTests(StoreCase):
    def test_idempotent_reimport(self):
        batch = [ev("e1", "u1", BASE, "open"), ev("e2", "u1", BASE + 10, "buy")]
        first = self.ingest(batch)
        second = self.ingest(batch)
        self.assertEqual(first["inserted"], 2)
        self.assertEqual(second["inserted"], 0)
        self.assertEqual(second["unchanged"], 2)
        self.assertEqual(self.store.event_count(), 2)

    def test_correction_records_revision(self):
        self.ingest([ev("e1", "u1", BASE, "open")])
        out = self.ingest([ev("e1", "u1", BASE - 1000, "open")])
        self.assertEqual(out["corrected"], 1)
        self.assertEqual(self.store.event_count(), 1)
        latest = self.store.all_events()[0]
        self.assertEqual(latest["ts_ms"], BASE - 1000)
        self.assertEqual(latest["revision"], 1)
        history = self.store.event_revisions("e1")
        self.assertEqual(len(history), 1)
        self.assertIn("old_row", history[0])

    def test_duplicate_id_in_one_batch_is_last_wins(self):
        out = self.ingest(
            [ev("e1", "u1", BASE, "open"), ev("e1", "u1", BASE + 5, "open")]
        )
        self.assertEqual(out["inserted"], 1)
        self.assertEqual(self.store.all_events()[0]["ts_ms"], BASE + 5)

    def test_invalid_lines_are_reported_not_dropped_silently(self):
        out = self.ingest(
            json.dumps(
                [
                    ev("e1", "u1", BASE, "open"),
                    {"event_id": "bad", "user_id": "", "ts": BASE, "event": "x"},
                ]
            )
        )
        self.assertEqual(out["inserted"], 1)
        self.assertEqual(len(out["rejected"]), 1)
        self.assertIn("user_id", out["rejected"][0]["error"])

    def test_jsonl_with_bad_json(self):
        out = self.ingest(
            json.dumps(ev("e1", "u1", BASE, "open"))
            + "\n{not json}\n"
            + json.dumps(ev("e2", "u1", BASE + 1, "next"))
        )
        self.assertEqual(out["inserted"], 2)
        self.assertEqual(out["rejected"][0]["line"], 2)

    def test_identity_merge_requires_target(self):
        from pointwise.errors import ValidationError

        with self.assertRaises(ValidationError) as ctx:
            self.ingest(
                [
                    {
                        "event_id": "m1",
                        "user_id": "u1",
                        "ts": BASE,
                        "event": "identity_merge",
                        "kind": "identity_merge",
                    }
                ]
            )
        self.assertIn("merge_session", json.dumps(ctx.exception.details))


class RuleTests(StoreCase):
    def test_hash_ignores_version_but_not_content(self):
        r1 = rules([{"name": "A", "event_name": "a"}])
        r2 = rules([{"name": "A", "event_name": "a"}])
        r2.version = 9
        self.assertEqual(r1.rules_hash, r2.rules_hash)
        r3 = rules([{"name": "A", "event_name": "a"}, {"name": "B", "event_name": "b"}])
        self.assertNotEqual(r1.rules_hash, r3.rules_hash)

    def test_validation(self):
        with self.assertRaises(Exception):
            rules([])
        # duplicate labels are legal: that's how a repeated step is modelled
        repeated = rules(
            [{"name": "A", "event_name": "a"}, {"name": "A", "event_name": "a"}]
        )
        self.assertEqual(len(repeated.steps), 2)
        with self.assertRaises(Exception):
            rules([{"name": "A", "event_name": "a", "min_wait_ms": 10, "max_wait_ms": 5}])

    def test_freeze_chain(self):
        svc = RuleService(self.store)
        a = svc.freeze("f", {"conversion_window_ms": DAY, "steps": [{"name": "A", "event_name": "a"}]})
        b = svc.freeze("f", {"conversion_window_ms": DAY, "steps": [{"name": "A", "event_name": "b"}]})
        self.assertEqual(a["version"], 1)
        self.assertEqual(b["version"], 2)
        self.assertEqual(b["prev_sha256"], a["sha256"])
        self.store.verify_chain("f")

    def test_freeze_rejects_changed_payload(self):
        from pointwise.errors import Conflict
        from pointwise.rules_service import RuleService

        svc = RuleService(self.store)
        preview = svc.preview("f", {"conversion_window_ms": DAY, "steps": [{"name": "A", "event_name": "a"}]})
        with self.assertRaises(Conflict):
            svc.freeze(
                "f",
                {"conversion_window_ms": DAY, "steps": [{"name": "A", "event_name": "b"}]},
                expected_hash=preview["rules_hash"],
            )

    def test_chain_tamper_detected(self):
        from pointwise.errors import Conflict

        svc = RuleService(self.store)
        svc.freeze("f", {"conversion_window_ms": DAY, "steps": [{"name": "A", "event_name": "a"}]})
        with self.store._lock:
            self.store._conn.execute(
                "UPDATE rules_versions SET payload=? WHERE funnel_id='f'", ('{"steps":[]}',)
            )
            self.store._conn.commit()
        with self.assertRaises(Conflict):
            self.store.verify_chain("f")


class FunnelSemanticsTests(StoreCase):
    def run_funnel(self, step_defs, events, window_ms=7 * DAY):
        self.ingest(events)
        r = rules(step_defs, window_ms)
        return evaluate(self.store, r), r

    def test_basic_and_unresolved_not_zero(self):
        events = [
            ev("e1", "u1", BASE, "open"),
            ev("e2", "u1", BASE + 10, "buy"),
            ev("e3", "u2", BASE + 5, "open"),
        ]
        result, _ = self.run_funnel(
            [{"name": "open", "event_name": "open"}, {"name": "buy", "event_name": "buy"}],
            events,
            window_ms=DAY,
        )
        self.assertEqual(result.entries, 2)
        self.assertEqual(result.conversions, 1)
        self.assertEqual(result.unresolved, 1)
        # classic rate over all entries, but flagged provisional
        self.assertEqual(result.conversion_rate, 0.5)
        self.assertTrue(result.provisional)
        self.assertEqual(result.settled_conversion_rate, 1.0)

    def test_no_entries_means_null_rate(self):
        result, _ = self.run_funnel(
            [{"name": "open", "event_name": "open"}],
            [ev("e1", "u1", BASE, "other")],
        )
        self.assertEqual(result.entries, 0)
        self.assertIsNone(result.conversion_rate)

    def test_repeated_steps_require_two_occurrences(self):
        result, _ = self.run_funnel(
            [
                {"name": "cart", "event_name": "cart"},
                {"name": "cart", "event_name": "cart"},
                {"name": "buy", "event_name": "buy"},
            ],
            [
                ev("e1", "u1", BASE, "cart"),
                ev("e2", "u1", BASE + 10, "buy"),
            ],
        )
        self.assertEqual(result.entries, 1)
        self.assertEqual(result.conversions, 0)
        result2, _ = self.run_funnel(
            [
                {"name": "cart", "event_name": "cart"},
                {"name": "cart", "event_name": "cart"},
                {"name": "buy", "event_name": "buy"},
            ],
            [
                ev("f1", "u2", BASE, "cart"),
                ev("f2", "u2", BASE + 10, "cart"),
                ev("f3", "u2", BASE + 20, "buy"),
            ],
        )
        self.assertEqual(result2.conversions, 1)

    def test_count_repeats_collapses_run(self):
        result, _ = self.run_funnel(
            [
                {"name": "cart", "event_name": "cart"},
                {"name": "cart2", "event_name": "cart", "count_repeats": True},
                {"name": "buy", "event_name": "buy"},
            ],
            [ev("e1", "u1", BASE, "cart"), ev("e2", "u1", BASE + 10, "buy")],
        )
        self.assertEqual(result.conversions, 1)

    def test_cross_session_wait_allowed_by_default(self):
        events = [
            ev("e1", "u1", BASE, "open", "session_start", "s1"),
            ev("e2", "u1", BASE + HOUR, "buy", "event", "s2"),
        ]
        result, _ = self.run_funnel(
            [{"name": "open", "event_name": "open"},
             {"name": "buy", "event_name": "buy", "same_session": False}],
            events,
        )
        self.assertEqual(result.conversions, 1)

    def test_same_session_blocks_cross_session(self):
        events = [
            ev("e1", "u1", BASE, "open", "session_start", "s1"),
            ev("e2", "u1", BASE + HOUR, "buy", "event", "s2"),
        ]
        result, _ = self.run_funnel(
            [{"name": "open", "event_name": "open"},
             {"name": "buy", "event_name": "buy", "same_session": True}],
            events,
        )
        self.assertEqual(result.conversions, 0)

    def test_same_session_does_not_cross_session_end(self):
        events = [
            ev("e1", "u1", BASE, "open", "session_start", "s1"),
            ev("e2", "u1", BASE + 1000, "close", "session_end", "s1"),
            ev("e3", "u1", BASE + 2000, "buy", "event", "s1"),
        ]
        result, _ = self.run_funnel(
            [{"name": "open", "event_name": "open"},
             {"name": "buy", "event_name": "buy", "same_session": True}],
            events,
        )
        self.assertEqual(result.conversions, 0)

    def test_wait_bounds(self):
        events = [
            ev("e1", "u1", BASE, "open"),
            ev("e2", "u1", BASE + 5000, "buy"),
        ]
        too_early, _ = self.run_funnel(
            [{"name": "open", "event_name": "open"},
             {"name": "buy", "event_name": "buy", "min_wait_ms": 6000}],
            events,
        )
        self.assertEqual(too_early.conversions, 0)

    def test_conversion_window_expiry_allows_new_attempt(self):
        events = [
            ev("e1", "u1", BASE, "open"),
            ev("e2", "u1", BASE + 2 * DAY, "open"),
            ev("e3", "u1", BASE + 2 * DAY + 10, "buy"),
        ]
        result, _ = self.run_funnel(
            [{"name": "open", "event_name": "open"},
             {"name": "buy", "event_name": "buy"}],
            events,
            window_ms=DAY,
        )
        self.assertEqual(result.conversions, 1)

    def test_identity_merge_joins_sessions(self):
        events = [
            ev("s1start", "u1", BASE, "open", "session_start", "anon"),
            ev("s1a", "u1", BASE + 10, "view", "event", "anon"),
            ev("s2start", "u1", BASE + 2 * HOUR, "open", "session_start", "known"),
            ev("s2a", "u1", BASE + 2 * HOUR + 10, "buy", "event", "known"),
        ]
        before, _ = self.run_funnel(
            [{"name": "view", "event_name": "view"},
             {"name": "buy", "event_name": "buy", "same_session": True}],
            events,
        )
        self.assertEqual(before.conversions, 0)

        self.ingest(
            [
                ev(
                    "merge1",
                    "u1",
                    BASE + 3 * HOUR,
                    "identity_merge",
                    "identity_merge",
                    "known",
                    {"merge_session": "anon"},
                )
            ]
        )
        after = evaluate(
            self.store,
            rules(
                [{"name": "view", "event_name": "view"},
                 {"name": "buy", "event_name": "buy", "same_session": True}]
            ),
        )
        self.assertEqual(after.conversions, 1)

    def test_sessions_are_derived_per_user(self):
        """Another user's interleaved event must not bridge a user's own gap."""

        events = [
            ev("a1", "a", BASE, "open", "session_start", "sa"),
            ev("b1", "b", BASE + 10_000, "open", "session_start", "sb"),
            # More than the session gap after a1, but only 5 s after b1.  If
            # derivation looked at the global previous row this would stay in
            # sa; per user it must open a new session for a.
            ev("a2", "a", BASE + 40 * 60 * 1000, "buy", "event", "sa"),
        ]
        self.ingest(events)
        r = rules(
            [{"name": "open", "event_name": "open"},
             {"name": "buy", "event_name": "buy", "same_session": True}]
        )
        self.assertEqual(evaluate(self.store, r).conversions, 0)

    def test_sessions_are_namespaced_per_user(self):
        """Two people may send the same payload session id; they stay apart."""

        events = [
            ev("a1", "a", BASE, "open", "session_start", "s1"),
            ev("b1", "b", BASE + 10, "open", "session_start", "s1"),
            # b's merge names "s1", which is also a's session id.  It must only
            # touch b's own window.
            ev("bm", "b", BASE + 20, "identity_merge", "identity_merge", "s1",
               {"merge_session": "s1"}),
            ev("a2", "a", BASE + 30, "buy", "event", "s1"),
        ]
        self.ingest(events)
        r = rules(
            [{"name": "open", "event_name": "open"},
             {"name": "buy", "event_name": "buy", "same_session": True}]
        )
        self.assertEqual(evaluate(self.store, r).conversions, 1)
        from pointwise.sessions import derive_sessions

        sm = derive_sessions(self.store.all_events(), 30 * 60 * 1000)
        self.assertEqual(len(sm.sessions), 2)
        self.assertEqual({s.ends_with_session_end for s in sm.sessions}, {False})
        self.assertEqual(max(len(s.event_seqs) for s in sm.sessions), 2)

    def test_late_event_can_split_a_session(self):
        """A back-filled session_end after the fact must split the window."""

        events = [
            ev("e1", "u1", BASE, "open", "session_start", "s1"),
            ev("e4", "u1", BASE + 5000, "buy", "event", "s1"),
        ]
        self.ingest(events)
        r = rules(
            [{"name": "open", "event_name": "open"},
             {"name": "buy", "event_name": "buy", "same_session": True}]
        )
        self.assertEqual(evaluate(self.store, r).conversions, 1)

        # Late event at BASE+1000 ends the session.  The purchase now falls
        # into a separate derived window, so the same-session rule fails.
        self.ingest([ev("e2", "u1", BASE + 1000, "close", "session_end", "s1")])
        after = evaluate(self.store, r)
        self.assertEqual(after.conversions, 0)
        # ...but a fresh entry event in the second window is a genuine retry,
        # and that attempt does convert.  (Attempts are independent candidates;
        # one stalled attempt does not poison a later one.)
        self.ingest([ev("e3", "u1", BASE + 2000, "open", "session_start", "s2")])
        after_retry = evaluate(self.store, r)
        self.assertEqual(after_retry.conversions, 1)
        self.assertEqual(after_retry.entries, 1)


if __name__ == "__main__":
    unittest.main()
