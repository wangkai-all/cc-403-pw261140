"""Command-line entry points.

    python -m pointwise serve   --db data/pointwise.db --port 8787
    python -m pointwise collect --db data/pointwise.db --file events.jsonl
    python -m pointwise rebuild --db data/pointwise.db --funnel checkout [--version N]
    python -m pointwise seed    --db data/pointwise.db

``collect`` and ``serve`` are independent processes sharing the SQLite file.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time

from .api import App, make_server
from .collector import ingest
from .errors import PointwiseError
from .store import Store


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(prog="pointwise", description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)

    serve = sub.add_parser("serve", help="run the API + web console")
    serve.add_argument("--db", default="data/pointwise.db")
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=8787)
    serve.add_argument("--session-gap-ms", type=int, default=30 * 60 * 1000)
    serve.add_argument("--interval-events", type=int, default=256)

    collect = sub.add_parser("collect", help="append a JSONL/JSON file to the log")
    collect.add_argument("--db", default="data/pointwise.db")
    collect.add_argument("--file", required=True)
    collect.add_argument("--format", choices=["jsonl", "json"], default="jsonl")

    seed = sub.add_parser("seed", help="generate and ingest synthetic behaviour data")
    seed.add_argument("--db", default="data/pointwise.db")
    seed.add_argument("--users", type=int, default=400)
    seed.add_argument("--days", type=int, default=14)
    seed.add_argument("--seed", type=int, default=7)
    seed.add_argument("--out", default="")

    rebuild = sub.add_parser("rebuild", help="run a rebuild to completion")
    rebuild.add_argument("--db", default="data/pointwise.db")
    rebuild.add_argument("--funnel", required=True)
    rebuild.add_argument("--version", type=int, default=None)
    rebuild.add_argument("--interval-events", type=int, default=256)

    args = parser.parse_args(argv)

    if args.command == "serve":
        server = make_server(
            args.db, args.host, args.port, args.session_gap_ms, args.interval_events
        )
        print(f"pointwise listening on http://{args.host}:{args.port}/ (db={args.db})")
        try:
            server.serve_forever()
        except KeyboardInterrupt:
            pass
        finally:
            server.shutdown()
        return 0

    if args.command == "collect":
        with open(args.file, "r", encoding="utf-8") as fh:
            text = fh.read()
        payload = text if args.format == "jsonl" else json.loads(text)
        # The collector only needs the event log; opening the full app would
        # spawn report/rebuild machinery in a process that should stay dumb.
        store = Store(args.db)
        try:
            stats = ingest(store, payload, source=args.file)
        finally:
            store.close()
        print(json.dumps(stats, indent=2))
        return 0

    if args.command == "seed":
        from .seed import followups, generate

        events = generate(users=args.users, days=args.days, seed=args.seed)
        late = followups(events, seed=args.seed)
        store = Store(args.db)
        try:
            stats = ingest(store, events, source="seed")
            # Second delivery: corrections and one back-filled event, so the
            # demo log exercises revisions and a repeated-pass rebuild.
            after = ingest(store, late, source="seed-followup")
        finally:
            store.close()
        if args.out:
            with open(args.out, "w", encoding="utf-8") as fh:
                for ev in events + late:
                    fh.write(json.dumps(ev, sort_keys=True) + "\n")
        merged = {
            "inserted": stats["inserted"] + after["inserted"],
            "corrected": stats["corrected"] + after["corrected"],
            "unchanged": stats["unchanged"] + after["unchanged"],
            "users": len(set(stats.get("affected_users", [])) | set(after.get("affected_users", []))),
            "events": len(events) + len(late),
            "input_digest": after["input_digest"],
        }
        print(json.dumps(merged, indent=2))
        return 0

    if args.command == "rebuild":
        app = App(args.db, interval_events=args.interval_events)
        rules = app.rules.get(args.funnel, args.version)
        row = (
            app.store.get_rules_version(args.funnel, rules.version)
            if args.version is not None
            else app.store.latest_rules_row(args.funnel)
        )
        job = app.rebuilds.start(args.funnel, rules, {"sha256": row["sha256"]})
        job = app.rebuilds.wait(job.job_id, timeout=600)
        print(json.dumps(job.to_dict(), indent=2))
        app.store.close()
        return 0 if job.status == "complete" else 1

    return 2


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except PointwiseError as exc:
        print(json.dumps(exc.as_dict(), indent=2), file=sys.stderr)
        raise SystemExit(1)
