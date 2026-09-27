"""Measure HTTP throughput and acceptance-to-committed-visibility latency.

The observer uses PostgreSQL's clock and sees only committed rows. Polling makes
latencies upper bounds, unlike stored_at (which is set before the insert commits).
"""

import argparse
import asyncio
import hashlib
import json
import math
import os
import platform
import statistics
import time
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import httpx
import psycopg
from psycopg.rows import dict_row

from app.config import Settings


def percentile(values, fraction):
    return sorted(values)[max(0, math.ceil(len(values) * fraction) - 1)]


async def run(args):
    s = Settings()
    run_id = uuid4().hex
    service = "load-" + run_id
    payloads = [
        json.dumps(
            {
                "event_id": str(uuid4()),
                "timestamp": datetime.now(UTC).isoformat(),
                "service": service,
                "severity": "ERROR" if index % 4 == 0 else "INFO",
                "message": "x" * args.message_bytes,
                "metadata": {"synthetic": True, "run_id": run_id},
            },
            separators=(",", ":"),
        ).encode()
        for index in range(args.events)
    ]
    seen, accepted, failures, poll_gaps = {}, set(), [], []
    done = asyncio.Event()
    conn = await psycopg.AsyncConnection.connect(
        s.database_url, autocommit=True, row_factory=dict_row
    )
    db_version = (await (await conn.execute("SHOW server_version")).fetchone())["server_version"]
    started_at = datetime.now(UTC).isoformat()
    started = time.perf_counter()

    async def observe():
        last_poll = time.perf_counter()
        while time.perf_counter() - started < args.timeout:
            # ponytail: scan this run's rows; use batched ID probes for million-event benchmarks.
            cursor = await conn.execute(
                "SELECT event_id, accepted_at, clock_timestamp() AS observed_at "
                "FROM events WHERE service = %s",
                (service,),
            )
            rows = await cursor.fetchall()
            for row in rows:
                event_id = str(row["event_id"])
                if event_id not in seen:
                    delay = (row["observed_at"] - row["accepted_at"]).total_seconds() * 1000
                    seen[event_id] = {
                        "event_id": event_id,
                        "accepted_at": row["accepted_at"].isoformat(),
                        "observed_at": row["observed_at"].isoformat(),
                        "latency_ms": delay,
                    }
            current = time.perf_counter()
            poll_gaps.append((current - last_poll) * 1000)
            last_poll = current
            if done.is_set() and accepted <= seen.keys():
                return
            await asyncio.sleep(args.poll_ms / 1000)

    try:
        observer = asyncio.create_task(observe())
        iterator = iter(payloads)
        async with httpx.AsyncClient(
            base_url=args.url,
            headers={
                "X-API-Key": os.getenv("API_KEY", s.api_key),
                "Content-Type": "application/json",
            },
            limits=httpx.Limits(max_connections=args.concurrency),
            timeout=30,
        ) as client:

            async def send():
                for body in iterator:
                    try:
                        response = await client.post("/events", content=body)
                        response.raise_for_status()
                        if response.status_code != 202:
                            raise ValueError("expected 202")
                        accepted.add(response.json()["event_id"])
                    except (httpx.HTTPError, ValueError) as exc:
                        failures.append(type(exc).__name__)

            await asyncio.gather(*(send() for _ in range(args.concurrency)))
        submission_seconds = time.perf_counter() - started
        done.set()
        await observer
        total_seconds = time.perf_counter() - started
    finally:
        await conn.close()
    samples = [seen[key] for key in sorted(accepted & seen.keys())]
    values = [sample["latency_ms"] for sample in samples]
    result = {
        "run_id": run_id,
        "started_at": started_at,
        "environment": args.environment,
        "client_platform": platform.platform(),
        "client_python": platform.python_version(),
        "postgres_version": db_version,
        "workers": args.workers,
        "events_requested": args.events,
        "accepted": len(accepted),
        "persisted_observed": len(samples),
        "failures": failures,
        "concurrency": args.concurrency,
        "message_bytes": args.message_bytes,
        "payload_bytes_min": min(map(len, payloads)),
        "payload_bytes_max": max(map(len, payloads)),
        "submission_seconds": submission_seconds,
        "total_seconds": total_seconds,
        "accepted_events_per_second": len(accepted) / submission_seconds,
        "persisted_events_per_second": len(samples) / total_seconds,
        "acceptance_to_visibility_ms_median": statistics.median(values) if values else None,
        "acceptance_to_visibility_ms_p95": percentile(values, 0.95) if values else None,
        "poll_interval_ms_requested": args.poll_ms,
        "poll_gap_ms_median": statistics.median(poll_gaps),
        "poll_gap_ms_p95": percentile(poll_gaps, 0.95),
        "measurement": (
            "Redis receipt clock to first PostgreSQL committed-row observation; polling upper bound"
        ),
        "source_sha256": {
            str(path): hashlib.sha256(path.read_bytes()).hexdigest()
            for folder in ("app", "migrations", "scripts")
            for path in sorted(Path(folder).glob("*"))
            if path.is_file()
        },
        "samples": samples,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2) + "\n")
    print(
        json.dumps(
            {
                key: value
                for key, value in result.items()
                if key not in {"samples", "source_sha256"}
            },
            indent=2,
        )
    )
    if failures or len(samples) != args.events or any(value < 0 for value in values):
        raise SystemExit("incomplete or clock-invalid run; inspect result file")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default="http://localhost:8000")
    parser.add_argument("--events", type=int, default=2000)
    parser.add_argument("--concurrency", type=int, default=16)
    parser.add_argument("--message-bytes", type=int, default=512)
    parser.add_argument("--poll-ms", type=float, default=10)
    parser.add_argument("--timeout", type=float, default=120)
    parser.add_argument("--workers", type=int, default=1, help="record actual running worker count")
    parser.add_argument("--environment", default="unspecified; record hardware before sharing")
    parser.add_argument("--output", type=Path, default=Path("artifacts/load.json"))
    args = parser.parse_args()
    if (
        min(
            args.events,
            args.concurrency,
            args.message_bytes,
            args.workers,
            args.poll_ms,
            args.timeout,
        )
        <= 0
    ):
        parser.error("counts and intervals must be positive")
    asyncio.run(run(args))


if __name__ == "__main__":
    main()
