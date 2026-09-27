# Measured local burst performance

Measured on **2026-09-27 UTC** (September 26 local time), with the actual Compose
API, worker, PostgreSQL, and Redis processes. No performance values below are
estimates. This is a short closed-loop workload, not a sustained capacity test.

## Environment and workload

- Host: Apple M4 Pro, 12 CPU cores, 24 GiB RAM, macOS 15.8, arm64.
- Containers: Colima 0.10.3 / VZ, configured with 4 vCPUs and 6 GiB RAM;
  Docker engine 29.5.2, Compose 5.5.1. Shared laptop, not an isolated benchmark host.
- Runtime: one Uvicorn API process and **one worker**; container Python 3.13.15,
  PostgreSQL 17.11, Redis 7.2.16 with AOF `everysec`, PostgreSQL default commit durability.
- Load client/observer: Python 3.13.7 on the host; same pinned Python dependencies.
- Each run: **2,000 distinct events**, **16 concurrent HTTP clients**, no pacing
  or application retries; 25% ERROR / 75% INFO. Message size 512 ASCII bytes;
  complete compact JSON payloads **765–766 bytes** (severity length differs).
- One request per event. Every event has a generated UUID, timestamp, unique
  per-run service, and synthetic metadata. No real application data is used.
- The service was already running after smoke tests. Runs 2/3 reused the same
  database without deleting previous runs; queries isolate each run by service.
  No dedicated warmup phase or background generator was used. The detector
  remained enabled at its default settings.

## All recorded runs

- **Run 1**, 05:21:14 UTC: 2,000 accepted / 2,000 observed committed, zero failed
  requests. Submission **3.493 s**, total including final observation **3.507 s**.
  Acceptance **572.6 events/s**; persistence **570.3 events/s**.
  Acceptance-to-visibility median **17.74 ms**, p95 **43.06 ms**.
  Actual polling-gap median **29.42 ms**, p95 **51.14 ms**.
- **Run 2**, 05:22:20 UTC: 2,000 accepted / 2,000 observed committed, zero failed
  requests. Submission **3.519 s**, total **3.532 s**.
  Acceptance **568.3 events/s**; persistence **566.3 events/s**.
  Acceptance-to-visibility median **16.68 ms**, p95 **41.29 ms**.
  Actual polling-gap median **29.08 ms**, p95 **52.44 ms**.
- **Run 3**, 05:22:24 UTC: 2,000 accepted / 2,000 observed committed, zero failed
  requests. Submission **3.515 s**, total **3.525 s**.
  Acceptance **569.1 events/s**; persistence **567.4 events/s**.
  Acceptance-to-visibility median **17.12 ms**, p95 **40.78 ms**.
  Actual polling-gap median **29.82 ms**, p95 **48.77 ms**.

Across the three bursts, all **6,000** requested events were accepted and
observed committed. Per-run persistence throughput ranged **566.3–570.3 events/s**.
These statements do not imply exactly-once queue delivery or survival of host failure.

## Measurement method and limits

Each event's `accepted_at` comes from Redis `TIME` inside the enqueue script,
immediately before `XADD`. A separate PostgreSQL connection polls committed rows
and records `clock_timestamp()` on their first visible query result. Both servers
share the same VM clock. Host clock differences do not enter the latency subtraction.

The measurement is an **upper bound on acceptance-to-database-commit latency**:
an event may commit between polls and remain unobserved until the next query.
The requested sleep is 10 ms, but actual query/scheduling gaps were 29–30 ms at
the median. The observer scans each run's rows on every poll, so observation work
affects both measured throughput and latency. Do not subtract an assumed polling
overhead or present these as exact commit timestamps. `stored_at` is assigned
before commit and deliberately is not used as the persistence measurement.

Acceptance events/s divides successful 202 responses by elapsed submission time.
Persistence events/s divides observed committed rows by elapsed time through the
final observation. Durations use the client monotonic clock. Median uses
`statistics.median`; p95 uses nearest rank (`ceil(0.95 * N)`). Failures, missing
rows, or negative latencies cause the script to exit nonzero after saving evidence.

Unverified: sustained maximum throughput, saturation point, high-cardinality
service workloads, larger metadata, multiple workers under load, network-separated
datastores, production tail latency, failover durability, and long-term storage growth.
Next useful measurement: a paced 15-minute soak with concurrent search and a larger
dataset, then compare one versus two workers. Do not extrapolate these bursts.

## Reproduction

From the repository root, with no other generator running:

```sh
cp .env.example .env  # on a fresh checkout; preserve an existing configuration
make install
docker compose up --build -d --scale worker=1
.venv/bin/python -m scripts.smoke
.venv/bin/python -m scripts.load_test \
  --events 2000 --concurrency 16 --message-bytes 512 --workers 1 \
  --environment 'Apple M4 Pro, 12 host CPUs, 24 GiB RAM, macOS 15.8; Colima VZ arm64 4 vCPUs/6 GiB; one API process and one worker; PostgreSQL 17.11, Redis 7.2.16 AOF everysec' \
  --output artifacts/load-2000.json
```

The recorded repeats used the same command with output paths
`artifacts/load-2000-repeat2.json` and `artifacts/load-2000-repeat3.json`.
Record your own environment string; do not reuse the hardware claim on another machine.
Container tags can advance patch versions, so record your versions as well.

Evidence is intentionally checked in separately from ignored ad-hoc artifacts:

- [summary.json](benchmarks/summary.json): unrounded values, source SHA-256 hashes,
  run IDs, timestamps, and environment for all three runs.
- [run-1.json.gz](benchmarks/run-1.json.gz), [run-2.json.gz](benchmarks/run-2.json.gz),
  [run-3.json.gz](benchmarks/run-3.json.gz): complete original outputs with every
  synthetic event ID, acceptance timestamp, observation timestamp, and latency.

Inspect/recalculate without third-party analysis tools:

```sh
.venv/bin/python - <<'PY'
import gzip, json, math, statistics
from pathlib import Path
for path in sorted(Path('docs/benchmarks').glob('run-*.json.gz')):
    result = json.loads(gzip.decompress(path.read_bytes()))
    values = sorted(row['latency_ms'] for row in result['samples'])
    print(path.name, len(values), statistics.median(values), values[math.ceil(.95 * len(values)) - 1])
PY
```
