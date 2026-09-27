# Independent audit and focused corrections

Audit date: 2026-09-27 UTC. Baseline: public commit
[`34ebe850e740253ef0c4e22dd9a4f73242766b05`](https://github.com/Fahao1/cloud-log-ingestion-incident-detection/tree/34ebe850e740253ef0c4e22dd9a4f73242766b05).
The README, previous completion claims, and green CI were treated as hypotheses.
The audit used a fresh public clone, new local Compose volumes, source inspection,
real Redis/PostgreSQL probes, and independent calculation of the raw measurements.
Findings were reported to the owner before changing application code. Corrections
are on `audit/reliability-boundaries`; no merge or cloud deployment is authorized
by this report. This audit does not prove the absence of other bugs.

## Findings, ranked by impact

All locations below refer to the baseline commit; links to regression tests refer
to the corrected branch. Critical: **none verified**. High: two; medium: four;
low: two. All eight were verified; the benchmark observer-exception path was
first identified by inspection, then reproduced before its fix.

### High H1: accepted timestamps can make stored events unreadable

Location: `app/models.py:13`, `app/db.py:28,65`.

Reproduction: POST an otherwise valid event with timestamp
`9999-12-31T23:59:59-01:00` or `0001-01-01T00:00:00+01:00`. Both returned 202.
PostgreSQL stored UTC year 10000 / 1 BC, respectively. Searching those rows
returned 503 while `/ready` remained 200. The future row also poisoned the first
unfiltered search page. Psycopg cannot deserialize these values into Python dates.

Correction: shared timestamp validation normalizes to UTC and rejects overflow
at ingestion, queue validation, query bounds, and cursor decoding. Tests reject
both offset-overflow inputs and round-trip valid UTC years 1 and 9999 through
real persistence and pagination. No deployed migration was rewritten.

Existing databases need an operator check; validation cannot repair preexisting
rows. This read-only SQL returns text safely even for unrepresentable dates:

```sql
SELECT event_id, timestamp::text
FROM events
WHERE timestamp < TIMESTAMPTZ '0001-01-01 00:00:00+00'
   OR timestamp > TIMESTAMPTZ '9999-12-31 23:59:59.999999+00';
```

Review/export any results, then correct or quarantine them under your retention
policy. Do not silently delete historical evidence. Audit-generated invalid rows
were removed only from the disposable audit database after reproduction.

### High H2: memory pressure stalls queue draining

Location: `app/queue.py:22-45,56-62`, `app/worker.py:35,76-77,103`.

Reproduction on disposable Redis: enqueue entries, create a pending delivery,
save `CONFIG GET maxmemory`, set `maxmemory` below current usage, and try group
setup/finalization. `XREADGROUP` could still read; repeated `XGROUP CREATE` and
the finalizer's first counter write raised OOM. Source/pending entries remained,
but workers could not drain them without operator-created headroom. Restore the
original setting in `finally`; this setting affects the entire Redis server.

Correction: existing-group discovery handles OOM during repeated creation.
Consumer finalization and small retry/failure writes use native Lua `allow-oom`;
enqueue does not. The real-service regression rejects new ingestion while
exercising DB failure recording, reclaim, bounded DLQ movement, and successful
commit/ack/deletion under the same memory pressure. No eviction is enabled.

Limit: these consumer writes can exceed the logical memory target. Physical OOM,
disk exhaustion, an absent group that cannot be created, and unbounded retained
DLQ history still need operational headroom/retention. This fix is not a hard
RAM bound or a promise of durability during a host failure. Native behavior:
[Redis Lua flags](https://redis.io/docs/latest/develop/programmability/lua-api/).

### Medium M1: malformed stream bytes bypass quarantine

Location: `app/queue.py:52-54`, `app/worker.py:26-38,114`.

Reproduction: directly `XADD` a message with `data=b'\xff'` into an isolated
stream, then consume and reclaim it. Strict client decoding raised
`UnicodeDecodeError` before payload validation or the attempt-limit check on
every delivery. This requires a malformed queue writer; normal HTTP validation
does not produce it. It contradicts the stated queue trust-boundary behavior.

Correction: Redis decoding uses `surrogateescape`, allowing validation to reject
the payload while preserving its original bytes in the DLQ. A real-service test
verifies byte-for-byte DLQ data, safe reason, empty pending state, and progress
of a valid message immediately afterward. DLQ tools must support binary data.

### Medium M2: malformed query inputs return 5xx

Location: `app/main.py:63-68,159-164,183`.

Reproduction: base64-encode `["2026-01-01T00:00:00Z",123]` as a cursor. The UUID
constructor raised uncaught `AttributeError` (also for list/object/boolean IDs).
Search with `q=bad%00text` or `service=bad%00service`; psycopg rejected the NUL and
the API incorrectly returned 503. The incident service filter had the same gap.

Correction: Pydantic validates the entire cursor tuple; shared text validation
rejects NUL before database access. Regression cases cover all reproduced cursor
types, both service endpoints, text search, and timestamp-bound overflows.

### Medium M3: search terms leak into default access logs

Location: `Dockerfile:16`, README's direct Uvicorn command.

Reproduction: GET `/events?q=synthetic-sensitive-marker-audit`, then inspect API
container logs. The complete marker appeared in the request URL. Search terms
can contain sensitive event contents even when POST bodies are never logged.

Correction: disable Uvicorn access logging in both documented launch paths. CI
runs the actual Compose smoke search and fails if a `GET /events` request appears
in API logs. Operators must separately configure proxies to omit queries/secrets.
Worker service names/counts and raw event/DLQ contents remain intentionally stored;
no automatic content redaction or production privacy certification is claimed.

### Medium M4: benchmark deadline and error evidence are incomplete

Location: `scripts/load_test.py:60,85-115`.

Reproduction: run three requests through a transport delayed by 120 ms per
request with `--timeout 0.02`. All three were submitted over roughly 366 ms;
only observation stopped at the deadline. A regression with 200 ms responses
also reproduced the problem. Injecting a PostgreSQL observer/connection error
raised before writing an artifact.

Correction: one asyncio deadline covers connection, submission and observation;
TaskGroup cancellation joins outstanding work. Failure class names and partial
counts are written even when setup/observation fails, including zero-poll cases.
Three regression cases verify cancellation, saved evidence, and nonzero exit.
Successful-run throughput timing still begins after the database version query,
as in the original script. A canceled request has ambiguous server acceptance;
partial evidence must never be promoted as a complete benchmark.

### Low L1: historical replay explanation contradicts the actual rule

Location: `README.md:227-229`.

Reproduction: ingest fresh UUIDs with year-2000 client timestamps and run the
detector with a shortened sustain interval. A real database evaluation opened
an incident counting all four events. Receipt time, not client time, drives it.

Correction: explain that old client timestamps with new UUIDs count as current
traffic. Existing-UUID retries keep their original stored receipt. The rule is
unchanged; a regression makes the documented receipt-time behavior explicit.

### Low L2: smoke test overstates deduplication proof

Location: `scripts/smoke.py:28-35`, `docs/verification.md`.

Reproduction by control-flow inspection: after two POSTs, the polling loop exits
as soon as one matching row appears, without waiting for both queue deliveries.
A first successful insertion is enough to print “deduplication,” so that output
does not establish processing of the second delivery.

Correction: narrow the smoke result and historical verification description to
readiness/ingestion/persistence/search. Existing real-service tests do verify
concurrent duplicate delivery and commit-before-ack recovery; those remain the
deduplication evidence. No extra synchronization was added just for smoke output.

## Checks without additional verified findings

- **Transactions and crashes:** traced psycopg transaction context exit before
  acknowledgment, UUID conflict handling, and stale consumer fencing. Existing
  real-service tests cover pre-commit abandonment, simulated disconnect immediately
  after commit, concurrent inserts, retry exhaustion, SQL rollback, and poison
  JSON. These tests establish their specific boundaries, not arbitrary scheduling.
- **Capacity and metrics:** atomic capacity includes unread and pending entries;
  no MAXLEN trimming. Processed metrics count finalized deliveries, including
  duplicates; counters are operational and can diverge under crashes. Queue and
  DLQ gauges, cumulative histogram buckets, and documented clock caveats agree
  with code. Readiness checks reachability/schema, not worker health or writable
  queue headroom; `/ready` can remain 200 under logical Redis OOM.
- **Search:** parameterized SQL, exclusive end/inclusive start, severity/service
  filters, limit bounds, English message-only full-text search, and descending
  `(timestamp,event_id)` keysets agree with the schema and tests. Pagination is
  not a snapshot; broad searches and detector scans still have scaling limits.
- **Incidents:** inspected threshold inclusivity, receipt window, sustained sampled
  breach, missing-evaluation reset, healthy reset, cooldown persistence, and the
  advisory-lock transaction. Existing tests exercise concurrent evaluators and
  cooldown; there is no distributed failover or external-notification guarantee.
- **Migrations/configuration:** migrations run in a transaction under a lock and
  can be repeated; Compose waits for migration success. Development passwords,
  empty API key, local superuser privileges and loopback ports are explicit local
  choices. The image runs non-root. Deployment still needs strong credentials,
  TLS, a restricted database role, private networking, timeouts, retention, backups,
  monitoring, and restore/replay exercises. Image tags are not digest-pinned.
- **Authentication and limits:** exercised missing/wrong/configured key behavior,
  bounded body buffering including chunking, invalid JSON/severity/metadata, Unicode,
  and sanitized validation responses. Health/docs are intentionally public. No
  tenant isolation, API rate limiting, or slow-client defense is implemented;
  the documented deployment proxy must provide its own controls.
- **Repository hygiene:** reviewed tracked source, configs, docs, and decompressed
  synthetic samples. No real credentials or private log payloads were identified.
  Local `.env`, venv, caches, arbitrary benchmark output and build products are
  ignored. This is a targeted review, not an exhaustive secret/dependency scan.
- **Licenses/reuse:** fetched upstream commit
  `2d55e0d3a2b64b77393f6797ec7a1e081d5a8386` from
  [log-observability](https://github.com/Shreyash021104/log-observability/tree/2d55e0d3a2b64b77393f6797ec7a1e081d5a8386).
  Group creation/BUSYGROUP and SQL search adaptations match the declared files.
  `licenses/log-observability-MIT.txt` is byte-identical to upstream's required
  MIT notice. Own MIT license, source attribution and image COPY retain notices.
  Installed redis-py is not vendored. No undeclared copied alerting platform was
  found; maintenance observations in THIRD_PARTY are explicitly historical.

## Executed verification

Baseline clean public checkout: `make install`, `make check`, `make test`
(16 passed / 10 skipped), `cp .env.example .env`,
`docker compose up --build --wait --wait-timeout 180`, `/ready`, container smoke,
and `make integration` (26 passed). The baseline GitHub CI runs were also green.

The new malformed-input, binary-stream, memory-pressure, and benchmark-failure
regressions were run against the original application and failed before fixes.
After correction: `make check`, `make test` (30 passed / 13 skipped), and
`make integration` (**43 passed**, including 13 real-service integration tests).
The only warning is the existing upstream Starlette test-client deprecation.

[Pull request #1](https://github.com/Fahao1/cloud-log-ingestion-incident-detection/pull/1)
contains these corrections and remains unmerged.
[GitHub Actions run 36300086438](https://github.com/Fahao1/cloud-log-ingestion-incident-detection/actions/runs/36300086438)
passed both jobs on implementation commit `49192b474ea4570fc6bf33b191af53ebb0b71ac5`:
formatting/lint, **43 pytest tests**, fresh Compose build/start/migration/smoke,
and the search-access-log privacy check. Subsequent audit documentation updates
use the same workflow; the PR's checks show the latest head result.

The patched Docker build was started with fresh audit-only volumes using the
same README command; migrations, readiness and smoke passed. Original project
volumes and historical benchmark evidence were preserved.

An actual Compose outage drill on the patched image stopped PostgreSQL, verified
202 ingestion / 200 liveness / 503 readiness, observed pending work, killed the
worker with SIGKILL, restarted PostgreSQL and the worker, and confirmed exactly
one row for the original UUID after recovery. Stopping Redis produced 503
ingestion / 200 liveness / 503 readiness. Services recovered afterward. Results:
[audit-failure-drill.json](audit-failure-drill.json). This exercises a real
pre-commit process death; the exact post-commit/pre-ack boundary remains a
deterministic injected disconnect in the integration suite.

The logging regression was also run against the patched image: a synthetic
search marker was absent from API logs, and CI's `GET /events` log check passed.

## Separate post-fix benchmark check

The corrected benchmark also completed against the actual rebuilt API/worker;
its output is [audit-run.json.gz](benchmarks/audit-run.json.gz). This is additional
evidence, not a replacement for any original result. Its source hashes match
the fixed application/benchmark files in this PR.

- Start: 2026-09-27 06:23:00 UTC. Same M4 Pro/12-core/24-GiB macOS 15.8 host,
  Colima VZ 4-vCPU/6-GiB VM; Python 3.13.7 client, container Python 3.13.15,
  PostgreSQL 17.11, Redis 7.2.16 AOF everysec. Shared laptop.
- One API process and one worker; 2,000 distinct events, concurrency 16, 512-byte
  messages, 765–766-byte JSON payloads, 25% ERROR. Default detector enabled.
  Fresh audit database had smoke/outage samples, no original benchmark history.
  One synthetic search used to verify log privacy may overlap the beginning;
  this was not a controlled performance comparison.
- 2,000 accepted and observed committed, zero recorded request failures.
  Submission 3.239552 s; through final observation 3.258937 s.
  Acceptance 617.3693 events/s; persistence **613.6970 events/s**.
  Median **17.068 ms**, nearest-rank p95 **39.737 ms**, using the same Redis receipt
  to first PostgreSQL committed-observation method. Requested polling interval
  10 ms; actual poll gaps median 28.5512 ms / p95 49.1987 ms.

Command (after fresh Compose setup, from repository root):

```sh
.venv/bin/python -m scripts.load_test \
  --events 2000 --concurrency 16 --message-bytes 512 --workers 1 \
  --environment 'Apple M4 Pro, 12 host CPUs, 24 GiB RAM, macOS 15.8; Colima VZ arm64 4 vCPUs/6 GiB; audit branch, one API and one worker; PostgreSQL 17.11, Redis 7.2.16 AOF everysec; shared laptop' \
  --output artifacts/audit-load-2000.json
```

One short run checks script execution and stores fresh evidence. It does not
establish an improvement over the original runs or sustained capacity. The
resume retains the explicitly historical three-run figures.

## Historical benchmark integrity and resume verdict

For each of the three original raw runs, independently recomputed all latency
differences from timestamps, sample counts, unique IDs, median, nearest-rank p95,
and both throughput divisions. All matched the raw aggregates and summary.
All recorded source hashes matched the audited baseline's files. Each run has
2,000 distinct persisted IDs with zero recorded request failures; 6,000 total.
The current host is an M4 Pro with 12 cores/24 GiB; Docker reports 4 CPUs and
approximately 6 GiB, consistent with the recorded configuration. This verifies
current hardware and artifact consistency, not independent attestation of every
condition during the historical runs.

Original compressed-file SHA-256 digests, retained unchanged:

```text
run-1.json.gz  15815fae3289e9d52fcd96383b95cdd5bd8db8df57c2f0f6fd84777479311960
run-2.json.gz  2891a3cab5492f0ec62a417f167cc59f1640c0cbcf00f89100e46e56c26e3101
run-3.json.gz  df6fb0ea71e22110707624812ccdb38fedbfd12ba051e4b5de541b6bc22d6c38
```

The existing **566–570 persisted events/s**, **16.7–17.7 ms median** and
**40.8–43.1 ms p95** are supported as three short historical local bursts.
Latencies include polling overhead and are upper bounds on commit latency.
These are not production numbers or measurements of the subsequent fixes.

Resume wording now identifies that baseline explicitly, separates pytest crash
boundaries from local outage drills, and accurately describes AI-assisted owner
contribution. Keep authorship wording limited to work personally understood and
reproduced. Do not claim production availability, zero data loss, reduced incident
response time, or sole authorship of adapted/assisted components.

Verdict: a useful interview portfolio after reviewing the corrections and learning
the failure tradeoffs; not established as production-ready. Cloud deployment,
host-loss durability, disk-full recovery, failover, long-running saturation,
multiworker performance, production security controls and actual business impact
remain unverified. Explain the known ceilings rather than claiming this audit
proves universal correctness.
