# Third-party code and reuse decisions

Research performed September 26, 2026, before implementation. Repository metadata,
source, licenses, and HEAD commits were fetched from the GitHub API. Maintenance
dates below are observations, not promises of continued support.

## Adapted code

**[Shreyash021104/log-observability](https://github.com/Shreyash021104/log-observability)**

- Commit: `2d55e0d3a2b64b77393f6797ec7a1e081d5a8386`.
- License: MIT. Copyright (c) 2026 Shreyash Patange.
- Full required notice: [licenses/log-observability-MIT.txt](licenses/log-observability-MIT.txt).
- Last push: 2026-07-28; not archived at inspection. A small recent project,
  not a mature reliability library; no maintenance SLA is assumed.
- Relevant features: FastAPI ingestion, Redis Streams consumer groups,
  PostgreSQL generated full-text vectors and filters, threshold/cooldown alerting.
- Adaptation: upstream
  [`app/bus.py`](https://github.com/Shreyash021104/log-observability/blob/2d55e0d3a2b64b77393f6797ec7a1e081d5a8386/app/bus.py)
  consumer-group creation / BUSYGROUP handling → `app/queue.py:Queue.ensure_group`.
- Adaptation: upstream
  [`app/db.py`](https://github.com/Shreyash021104/log-observability/blob/2d55e0d3a2b64b77393f6797ec7a1e081d5a8386/app/db.py)
  generated English `tsvector`, GIN index, and parameterized full-text/filter query
  pattern → `migrations/001_initial.sql` and `app/db.py:search`.
- Changed: UUID idempotency, cursor pagination, time indexes and schema fields.
  The bounded queue, crash recovery, retry/dead-letter logic, persistent sustained
  incident detector, metrics, validation, and tests were implemented for this project.
- Deliberately not copied: MAXLEN trimming (can destroy pending work), ingestion
  parser (insufficient validation/size bounds for this contract), original worker
  (no bounded recovery), and in-memory cooldown (not safe across restarts/workers).
- Verification: real-service integration tests cover the adapted group creation
  and search alongside deduplication, recovery, capacity protection, and incidents.

## Dependencies considered for reuse

**[redis/redis-py](https://github.com/redis/redis-py)**

- Inspected commit: `ba6976bc2b8d5daed034be982c30194770ed1c15`.
- MIT; last push 2026-09-25, not archived. Actively maintained at inspection.
- Relevant component: maintained `XADD`, `XREADGROUP`, `XAUTOCLAIM`, `XPENDING`,
  Lua execution, and connection handling. Used as an installed dependency,
  pinned to `redis==6.4.0` in `requirements.lock`, not vendored or modified.
- Files adapted: none. Upstream distribution retains its license notices.

**[prometheus/alertmanager](https://github.com/prometheus/alertmanager)**

- Inspected commit: `6a7967995e910f6cf0400295a742a38675d2011d`.
- Apache-2.0; last push 2026-09-26, not archived. Actively maintained at inspection.
- Relevant features: grouped alerts, notification routing, deduplication,
  silences, and receiver integrations.
- Decision: no code adapted. A separate Go service and routing subsystem would
  exceed this project's needs. A PostgreSQL transaction and persisted cooldown
  solve the narrow requirement. Reconsider if multi-channel routing is required.
- Files adapted: none.

## Other references

- [Redis streaming guide](https://redis.io/docs/latest/develop/use-cases/streaming/redis-py/):
  consumer-group delivery and recovery concepts; no code copied.
- [Uptrace](https://github.com/uptrace/uptrace): inspected as a larger ingestion,
  search, and alerting platform. AGPL-3.0, recent active public project; no code
  copied, no commit-level adaptation. Its ClickHouse/Go/UI stack is outside scope.
- [PostgreSQL full-text search](https://www.postgresql.org/docs/17/textsearch.html):
  SQL semantics; no prose or code copied.

## Distribution

This project is MIT-licensed. Adapted code retains the upstream MIT notice in
the repository and Docker image. Python libraries are installed from their
official distributions, with versions pinned in `requirements.lock`; they are
not relicensed by this project. The PostgreSQL, Redis 7.2, and Python container
images retain their upstream licenses. Redis server licensing differs by
version; this project uses the Redis 7.2 line, not an unqualified `latest` tag.
