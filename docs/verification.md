# Verification record

Local verification performed 2026-09-27 UTC on macOS/arm64 with real Docker
containers. This file distinguishes executed checks from deployment plans.

## Executed commands

- `make install`: editable source installation with exact pinned dependencies.
- `make check`: Ruff formatting and lint checks passed.
- `make test`: 16 passed; 10 integration tests explicitly skipped without opt-in.
- `make integration`: **26 passed**, including 10 real Redis/PostgreSQL integration
  tests. No SQLite or fake Redis substitution. One upstream Starlette test-client
  deprecation warning remains visible.
- `docker compose up --build -d`: built images, ran migration, started all services.
- `docker compose up --build --wait --wait-timeout 180`: dependency conditions
  and health checks completed.
- `docker compose exec -T api python -m scripts.smoke`: readiness, HTTP acceptance,
  worker persistence, full-text search, and duplicate UUID checks passed.
- Three 2,000-event load runs: 6,000 accepted and observed committed; zero request
  failures. Full evidence and measurement caveats: [benchmark.md](benchmark.md).

Integration tests apply migrations twice, use a temporary database and isolated
Redis keys, and remove them afterward. Test source is in `tests/`. Runtime
configuration was the checked-in example, with private test namespaces and
shorter deterministic failure settings inside fixtures.

## Dependency outage / process crash exercise

Executed against only this project's local Compose stack:

1. Stop PostgreSQL. Assert ingestion remains `202`, `/health` remains `200`, and
   `/ready` returns `503`. Confirm an event is pending in Redis.
2. Send SIGKILL to the worker, restart it and PostgreSQL, and wait for idle-claim
   recovery. Confirm the original UUID becomes searchable.
3. Stop Redis. Assert ingestion returns `503`, `/health` remains `200`, and
   `/ready` returns `503`.
4. Restart backing services and confirm readiness recovers.

Execution output is preserved in [failure-drill.json](failure-drill.json).
This is a targeted local outage test; it does not establish host-crash durability,
replication/failover behavior, or availability guarantees.

## Publication and deployment

Pre-publication review examined all 40 tracked files (including decompressed
synthetic benchmark samples): no credential/private-path patterns were found,
relative documentation links resolved, the upstream MIT notice was present,
and `.env`, virtual environments, caches, build outputs, and ad-hoc artifacts
were excluded. This was a targeted review, not a full security audit.

The workflow runs lint/format and integration tests, plus an independent Compose
build/start/smoke job. Publication and the remote Actions result are recorded after
the initial push. Repository: https://github.com/Fahao1/cloud-log-ingestion-incident-detection

No cloud provider account was used and no cloud deployment is claimed. See the
README deployment procedure for the remaining infrastructure, TLS, secret-store,
monitoring, backup, and retention work.
