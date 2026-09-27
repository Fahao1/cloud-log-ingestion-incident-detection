# Resume and interview notes

This project was scoped by its owner and implemented/verified with Codex coding
assistance. It adapts small MIT-licensed components; do not claim sole authorship
of those components, a production deployment, production availability, or business
impact. Use these bullets only if you can explain the code and reproduce the checks.

Each bullet follows contribution → method → technical challenge → measured or
verified result:

- Directed development of an AI-assisted log ingestion service using FastAPI,
  Redis Streams, and PostgreSQL, separating HTTP acceptance from database writes
  to handle bursts; all 6,000 synthetic events persisted across three local runs
  at 566–570 events/second on the original implementation (historical local benchmark).
- Specified and delivered an idempotent processing pipeline using UUID primary
  keys, commit-before-acknowledgment, pending-message recovery, and bounded retries,
  addressing duplicate delivery and worker crashes; verified recovery and
  dead-letter behavior against real Redis and PostgreSQL.
- Defined sustained error-rate detection using rolling SQL windows and persisted
  per-service cooldowns, addressing alert duplication across restarts and workers;
  verified concurrent detection, cooldown, and healthy-window reset in integration tests.
- Established a reproducible validation workflow using Docker Compose, pytest,
  Ruff, and GitHub Actions, exercising commit/ack failure boundaries, malformed
  inputs, and memory-pressure recovery; 43 automated tests and a fresh Compose
  smoke test passed locally. The original CI ran 26 tests; see the audit PR's
  checks for the expanded suite. Separate local exercises verified dependency outages.

The first bullet's throughput is local burst throughput, not production capacity.
It applies to the source hashes in the original evidence, not to later audit fixes.
The audit and fixes also used Codex assistance; describe your role as scoping,
reviewing, and reproducing the work to the extent you have personally done those things.
Per-run median acceptance-to-observed-commit latency was 16.7–17.7 ms and p95 was
40.8–43.1 ms, with polling overhead included. Sustained capacity, cloud deployment,
availability, and business outcomes **still need verification**. Avoid translating
these results into unsupported claims such as reduced incident response time.

Interview topics: the two crash windows around commit/ack; first-write-wins UUID
semantics; Redis AOF loss window; why MAXLEN trimming is dangerous for pending
work; keyset pagination; English full-text search limitations; detector locking;
counter semantics; and commit-latency observation error.
