from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta

import pytest
import redis

from app.db import persist
from app.detector import evaluate
from app.models import Event
from app.worker import Worker
from tests.test_api import event

pytestmark = pytest.mark.integration


def age_pending(s, queue):
    for entry in queue.r.xpending_range(s.stream, s.group, "-", "+", 100):
        queue.r.xclaim(
            s.stream,
            s.group,
            entry["consumer"],
            0,
            [entry["message_id"]],
            idle=s.reclaim_idle_ms + 1,
            justid=True,
        )


def fail_database(pool):
    with pool.connection() as conn:
        conn.execute("""CREATE FUNCTION fail_insert() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN RAISE EXCEPTION 'injected database failure' USING ERRCODE = '40001'; END $$""")
        conn.execute("""CREATE TRIGGER test_failure BEFORE INSERT ON events
            FOR EACH ROW EXECUTE FUNCTION fail_insert()""")


def test_ingestion_search_pagination_and_metrics(stack):
    s, queue, pool, worker, client = stack
    timestamp = datetime.now(UTC).isoformat()
    ids = []
    for index in range(5):
        payload = event(timestamp=timestamp, severity="ERROR" if index < 4 else "INFO")
        ids.append(payload["event_id"])
        assert client.post("/events", json=payload).status_code == 202
    assert queue.r.xlen(s.stream) == 5
    for _ in ids:
        worker.step(block_ms=1)
    params = {"service": "checkout", "severity": "ERROR", "q": '"provider timeout"', "limit": 2}
    first = client.get("/events", params=params).json()
    second = client.get("/events", params={**params, "cursor": first["next_cursor"]}).json()
    found = first["items"] + second["items"]
    assert len({row["event_id"] for row in found}) == 4
    assert second["next_cursor"] is None
    assert client.get("/events", params={"q": "' OR 1=1 --"}).status_code == 200
    assert not client.get("/events", params={"service": "absent"}).json()["items"]
    assert not client.get("/events", params={"end": "2000-01-01T00:00:00Z"}).json()["items"]
    assert len(client.get("/events", params={"start": "2000-01-01T00:00:00Z"}).json()["items"]) == 5
    assert client.get("/ready").json() == {"redis": True, "postgres": True}
    metrics = client.get("/metrics").text
    for line in (
        "accepted_events_total 5",
        "processed_messages_total 5",
        "queue_backlog 0",
        "processing_delay_seconds_count 5",
    ):
        assert line in metrics


def test_duplicate_concurrent_delivery_first_write_wins(stack):
    s, queue, pool, worker, client = stack
    payload = event()
    for _ in range(2):
        client.post("/events", json=payload)
    other = Worker(s, queue, pool, "worker-two")
    with ThreadPoolExecutor(2) as executor:
        futures = [executor.submit(w.step, 1) for w in (worker, other)]
        assert [f.result() for f in futures] == [1, 1]
    client.post("/events", json={**payload, "message": "replacement is ignored"})
    worker.step(1)
    results = client.get("/events").json()["items"]
    assert len(results) == 1 and results[0]["message"] == payload["message"]
    assert queue.r.xpending(s.stream, s.group)["pending"] == 0


def test_crash_before_commit_and_stale_owner_fenced(stack):
    s, queue, pool, worker, client = stack
    client.post("/events", json=event())
    original = queue.r.xreadgroup(s.group, "dead-worker", {s.stream: ">"}, count=1)[0][1][0]
    assert not client.get("/events").json()["items"]
    age_pending(s, queue)
    claimed = queue.r.xautoclaim(
        s.stream, s.group, worker.consumer, s.reclaim_idle_ms, "0-0", count=1
    )[1]
    assert queue.finish("dead-worker", *original, "processed") == 0
    worker.process(*claimed[0])
    assert len(client.get("/events").json()["items"]) == 1


def test_commit_then_ack_loss_recovers_without_duplicate(stack, monkeypatch):
    s, queue, pool, worker, client = stack
    client.post("/events", json=event())
    finish = queue.finish

    def disconnect(*args):
        raise redis.ConnectionError("simulated crash after commit")

    monkeypatch.setattr(queue, "finish", disconnect)
    with pytest.raises(redis.ConnectionError):
        worker.step(1)
    assert len(client.get("/events").json()["items"]) == 1
    assert queue.r.xpending(s.stream, s.group)["pending"] == 1
    monkeypatch.setattr(queue, "finish", finish)
    age_pending(s, queue)
    Worker(s, queue, pool, "replacement").step(1)
    assert len(client.get("/events").json()["items"]) == 1
    assert queue.r.xlen(s.stream) == 0
    assert queue.r.hget(s.metrics_key, "duplicate_deliveries_total") == "1"


def test_transient_database_failure_remains_pending_then_recovers(stack):
    s, queue, pool, worker, client = stack
    fail_database(pool)
    client.post("/events", json=event())
    worker.step(1)
    assert queue.r.xpending(s.stream, s.group)["pending"] == 1
    assert not client.get("/events").json()["items"]
    with pool.connection() as conn:
        conn.execute("DROP TRIGGER test_failure ON events")
    age_pending(s, queue)
    worker.step(1)
    assert len(client.get("/events").json()["items"]) == 1
    assert queue.r.hget(s.metrics_key, "retries_total") == "1"


def test_bounded_database_failures_and_poison_payload_go_to_dlq(stack):
    s, queue, pool, worker, client = stack
    fail_database(pool)
    response = client.post("/events", json=event())
    worker.step(1)
    age_pending(s, queue)
    worker.step(1)
    dead = queue.r.xrange(s.dead_stream)
    assert len(dead) == 1
    assert dead[0][1]["source_id"] == response.json()["queue_id"]
    assert dead[0][1]["attempts"] == "2"
    assert "40001" in dead[0][1]["reason"]
    assert queue.r.xlen(s.stream) == 0
    queue.r.xadd(s.stream, {"data": "{bad", "accepted_at": "not-a-time"})
    worker.step(1)
    assert queue.r.xlen(s.dead_stream) == 2
    assert queue.r.xpending(s.stream, s.group)["pending"] == 0


def test_repeated_worker_crashes_are_bounded(stack):
    s, queue, pool, worker, client = stack
    client.post("/events", json=event())
    queue.r.xreadgroup(s.group, "dead-one", {s.stream: ">"}, count=1)
    age_pending(s, queue)
    queue.r.xautoclaim(s.stream, s.group, "dead-two", s.reclaim_idle_ms, "0-0", count=1)
    age_pending(s, queue)
    worker.step(1)
    dead = queue.r.xrange(s.dead_stream)
    assert dead[0][1]["reason"] == "delivery limit after worker loss"
    assert not client.get("/events").json()["items"]


def test_queue_capacity_does_not_trim_pending_messages(stack):
    s, queue, pool, worker, client = stack
    queue.settings = s.model_copy(update={"queue_capacity": 1})
    assert queue.publish(Event(**event()))
    message = queue.r.xreadgroup(s.group, "slow", {s.stream: ">"}, count=1)[0][1][0]
    assert queue.publish(Event(**event())) == []
    assert queue.r.xrange(s.stream)[0] == message


def test_non_utf8_payload_is_quarantined_and_next_event_progresses(stack):
    s, queue, pool, worker, client = stack
    source_id = queue.r.xadd(s.stream, {"data": b"\xff", "accepted_at": "1"})
    assert client.post("/events", json=event()).status_code == 202
    assert worker.step(1) == 1
    assert worker.step(1) == 1
    raw = redis.Redis.from_url(s.redis_url)
    try:
        dead = raw.xrange(s.dead_stream)[0][1]
        assert dead[b"data"] == b"\xff"
        assert dead[b"source_id"].decode() == source_id
        assert dead[b"reason"] == b"invalid queue payload"
    finally:
        raw.close()
    assert queue.r.xpending(s.stream, s.group)["pending"] == 0
    assert len(client.get("/events").json()["items"]) == 1


def test_memory_pressure_rejects_ingestion_but_allows_recovery(stack):
    # Only run against disposable test services: maxmemory is server-wide.
    s, queue, pool, worker, client = stack
    client.post("/events", json=event())
    original = queue.r.config_get("maxmemory")["maxmemory"]
    fail_database(pool)
    try:
        queue.r.config_set("maxmemory", 1)
        assert client.post("/events", json=event()).status_code == 503
        queue.ensure_group()
        assert worker.step(1) == 1  # A DB failure still records its reason under Redis OOM.
        assert queue.r.hget(s.metrics_key, "database_failures_total") == "1"
        age_pending(s, queue)
        assert worker.step(1) == 1
        assert queue.r.xlen(s.dead_stream) == 1
        assert queue.r.xlen(s.stream) == 0
        assert queue.r.hget(s.metrics_key, "retries_total") == "1"
    finally:
        queue.r.config_set("maxmemory", original)
    with pool.connection() as conn:
        conn.execute("DROP TRIGGER test_failure ON events")
    client.post("/events", json=event())
    try:
        queue.r.config_set("maxmemory", 1)
        assert worker.step(1) == 1  # Commit and successful finalization can drain the queue.
        assert queue.r.xlen(s.stream) == 0
        assert queue.r.xpending(s.stream, s.group)["pending"] == 0
        assert len(client.get("/events").json()["items"]) == 1
    finally:
        queue.r.config_set("maxmemory", original)


def test_utc_boundaries_round_trip_and_old_client_timestamps_count(stack):
    s, queue, pool, worker, client = stack
    for timestamp in (
        "0001-01-01T00:00:00Z",
        "9999-12-31T23:59:59.999999Z",
        "2000-01-01T00:00:00Z",
        "2000-01-01T00:00:00Z",
    ):
        assert client.post("/events", json=event(timestamp=timestamp)).status_code == 202
        worker.step(1)
    page = client.get("/events", params={"limit": 1}).json()
    assert page["items"][0]["timestamp"].startswith("9999-")
    assert len(client.get("/events", params={"cursor": page["next_cursor"]}).json()["items"]) == 3
    incident = evaluate(pool, s.model_copy(update={"alert_sustain_seconds": 0}))[0]
    assert incident["total_events"] == incident["error_events"] == 4


def prepare_evaluation(pool, age=3):
    with pool.connection() as conn:
        conn.execute(
            "UPDATE detector_state SET evaluated_at = clock_timestamp() - interval '1.1 seconds'"
        )
        conn.execute(
            "UPDATE alert_state SET breach_since = clock_timestamp() - make_interval(secs => %s)",
            (age,),
        )


def test_sustained_incident_cooldown_recovery_and_service_isolation(stack):
    s, queue, pool, worker, client = stack
    now = datetime.now(UTC)
    for severity in ("ERROR", "CRITICAL", "INFO", "INFO"):
        persist(pool, Event(**event(severity=severity)), now)
    persist(pool, Event(**event(service="healthy", severity="INFO")), now)
    # Old receipt times cannot trigger an incident, regardless of client timestamp.
    for _ in range(4):
        persist(pool, Event(**event(service="old-errors")), now - timedelta(hours=1))
    assert evaluate(pool, s) == []  # first breach starts timer
    prepare_evaluation(pool)
    with ThreadPoolExecutor(2) as executor:
        outcomes = list(executor.map(lambda _: evaluate(pool, s), range(2)))
    assert sum(map(len, outcomes)) == 1
    incidents = client.get("/incidents", params={"service": "checkout"}).json()["items"]
    assert len(incidents) == 1 and incidents[0]["error_ratio"] == 0.5
    assert incidents[0]["rule"]["sustain_seconds"] == 2
    prepare_evaluation(pool)
    assert evaluate(pool, s) == []  # cooldown persists across evaluate calls/workers
    with pool.connection() as conn:
        conn.execute(
            "UPDATE alert_state SET last_alert_at = clock_timestamp() - interval '301 seconds'"
        )
    prepare_evaluation(pool)
    assert len(evaluate(pool, s)) == 1
    first = client.get("/incidents?limit=1").json()
    second = client.get("/incidents", params={"before_id": first["next_before_id"]}).json()
    assert len(second["items"]) == 1
    with pool.connection() as conn:
        conn.execute("DELETE FROM events WHERE service = 'checkout'")
    prepare_evaluation(pool)
    evaluate(pool, s)
    with pool.connection() as conn:
        assert (
            conn.execute(
                "SELECT breach_since FROM alert_state WHERE service='checkout'"
            ).fetchone()["breach_since"]
            is None
        )


def test_detector_gap_restarts_sustain_timer(stack):
    s, queue, pool, worker, client = stack
    for _ in range(4):
        persist(pool, Event(**event()), datetime.now(UTC))
    evaluate(pool, s)
    prepare_evaluation(pool, age=30)
    with pool.connection() as conn:
        conn.execute(
            "UPDATE detector_state SET evaluated_at = clock_timestamp() - interval '10 seconds'"
        )
    assert evaluate(pool, s) == []
