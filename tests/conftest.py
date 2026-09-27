import os
from uuid import uuid4

import psycopg
import pytest
from fastapi.testclient import TestClient
from psycopg import sql
from psycopg.conninfo import make_conninfo

from app.config import Settings
from app.db import create_pool
from app.main import create_app
from app.migrate import migrate
from app.queue import Queue
from app.worker import Worker


@pytest.fixture(scope="session")
def database_url():
    if os.getenv("RUN_INTEGRATION") != "1":
        pytest.skip("set RUN_INTEGRATION=1 to use real PostgreSQL and Redis")
    base = Settings().database_url
    name = "logtest_" + uuid4().hex
    with psycopg.connect(base, autocommit=True) as conn:
        conn.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
    url = make_conninfo(base, dbname=name)
    migrate(url)
    migrate(url)  # A second migration run is a no-op.
    yield url
    with psycopg.connect(base, autocommit=True) as conn:
        conn.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(name)))


@pytest.fixture
def stack(database_url):
    settings = Settings(
        database_url=database_url,
        stream="test:" + uuid4().hex,
        api_key="",
        db_timeout_ms=500,
        reclaim_idle_ms=4000,
        max_attempts=2,
        alert_check_seconds=1,
        alert_min_events=4,
        alert_min_errors=2,
        alert_error_ratio=0.5,
        alert_sustain_seconds=2,
    )
    queue, pool = Queue(settings), create_pool(settings)
    queue.ensure_group()
    with pool.connection() as conn:
        conn.execute("TRUNCATE events, incidents, alert_state RESTART IDENTITY")
        conn.execute("UPDATE detector_state SET evaluated_at = 'epoch'")
    try:
        with TestClient(create_app(settings)) as client:
            yield settings, queue, pool, Worker(settings, queue, pool, "worker-one"), client
    finally:
        with pool.connection() as conn:
            conn.execute("DROP TRIGGER IF EXISTS test_failure ON events")
            conn.execute("DROP FUNCTION IF EXISTS fail_insert()")
        pool.close()
        for key in queue.r.scan_iter(match=f"{settings.stream}*"):
            queue.r.delete(key)
        queue.r.close()
