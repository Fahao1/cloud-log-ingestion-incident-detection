import argparse
import asyncio
import json

import httpx
import psycopg
import pytest

from scripts import load_test
from scripts.load_test import percentile


def test_p95_uses_nearest_rank():
    assert percentile(list(range(1, 101)), 0.95) == 95
    assert percentile([12], 0.95) == 12


@pytest.mark.parametrize("fault", ["slow_requests", "observer", "connection"])
def test_failed_benchmark_cancels_work_and_preserves_evidence(tmp_path, monkeypatch, fault):
    requests = []

    class Connection:
        closed = False

        async def execute(self, query, *args):
            if "FROM events" in query and fault == "observer":
                raise psycopg.OperationalError("synthetic observer failure")
            return self

        async def fetchone(self):
            return {"server_version": "test"}

        async def fetchall(self):
            return []

        async def close(self):
            self.closed = True

    conn = Connection()

    async def connect(*args, **kwargs):
        if fault == "connection":
            raise psycopg.OperationalError("synthetic connection failure")
        return conn

    async def respond(request):
        requests.append(request)
        await asyncio.sleep(0.2)
        return httpx.Response(202, json={"event_id": json.loads(request.content)["event_id"]})

    client_class = httpx.AsyncClient
    monkeypatch.setattr(load_test.psycopg.AsyncConnection, "connect", connect)
    monkeypatch.setattr(
        load_test.httpx,
        "AsyncClient",
        lambda **kwargs: client_class(**kwargs, transport=httpx.MockTransport(respond)),
    )
    args = argparse.Namespace(
        events=3,
        concurrency=1,
        message_bytes=10,
        workers=1,
        poll_ms=1,
        timeout=0.02,
        url="http://test",
        environment="synthetic regression",
        output=tmp_path / "failed.json",
    )

    async def exercise():
        with pytest.raises(SystemExit):
            await load_test.run(args)
        assert not [task for task in asyncio.all_tasks() if task is not asyncio.current_task()]

    asyncio.run(exercise())
    result = json.loads(args.output.read_text())
    assert result["failures"]
    assert result["accepted"] == result["persisted_observed"] == 0
    assert len(requests) < args.events
    assert result["acceptance_to_visibility_ms_median"] is None
    assert fault == "connection" or conn.closed
