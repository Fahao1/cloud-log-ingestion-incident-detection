import asyncio
import json
from datetime import UTC, datetime
from unittest.mock import Mock
from uuid import uuid4

import pytest
import redis
from fastapi.testclient import TestClient

from app.config import Settings
from app.main import BodyLimit, create_app, decode_cursor


def event(**changes):
    return {
        "event_id": str(uuid4()),
        "timestamp": datetime.now(UTC).isoformat(),
        "service": "checkout",
        "severity": "ERROR",
        "message": "payment provider timeout",
        "metadata": {"synthetic": True},
        **changes,
    }


@pytest.fixture
def client():
    app = create_app(Settings(api_key="", max_request_bytes=16384))
    with TestClient(app) as client:
        app.state.queue.publish = Mock(return_value=["1-0", "1800000000.000000"])
        yield client


@pytest.mark.parametrize(
    "change",
    [
        {"event_id": "bad-id"},
        {"timestamp": "2026-01-01T00:00:00"},
        {"service": ""},
        {"service": "a service"},
        {"severity": "fatal"},
        {"message": " "},
        {"extra": 1},
        {"metadata": []},
        {"message": "bad\x00text"},
        {"metadata": {"nested": ["\x00"]}},
    ],
)
def test_validation(client, change):
    assert client.post("/events", json=event(**change)).status_code == 422
    client.app.state.queue.publish.assert_not_called()


def test_ingestion_response_and_size(client):
    payload = event()
    response = client.post("/events", json=payload)
    assert response.status_code == 202
    assert response.json()["event_id"] == payload["event_id"]
    assert response.json()["status"] == "queued"
    assert client.post("/events", content=b"x" * 16385).status_code == 413
    assert client.post("/events", content=b"{bad").status_code == 422
    for metadata in ({"value": float("inf")}, {"value": "\ud800"}):
        response = client.post(
            "/events",
            content=json.dumps(event(metadata=metadata)),
            headers={"Content-Type": "application/json"},
        )
        assert response.status_code == 422
        assert "input" not in response.json()["detail"][0]


def test_chunked_limit():
    async def run():
        chunks = iter(
            [
                {"type": "http.request", "body": b"12345", "more_body": True},
                {"type": "http.request", "body": b"67890", "more_body": False},
            ]
        )
        messages = []

        async def receive():
            return next(chunks)

        async def send(message):
            messages.append(message)

        async def downstream(*args):
            pytest.fail("oversized request reached application")

        await BodyLimit(downstream, 8)({"type": "http", "headers": []}, receive, send)
        assert messages[0]["status"] == 413

    asyncio.run(run())


def test_queue_unavailable_and_capacity(client):
    client.app.state.queue.publish.side_effect = redis.ConnectionError("offline")
    response = client.post("/events", json=event())
    assert response.status_code == 503 and response.headers["retry-after"] == "1"
    client.app.state.queue.publish.side_effect = None
    client.app.state.queue.publish.return_value = []
    assert client.post("/events", json=event()).status_code == 503


def test_auth_and_liveness():
    with TestClient(create_app(Settings(api_key="unit-test-key"))) as client:
        assert client.get("/health").status_code == 200
        for path in ("/events", "/incidents", "/metrics"):
            assert client.get(path).status_code == 401
        client.app.state.queue.publish = Mock(return_value=["1-0", "1800000000"])
        assert client.post("/events", json=event()).status_code == 401
        assert (
            client.post("/events", json=event(), headers={"X-API-Key": "unit-test-key"}).status_code
            == 202
        )


def test_bad_filters_and_cursor(client):
    for query in ("limit=0", "severity=fatal", "cursor=bad!", "start=2026-01-01T00:00:00"):
        assert client.get("/events?" + query).status_code == 422
    assert (
        client.get(
            "/events", params={"start": "2026-02-01T00:00:00Z", "end": "2026-01-01T00:00:00Z"}
        ).status_code
        == 422
    )
    import base64

    with pytest.raises(Exception) as exc:
        decode_cursor(base64.b64encode(json.dumps(["naive", "no-uuid"]).encode()).decode())
    assert exc.value.status_code == 422
