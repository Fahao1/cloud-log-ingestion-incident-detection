import base64
import binascii
import hmac
import json
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from typing import Annotated
from uuid import UUID

import psycopg
import redis
from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse, Response
from psycopg_pool import PoolTimeout
from pydantic import TypeAdapter

from app.config import Settings
from app.db import create_pool, search
from app.models import Event, SafeText, Severity, Timestamp
from app.queue import Queue


class BodyLimit:
    """Bound actual received bytes, including chunked requests without Content-Length."""

    def __init__(self, app, limit):
        self.app, self.limit = app, limit

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            return await self.app(scope, receive, send)
        body = bytearray()
        while True:
            message = await receive()
            if message["type"] == "http.disconnect":
                return
            body.extend(message.get("body", b""))
            if len(body) > self.limit:
                return await JSONResponse({"detail": "request body too large"}, 413)(
                    scope, receive, send
                )
            if not message.get("more_body", False):
                break
        delivered = False

        async def replay():
            nonlocal delivered
            if not delivered:
                delivered = True
                return {"type": "http.request", "body": bytes(body), "more_body": False}
            return await receive()

        await self.app(scope, replay, send)


def encode_cursor(row):
    return base64.urlsafe_b64encode(
        json.dumps([row["timestamp"].isoformat(), str(row["event_id"])]).encode()
    ).decode()


def decode_cursor(value):
    try:
        return TypeAdapter(tuple[Timestamp, UUID]).validate_json(
            base64.b64decode(value, altchars=b"-_", validate=True)
        )
    except (ValueError, TypeError, binascii.Error) as exc:
        raise HTTPException(422, "invalid cursor") from exc


def create_app(settings=None):
    s = settings or Settings()

    @asynccontextmanager
    async def lifespan(app):
        app.state.queue = Queue(s)
        app.state.pool = create_pool(s)
        yield
        app.state.queue.r.close()
        app.state.pool.close()

    app = FastAPI(
        title="Cloud Log Ingestion and Incident Detection", version="0.1.0", lifespan=lifespan
    )
    app.add_middleware(BodyLimit, limit=s.max_request_bytes)

    def authorize(x_api_key: Annotated[str | None, Header()] = None):
        if s.api_key and not hmac.compare_digest((x_api_key or "").encode(), s.api_key.encode()):
            raise HTTPException(401, "invalid or missing X-API-Key")

    protected = [Depends(authorize)]

    @app.exception_handler(RequestValidationError)
    async def invalid_request(request, exc):
        # Do not echo potentially sensitive event bodies (or non-JSON values) in errors.
        return JSONResponse(
            {
                "detail": [
                    {key: error[key] for key in ("loc", "msg", "type")} for error in exc.errors()
                ]
            },
            422,
        )

    @app.exception_handler(redis.RedisError)
    async def redis_unavailable(request, exc):
        return JSONResponse(
            {"detail": "queue unavailable; retry with the same event_id"},
            503,
            headers={"Retry-After": "1"},
        )

    async def database_unavailable(request, exc):
        return JSONResponse({"detail": "database unavailable"}, 503, headers={"Retry-After": "1"})

    app.add_exception_handler(psycopg.Error, database_unavailable)
    app.add_exception_handler(PoolTimeout, database_unavailable)

    @app.get("/health", tags=["operations"])
    def health():
        return {"status": "alive"}

    @app.get("/ready", tags=["operations"])
    def ready(request: Request):
        checks = {"redis": False, "postgres": False}
        try:
            checks["redis"] = bool(request.app.state.queue.r.ping())
        except redis.RedisError:
            pass
        try:
            with request.app.state.pool.connection() as conn:
                checks["postgres"] = bool(
                    conn.execute(
                        "SELECT 1 FROM schema_migrations WHERE version='001_initial.sql'"
                    ).fetchone()
                )
        except (psycopg.Error, PoolTimeout):
            pass
        return JSONResponse(checks, 200 if all(checks.values()) else 503)

    @app.post("/events", status_code=202, dependencies=protected, tags=["events"])
    def ingest(event: Event, request: Request):
        result = request.app.state.queue.publish(event)
        if not result:
            raise HTTPException(
                503, "queue capacity reached; retry later", headers={"Retry-After": "1"}
            )
        message_id, accepted = result
        return {
            "status": "queued",
            "event_id": str(event.event_id),
            "queue_id": message_id,
            "accepted_at": datetime.fromtimestamp(float(accepted), UTC).isoformat(),
        }

    @app.get("/events", dependencies=protected, tags=["events"])
    def events(
        request: Request,
        service: Annotated[SafeText | None, Query(max_length=100)] = None,
        severity: Severity | None = None,
        start: Timestamp | None = None,
        end: Timestamp | None = None,
        q: Annotated[SafeText | None, Query(min_length=1, max_length=200)] = None,
        cursor: Annotated[str | None, Query(max_length=300)] = None,
        limit: Annotated[int, Query(ge=1, le=200)] = 50,
    ):
        if start and end and start >= end:
            raise HTTPException(422, "start must be before end")
        before = decode_cursor(cursor) if cursor else None
        rows = search(request.app.state.pool, service, severity, start, end, q, before, limit)
        return {
            "items": rows[:limit],
            "next_cursor": encode_cursor(rows[limit - 1]) if len(rows) > limit else None,
        }

    @app.get("/incidents", dependencies=protected, tags=["incidents"])
    def incidents(
        request: Request,
        service: Annotated[SafeText | None, Query(max_length=100)] = None,
        before_id: Annotated[int | None, Query(ge=1)] = None,
        limit: Annotated[int, Query(ge=1, le=200)] = 50,
    ):
        clauses, args = [], []
        if service is not None:
            clauses.append("service = %s")
            args.append(service)
        if before_id is not None:
            clauses.append("id < %s")
            args.append(before_id)
        where = " AND ".join(clauses) or "TRUE"
        with request.app.state.pool.connection() as conn:
            rows = conn.execute(
                f"SELECT * FROM incidents WHERE {where} ORDER BY id DESC LIMIT %s",
                [*args, limit + 1],
            ).fetchall()
        return {
            "items": rows[:limit],
            "next_before_id": rows[limit - 1]["id"] if len(rows) > limit else None,
        }

    @app.get("/metrics", dependencies=protected, tags=["operations"])
    def metrics(request: Request):
        r = request.app.state.queue.r
        counts = r.hgetall(s.metrics_key)
        lines = []
        for name in (
            "accepted_events_total",
            "processed_messages_total",
            "duplicate_deliveries_total",
            "retries_total",
            "dead_lettered_events_total",
            "database_failures_total",
        ):
            lines.extend(
                [f"# TYPE logservice_{name} counter", f"logservice_{name} {counts.get(name, 0)}"]
            )
        groups = r.xinfo_groups(s.stream) if r.exists(s.stream) else []
        pending = next((g["pending"] for g in groups if g["name"] == s.group), 0)
        for name, value in {
            "queue_backlog": r.xlen(s.stream),
            "pending_messages": pending,
            "dead_letter_backlog": r.xlen(s.dead_stream),
        }.items():
            lines.extend([f"# TYPE logservice_{name} gauge", f"logservice_{name} {value}"])
        prefix = "logservice_processing_delay_seconds"
        lines.append(f"# TYPE {prefix} histogram")
        for bound in ("0.01", "0.05", "0.1", "0.5", "1", "5", "30", "60"):
            lines.append(f'{prefix}_bucket{{le="{bound}"}} {counts.get("delay_le_" + bound, 0)}')
        count = counts.get("processing_delay_seconds_count", 0)
        lines.extend(
            [
                f'{prefix}_bucket{{le="+Inf"}} {count}',
                f"{prefix}_count {count}",
                f"{prefix}_sum {counts.get('processing_delay_seconds_sum', 0)}",
            ]
        )
        return Response("\n".join(lines) + "\n", media_type="text/plain; version=0.0.4")

    return app


app = create_app()
