import logging
import math
import signal
import socket
import time
from datetime import UTC, datetime
from uuid import uuid4

import psycopg
import redis
from psycopg_pool import PoolTimeout
from pydantic import ValidationError

from app.config import Settings
from app.db import create_pool, persist
from app.detector import evaluate
from app.models import Event
from app.queue import Queue

log = logging.getLogger(__name__)


class Worker:
    def __init__(self, settings, queue, pool, consumer=None):
        self.s, self.queue, self.pool = settings, queue, pool
        self.consumer = consumer or f"{socket.gethostname()}-{uuid4().hex[:8]}"
        self.claim_cursor = "0-0"

    def step(self, block_ms=1000):
        s, r = self.s, self.queue.r
        claimed = r.xautoclaim(
            s.stream, s.group, self.consumer, s.reclaim_idle_ms, self.claim_cursor, count=1
        )
        self.claim_cursor, messages = claimed[:2]
        if messages:
            r.hincrby(s.metrics_key, "retries_total", len(messages))
        else:
            response = r.xreadgroup(
                s.group, self.consumer, {s.stream: ">"}, count=1, block=block_ms
            )
            messages = response[0][1] if response else []
        for message_id, fields in messages:
            self.process(message_id, fields)
        return len(messages)

    def process(self, message_id, fields):
        s, r = self.s, self.queue.r
        pending = r.xpending_range(s.stream, s.group, message_id, message_id, 1)
        if not pending or pending[0]["consumer"] != self.consumer:
            return
        attempts = pending[0]["times_delivered"]
        if attempts > s.max_attempts:
            reason = (
                r.hget(f"{s.stream}:failures", message_id) or "delivery limit after worker loss"
            )
            self.queue.finish(self.consumer, message_id, fields, "dead", reason)
            return
        try:
            event = Event.model_validate_json(fields["data"])
            epoch = float(fields["accepted_at"])
            if not math.isfinite(epoch) or epoch <= 0:
                raise ValueError("invalid acceptance time")
            accepted = datetime.fromtimestamp(epoch, UTC)
        except (ValidationError, ValueError, KeyError, OverflowError):
            self.queue.finish(self.consumer, message_id, fields, "dead", "invalid queue payload")
            return
        try:
            inserted = persist(self.pool, event, accepted)
        except (psycopg.Error, PoolTimeout) as exc:
            # Exception classes/SQLSTATE are useful without leaking payloads or connection strings.
            reason = f"{type(exc).__name__}:{getattr(exc, 'sqlstate', None) or 'unavailable'}"
            r.hincrby(s.metrics_key, "database_failures_total", 1)
            r.hset(f"{s.stream}:failures", message_id, reason)
            if attempts >= s.max_attempts:
                self.queue.finish(self.consumer, message_id, fields, "dead", reason)
            log.warning(
                "database operation failed id=%s attempt=%s reason=%s", message_id, attempts, reason
            )
            return
        self.queue.finish(
            self.consumer, message_id, fields, "processed" if inserted else "duplicate"
        )


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    s = Settings()
    queue, pool = Queue(s), create_pool(s)
    worker = Worker(s, queue, pool)
    running = True

    def stop(*_):
        nonlocal running
        running = False

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    next_check = 0
    try:
        while running:
            try:
                queue.ensure_group()
                worker.step()
                if time.monotonic() >= next_check:
                    next_check = time.monotonic() + s.alert_check_seconds
                    for incident in evaluate(pool, s):
                        log.warning(
                            "INCIDENT id=%s service=%s errors=%s total=%s",
                            incident["id"],
                            incident["service"],
                            incident["error_events"],
                            incident["total_events"],
                        )
            except (redis.RedisError, psycopg.Error, PoolTimeout) as exc:
                log.warning("worker dependency unavailable: %s", type(exc).__name__)
                time.sleep(1)
    finally:
        queue.r.close()
        pool.close()


if __name__ == "__main__":
    main()
