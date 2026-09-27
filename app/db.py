"""Parameterized SQL; the full-text/filter pattern adapts log-observability app/db.py.

MIT notice in licenses/log-observability-MIT.txt. All reliability logic is original.
"""

from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from psycopg_pool import ConnectionPool

from app.config import Settings


def create_pool(settings: Settings):
    return ConnectionPool(
        settings.database_url,
        min_size=0,
        max_size=8,
        timeout=2,
        open=True,
        kwargs={
            "row_factory": dict_row,
            "connect_timeout": 2,
            "options": f"-c statement_timeout={settings.db_timeout_ms} -c lock_timeout=2000",
        },
    )


def persist(pool, event, accepted_at):
    with pool.connection() as conn:
        inserted = conn.execute(
            """INSERT INTO events
               (event_id, timestamp, service, severity, message, metadata, accepted_at)
               VALUES (%s, %s, %s, %s, %s, %s, %s)
               ON CONFLICT (event_id) DO NOTHING RETURNING event_id""",
            (
                event.event_id,
                event.timestamp,
                event.service,
                event.severity,
                event.message,
                Jsonb(event.metadata),
                accepted_at,
            ),
        ).fetchone()
    # The pool context commits before returning; only then may the worker acknowledge.
    return inserted is not None


def search(pool, service, severity, start, end, q, before, limit):
    clauses, args = [], []
    for value, clause in [
        (service, "service = %s"),
        (severity, "severity = %s"),
        (start, "timestamp >= %s"),
        (end, "timestamp < %s"),
        (q, "search @@ websearch_to_tsquery('english', %s)"),
    ]:
        if value is not None:
            clauses.append(clause)
            args.append(value)
    if before:
        clauses.append("(timestamp, event_id) < (%s, %s)")
        args.extend(before)
    where = " AND ".join(clauses) or "TRUE"
    with pool.connection() as conn:
        return conn.execute(
            "SELECT event_id, timestamp, service, severity, message, metadata, "
            "accepted_at, stored_at "
            f"FROM events WHERE {where} ORDER BY timestamp DESC, event_id DESC LIMIT %s",
            [*args, limit + 1],
        ).fetchall()
