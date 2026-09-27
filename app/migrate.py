"""Ordered, transactional SQL migrations with a PostgreSQL advisory lock."""

from pathlib import Path

import psycopg

from app.config import Settings


def migrate(database_url=None):
    with psycopg.connect(database_url or Settings().database_url) as conn:
        conn.execute("SELECT pg_advisory_xact_lock(48151001)")
        conn.execute("CREATE TABLE IF NOT EXISTS schema_migrations (version text PRIMARY KEY)")
        for path in sorted((Path(__file__).resolve().parent.parent / "migrations").glob("*.sql")):
            if not conn.execute(
                "SELECT 1 FROM schema_migrations WHERE version = %s", (path.name,)
            ).fetchone():
                conn.execute(path.read_text())
                conn.execute("INSERT INTO schema_migrations VALUES (%s)", (path.name,))


if __name__ == "__main__":
    migrate()
