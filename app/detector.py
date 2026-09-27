"""Sustained rolling error ratio; state and cooldown survive worker restarts."""

from datetime import timedelta

from psycopg.types.json import Jsonb


def evaluate(pool, settings):
    s = settings
    opened = []
    with pool.connection() as conn:
        # ponytail: one detector lock; partition by service if this scan becomes a bottleneck.
        if not conn.execute("SELECT pg_try_advisory_xact_lock(48151002) AS locked").fetchone()[
            "locked"
        ]:
            return []
        now = conn.execute("SELECT clock_timestamp() AS now").fetchone()["now"]
        previous = conn.execute("SELECT evaluated_at FROM detector_state").fetchone()[
            "evaluated_at"
        ]
        if (now - previous).total_seconds() < s.alert_check_seconds:
            return []
        rows = conn.execute(
            """SELECT service, count(*) AS total,
               count(*) FILTER (WHERE severity IN ('ERROR', 'CRITICAL')) AS errors
               FROM events WHERE accepted_at >= %s AND accepted_at <= %s GROUP BY service""",
            (now - timedelta(seconds=s.alert_window_seconds), now),
        ).fetchall()
        states = {
            row["service"]: row for row in conn.execute("SELECT * FROM alert_state").fetchall()
        }
        counts = {row["service"]: row for row in rows}
        for service in states.keys() | counts.keys():
            row = counts.get(service, {"total": 0, "errors": 0})
            ratio = row["errors"] / row["total"] if row["total"] else 0
            state = states.get(service, {})
            breach = state.get("breach_since")
            last = state.get("last_alert_at")
            qualifies = (
                row["total"] >= s.alert_min_events
                and row["errors"] >= s.alert_min_errors
                and ratio >= s.alert_error_ratio
            )
            # Missing evaluations cannot prove a sustained breach. Restart the timer after a gap.
            if (now - previous).total_seconds() > 2 * s.alert_check_seconds:
                breach = None
            breach = (breach or now) if qualifies else None
            if (
                breach is not None
                and (now - breach).total_seconds() >= s.alert_sustain_seconds
                and (last is None or (now - last).total_seconds() >= s.alert_cooldown_seconds)
            ):
                rule = {
                    key.removeprefix("alert_"): value
                    for key, value in s.model_dump().items()
                    if key.startswith("alert_")
                }
                opened.append(
                    conn.execute(
                        """INSERT INTO incidents
                           (service, opened_at, breach_since, total_events, error_events,
                            error_ratio, rule) VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING *""",
                        (service, now, breach, row["total"], row["errors"], ratio, Jsonb(rule)),
                    ).fetchone()
                )
                last = now
            conn.execute(
                """INSERT INTO alert_state VALUES (%s, %s, %s)
                   ON CONFLICT (service) DO UPDATE SET
                   breach_since = EXCLUDED.breach_since, last_alert_at = EXCLUDED.last_alert_at""",
                (service, breach, last),
            )
        conn.execute("UPDATE detector_state SET evaluated_at = %s", (now,))
    return opened
