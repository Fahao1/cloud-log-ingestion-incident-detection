CREATE TABLE events (
    event_id uuid PRIMARY KEY,
    timestamp timestamptz NOT NULL,
    service text NOT NULL,
    severity text NOT NULL CHECK (severity IN ('DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL')),
    message text NOT NULL,
    metadata jsonb NOT NULL DEFAULT '{}',
    accepted_at timestamptz NOT NULL,
    stored_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    -- Adapted from log-observability app/db.py, MIT; see THIRD_PARTY.md.
    search tsvector GENERATED ALWAYS AS (to_tsvector('english', message)) STORED
);
CREATE INDEX events_search_idx ON events USING GIN (search);
CREATE INDEX events_time_idx ON events (timestamp DESC, event_id DESC);
CREATE INDEX events_service_time_idx ON events (service, timestamp DESC, event_id DESC);
CREATE INDEX events_severity_time_idx ON events (severity, timestamp DESC, event_id DESC);
CREATE INDEX events_accepted_idx ON events (accepted_at, service) INCLUDE (severity);

CREATE TABLE incidents (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    service text NOT NULL,
    opened_at timestamptz NOT NULL,
    breach_since timestamptz NOT NULL,
    total_events integer NOT NULL,
    error_events integer NOT NULL,
    error_ratio double precision NOT NULL,
    rule jsonb NOT NULL
);
CREATE INDEX incidents_time_idx ON incidents (id DESC);
CREATE INDEX incidents_service_idx ON incidents (service, id DESC);

CREATE TABLE alert_state (
    service text PRIMARY KEY,
    breach_since timestamptz,
    last_alert_at timestamptz
);
CREATE TABLE detector_state (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    evaluated_at timestamptz NOT NULL
);
INSERT INTO detector_state VALUES (true, 'epoch');
