from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: str = "postgresql://logservice:local-development@localhost:5432/logservice"
    redis_url: str = "redis://localhost:6379/0"
    api_key: str = ""
    stream: str = "logs:events"
    group: str = "persist"
    max_request_bytes: int = Field(16384, ge=512, le=1048576)
    queue_capacity: int = Field(100000, ge=1)
    reclaim_idle_ms: int = Field(30000, ge=100)
    max_attempts: int = Field(5, ge=1, le=100)
    db_timeout_ms: int = Field(5000, ge=100)
    alert_window_seconds: int = Field(60, ge=1)
    alert_min_events: int = Field(20, ge=1)
    alert_min_errors: int = Field(5, ge=1)
    alert_error_ratio: float = Field(0.25, gt=0, le=1)
    alert_sustain_seconds: int = Field(30, ge=0)
    alert_cooldown_seconds: int = Field(300, ge=1)
    alert_check_seconds: int = Field(5, ge=1)

    @model_validator(mode="after")
    def lease_exceeds_database_timeout(self):
        if self.reclaim_idle_ms <= self.db_timeout_ms + 3000:
            raise ValueError("RECLAIM_IDLE_MS must exceed DB_TIMEOUT_MS + 3000")
        return self

    @property
    def dead_stream(self):
        return f"{self.stream}:dead"

    @property
    def metrics_key(self):
        return f"{self.stream}:metrics"
