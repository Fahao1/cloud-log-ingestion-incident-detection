from datetime import UTC
from typing import Annotated, Literal
from uuid import UUID

from pydantic import (
    AfterValidator,
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    JsonValue,
    field_validator,
)

Severity = Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]


def utc_timestamp(value):
    # PostgreSQL accepts years outside Python's range; psycopg cannot read them back.
    try:
        return value.astimezone(UTC)
    except (OverflowError, ValueError) as exc:
        raise ValueError("timestamp must be representable in UTC (years 1 through 9999)") from exc


def postgres_text(value):
    if "\x00" in value:
        raise ValueError("NUL characters are not supported")
    value.encode("utf-8")
    return value


Timestamp = Annotated[AwareDatetime, AfterValidator(utc_timestamp)]
SafeText = Annotated[str, AfterValidator(postgres_text)]


class Event(BaseModel):
    model_config = ConfigDict(extra="forbid")

    event_id: UUID
    timestamp: Timestamp
    service: str = Field(min_length=1, max_length=100, pattern=r"^[a-zA-Z0-9][a-zA-Z0-9_.-]*$")
    severity: Severity
    message: str = Field(min_length=1, max_length=8192)
    metadata: dict[str, JsonValue] = Field(default_factory=dict)

    @field_validator("message")
    @classmethod
    def nonblank(cls, value):
        if not value.strip():
            raise ValueError("message must contain non-whitespace text")
        return value

    @field_validator("message", "metadata")
    @classmethod
    def postgres_safe_json(cls, value):
        # JSONB/text reject NUL and invalid Unicode; catch them at the trust boundary.
        def check(item):
            if isinstance(item, str):
                postgres_text(item)
            elif isinstance(item, dict):
                for key, child in item.items():
                    check(key)
                    check(child)
            elif isinstance(item, list):
                for child in item:
                    check(child)
            elif isinstance(item, float):
                import math

                if not math.isfinite(item):
                    raise ValueError("metadata numbers must be finite")

        check(value)
        return value
