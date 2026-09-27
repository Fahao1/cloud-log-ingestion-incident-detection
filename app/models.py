from typing import Literal
from uuid import UUID

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, JsonValue, field_validator

Severity = Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]


class Event(BaseModel):
    model_config = ConfigDict(extra="forbid")

    event_id: UUID
    timestamp: AwareDatetime
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
                if "\x00" in item:
                    raise ValueError("NUL characters are not supported")
                item.encode("utf-8")
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
