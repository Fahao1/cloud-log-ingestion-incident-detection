"""Exercise a deployed API and worker, with no database access in the test client."""

import os
import time
from datetime import UTC, datetime
from uuid import uuid4

import httpx


def main():
    service = "smoke-" + uuid4().hex[:8]
    with httpx.Client(
        base_url=os.getenv("API_URL", "http://localhost:8000"),
        headers={"X-API-Key": os.getenv("API_KEY", "")},
        timeout=10,
    ) as client:
        assert client.get("/health").status_code == 200
        assert client.get("/ready").status_code == 200
        payload = {
            "event_id": str(uuid4()),
            "timestamp": datetime.now(UTC).isoformat(),
            "service": service,
            "severity": "ERROR",
            "message": "synthetic smoke timeout",
        }
        for _ in range(2):
            assert client.post("/events", json=payload).status_code == 202
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            response = client.get("/events", params={"service": service, "q": "timeout"})
            response.raise_for_status()
            if response.json()["items"]:
                assert len(response.json()["items"]) == 1
                print("PASS: readiness, HTTP ingestion, worker persistence, search")
                return
            time.sleep(0.1)
        raise SystemExit("event did not become searchable within 20 seconds")


if __name__ == "__main__":
    main()
