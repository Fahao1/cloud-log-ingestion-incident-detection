"""A repeatable traffic pattern that can sustain the default error rule."""

import argparse
import os
import time
from datetime import UTC, datetime
from uuid import uuid4

import httpx


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default="http://localhost:8000")
    parser.add_argument("--events", type=int, default=120)
    parser.add_argument("--interval", type=float, default=0.5)
    parser.add_argument("--service", default="checkout")
    args = parser.parse_args()
    with httpx.Client(headers={"X-API-Key": os.getenv("API_KEY", "")}) as client:
        for index in range(args.events):
            response = client.post(
                args.url + "/events",
                json={
                    "event_id": str(uuid4()),
                    "timestamp": datetime.now(UTC).isoformat(),
                    "service": args.service,
                    "severity": "ERROR" if index % 2 else "INFO",
                    "message": "Payment provider timeout" if index % 2 else "Payment received",
                    "metadata": {"synthetic": True, "sequence": index},
                },
            )
            response.raise_for_status()
            print(response.json())
            time.sleep(args.interval)


if __name__ == "__main__":
    main()
