"""Measure a fixed batch of sleep jobs against a running local stack."""

import argparse
import json
import os
import platform
import statistics
import time

import httpx


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default="http://127.0.0.1:8000")
    parser.add_argument("--count", type=int, default=12)
    parser.add_argument("--seconds", type=float, default=2)
    parser.add_argument("--workers", type=int, required=True, help="Number of workers started in Compose")
    args = parser.parse_args()
    if args.count < 1 or args.seconds < 0:
        parser.error("count must be positive and seconds nonnegative")
    with httpx.Client(base_url=args.url, timeout=10) as client:
        started = time.monotonic()
        ids = []
        for _ in range(args.count):
            response = client.post("/jobs", json={"task_type": "sleep", "input": {"seconds": args.seconds}})
            response.raise_for_status()
            ids.append(response.json()["id"])
        states = {}
        while len(states) < len(ids):
            for job_id in ids:
                if job_id in states:
                    continue
                response = client.get(f"/jobs/{job_id}")
                response.raise_for_status()
                job = response.json()
                if job["status"] in ("completed", "failed"):
                    attempts = client.get(f"/jobs/{job_id}/attempts")
                    attempts.raise_for_status()
                    states[job_id] = (job, attempts.json()["items"])
            time.sleep(0.2)
        elapsed = time.monotonic() - started
    from datetime import datetime

    delays = []
    for job, attempts in states.values():
        if attempts:
            created = datetime.fromisoformat(job["created_at"])
            first_start = datetime.fromisoformat(attempts[0]["started_at"])
            delays.append((first_start - created).total_seconds())
    print(json.dumps({
        "platform": platform.platform(), "cpu_count": os.cpu_count(),
        "workers": args.workers, "jobs": args.count, "sleep_seconds": args.seconds,
        "wall_seconds": round(elapsed, 3),
        "queue_delay_mean_seconds": round(statistics.mean(delays), 3) if delays else None,
        "queue_delay_max_seconds": round(max(delays), 3) if delays else None,
        "completed": sum(job["status"] == "completed" for job, _ in states.values()),
        "failed": sum(job["status"] == "failed" for job, _ in states.values()),
    }, indent=2))


if __name__ == "__main__":
    main()
