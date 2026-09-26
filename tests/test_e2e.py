"""Run with DJS_E2E=1 against a running API, coordinator, and workers."""

import os
import time

import httpx
import pytest

pytestmark = pytest.mark.skipif(os.getenv("DJS_E2E") != "1", reason="requires running Compose services")


@pytest.fixture
def client():
    with httpx.Client(base_url=os.getenv("DJS_API_URL", "http://api:8000"), timeout=5) as connection:
        yield connection


def submit(client, task_type, payload, **options):
    response = client.post("/jobs", json={"task_type": task_type, "input": payload, **options})
    assert response.status_code == 202, response.text
    assert response.headers["Location"] == response.json()["status_url"]
    return response.json()["id"]


def terminal(client, job_id, wait_seconds=40):
    deadline = time.monotonic() + wait_seconds
    while time.monotonic() < deadline:
        response = client.get(f"/jobs/{job_id}")
        response.raise_for_status()
        job = response.json()
        if job["status"] in ("completed", "failed"):
            return job
        time.sleep(0.2)
    pytest.fail(f"job {job_id} did not finish")


def attempts(client, job_id):
    response = client.get(f"/jobs/{job_id}/attempts")
    response.raise_for_status()
    return response.json()["items"]


def test_normal_validation_and_listing(client):
    invalid = client.post("/jobs", json={"task_type": "text_stats", "input": {"missing": "text"}})
    assert invalid.status_code == 422
    job_id = submit(client, "text_stats", {"text": "hello world"})
    job = terminal(client, job_id)
    assert job["status"] == "completed"
    assert job["result"] == {"characters": 11, "words": 2}
    assert [a["outcome"] for a in attempts(client, job_id)] == ["success"]
    listing = client.get("/jobs", params={"status": "completed", "task_type": "text_stats"})
    listing.raise_for_status()
    assert any(item["id"] == job_id for item in listing.json()["items"])


def test_retries_permanent_failure_exhaustion_and_timeout(client):
    retry_id = submit(client, "fail_until_attempt", {"succeed_on_attempt": 2})
    assert terminal(client, retry_id)["status"] == "completed"
    assert [a["outcome"] for a in attempts(client, retry_id)] == ["handler_error", "success"]

    permanent_id = submit(client, "fail_permanently", {})
    assert terminal(client, permanent_id)["status"] == "failed"
    assert len(attempts(client, permanent_id)) == 1

    exhausted_id = submit(client, "fail_until_attempt", {"succeed_on_attempt": 3}, max_attempts=1)
    assert terminal(client, exhausted_id)["status"] == "failed"
    assert len(attempts(client, exhausted_id)) == 1

    timeout_id = submit(client, "sleep", {"seconds": 3}, timeout_seconds=1, max_attempts=1)
    assert terminal(client, timeout_id)["status"] == "failed"
    assert attempts(client, timeout_id)[0]["outcome"] == "timeout"


def test_three_workers_run_independent_jobs_together(client):
    ids = [submit(client, "sleep", {"seconds": 5}) for _ in range(3)]
    deadline = time.monotonic() + 12
    while time.monotonic() < deadline:
        states = [client.get(f"/jobs/{job_id}").json()["status"] for job_id in ids]
        if states == ["running", "running", "running"]:
            workers = [attempts(client, job_id)[0]["worker_id"] for job_id in ids]
            assert len(set(workers)) == 3
            break
        time.sleep(0.2)
    else:
        pytest.fail("three jobs were not running concurrently")
    assert all(terminal(client, job_id)["status"] == "completed" for job_id in ids)
