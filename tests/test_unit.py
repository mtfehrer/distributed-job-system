from fastapi.testclient import TestClient
from sqlalchemy.exc import SQLAlchemyError

from app.lifecycle import retry_delay
from app.main import app
from app.tasks import PermanentTaskError, RetryableTaskError, run_task


def test_retry_schedule_is_capped_and_jitter_bounded():
    assert retry_delay(1, 0) == 5
    assert retry_delay(2, 1) == 11
    assert retry_delay(10, 0) == 60


def test_task_results_and_demo_failure():
    assert run_task("text_stats", {"text": "hello  world"}, 1) == {"characters": 12, "words": 2}
    try:
        run_task("fail_until_attempt", {"succeed_on_attempt": 2}, 1)
    except RetryableTaskError:
        pass
    else:
        assert False, "first attempt must fail"
    assert run_task("fail_until_attempt", {"succeed_on_attempt": 2}, 2) == {"succeeded_on_attempt": 2}
    try:
        run_task("fail_permanently", {}, 1)
    except PermanentTaskError:
        pass
    else:
        assert False, "permanent demo task must fail"


def test_invalid_requests_have_one_error_shape():
    client = TestClient(app)
    for body in (
        {"task_type": "unknown", "input": {}},
        {"task_type": "sleep", "input": {"seconds": -1}},
        {"task_type": "text_stats", "input": {"text": "x", "extra": 1}},
        {"task_type": "sleep", "input": {"seconds": 1}, "max_attempts": 0},
    ):
        response = client.post("/jobs", json=body)
        assert response.status_code == 422
        assert set(response.json()["error"]) == {"code", "message"}


def test_submission_does_not_acknowledge_database_failure(monkeypatch):
    class UnavailableSession:
        @staticmethod
        def begin():
            raise SQLAlchemyError("database is unavailable")

    monkeypatch.setattr("app.main.SessionLocal", UnavailableSession)
    response = TestClient(app).post("/jobs", json={"task_type": "text_stats", "input": {"text": "hi"}})
    assert response.status_code == 503
    assert response.json()["error"]["code"] == "database_unavailable"
