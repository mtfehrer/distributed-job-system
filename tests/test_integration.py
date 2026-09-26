"""Run with DJS_INTEGRATION=1 against PostgreSQL and Redis on a disposable database."""

import dataclasses
import os
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta

import pytest
import redis
from sqlalchemy import create_engine, select, text
from sqlalchemy.orm import sessionmaker

from app import coordinator
from app.config import settings
from app.lifecycle import claim, database_now, finish, heartbeat, recover_expired
from app.models import Base, Job, JobAttempt

pytestmark = pytest.mark.skipif(os.getenv("DJS_INTEGRATION") != "1", reason="requires opt-in PostgreSQL and Redis")


@pytest.fixture
def environment(monkeypatch):
    root_engine = create_engine(settings.database_url)
    schema = "test_" + uuid.uuid4().hex
    with root_engine.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA "{schema}"'))
    engine = create_engine(settings.database_url, connect_args={"options": f"-csearch_path={schema}"})

    Base.metadata.create_all(engine)
    sessions = sessionmaker(bind=engine, expire_on_commit=False)
    queue_key = "djs:test:" + uuid.uuid4().hex
    client = redis.Redis.from_url(settings.redis_url)
    client.ping()
    monkeypatch.setattr(coordinator, "SessionLocal", sessions)
    monkeypatch.setattr(coordinator, "settings", dataclasses.replace(settings, queue_key=queue_key))
    try:
        yield sessions, client, queue_key
    finally:
        client.delete(queue_key)
        engine.dispose()
        with root_engine.begin() as conn:
            conn.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
        root_engine.dispose()


def make_job(sessions, task_type="text_stats", max_attempts=3):
    with sessions.begin() as session:
        job = Job(task_type=task_type, input={"text": "hi"}, max_attempts=max_attempts, timeout_seconds=30)
        session.add(job)
        session.flush()
        return job.id


def test_atomic_claim_duplicate_message_and_stale_result(environment):
    sessions, _, _ = environment
    job_id = make_job(sessions)

    def take(worker_id):
        with sessions() as session:
            return claim(session, job_id, worker_id)

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(take, ("a", "b")))
    owned = [value for value in results if value is not None]
    assert len(owned) == 1
    first = owned[0]
    with sessions() as session:
        assert claim(session, job_id, "duplicate") is None
        session.rollback()
        now = database_now(session)
        session.rollback()
    with sessions.begin() as session:
        job = session.get(Job, job_id)
        job.lease_expires_at = now - timedelta(seconds=1)
    with sessions() as session:
        assert heartbeat(session, job_id, first["attempt_id"]) is False
        assert finish(session, job_id, first["attempt_id"], "success", result={"bad": True}) is False
        assert recover_expired(session) == 1
        assert claim(session, job_id, "too_early") is None
    with sessions.begin() as session:
        job = session.get(Job, job_id)
        assert job.status == "retrying"
        job.available_at = database_now(session) - timedelta(seconds=1)
    with sessions() as session:
        second = claim(session, job_id, "replacement")
        assert second["attempt_number"] == 2
        assert finish(session, job_id, first["attempt_id"], "success", result={"bad": True}) is False
        assert finish(session, job_id, second["attempt_id"], "success", result={"ok": True}) is True
        assert claim(session, job_id, "late") is None
        attempts = session.scalars(select(JobAttempt).where(JobAttempt.job_id == job_id).order_by(JobAttempt.attempt_number)).all()
        assert [a.outcome for a in attempts] == ["worker_lost", "success"]


def test_redis_reconciliation_and_retry_exhaustion(environment):
    sessions, client, queue_key = environment
    job_id = make_job(sessions, max_attempts=1)
    assert coordinator.dispatch_batch(client) == 1
    assert uuid.UUID(client.brpop(queue_key, timeout=1)[1].decode()) == job_id
    assert coordinator.dispatch_batch(client) == 0
    with sessions.begin() as session:
        job = session.get(Job, job_id)
        job.next_dispatch_at = database_now(session) - timedelta(seconds=1)
    assert coordinator.dispatch_batch(client) == 1
    with sessions() as session:
        work = claim(session, job_id, "worker")
        assert finish(session, job_id, work["attempt_id"], "handler_error", "transient_error", "demo", True)
        assert claim(session, job_id, "duplicate") is None
        job = session.get(Job, job_id)
        assert job.status == "failed"
        assert job.attempt_count == 1
