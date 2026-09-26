import random
import uuid
from datetime import timedelta

from sqlalchemy import func, or_, select
from sqlalchemy.orm import Session

from app.config import settings
from app.models import Job, JobAttempt


def database_now(session: Session):
    return session.scalar(select(func.clock_timestamp()))


def retry_delay(attempt_number: int, jitter: float | None = None) -> float:
    if attempt_number < 1:
        raise ValueError("attempt_number must be positive")
    if jitter is None:
        jitter = random.uniform(0, 1)
    if not 0 <= jitter <= 1:
        raise ValueError("jitter must be between 0 and 1")
    return min(5 * (2 ** (attempt_number - 1)), 60) + jitter


def claim(session: Session, job_id: uuid.UUID, worker_id: str):
    with session.begin():
        job = session.scalar(select(Job).where(Job.id == job_id).with_for_update())
        now = database_now(session)
        if job is None or job.status not in ("queued", "retrying") or job.available_at > now:
            return None
        if job.attempt_count >= job.max_attempts:
            return None
        attempt_id = uuid.uuid4()
        deadline = now + timedelta(seconds=job.timeout_seconds)
        job.status = "running"
        job.attempt_count += 1
        job.current_attempt_id = attempt_id
        job.worker_id = worker_id
        job.lease_expires_at = now + timedelta(seconds=settings.lease_seconds)
        job.updated_at = now
        attempt = JobAttempt(
            id=attempt_id, job_id=job.id, attempt_number=job.attempt_count,
            worker_id=worker_id, started_at=now, execution_deadline=deadline,
        )
        session.add(attempt)
        session.flush()
        return {
            "job_id": job.id, "attempt_id": attempt_id, "attempt_number": job.attempt_count,
            "task_type": job.task_type, "input": job.input, "deadline": deadline,
            "timeout_seconds": job.timeout_seconds,
            "queue_wait_seconds": (now - job.created_at).total_seconds(),
        }


def _owned(session: Session, job_id: uuid.UUID, attempt_id: uuid.UUID, now):
    job = session.scalar(select(Job).where(Job.id == job_id).with_for_update())
    if job is None or job.status != "running" or job.current_attempt_id != attempt_id:
        return None
    if job.lease_expires_at is None or job.lease_expires_at <= now:
        return None
    return job


def heartbeat(session: Session, job_id: uuid.UUID, attempt_id: uuid.UUID) -> bool:
    with session.begin():
        now = database_now(session)
        job = _owned(session, job_id, attempt_id, now)
        if job is None:
            return False
        attempt = session.get(JobAttempt, attempt_id)
        if now >= attempt.execution_deadline:
            return False
        job.lease_expires_at = now + timedelta(seconds=settings.lease_seconds)
        job.updated_at = now
        return True


def _close_attempt(job: Job, attempt: JobAttempt, now, outcome: str, code: str | None, message: str | None, retryable: bool, result: dict | None = None):
    attempt.ended_at = now
    attempt.outcome = outcome
    attempt.error_code = code
    attempt.error_message = message
    job.current_attempt_id = None
    job.worker_id = None
    job.lease_expires_at = None
    job.updated_at = now
    if outcome == "success":
        job.status = "completed"
        job.result = result
        job.finished_at = now
        job.last_error_code = None
        job.last_error_message = None
    else:
        job.last_error_code = code
        job.last_error_message = message
        if retryable and job.attempt_count < job.max_attempts:
            job.status = "retrying"
            job.available_at = now + timedelta(seconds=retry_delay(job.attempt_count))
            job.next_dispatch_at = job.available_at
        else:
            job.status = "failed"
            job.finished_at = now


def finish(session: Session, job_id: uuid.UUID, attempt_id: uuid.UUID, outcome: str, code: str | None = None, message: str | None = None, retryable: bool = False, result: dict | None = None) -> bool:
    with session.begin():
        now = database_now(session)
        job = _owned(session, job_id, attempt_id, now)
        if job is None:
            return False
        attempt = session.get(JobAttempt, attempt_id)
        if outcome == "success" and now >= attempt.execution_deadline:
            return False
        _close_attempt(job, attempt, now, outcome, code, message, retryable, result)
        return True


def recover_expired(session: Session, limit: int = 100) -> int:
    recovered = 0
    # Row locks ensure a heartbeat, result commit, or another coordinator cannot race recovery.
    with session.begin():
        now = database_now(session)
        rows = session.scalars(
            select(Job).join(JobAttempt, Job.current_attempt_id == JobAttempt.id)
            .where(Job.status == "running", or_(Job.lease_expires_at <= now, JobAttempt.execution_deadline <= now))
            .order_by(Job.lease_expires_at, Job.id).limit(limit).with_for_update(of=Job, skip_locked=True)
        ).all()
        for job in rows:
            attempt = session.get(JobAttempt, job.current_attempt_id)
            timed_out = attempt.execution_deadline <= now
            code = "timeout" if timed_out else "worker_lost"
            _close_attempt(job, attempt, now, code, code, "Attempt timed out" if timed_out else "Worker lease expired", True)
            recovered += 1
    return recovered
