import json
import uuid
from typing import Literal

import redis
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, Field, ValidationError
from sqlalchemy import func, select, text
from sqlalchemy.exc import SQLAlchemyError

from app.config import settings
from app.db import SessionLocal
from app.logging import configure_logging, event
from app.models import Job, JobAttempt
from app.tasks import TASK_SCHEMAS

app = FastAPI(title="Distributed Job System", version="0.1.0")
logger = configure_logging("api")


class SubmitJob(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    task_type: str
    input: dict
    max_attempts: int = Field(default=3, ge=1, le=settings.max_attempts)
    timeout_seconds: int = Field(default=60, ge=1, le=settings.max_timeout_seconds)


def error(status: int, code: str, message: str):
    return JSONResponse(status_code=status, content={"error": {"code": code, "message": message}})


@app.exception_handler(RequestValidationError)
async def validation_error_handler(request: Request, exc: RequestValidationError):
    return error(422, "validation_error", "Request does not match the required schema")


@app.exception_handler(HTTPException)
async def http_error_handler(request: Request, exc: HTTPException):
    detail = exc.detail if isinstance(exc.detail, str) else "Request failed"
    return error(exc.status_code, "not_found" if exc.status_code == 404 else "request_error", detail)


def job_view(job: Job):
    return {
        "id": job.id, "task_type": job.task_type, "status": job.status,
        "attempt_count": job.attempt_count, "max_attempts": job.max_attempts,
        "timeout_seconds": job.timeout_seconds, "input": job.input,
        "result": job.result, "last_error": (
            {"code": job.last_error_code, "message": job.last_error_message}
            if job.last_error_code else None
        ),
        "available_at": job.available_at, "created_at": job.created_at,
        "updated_at": job.updated_at, "finished_at": job.finished_at,
    }


@app.post("/jobs", status_code=202)
def submit_job(body: SubmitJob):
    schema = TASK_SCHEMAS.get(body.task_type)
    if schema is None:
        return error(422, "unknown_task_type", "Task type is not registered")
    try:
        payload = schema.model_validate(body.input).model_dump()
    except ValidationError:
        return error(422, "invalid_task_input", "Input does not match the task schema")
    if len(json.dumps(payload, ensure_ascii=False).encode("utf-8")) > settings.max_input_bytes:
        return error(422, "input_too_large", "Task input exceeds 16384 bytes")
    try:
        with SessionLocal.begin() as session:
            job = Job(task_type=body.task_type, input=payload, max_attempts=body.max_attempts, timeout_seconds=body.timeout_seconds)
            session.add(job)
            session.flush()
            job_id = job.id
    except SQLAlchemyError:
        event(logger, "submission_database_unavailable")
        return error(503, "database_unavailable", "Durable submission is unavailable")
    event(logger, "job_accepted", job_id=job_id)
    url = f"/jobs/{job_id}"
    return JSONResponse(status_code=202, headers={"Location": url}, content={"id": str(job_id), "status": "queued", "status_url": url})


@app.get("/jobs/{job_id}")
def get_job(job_id: uuid.UUID):
    try:
        with SessionLocal() as session:
            job = session.get(Job, job_id)
            if job is None:
                raise HTTPException(404, "Job not found")
            return job_view(job)
    except SQLAlchemyError:
        return error(503, "database_unavailable", "Job storage is unavailable")


@app.get("/jobs")
def list_jobs(
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=20, ge=1, le=100),
    status: Literal["queued", "running", "retrying", "completed", "failed"] | None = None,
    task_type: str | None = None,
):
    try:
        with SessionLocal() as session:
            filters = []
            if status:
                filters.append(Job.status == status)
            if task_type:
                filters.append(Job.task_type == task_type)
            total = session.scalar(select(func.count()).select_from(Job).where(*filters))
            jobs = session.scalars(
                select(Job).where(*filters).order_by(Job.created_at.desc(), Job.id.desc())
                .offset((page - 1) * page_size).limit(page_size)
            ).all()
            return {"items": [job_view(job) for job in jobs], "page": page, "page_size": page_size, "total": total}
    except SQLAlchemyError:
        return error(503, "database_unavailable", "Job storage is unavailable")


@app.get("/jobs/{job_id}/attempts")
def list_attempts(job_id: uuid.UUID):
    try:
        with SessionLocal() as session:
            if session.get(Job, job_id) is None:
                raise HTTPException(404, "Job not found")
            attempts = session.scalars(
                select(JobAttempt).where(JobAttempt.job_id == job_id).order_by(JobAttempt.attempt_number)
            ).all()
            return {"items": [
                {
                    "id": a.id, "attempt_number": a.attempt_number, "worker_id": a.worker_id,
                    "started_at": a.started_at, "ended_at": a.ended_at,
                    "execution_deadline": a.execution_deadline, "outcome": a.outcome,
                    "error": {"code": a.error_code, "message": a.error_message} if a.error_code else None,
                } for a in attempts
            ]}
    except SQLAlchemyError:
        return error(503, "database_unavailable", "Job storage is unavailable")


@app.get("/health/live")
def live():
    return {"status": "ok"}


@app.get("/health/ready")
def ready():
    try:
        with SessionLocal() as session:
            session.execute(text("SELECT 1"))
    except SQLAlchemyError:
        return error(503, "database_unavailable", "PostgreSQL is unavailable")
    try:
        redis.Redis.from_url(settings.redis_url, socket_connect_timeout=1, socket_timeout=1).ping()
        queue_status = "ok"
    except redis.RedisError:
        queue_status = "degraded"
    return {"status": "ok", "postgres": "ok", "redis": queue_status}
