import json
import multiprocessing as mp
import os
import signal
import socket
import time
import uuid

import redis

from app.config import settings
from app.db import SessionLocal
from app.lifecycle import claim, finish, heartbeat
from app.logging import configure_logging, event
from app.tasks import PermanentTaskError, RetryableTaskError, run_task

logger = configure_logging("worker")
stopping = False


def stop(*_):
    global stopping
    stopping = True


def execute_child(connection, task_type: str, payload: dict, attempt_number: int):
    try:
        result = run_task(task_type, payload, attempt_number)
        if len(json.dumps(result, ensure_ascii=False).encode("utf-8")) > settings.max_result_bytes:
            raise PermanentTaskError("Task result exceeds 65536 bytes")
        connection.send(("success", None, None, False, result))
    except RetryableTaskError as exc:
        connection.send(("handler_error", "transient_error", str(exc)[:500], True, None))
    except PermanentTaskError as exc:
        connection.send(("handler_error", "permanent_error", str(exc)[:500], False, None))
    except Exception:
        connection.send(("handler_error", "handler_error", "Task handler failed", True, None))
    finally:
        connection.close()


def stop_child(process: mp.Process):
    if process.is_alive():
        process.terminate()
        process.join(timeout=2)
        if process.is_alive():
            process.kill()
    process.join(timeout=2)


def supervise(work: dict, worker_id: str):
    parent, child = mp.get_context("spawn").Pipe(duplex=False)
    process = mp.get_context("spawn").Process(
        target=execute_child,
        args=(child, work["task_type"], work["input"], work["attempt_number"]),
    )
    process.start()
    child.close()
    started = time.monotonic()
    next_heartbeat = started + settings.heartbeat_interval
    shutdown_deadline = None
    outcome = None
    try:
        while True:
            now = time.monotonic()
            if parent.poll(0.2):
                try:
                    outcome = parent.recv()
                except EOFError:
                    outcome = ("handler_error", "worker_process_error", "Task process ended without a result", True, None)
                break
            if not process.is_alive():
                outcome = ("handler_error", "worker_process_error", "Task process ended without a result", True, None)
                break
            if now - started >= work["timeout_seconds"]:
                outcome = ("timeout", "timeout", "Attempt timed out", True, None)
                break
            if stopping and shutdown_deadline is None:
                shutdown_deadline = now + settings.shutdown_grace_seconds
            if shutdown_deadline is not None and now >= shutdown_deadline:
                outcome = ("worker_lost", "worker_shutdown", "Worker stopped during task", True, None)
                break
            if now >= next_heartbeat:
                try:
                    with SessionLocal() as session:
                        owned = heartbeat(session, work["job_id"], work["attempt_id"])
                except Exception as exc:
                    event(logger, "heartbeat_error", worker_id=worker_id, job_id=work["job_id"], attempt_id=work["attempt_id"], error_type=type(exc).__name__)
                    break
                if not owned:
                    event(logger, "ownership_lost", worker_id=worker_id, job_id=work["job_id"], attempt_id=work["attempt_id"])
                    break
                next_heartbeat = now + settings.heartbeat_interval
    finally:
        stop_child(process)
        parent.close()
    if outcome is None:
        return
    status, code, message, retryable, result = outcome
    try:
        with SessionLocal() as session:
            accepted = finish(session, work["job_id"], work["attempt_id"], status, code, message, retryable, result)
        event(logger, "attempt_finished" if accepted else "late_result_rejected", worker_id=worker_id, job_id=work["job_id"], attempt_id=work["attempt_id"], outcome=status, duration_seconds=round(time.monotonic() - started, 3))
    except Exception as exc:
        event(logger, "result_database_error", worker_id=worker_id, job_id=work["job_id"], attempt_id=work["attempt_id"], error_type=type(exc).__name__)


def main():
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    worker_id = f"{socket.gethostname()}:{os.getpid()}:{uuid.uuid4().hex[:8]}"
    client = redis.Redis.from_url(settings.redis_url, socket_connect_timeout=2, socket_timeout=3)
    delay = 1
    while not stopping:
        try:
            item = client.brpop(settings.queue_key, timeout=2)
            if item is None:
                continue
            if stopping:
                try:
                    client.rpush(settings.queue_key, item[1])
                except redis.RedisError:
                    pass  # PostgreSQL reconciliation will republish the job.
                break
            try:
                job_id = uuid.UUID(item[1].decode())
            except (ValueError, UnicodeDecodeError):
                event(logger, "invalid_queue_message", worker_id=worker_id)
                continue
            with SessionLocal() as session:
                work = claim(session, job_id, worker_id)
            if work is None:
                continue
            event(logger, "attempt_started", worker_id=worker_id, job_id=job_id, attempt_id=work["attempt_id"], queue_wait_seconds=round(work["queue_wait_seconds"], 3))
            supervise(work, worker_id)
            delay = 1
        except Exception as exc:
            event(logger, "dependency_error", worker_id=worker_id, error_type=type(exc).__name__)
            time.sleep(delay)
            delay = min(delay * 2, 30)


if __name__ == "__main__":
    main()
