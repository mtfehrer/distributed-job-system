# Distributed Job System

A local FastAPI service that accepts jobs immediately, stores their state in PostgreSQL, and executes registered tasks in independent workers. Redis carries ready-job notifications. PostgreSQL remains authoritative, so the coordinator can republish work after a lost Redis message or recover an expired worker lease.

## Start

Docker Compose and access to its daemon are required. The default configuration binds the API to `127.0.0.1:8000`; PostgreSQL and Redis stay on the Compose network.

```bash
cp .env.example .env
docker compose up --build --scale worker=3
```

The `migrate` service runs `alembic upgrade head` before application services start. To apply a migration explicitly later, run `docker compose run --rm migrate`. Data lives in named Compose volumes. Open [the interactive API documentation](http://127.0.0.1:8000/docs) or use the examples below. `.env` is ignored by Git; change its example password for a shared machine.

```bash
curl -i -X POST http://127.0.0.1:8000/jobs \
  -H 'Content-Type: application/json' \
  -d '{"task_type":"text_stats","input":{"text":"hello world"}}'

curl http://127.0.0.1:8000/jobs/JOB_ID
curl http://127.0.0.1:8000/jobs/JOB_ID/attempts
curl 'http://127.0.0.1:8000/jobs?status=completed&page=1&page_size=20'
curl http://127.0.0.1:8000/health/ready
```

The `Location` header of a successful POST contains the job URL. Submission responds with `202` only after the database commit. `/health/ready` requires PostgreSQL and reports Redis as `degraded` if the queue is unavailable; durable submissions can continue then.

## Tasks and behavior

- `sleep`: `{"seconds": 2}` returns the requested duration. Each worker executes one task at a time in a supervised child process.
- `text_stats`: `{"text": "hello world"}` returns `{"characters": 11, "words": 2}`.
- `fail_until_attempt`: `{"succeed_on_attempt": 3}` is a development/demo task that raises a controlled retryable error until attempt 3.
- `fail_permanently`: `{}` is a development/demo task that raises a nonretryable error on its first attempt.

Unknown task types, extra task fields, invalid options, and oversized inputs are rejected with `422`. Input is limited to 16 KiB, results to 64 KiB, attempts to 10, and each attempt timeout to 3600 seconds. The defaults are three total attempts and a 60-second attempt timeout.

Jobs progress through `queued → running → completed`, or `running → retrying → running` until success or a terminal `failed` state. Retry delays start at 5 seconds, double up to 60 seconds, and add up to 1 second of jitter. Attempts and sanitized error codes remain queryable. List order is newest first by `(created_at, id)` with a maximum page size of 100.

## Reliability model

The coordinator scans PostgreSQL every two seconds for due work and publishes job IDs to a Redis list. It marks a job for another notification 30 seconds later, so a lost or cleared queue message is eventually replaced. The worker atomically claims the row before starting an attempt. Duplicate or stale messages cannot claim running or terminal jobs. An attempt ID and unexpired lease are required for heartbeats and final writes, which prevents a late worker from overwriting a newer attempt.

Each handler runs in a child process. Its supervisor renews a 30-second lease every five seconds, terminates timed-out execution, and allows up to ten seconds for the active task to finish on graceful shutdown. The coordinator closes expired leases or deadlines and schedules retries when attempts remain. PostgreSQL timestamps decide claim eligibility and ownership. If a worker loses its database connection, it stops its child; the coordinator recovers the attempt after the lease expires. Both worker and coordinator use capped reconnect delays.

Delivery is **at least once** while PostgreSQL stays durable and services eventually recover. A task can run again after a crash or partition. Attempt fencing protects job records but cannot undo an external side effect. The included tasks are repeatable; future side-effecting handlers need their own idempotency design. This local service has no authentication or rate limit and should not be exposed publicly.

## Repeatable checks

Unit tests run without services:

```bash
python -m venv .venv
.venv/bin/pip install -r requirements.txt
.venv/bin/python -m pytest -q tests/test_unit.py
```

The integration tests use isolated temporary PostgreSQL schemas and unique Redis list keys. Run them against a disposable local stack:

```bash
docker compose up -d --build --scale worker=3
docker compose run --rm -e DJS_INTEGRATION=1 -e DJS_E2E=1 api python -m pytest -q
```

For a visible retry, submit `{"task_type":"fail_until_attempt","input":{"succeed_on_attempt":3}}` and poll its job and attempts URLs. Submit `fail_permanently` to check a terminal failure without retries. For timeout, submit a `sleep` of 10 seconds with `timeout_seconds: 1` and `max_attempts: 1`. To demonstrate crash recovery, submit a longer `sleep`, wait for `running`, then run `docker compose kill worker`; after the lease expires, use `docker compose up -d --scale worker=3` and inspect its attempts. To demonstrate Redis reconciliation, stop the queue briefly with `docker compose stop redis`, submit a job, and start it again with `docker compose start redis`.

The optional benchmark records hardware, wall time, queue delay, and outcomes for a fixed sleep batch. Run it once with one worker and again with three; compare only actual measurements. This demonstrates concurrent waiting, not CPU scaling.

```bash
docker compose up -d --scale worker=1
.venv/bin/python scripts/benchmark.py --workers 1
docker compose up -d --scale worker=3
.venv/bin/python scripts/benchmark.py --workers 3
```
