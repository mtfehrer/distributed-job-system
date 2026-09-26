import signal
import time
from datetime import timedelta

import redis
from sqlalchemy import select

from app.config import settings
from app.db import SessionLocal
from app.lifecycle import database_now, recover_expired
from app.logging import configure_logging, event
from app.models import Job

logger = configure_logging("coordinator")
stopping = False


def stop(*_):
    global stopping
    stopping = True


def dispatch_batch(client: redis.Redis, limit: int = 100) -> int:
    sent = 0
    with SessionLocal() as session:
        with session.begin():
            now = database_now(session)
            jobs = session.scalars(
                select(Job).where(
                    Job.status.in_(("queued", "retrying")),
                    Job.available_at <= now,
                    Job.next_dispatch_at <= now,
                ).order_by(Job.available_at, Job.id).limit(limit).with_for_update(skip_locked=True)
            ).all()
            for job in jobs:
                client.lpush(settings.queue_key, str(job.id))
                job.next_dispatch_at = now + timedelta(seconds=settings.redispatch_interval)
                sent += 1
    return sent


def main():
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    client = redis.Redis.from_url(settings.redis_url, socket_connect_timeout=2, socket_timeout=2)
    delay = 1
    while not stopping:
        try:
            with SessionLocal() as session:
                recovered = recover_expired(session)
            published = dispatch_batch(client)
            if recovered or published:
                event(logger, "scan", recovered_leases=recovered, published=published)
            delay = 1
            time.sleep(settings.scan_interval)
        except Exception as exc:
            event(logger, "dependency_error", error_type=type(exc).__name__)
            time.sleep(delay)
            delay = min(delay * 2, 30)


if __name__ == "__main__":
    main()
