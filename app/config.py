import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    database_url: str = os.getenv("DATABASE_URL", "postgresql+psycopg://djs:djs@localhost:5432/djs")
    redis_url: str = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    queue_key: str = os.getenv("QUEUE_KEY", "djs:ready")
    scan_interval: float = float(os.getenv("SCAN_INTERVAL", "2"))
    redispatch_interval: int = int(os.getenv("REDISPATCH_INTERVAL", "30"))
    heartbeat_interval: float = float(os.getenv("HEARTBEAT_INTERVAL", "5"))
    lease_seconds: int = int(os.getenv("LEASE_SECONDS", "30"))
    shutdown_grace_seconds: float = float(os.getenv("SHUTDOWN_GRACE_SECONDS", "10"))
    max_input_bytes: int = 16_384
    max_result_bytes: int = 65_536
    max_attempts: int = 10
    max_timeout_seconds: int = 3_600


settings = Settings()
