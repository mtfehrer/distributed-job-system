import json
import logging
import os
import sys


def configure_logging(service: str) -> logging.Logger:
    logger = logging.getLogger(f"djs.{service}")
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(logging.Formatter("%(message)s"))
        logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    logger.propagate = False
    logger.service = service
    return logger


def event(logger: logging.Logger, name: str, **fields) -> None:
    logger.info(json.dumps({"service": getattr(logger, "service", os.getenv("SERVICE_NAME", "app")), "event": name, **fields}, default=str))
