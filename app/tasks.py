import time

from pydantic import BaseModel, ConfigDict, Field


class TaskInput(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)


class SleepInput(TaskInput):
    seconds: float = Field(ge=0, le=3600, allow_inf_nan=False)


class TextStatsInput(TaskInput):
    text: str = Field(max_length=16_000)


class FailUntilAttemptInput(TaskInput):
    succeed_on_attempt: int = Field(ge=1, le=10)


class FailPermanentlyInput(TaskInput):
    pass


TASK_SCHEMAS = {
    "sleep": SleepInput,
    "text_stats": TextStatsInput,
    "fail_until_attempt": FailUntilAttemptInput,
    "fail_permanently": FailPermanentlyInput,
}


class RetryableTaskError(Exception):
    pass


class PermanentTaskError(Exception):
    pass


def run_task(task_type: str, payload: dict, attempt_number: int) -> dict:
    if task_type == "sleep":
        time.sleep(payload["seconds"])
        return {"seconds": payload["seconds"]}
    if task_type == "text_stats":
        value = payload["text"]
        return {"characters": len(value), "words": len(value.split())}
    if task_type == "fail_until_attempt":
        if attempt_number < payload["succeed_on_attempt"]:
            raise RetryableTaskError("Demo task requested another attempt")
        return {"succeeded_on_attempt": attempt_number}
    if task_type == "fail_permanently":
        raise PermanentTaskError("Demo task requested permanent failure")
    raise PermanentTaskError("Task type is not registered")
