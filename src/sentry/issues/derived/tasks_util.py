from __future__ import annotations

import logging
from collections.abc import Mapping
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Protocol

from sentry.issues.derived.check import CheckFailure, CheckId, CheckInvalidated, CheckResult
from sentry.taskworker.selfchain_idempotency import already_spawned, mark_spawned
from sentry.utils import metrics

if TYPE_CHECKING:
    from sentry.issues.derived.processing import GenerationId
    from sentry.issues.derived.promote import PromotionResult

logger = logging.getLogger(__name__)


class _TaskState(Protocol):
    id: str


class SpawnState:
    """Ergonomic wrapper around self-chain ``already_spawned`` / ``mark_spawned``.

    Construct once from ``current_task()`` and the task's self-chain key. Methods are no-ops when
    there is no activation (eager/sync calls).
    """

    def __init__(self, task_state: _TaskState | None, task_key: str) -> None:
        self.task_key = task_key
        self.activation_id: str | None = task_state.id if task_state is not None else None

    def already_spawned(self) -> bool:
        if self.activation_id is None:
            return False
        return already_spawned(self.task_key, self.activation_id)

    def mark_spawned(self) -> None:
        if self.activation_id is None:
            return
        mark_spawned(self.task_key, self.activation_id)


def _record_check_result(result: CheckResult) -> None:
    outcome = "no_result" if isinstance(result, CheckInvalidated) else "success"
    if isinstance(result, CheckFailure):
        outcome = "mismatch"
        logger.warning(
            "check_derived_data.mismatch",
            extra={
                "group_id": result.group_id,
                "cursor_date": result.cursor_date.isoformat(),
                "cursor_id": result.cursor_id,
                "differences": {
                    feature.name: difference for feature, difference in result.differences.items()
                },
            },
        )
    metrics.incr(
        "issues.derived.check_group",
        sample_rate=1.0,
        tags={"result": outcome},
    )


def _record_batch_metrics(
    processed: Mapping[PromotionResult, int],
    *,
    metric_name: str,
    tag_extra: dict[str, str] | None = None,
) -> None:
    for result, count in processed.items():
        tags = {"result": result.value}
        if tag_extra:
            tags.update(tag_extra)
        metrics.incr(metric_name, amount=count, sample_rate=1.0, tags=tags)


def _resume_generation_id(
    group_id: int,
    resume_generated_at: str | None,
    resume_pipeline_hash: str | None,
) -> GenerationId | None:
    """Reconstruct a task-safe serialized ``GenerationId``."""
    from sentry.issues.derived.processing import GenerationId

    if resume_generated_at is None or resume_pipeline_hash is None:
        return None
    return GenerationId(
        group_id,
        datetime.fromisoformat(resume_generated_at).replace(tzinfo=timezone.utc),
        resume_pipeline_hash,
    )


def _resume_check_id(
    group_id: int,
    invocation_id: str | None,
    generated_at: str | None,
    cursor_date: str | None,
    cursor_id: int | None,
    pipeline_hash: str | None,
) -> CheckId | None:
    if None in (invocation_id, generated_at, cursor_date, cursor_id, pipeline_hash):
        return None
    assert invocation_id is not None
    assert generated_at is not None
    assert cursor_date is not None
    assert cursor_id is not None
    assert pipeline_hash is not None
    return CheckId(
        invocation_id,
        group_id,
        datetime.fromisoformat(generated_at).replace(tzinfo=timezone.utc),
        datetime.fromisoformat(cursor_date).replace(tzinfo=timezone.utc),
        cursor_id,
        pipeline_hash,
    )
