from __future__ import annotations

import logging
import random
import time
from bisect import bisect_left
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from math import ceil
from typing import Literal

from django.db import router
from django.db.models import Max, Min
from django.db.utils import OperationalError
from pydantic import BaseModel, Field

from sentry import options
from sentry.issues.derived.processing import PIPELINE
from sentry.issues.derived.promote import PromotionResult, build_and_promote_batch
from sentry.issues.derived.tasks_util import _record_batch_metrics, _resume_generation_id
from sentry.issues.models.groupderiveddata import GroupDerivedData
from sentry.utils import metrics
from sentry.utils.db import statement_timeout
from sentry.workflow_engine.caches.mapping import CacheMapping

logger = logging.getLogger(__name__)

_MAX_STALE_HASHES = 5
_STALE_HASH_DISCOVERY_TIMEOUT = timedelta(seconds=15)
_MAX_CHECK_GROUPS = 10_000
_GROUP_ID_RANGE_QUERY_TIMEOUT = timedelta(seconds=40)
# Use exact boundaries below this requested row count; estimate density above it.
_MAX_EXACT_RANGE_ROWS = 10_000
# Each probe reads this many matching IDs and extrapolates the following number of ranges.
_RANGE_DENSITY_SAMPLE_SIZE = 100
_RANGES_PER_DENSITY_SAMPLE = 5
# Bound total probe work for regions with large scheduling budgets.
_MAX_RANGE_DENSITY_SAMPLES = 200
# Three days allows meaningful progress while bounding how long optimistic gaps persist.
_STATE_TTL_SECONDS = 3 * 24 * 60 * 60
# Bump when the state shape changes; incompatible cached state is discarded on load.
CURRENT_STATE_VERSION = 1


class HealSchedulerState(BaseModel):
    version: int = CURRENT_STATE_VERSION
    head_hash: str | None = None
    stale: dict[str, int] = Field(default_factory=dict)
    # Routine saves do not refresh this fixed sweep epoch.
    discovered_at: datetime | None = None


_state_cache = CacheMapping[Literal["state"], HealSchedulerState](
    lambda key: key,
    namespace="issues-derived-heal",
    ttl_seconds=_STATE_TTL_SECONDS,
)


def load_state() -> HealSchedulerState | None:
    state = _state_cache.get("state")
    if not isinstance(state, HealSchedulerState) or state.version != CURRENT_STATE_VERSION:
        return None
    if state.discovered_at is None or state.discovered_at.tzinfo is None:
        return None
    if datetime.now(timezone.utc) - state.discovered_at >= timedelta(seconds=_STATE_TTL_SECONDS):
        return None
    return state


def save_state(state: HealSchedulerState) -> None:
    _state_cache.set("state", state)


@dataclass(frozen=True)
class RegenerationRequest:
    target_hash: str | None
    group_id_start: int
    group_id_end: int
    resume_generated_at: str | None = None
    resume_pipeline_hash: str | None = None
    rows_found_before: int = 0
    range_overflowed: bool = False


@dataclass(frozen=True)
class RegenerationResult:
    processed: Mapping[PromotionResult, int]
    total: int
    continuation: RegenerationRequest | None = None
    continuation_reason: str | None = None


@dataclass(frozen=True)
class GroupIdRangeResult:
    ranges: list[tuple[int, int]]
    drained: bool


def _discover_stale_pipeline_hashes(current_hash: str, limit: int) -> list[str]:
    """Return distinct non-null pipeline hashes other than the current hash."""
    # A simple select distinct scans all stale rows. Walking the btree one hash at
    # a time keeps this proportional to the small number of deployed hashes.
    results: list[str] = []
    cursor: str | None = ""
    using = router.db_for_read(GroupDerivedData)
    with statement_timeout(using, _STALE_HASH_DISCOVERY_TIMEOUT):
        while len(results) < limit:
            cursor = (
                GroupDerivedData.objects.using(using)
                .filter(pipeline_hash__gt=cursor)
                .order_by("pipeline_hash")
                .values_list("pipeline_hash", flat=True)
                .first()
            )
            if cursor is None:
                break
            if cursor != current_hash:
                results.append(cursor)
    return results


def _pick_random_fresh_group_ranges(
    pipeline_hash: str, *, batch_size: int, task_count: int
) -> list[tuple[int, int]]:
    """Pick contiguous check ranges from one random anchor (slide-to-fill if short)."""
    if batch_size <= 0 or task_count <= 0:
        return []

    need = min(batch_size * task_count, _MAX_CHECK_GROUPS)
    fresh = GroupDerivedData.objects.filter(pipeline_hash=pipeline_hash)
    bounds = fresh.aggregate(min_group_id=Min("group_id"), max_group_id=Max("group_id"))
    min_group_id = bounds["min_group_id"]
    max_group_id = bounds["max_group_id"]
    if min_group_id is None or max_group_id is None:
        return []

    random_start = random.randint(min_group_id, max_group_id)
    group_ids = list(
        fresh.filter(group_id__gte=random_start)
        .order_by("group_id")
        .values_list("group_id", flat=True)[:need]
    )
    if len(group_ids) < need:
        group_ids = list(fresh.order_by("-group_id").values_list("group_id", flat=True)[:need])
        group_ids.reverse()
    if not group_ids:
        return []

    ranges: list[tuple[int, int]] = []
    for index in range(0, len(group_ids), batch_size):
        chunk = group_ids[index : index + batch_size]
        ranges.append((chunk[0], chunk[-1] + 1))
    return ranges


def _exact_group_id_ranges(
    group_ids: Sequence[int], *, range_size: int, max_ranges: int
) -> list[tuple[int, int]]:
    """Build exact ranges from ordered IDs, using an optional lookahead ID as the final end."""
    if not group_ids:
        return []

    requested_rows = range_size * max_ranges
    starts = group_ids[:requested_rows:range_size]
    last_end = group_ids[requested_rows] if len(group_ids) > requested_rows else group_ids[-1] + 1
    return list(zip(starts, [*starts[1:], last_end]))


def _estimate_group_id_ranges(
    density_sample: Sequence[int], *, range_size: int, range_count: int
) -> list[tuple[int, int]]:
    """Build contiguous ranges sized from the density of an ordered ID sample."""
    sample_width = density_sample[-1] - density_sample[0] + 1
    estimated_width = ceil(sample_width * range_size / len(density_sample))
    first_group_id = density_sample[0]
    return [
        (
            first_group_id + index * estimated_width,
            first_group_id + (index + 1) * estimated_width,
        )
        for index in range(range_count)
    ]


def group_id_ranges_for_hash(
    pipeline_hash: str | None, *, range_size: int, max_ranges: int, group_id_lower_bound: int = 0
) -> GroupIdRangeResult:
    """Estimate ranges covering rows with a pipeline hash.

    Returns at most ``max_ranges`` ascending disjoint ranges targeting
    ``range_size`` rows. ``drained`` is true only when a valid query found no
    rows at or above ``group_id_lower_bound``.
    """
    if range_size <= 0 or max_ranges <= 0:
        return GroupIdRangeResult(ranges=[], drained=False)

    matching_group_ids = (
        GroupDerivedData.objects.filter(pipeline_hash=pipeline_hash)
        .order_by("pipeline_hash", "group_id")
        .values_list("group_id", flat=True)
    )
    using = matching_group_ids.db
    matching_group_ids = matching_group_ids.using(using)
    query_deadline = time.monotonic() + _GROUP_ID_RANGE_QUERY_TIMEOUT.total_seconds()

    with metrics.timer("issues.derived.group_id_range_query"):

        def fetch_group_ids(start: int, limit: int) -> list[int]:
            remaining_seconds = query_deadline - time.monotonic()
            if remaining_seconds <= 0.001:
                raise OperationalError("group ID range query budget exceeded")

            with statement_timeout(using, timedelta(seconds=remaining_seconds)):
                return list(matching_group_ids.filter(group_id__gte=start)[:limit])

        requested_rows = range_size * max_ranges
        if requested_rows <= _MAX_EXACT_RANGE_ROWS:
            group_ids = fetch_group_ids(group_id_lower_bound, requested_rows + 1)
            if not group_ids:
                return GroupIdRangeResult(ranges=[], drained=True)

            return GroupIdRangeResult(
                ranges=_exact_group_id_ranges(
                    group_ids,
                    range_size=range_size,
                    max_ranges=max_ranges,
                ),
                drained=False,
            )

        result_ranges: list[tuple[int, int]] = []
        next_group_id = group_id_lower_bound
        density_sample_count = min(
            ceil(max_ranges / _RANGES_PER_DENSITY_SAMPLE),
            _MAX_RANGE_DENSITY_SAMPLES,
        )
        ranges_per_sample, samples_with_extra_range = divmod(max_ranges, density_sample_count)
        range_counts = [ranges_per_sample + 1] * samples_with_extra_range + [ranges_per_sample] * (
            density_sample_count - samples_with_extra_range
        )

        for ranges_for_sample in range_counts:
            sampled_group_ids = fetch_group_ids(next_group_id, _RANGE_DENSITY_SAMPLE_SIZE + 1)
            if not sampled_group_ids:
                return GroupIdRangeResult(ranges=result_ranges, drained=not result_ranges)
            if len(sampled_group_ids) <= _RANGE_DENSITY_SAMPLE_SIZE:
                starts = sampled_group_ids[::range_size]
                ends = starts[1:] + [sampled_group_ids[-1] + 1]
                result_ranges.extend(zip(starts, ends))
                break

            density_sample = sampled_group_ids[:-1]
            estimated_ranges = _estimate_group_id_ranges(
                density_sample,
                range_size=range_size,
                range_count=ranges_for_sample,
            )
            result_ranges.extend(estimated_ranges)
            next_group_id = estimated_ranges[-1][1]

    return GroupIdRangeResult(ranges=result_ranges[:max_ranges], drained=False)


def heal_stale_derived_data(
    *,
    enqueue_regeneration: Callable[[RegenerationRequest], None],
    enqueue_check: Callable[[int, int], None],
) -> None:
    """Schedule healing for outdated and explicitly invalidated derived data."""
    started_at = time.monotonic()
    logger.info("heal_stale_derived_data.started")

    if not options.get("issues.derived.heal-enabled"):
        logger.info("heal_stale_derived_data.disabled")
        return

    batch_size = options.get("issues.derived.heal-batch-size")
    max_tasks = options.get("issues.derived.heal-max-tasks")
    current_hash = PIPELINE.pipeline_hash

    if batch_size <= 0 or max_tasks <= 0:
        logger.error(
            "heal_stale_derived_data.invalid_batch_configuration",
            extra={"batch_size": batch_size, "max_tasks": max_tasks},
        )
        return

    logger.info(
        "heal_stale_derived_data.configuration_loaded",
        extra={
            "batch_size": batch_size,
            "max_tasks": max_tasks,
            "pipeline_hash": current_hash,
        },
    )

    state = load_state()
    if state is None:
        state = HealSchedulerState()
        logger.info("heal_stale_derived_data.state_regenerated")
    else:
        logger.info("heal_stale_derived_data.state_loaded")

    hash_state_changed = False
    if state.head_hash != current_hash:
        if state.head_hash is not None:
            state.stale.setdefault(state.head_hash, 0)
        state.head_hash = current_hash
        hash_state_changed = True
    if state.stale.pop(current_hash, None) is not None:
        hash_state_changed = True

    if not state.stale:
        discovery_started_at = time.monotonic()
        logger.info("heal_stale_derived_data.stale_hash_discovery_started")
        metrics.incr(
            "issues.derived.heal_stale_hash_discovery",
            sample_rate=1.0,
            tags={"reason": "no_state" if state.discovered_at is None else "stale_empty"},
        )
        try:
            with metrics.timer("issues.derived.heal_stale_hash_discovery_duration"):
                stale_hashes = _discover_stale_pipeline_hashes(current_hash, _MAX_STALE_HASHES)
        except OperationalError:
            logger.exception("heal_stale_derived_data.stale_hash_discovery_failed")
            metrics.incr("issues.derived.heal_stale_hash_discovery_failed", sample_rate=1.0)
            stale_hashes = []
        else:
            state.discovered_at = state.discovered_at or datetime.now(timezone.utc)
            state.stale.update(dict.fromkeys(stale_hashes, 0))
            hash_state_changed = True
            logger.info(
                "heal_stale_derived_data.stale_hash_discovery_complete",
                extra={
                    "stale_hashes": stale_hashes,
                    "elapsed": time.monotonic() - discovery_started_at,
                },
            )
    else:
        stale_hashes = list(state.stale)
        logger.info(
            "heal_stale_derived_data.stale_hash_discovery_skipped",
            extra={"stale_hash_count": len(state.stale)},
        )

    if hash_state_changed:
        save_state(state)

    remaining = max_tasks
    scheduled_per_hash: dict[str, int] = {}
    for stale_hash in [None, *stale_hashes]:
        if remaining <= 0:
            break
        hash_kind = "null" if stale_hash is None else "stale"
        lower_bound = 0 if stale_hash is None else state.stale[stale_hash]
        logger.info(
            "heal_stale_derived_data.range_selection_started",
            extra={
                "hash_kind": hash_kind,
                "remaining_budget": remaining,
                "group_id_lower_bound": lower_bound,
            },
        )
        range_selection_started_at = time.monotonic()
        try:
            range_result = group_id_ranges_for_hash(
                stale_hash,
                range_size=batch_size,
                max_ranges=remaining,
                group_id_lower_bound=lower_bound,
            )
        except OperationalError:
            logger.exception(
                "heal_stale_derived_data.range_selection_failed",
                extra={
                    "hash_kind": hash_kind,
                    "elapsed": time.monotonic() - range_selection_started_at,
                    "pipeline_hash": stale_hash,
                    "group_id_lower_bound": lower_bound,
                },
            )
            metrics.incr(
                "issues.derived.heal_range_selection_failed",
                sample_rate=1.0,
                tags={"hash_kind": hash_kind},
            )
            continue
        ranges = range_result.ranges
        logger.info(
            "heal_stale_derived_data.range_selection_complete",
            extra={
                "hash_kind": hash_kind,
                "range_count": len(ranges),
                "remaining_budget": remaining,
                "elapsed": time.monotonic() - range_selection_started_at,
            },
        )
        if stale_hash is not None and range_result.drained:
            del state.stale[stale_hash]
            logger.info(
                "heal_stale_derived_data.stale_hash_retired",
                extra={"pipeline_hash": stale_hash, "group_id_mark": lower_bound},
            )
            save_state(state)
            continue
        if not ranges:
            continue
        logger.info(
            "heal_stale_derived_data.batch_dispatch_started",
            extra={"hash_kind": hash_kind, "task_count": len(ranges)},
        )
        for start, end in ranges:
            enqueue_regeneration(
                RegenerationRequest(
                    target_hash=stale_hash,
                    group_id_start=start,
                    group_id_end=end,
                )
            )
        remaining -= len(ranges)
        scheduled_per_hash["null" if stale_hash is None else stale_hash] = len(ranges)
        if stale_hash is not None:
            old_mark = state.stale[stale_hash]
            new_mark = ranges[-1][1]
            state.stale[stale_hash] = new_mark
            save_state(state)
            logger.info(
                "heal_stale_derived_data.stale_hash_mark_advanced",
                extra={
                    "pipeline_hash": stale_hash,
                    "old_group_id_mark": old_mark,
                    "new_group_id_mark": new_mark,
                },
            )
        logger.info(
            "heal_stale_derived_data.batch_dispatch_complete",
            extra={
                "hash_kind": hash_kind,
                "task_count": len(ranges),
                "remaining_budget": remaining,
            },
        )
        metrics.incr(
            "issues.derived.heal_ranges_scheduled",
            amount=len(ranges),
            sample_rate=1.0,
            tags={"hash_kind": hash_kind},
        )

    task_count = max_tasks - remaining
    if task_count == 0:
        logger.info("heal_stale_derived_data.nothing_to_heal")
    else:
        logger.info(
            "heal_stale_derived_data.scheduled",
            extra={
                "stale_hashes": stale_hashes,
                "task_count": task_count,
                "tasks_per_hash": scheduled_per_hash,
                "batch_size": batch_size,
                "pipeline_hash": current_hash,
            },
        )

    check_budget = min(remaining, options.get("issues.derived.check-task-count"))
    if check_budget <= 0:
        logger.info(
            "heal_stale_derived_data.complete",
            extra={
                "heal_task_count": task_count,
                "check_task_count": 0,
                "elapsed": time.monotonic() - started_at,
            },
        )
        return

    logger.info(
        "heal_stale_derived_data.check_range_selection_started",
        extra={"check_budget": check_budget},
    )
    check_ranges = _pick_random_fresh_group_ranges(
        current_hash,
        batch_size=batch_size,
        task_count=check_budget,
    )
    logger.info(
        "heal_stale_derived_data.check_range_selection_complete",
        extra={"check_budget": check_budget, "range_count": len(check_ranges)},
    )
    logger.info(
        "heal_stale_derived_data.check_dispatch_started",
        extra={"task_count": len(check_ranges)},
    )
    for start, end in check_ranges:
        enqueue_check(start, end)

    logger.info(
        "heal_stale_derived_data.checks_scheduled",
        extra={
            "task_count": len(check_ranges),
            "pipeline_hash": current_hash,
            "heal_task_count": task_count,
            "remaining_budget": remaining,
        },
    )
    logger.info(
        "heal_stale_derived_data.complete",
        extra={
            "heal_task_count": task_count,
            "check_task_count": len(check_ranges),
            "elapsed": time.monotonic() - started_at,
        },
    )


def regenerate_stale_derived_data_batch(
    *,
    group_id_start: int,
    group_id_end: int,
    target_hash: str | None,
    timeout: timedelta,
    resume_generated_at: str | None = None,
    resume_pipeline_hash: str | None = None,
    rows_found_before: int = 0,
    range_overflowed: bool = False,
) -> RegenerationResult:
    """Rebuild rows in a range matching exactly ``target_hash``."""
    generation_id = _resume_generation_id(group_id_start, resume_generated_at, resume_pipeline_hash)
    batch_size = max(1, options.get("issues.derived.heal-batch-size"))
    group_ids = list(
        GroupDerivedData.objects.filter(
            pipeline_hash=target_hash,
            group_id__gte=group_id_start,
            group_id__lt=group_id_end,
        )
        .order_by("group_id")
        .values_list("group_id", flat=True)[: batch_size + 1]
    )
    range_overflow = len(group_ids) > batch_size
    if range_overflow:
        group_ids = group_ids[:batch_size]

    result = build_and_promote_batch(
        group_ids,
        timeout=timeout,
        initial_generation_id=generation_id,
        log_key="regenerate_stale_derived_data_batch",
    )

    continuation: RegenerationRequest | None = None
    continuation_reason: str | None = None
    if result.timeout_reason is not None:
        assert result.resume_from_group_id is not None
        gen_id = result.resume_generation_id
        rows_consumed = bisect_left(group_ids, result.resume_from_group_id)
        continuation = RegenerationRequest(
            target_hash=target_hash,
            group_id_start=result.resume_from_group_id,
            group_id_end=group_id_end,
            resume_generated_at=gen_id.generated_at.isoformat() if gen_id else None,
            resume_pipeline_hash=gen_id.pipeline_hash if gen_id else None,
            rows_found_before=rows_found_before + rows_consumed,
            range_overflowed=range_overflowed or range_overflow,
        )
        continuation_reason = result.timeout_reason
    elif range_overflow:
        continuation = RegenerationRequest(
            target_hash=target_hash,
            group_id_start=group_ids[-1] + 1,
            group_id_end=group_id_end,
            rows_found_before=rows_found_before + len(group_ids),
            range_overflowed=True,
        )
        continuation_reason = "range_overflow"
    else:
        metrics.distribution(
            "issues.derived.heal_range_rows_found",
            rows_found_before + len(group_ids),
            sample_rate=1.0,
            tags={
                "hash_kind": "null" if target_hash is None else "stale",
                "range_overflowed": str(range_overflowed).lower(),
            },
        )

    _record_batch_metrics(
        result.processed,
        metric_name="issues.derived.regenerate_stale_groups_processed",
    )
    return RegenerationResult(
        processed=result.processed,
        total=len(group_ids),
        continuation=continuation,
        continuation_reason=continuation_reason,
    )
