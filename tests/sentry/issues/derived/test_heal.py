from collections.abc import Sequence
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, call, patch

import pytest
from django.db.utils import OperationalError

from sentry.issues.action_log.publish import publish_action
from sentry.issues.action_log.types import ActionSource, GroupActionActor, ViewAction
from sentry.issues.derived.heal import (
    CURRENT_STATE_VERSION,
    GroupIdRangeResult,
    HealSchedulerState,
    RegenerationRequest,
    RegenerationResult,
    _discover_stale_pipeline_hashes,
    _estimate_group_id_ranges,
    _exact_group_id_ranges,
    _pick_random_fresh_group_ranges,
    _state_cache,
    group_id_ranges_for_hash,
    heal_stale_derived_data,
    load_state,
    regenerate_stale_derived_data_batch,
    save_state,
)
from sentry.issues.derived.processing import PIPELINE, GroupLogTimeout, process_group_log
from sentry.issues.models.groupderiveddata import GroupDerivedData
from sentry.models.group import Group
from sentry.testutils.cases import TestCase
from sentry.testutils.helpers.features import with_feature
from sentry.testutils.helpers.options import override_options
from sentry.testutils.outbox import outbox_runner

REGENERATION_TIMEOUT = timedelta(seconds=20)


class DerivedDataHealTestBase(TestCase):
    def create_unprocessed_groups(self, count: int) -> list[Group]:
        groups = []
        for _ in range(count):
            group = self.create_group(project=self.project)
            with outbox_runner():
                publish_action(
                    ViewAction(),
                    source=ActionSource.API,
                    group_id=group.id,
                    project=group.project,
                    actor=GroupActionActor.user(self.user.id),
                )
            GroupDerivedData.objects.filter(group_id=group.id).delete()
            groups.append(group)
        return groups


class HealSchedulerStateTest(TestCase):
    def test_cache_key(self) -> None:
        assert _state_cache.key("state") == "issues-derived-heal:state"

    def test_round_trip(self) -> None:
        state = HealSchedulerState(
            head_hash="current",
            stale={"stale": 42},
            discovered_at=datetime.now(timezone.utc),
        )

        with patch.object(_state_cache, "set") as cache_set:
            save_state(state)
        cached_state = cache_set.call_args.args[1]

        with patch.object(_state_cache, "get", return_value=cached_state):
            assert load_state() == state
        assert state.version == CURRENT_STATE_VERSION

    def test_missing(self) -> None:
        with patch.object(_state_cache, "get", return_value=None):
            assert load_state() is None

    def test_corrupt(self) -> None:
        with patch.object(_state_cache, "get", return_value="not-state"):
            assert load_state() is None

    def test_wrong_version(self) -> None:
        state = HealSchedulerState(
            version=CURRENT_STATE_VERSION + 1,
            head_hash="current",
            stale={},
            discovered_at=datetime.now(timezone.utc),
        )
        with patch.object(_state_cache, "get", return_value=state):
            assert load_state() is None

    def test_expired(self) -> None:
        state = HealSchedulerState(
            head_hash="current",
            discovered_at=datetime.now(timezone.utc) - timedelta(days=3),
        )

        with patch.object(_state_cache, "get", return_value=state):
            assert load_state() is None


@with_feature("projects:issue-action-log-write-to-db")
class HealStaleDerivedDataTest(DerivedDataHealTestBase):
    def setUp(self) -> None:
        super().setUp()
        load_state_patch = patch("sentry.issues.derived.heal.load_state", return_value=None)
        save_state_patch = patch("sentry.issues.derived.heal.save_state")
        load_state_patch.start()
        save_state_patch.start()
        self.addCleanup(load_state_patch.stop)
        self.addCleanup(save_state_patch.stop)
        self.enqueue_regeneration = MagicMock()
        self.enqueue_check = MagicMock()

    def _heal(self) -> None:
        def enqueue_regeneration(request: RegenerationRequest) -> None:
            self.enqueue_regeneration(
                target_hash=request.target_hash,
                group_id_start=request.group_id_start,
                group_id_end=request.group_id_end,
            )

        def enqueue_check(group_id_start: int, group_id_end: int) -> None:
            self.enqueue_check(
                group_id_start=group_id_start,
                group_id_end=group_id_end,
            )

        heal_stale_derived_data(
            enqueue_regeneration=enqueue_regeneration,
            enqueue_check=enqueue_check,
        )

    def _pick_stale_hash(self, seed: str = "0") -> str:
        h = seed * 16
        return h if PIPELINE.pipeline_hash != h else ("z" * 16)

    def test_finds_stale_groups_and_schedules_batch(self) -> None:
        groups = self.create_unprocessed_groups(2)
        group_ids = sorted(g.id for g in groups)

        for gid in group_ids:
            process_group_log(gid)

        stale = self._pick_stale_hash()
        GroupDerivedData.objects.filter(group_id=group_ids[0]).update(pipeline_hash=stale)

        with patch.object(self, "enqueue_regeneration") as mock_delay:
            self._heal()

        mock_delay.assert_called_once_with(
            target_hash=stale,
            group_id_start=group_ids[0],
            group_id_end=group_ids[0] + 1,
        )

    def test_logs_progress_through_scheduling_stages(self) -> None:
        groups = self.create_unprocessed_groups(2)
        group_ids = sorted(group.id for group in groups)
        for group_id in group_ids:
            process_group_log(group_id)

        stale = self._pick_stale_hash()
        GroupDerivedData.objects.filter(group_id=group_ids[0]).update(pipeline_hash=stale)

        with (
            override_options(
                {
                    "issues.derived.heal-batch-size": 1,
                    "issues.derived.heal-max-tasks": 2,
                    "issues.derived.check-task-count": 1,
                }
            ),
            patch("sentry.issues.derived.heal.random.randint", return_value=group_ids[1]),
            patch("sentry.issues.derived.heal.logger") as mock_logger,
            patch.object(self, "enqueue_regeneration"),
            patch.object(self, "enqueue_check"),
        ):
            self._heal()

        messages = [log_call.args[0] for log_call in mock_logger.info.call_args_list]
        assert messages == [
            "heal_stale_derived_data.started",
            "heal_stale_derived_data.configuration_loaded",
            "heal_stale_derived_data.state_regenerated",
            "heal_stale_derived_data.stale_hash_discovery_started",
            "heal_stale_derived_data.stale_hash_discovery_complete",
            "heal_stale_derived_data.range_selection_started",
            "heal_stale_derived_data.range_selection_complete",
            "heal_stale_derived_data.range_selection_started",
            "heal_stale_derived_data.range_selection_complete",
            "heal_stale_derived_data.batch_dispatch_started",
            "heal_stale_derived_data.stale_hash_mark_advanced",
            "heal_stale_derived_data.batch_dispatch_complete",
            "heal_stale_derived_data.scheduled",
            "heal_stale_derived_data.check_range_selection_started",
            "heal_stale_derived_data.check_range_selection_complete",
            "heal_stale_derived_data.check_dispatch_started",
            "heal_stale_derived_data.checks_scheduled",
            "heal_stale_derived_data.complete",
        ]
        range_selection_logs = [
            log_call
            for log_call in mock_logger.info.call_args_list
            if log_call.args[0] == "heal_stale_derived_data.range_selection_complete"
        ]
        assert all(log_call.kwargs["extra"]["elapsed"] >= 0 for log_call in range_selection_logs)

    def test_missing_state_discovers_and_reports_metric(self) -> None:
        with (
            override_options({"issues.derived.check-task-count": 0}),
            patch(
                "sentry.issues.derived.heal._discover_stale_pipeline_hashes", return_value=[]
            ) as discover,
            patch(
                "sentry.issues.derived.heal.group_id_ranges_for_hash",
                return_value=GroupIdRangeResult(ranges=[], drained=True),
            ),
            patch("sentry.issues.derived.heal.metrics.incr") as mock_incr,
        ):
            self._heal()

        discover.assert_called_once_with(PIPELINE.pipeline_hash, 5)
        assert (
            call(
                "issues.derived.heal_stale_hash_discovery",
                sample_rate=1.0,
                tags={"reason": "no_state"},
            )
            in mock_incr.call_args_list
        )

    def test_discovery_metric_reports_healed_state_when_nothing_is_stale(self) -> None:
        state = HealSchedulerState(
            head_hash=PIPELINE.pipeline_hash,
            discovered_at=datetime.now(timezone.utc),
        )
        with (
            override_options({"issues.derived.check-task-count": 0}),
            patch("sentry.issues.derived.heal.load_state", return_value=state),
            patch("sentry.issues.derived.heal._discover_stale_pipeline_hashes", return_value=[]),
            patch(
                "sentry.issues.derived.heal.group_id_ranges_for_hash",
                return_value=GroupIdRangeResult(ranges=[], drained=True),
            ),
            patch("sentry.issues.derived.heal.metrics.incr") as mock_incr,
        ):
            self._heal()

        assert (
            call(
                "issues.derived.heal_stale_hash_discovery",
                sample_rate=1.0,
                tags={"reason": "stale_empty"},
            )
            in mock_incr.call_args_list
        )

    def test_head_change_enqueues_old_hash_without_discovery(self) -> None:
        old_hash = self._pick_stale_hash()
        state = HealSchedulerState(
            head_hash=old_hash,
            discovered_at=datetime.now(timezone.utc),
        )
        with (
            override_options(
                {
                    "issues.derived.heal-max-tasks": 1,
                    "issues.derived.check-task-count": 0,
                }
            ),
            patch("sentry.issues.derived.heal.load_state", return_value=state),
            patch("sentry.issues.derived.heal.save_state") as mock_save,
            patch("sentry.issues.derived.heal._discover_stale_pipeline_hashes") as discover,
            patch(
                "sentry.issues.derived.heal.group_id_ranges_for_hash",
                side_effect=[
                    GroupIdRangeResult(ranges=[], drained=True),
                    GroupIdRangeResult(ranges=[(10, 20)], drained=False),
                ],
            ),
            patch.object(self, "enqueue_regeneration"),
        ):
            self._heal()

        discover.assert_not_called()
        assert mock_save.call_args.args[0].stale == {old_hash: 20}

    def test_head_change_removes_current_hash_from_stale_state(self) -> None:
        old_hash = self._pick_stale_hash()
        state = HealSchedulerState(
            head_hash=old_hash,
            stale={PIPELINE.pipeline_hash: 10},
            discovered_at=datetime.now(timezone.utc),
        )
        with (
            override_options(
                {
                    "issues.derived.heal-max-tasks": 1,
                    "issues.derived.check-task-count": 0,
                }
            ),
            patch("sentry.issues.derived.heal.load_state", return_value=state),
            patch("sentry.issues.derived.heal.save_state") as mock_save,
            patch("sentry.issues.derived.heal._discover_stale_pipeline_hashes") as discover,
            patch(
                "sentry.issues.derived.heal.group_id_ranges_for_hash",
                side_effect=[
                    GroupIdRangeResult(ranges=[], drained=True),
                    GroupIdRangeResult(ranges=[(1, 2)], drained=False),
                ],
            ),
            patch.object(self, "enqueue_regeneration"),
        ):
            self._heal()

        discover.assert_not_called()
        assert mock_save.call_args.args[0].stale == {old_hash: 2}

    def test_discovery_timeout_still_schedules_null_and_saves_retryable_state(self) -> None:
        with (
            override_options({"issues.derived.heal-max-tasks": 1}),
            patch(
                "sentry.issues.derived.heal._discover_stale_pipeline_hashes",
                side_effect=OperationalError,
            ),
            patch(
                "sentry.issues.derived.heal.group_id_ranges_for_hash",
                return_value=GroupIdRangeResult(ranges=[(1, 2)], drained=False),
            ),
            patch.object(self, "enqueue_regeneration") as delay,
            patch("sentry.issues.derived.heal.save_state") as mock_save,
            patch("sentry.issues.derived.heal.logger") as mock_logger,
        ):
            self._heal()

        delay.assert_called_once()
        saved_state = mock_save.call_args.args[0]
        assert saved_state.discovered_at is None
        mock_logger.exception.assert_called_once_with(
            "heal_stale_derived_data.stale_hash_discovery_failed"
        )

    def test_range_selection_timeout_is_reported_and_other_hashes_continue(self) -> None:
        stale_hash = self._pick_stale_hash()
        state = HealSchedulerState(
            head_hash=PIPELINE.pipeline_hash,
            stale={stale_hash: 10},
            discovered_at=datetime.now(timezone.utc),
        )
        with (
            override_options(
                {
                    "issues.derived.heal-max-tasks": 1,
                    "issues.derived.check-task-count": 0,
                }
            ),
            patch("sentry.issues.derived.heal.load_state", return_value=state),
            patch(
                "sentry.issues.derived.heal.group_id_ranges_for_hash",
                side_effect=[
                    OperationalError,
                    GroupIdRangeResult(ranges=[(10, 20)], drained=False),
                ],
            ),
            patch.object(self, "enqueue_regeneration") as delay,
            patch("sentry.issues.derived.heal.metrics.incr") as mock_incr,
            patch("sentry.issues.derived.heal.logger") as mock_logger,
        ):
            self._heal()

        delay.assert_called_once()
        failure_log = mock_logger.exception.call_args
        assert failure_log.args == ("heal_stale_derived_data.range_selection_failed",)
        assert failure_log.kwargs["extra"]["hash_kind"] == "null"
        assert failure_log.kwargs["extra"]["elapsed"] >= 0
        assert (
            call(
                "issues.derived.heal_range_selection_failed",
                sample_rate=1.0,
                tags={"hash_kind": "null"},
            )
            in mock_incr.call_args_list
        )

    def test_range_selection_timeout_does_not_advance_mark(self) -> None:
        stale = self._pick_stale_hash()
        state = HealSchedulerState(
            head_hash=PIPELINE.pipeline_hash,
            stale={stale: 10},
            discovered_at=datetime.now(timezone.utc),
        )
        with (
            override_options(
                {
                    "issues.derived.heal-max-tasks": 1,
                    "issues.derived.check-task-count": 0,
                }
            ),
            patch("sentry.issues.derived.heal.load_state", return_value=state),
            patch(
                "sentry.issues.derived.heal.group_id_ranges_for_hash",
                side_effect=[
                    GroupIdRangeResult(ranges=[], drained=True),
                    OperationalError,
                ],
            ),
            patch.object(self, "enqueue_regeneration") as delay,
            patch("sentry.issues.derived.heal.logger") as mock_logger,
        ):
            self._heal()

        assert state.stale == {stale: 10}
        delay.assert_not_called()
        failure_log = mock_logger.exception.call_args
        assert failure_log.args == ("heal_stale_derived_data.range_selection_failed",)
        assert failure_log.kwargs["extra"]["hash_kind"] == "stale"
        assert failure_log.kwargs["extra"]["pipeline_hash"] == stale
        assert failure_log.kwargs["extra"]["group_id_lower_bound"] == 10
        assert failure_log.kwargs["extra"]["elapsed"] >= 0

    def test_discovered_hashes_are_saved_before_range_selection(self) -> None:
        stale_hash = self._pick_stale_hash()
        with (
            patch(
                "sentry.issues.derived.heal._discover_stale_pipeline_hashes",
                return_value=[stale_hash],
            ),
            patch(
                "sentry.issues.derived.heal.group_id_ranges_for_hash",
                side_effect=RuntimeError("range selection timed out"),
            ),
            patch("sentry.issues.derived.heal.save_state") as mock_save,
        ):
            with pytest.raises(RuntimeError):
                self._heal()

        mock_save.assert_called_once()
        saved_state = mock_save.call_args.args[0]
        assert saved_state.head_hash == PIPELINE.pipeline_hash
        assert saved_state.stale == {stale_hash: 0}

    def test_non_empty_state_skips_discovery_and_advances_across_runs(self) -> None:
        stale_hash = self._pick_stale_hash()
        state = HealSchedulerState(
            head_hash=PIPELINE.pipeline_hash,
            stale={stale_hash: 10},
            discovered_at=datetime.now(timezone.utc),
        )
        with (
            override_options(
                {
                    "issues.derived.heal-max-tasks": 1,
                    "issues.derived.check-task-count": 0,
                }
            ),
            patch("sentry.issues.derived.heal.load_state", return_value=state),
            patch("sentry.issues.derived.heal.save_state") as first_save,
            patch("sentry.issues.derived.heal._discover_stale_pipeline_hashes") as discover,
            patch(
                "sentry.issues.derived.heal.group_id_ranges_for_hash",
                side_effect=[
                    GroupIdRangeResult(ranges=[], drained=True),
                    GroupIdRangeResult(ranges=[(10, 20)], drained=False),
                ],
            ) as first_ranges,
            patch.object(self, "enqueue_regeneration"),
        ):
            self._heal()

        persisted = first_save.call_args.args[0].copy(deep=True)
        assert persisted.stale == {stale_hash: 20}
        assert first_ranges.call_args_list[1].kwargs["group_id_lower_bound"] == 10
        discover.assert_not_called()

        with (
            override_options(
                {
                    "issues.derived.heal-max-tasks": 1,
                    "issues.derived.check-task-count": 0,
                }
            ),
            patch("sentry.issues.derived.heal.load_state", return_value=persisted),
            patch("sentry.issues.derived.heal.save_state") as second_save,
            patch("sentry.issues.derived.heal._discover_stale_pipeline_hashes") as discover,
            patch(
                "sentry.issues.derived.heal.group_id_ranges_for_hash",
                side_effect=[
                    GroupIdRangeResult(ranges=[], drained=True),
                    GroupIdRangeResult(ranges=[(20, 30)], drained=False),
                ],
            ) as second_ranges,
            patch.object(self, "enqueue_regeneration"),
        ):
            self._heal()

        assert second_save.call_args.args[0].stale == {stale_hash: 30}
        assert second_ranges.call_args_list[1].kwargs["group_id_lower_bound"] == 20
        discover.assert_not_called()

    def test_drained_hash_is_removed_then_rediscovered(self) -> None:
        stale_hash = self._pick_stale_hash()
        state = HealSchedulerState(
            head_hash=PIPELINE.pipeline_hash,
            stale={stale_hash: 10},
            discovered_at=datetime.now(timezone.utc),
        )
        options = {
            "issues.derived.heal-max-tasks": 1,
            "issues.derived.check-task-count": 0,
        }
        with (
            override_options(options),
            patch("sentry.issues.derived.heal.load_state", return_value=state),
            patch("sentry.issues.derived.heal.save_state") as first_save,
            patch("sentry.issues.derived.heal._discover_stale_pipeline_hashes") as discover,
            patch(
                "sentry.issues.derived.heal.group_id_ranges_for_hash",
                return_value=GroupIdRangeResult(ranges=[], drained=True),
            ),
        ):
            self._heal()

        retired = first_save.call_args.args[0].copy(deep=True)
        assert retired.stale == {}
        discover.assert_not_called()

        with (
            override_options(options),
            patch("sentry.issues.derived.heal.load_state", return_value=retired),
            patch("sentry.issues.derived.heal.save_state"),
            patch(
                "sentry.issues.derived.heal._discover_stale_pipeline_hashes",
                return_value=[stale_hash],
            ) as discover,
            patch(
                "sentry.issues.derived.heal.group_id_ranges_for_hash",
                side_effect=[
                    GroupIdRangeResult(ranges=[], drained=True),
                    GroupIdRangeResult(ranges=[(1, 2)], drained=False),
                ],
            ),
            patch.object(self, "enqueue_regeneration"),
        ):
            self._heal()

        discover.assert_called_once_with(PIPELINE.pipeline_hash, 5)

    def test_null_starts_at_zero_without_rewriting_state(self) -> None:
        stale_hash = self._pick_stale_hash()
        state = HealSchedulerState(
            head_hash=PIPELINE.pipeline_hash,
            stale={stale_hash: 50},
            discovered_at=datetime.now(timezone.utc),
        )
        with (
            override_options({"issues.derived.heal-max-tasks": 1}),
            patch("sentry.issues.derived.heal.load_state", return_value=state),
            patch("sentry.issues.derived.heal.save_state") as mock_save,
            patch(
                "sentry.issues.derived.heal.group_id_ranges_for_hash",
                return_value=GroupIdRangeResult(ranges=[(1, 2)], drained=False),
            ) as mock_ranges,
            patch.object(self, "enqueue_regeneration"),
        ):
            self._heal()

        mock_ranges.assert_called_once_with(
            None,
            range_size=500,
            max_ranges=1,
            group_id_lower_bound=0,
        )
        mock_save.assert_not_called()
        assert state.stale == {stale_hash: 50}

    def test_mark_is_saved_before_scheduling_the_next_hash(self) -> None:
        hash_a = self._pick_stale_hash("a")
        hash_b = self._pick_stale_hash("b")
        state = HealSchedulerState(
            head_hash=PIPELINE.pipeline_hash,
            stale={hash_a: 10, hash_b: 10},
            discovered_at=datetime.now(timezone.utc),
        )
        with (
            override_options(
                {
                    "issues.derived.heal-max-tasks": 2,
                    "issues.derived.check-task-count": 0,
                }
            ),
            patch("sentry.issues.derived.heal.load_state", return_value=state),
            patch("sentry.issues.derived.heal.save_state") as mock_save,
            patch(
                "sentry.issues.derived.heal.group_id_ranges_for_hash",
                side_effect=[
                    GroupIdRangeResult(ranges=[], drained=True),
                    GroupIdRangeResult(ranges=[(10, 20)], drained=False),
                    RuntimeError("range selection failed"),
                ],
            ),
            patch.object(self, "enqueue_regeneration"),
        ):
            with pytest.raises(RuntimeError):
                self._heal()

        mock_save.assert_called_once_with(state)
        assert state.stale == {hash_a: 20, hash_b: 10}

    def test_marks_are_saved_when_check_fan_out_fails(self) -> None:
        stale_hash = self._pick_stale_hash()
        state = HealSchedulerState(
            head_hash=PIPELINE.pipeline_hash,
            stale={stale_hash: 10},
            discovered_at=datetime.now(timezone.utc),
        )
        with (
            override_options(
                {
                    "issues.derived.heal-max-tasks": 2,
                    "issues.derived.check-task-count": 1,
                }
            ),
            patch("sentry.issues.derived.heal.load_state", return_value=state),
            patch("sentry.issues.derived.heal.save_state") as mock_save,
            patch(
                "sentry.issues.derived.heal.group_id_ranges_for_hash",
                side_effect=[
                    GroupIdRangeResult(ranges=[], drained=True),
                    GroupIdRangeResult(ranges=[(10, 20)], drained=False),
                ],
            ),
            patch.object(self, "enqueue_regeneration"),
            patch(
                "sentry.issues.derived.heal._pick_random_fresh_group_ranges",
                side_effect=RuntimeError("check selection failed"),
            ),
        ):
            with pytest.raises(RuntimeError):
                self._heal()

        mock_save.assert_called_once_with(state)
        assert state.stale == {stale_hash: 20}

    def test_no_stale_data(self) -> None:
        groups = self.create_unprocessed_groups(2)
        for g in groups:
            process_group_log(g.id)

        group_ids = sorted(group.id for group in groups)
        with (
            patch("sentry.issues.derived.heal.random.randint", return_value=group_ids[0]),
            patch.object(self, "enqueue_regeneration") as mock_regenerate,
            patch.object(self, "enqueue_check") as mock_check,
        ):
            self._heal()

        mock_regenerate.assert_not_called()
        # One anchor + contiguous fan-out; 2 groups fit in a single default batch.
        mock_check.assert_called_once_with(
            group_id_start=group_ids[0],
            group_id_end=group_ids[-1] + 1,
        )

    def test_schedules_contiguous_ranges_from_one_anchor(self) -> None:
        groups = self.create_unprocessed_groups(4)
        group_ids = sorted(group.id for group in groups)
        for group_id in group_ids:
            process_group_log(group_id)

        with (
            override_options(
                {
                    "issues.derived.check-task-count": 2,
                    "issues.derived.heal-batch-size": 2,
                }
            ),
            patch("sentry.issues.derived.heal.random.randint", return_value=group_ids[0]),
            patch.object(self, "enqueue_check") as mock_check,
        ):
            self._heal()

        assert mock_check.call_args_list == [
            call(group_id_start=group_ids[0], group_id_end=group_ids[1] + 1),
            call(group_id_start=group_ids[2], group_id_end=group_ids[3] + 1),
        ]

    def test_respects_killswitch(self) -> None:
        groups = self.create_unprocessed_groups(1)
        process_group_log(groups[0].id)
        GroupDerivedData.objects.filter(group_id=groups[0].id).update(
            pipeline_hash=self._pick_stale_hash()
        )

        with (
            override_options({"issues.derived.heal-enabled": False}),
            patch.object(self, "enqueue_regeneration") as mock_delay,
        ):
            self._heal()

        mock_delay.assert_not_called()

    def test_bails_on_invalid_batch_configuration(self) -> None:
        groups = self.create_unprocessed_groups(1)
        process_group_log(groups[0].id)
        GroupDerivedData.objects.filter(group_id=groups[0].id).update(
            pipeline_hash=self._pick_stale_hash()
        )

        with (
            override_options({"issues.derived.heal-max-tasks": 0}),
            patch.object(self, "enqueue_regeneration") as mock_delay,
            patch.object(self, "enqueue_check") as mock_check,
        ):
            self._heal()

        # Neither healing nor the "nothing to heal" check fan-out should fire.
        mock_delay.assert_not_called()
        mock_check.assert_not_called()

    def test_dispatches_one_task_per_stale_hash(self) -> None:
        groups = self.create_unprocessed_groups(3)
        group_ids = sorted(g.id for g in groups)

        for gid in group_ids:
            process_group_log(gid)

        hash_a = self._pick_stale_hash("0")
        hash_b = self._pick_stale_hash("y")
        GroupDerivedData.objects.filter(group_id__in=group_ids[:2]).update(pipeline_hash=hash_a)
        GroupDerivedData.objects.filter(group_id=group_ids[2]).update(pipeline_hash=hash_b)

        with patch.object(self, "enqueue_regeneration") as mock_delay:
            self._heal()

        # Ranges are computed per hash, so each hash gets its own task targeting
        # just that hash.
        assert [c.kwargs["target_hash"] for c in mock_delay.call_args_list] == [hash_a, hash_b]
        assert [
            (c.kwargs["group_id_start"], c.kwargs["group_id_end"])
            for c in mock_delay.call_args_list
        ] == [(group_ids[0], group_ids[1] + 1), (group_ids[2], group_ids[2] + 1)]

    def test_null_range_does_not_overlap_stale_hash_range(self) -> None:
        # NULL and stale-hash rows interleave in ID space, so the ranges overlap.
        # Each task must be scoped so the overlap isn't processed twice.
        groups = self.create_unprocessed_groups(4)
        group_ids = sorted(g.id for g in groups)
        for gid in group_ids:
            process_group_log(gid)

        stale = self._pick_stale_hash()
        GroupDerivedData.objects.filter(group_id__in=group_ids[::2]).update(pipeline_hash=None)
        GroupDerivedData.objects.filter(group_id__in=group_ids[1::2]).update(pipeline_hash=stale)

        with patch.object(self, "enqueue_regeneration") as mock_delay:
            self._heal()

        # NULL is scheduled first — it means an explicit invalidation. The two
        # ranges overlap, but their targets are disjoint.
        assert [
            (
                c.kwargs["target_hash"],
                c.kwargs["group_id_start"],
                c.kwargs["group_id_end"],
            )
            for c in mock_delay.call_args_list
        ] == [
            (None, group_ids[0], group_ids[2] + 1),
            (stale, group_ids[1], group_ids[3] + 1),
        ]

    def test_null_hash_is_prioritized_over_stale_hashes(self) -> None:
        groups = self.create_unprocessed_groups(2)
        group_ids = sorted(g.id for g in groups)
        for gid in group_ids:
            process_group_log(gid)

        stale = self._pick_stale_hash()
        # The stale-hash row sorts first, so ordering alone wouldn't pick NULL.
        GroupDerivedData.objects.filter(group_id=group_ids[0]).update(pipeline_hash=stale)
        GroupDerivedData.objects.filter(group_id=group_ids[1]).update(pipeline_hash=None)

        with (
            override_options({"issues.derived.heal-max-tasks": 1}),
            patch.object(self, "enqueue_regeneration") as mock_delay,
        ):
            self._heal()

        mock_delay.assert_called_once()
        assert mock_delay.call_args.kwargs["target_hash"] is None
        assert mock_delay.call_args.kwargs["group_id_start"] == group_ids[1]

    def test_null_hash_is_always_stale_without_being_listed(self) -> None:
        groups = self.create_unprocessed_groups(1)
        process_group_log(groups[0].id)
        GroupDerivedData.objects.filter(group_id=groups[0].id).update(pipeline_hash=None)

        with patch.object(self, "enqueue_regeneration") as mock_delay:
            self._heal()

        mock_delay.assert_called_once()
        kwargs = mock_delay.call_args.kwargs
        assert kwargs["target_hash"] is None
        assert kwargs["group_id_start"] == groups[0].id
        assert kwargs["group_id_end"] == groups[0].id + 1

    def test_respects_max_tasks(self) -> None:
        groups = self.create_unprocessed_groups(3)
        group_ids = sorted(g.id for g in groups)
        for gid in group_ids:
            process_group_log(gid)
        stale = self._pick_stale_hash()
        GroupDerivedData.objects.filter(group_id__in=group_ids).update(pipeline_hash=stale)

        with (
            override_options(
                {
                    "issues.derived.heal-batch-size": 1,
                    "issues.derived.heal-max-tasks": 2,
                }
            ),
            patch.object(self, "enqueue_regeneration") as mock_delay,
            patch.object(self, "enqueue_check") as mock_check,
        ):
            self._heal()

        # 3 chunks would be produced, but max_tasks caps to 2 and the third group
        # is left for the next invocation rather than folded into the last range.
        assert [
            (c.kwargs["group_id_start"], c.kwargs["group_id_end"])
            for c in mock_delay.call_args_list
        ] == [(group_ids[0], group_ids[1]), (group_ids[1], group_ids[2])]
        # Budget fully consumed by heal work, so no consistency checks fire.
        mock_check.assert_not_called()

    def test_schedules_checks_with_leftover_heal_budget(self) -> None:
        groups = self.create_unprocessed_groups(4)
        group_ids = sorted(g.id for g in groups)
        for gid in group_ids:
            process_group_log(gid)

        stale = self._pick_stale_hash()
        # One stale row leaves heal budget free for checks on the fresh rows.
        GroupDerivedData.objects.filter(group_id=group_ids[0]).update(pipeline_hash=stale)

        with (
            override_options(
                {
                    "issues.derived.heal-batch-size": 1,
                    "issues.derived.heal-max-tasks": 3,
                    "issues.derived.check-task-count": 5,
                }
            ),
            patch("sentry.issues.derived.heal.random.randint", return_value=group_ids[1]),
            patch.object(self, "enqueue_regeneration") as mock_regenerate,
            patch.object(self, "enqueue_check") as mock_check,
        ):
            self._heal()

        mock_regenerate.assert_called_once_with(
            target_hash=stale,
            group_id_start=group_ids[0],
            group_id_end=group_ids[0] + 1,
        )
        # Remaining budget is 2, so checks are capped there even though
        # check-task-count is higher.
        assert mock_check.call_args_list == [
            call(group_id_start=group_ids[1], group_id_end=group_ids[1] + 1),
            call(group_id_start=group_ids[2], group_id_end=group_ids[2] + 1),
        ]


@with_feature("projects:issue-action-log-write-to-db")
class PickRandomFreshGroupRangesTest(DerivedDataHealTestBase):
    def test_returns_contiguous_ranges_from_anchor(self) -> None:
        groups = self.create_unprocessed_groups(6)
        group_ids = sorted(group.id for group in groups)
        for group_id in group_ids:
            process_group_log(group_id)

        with patch("sentry.issues.derived.heal.random.randint", return_value=group_ids[1]):
            result = _pick_random_fresh_group_ranges(
                PIPELINE.pipeline_hash, batch_size=2, task_count=2
            )

        # need=4 and 5 rows remain at/after anchor → no slide.
        assert result == [
            (group_ids[1], group_ids[2] + 1),
            (group_ids[3], group_ids[4] + 1),
        ]

    def test_slides_window_to_fill_near_upper_bound(self) -> None:
        groups = self.create_unprocessed_groups(5)
        group_ids = sorted(group.id for group in groups)
        for group_id in group_ids:
            process_group_log(group_id)

        with patch("sentry.issues.derived.heal.random.randint", return_value=group_ids[-1]):
            result = _pick_random_fresh_group_ranges(
                PIPELINE.pipeline_hash, batch_size=2, task_count=1
            )

        # need=2 but only 1 row forward of the anchor → last 2 fresh rows.
        assert result == [(group_ids[-2], group_ids[-1] + 1)]

    def test_slides_to_all_rows_when_table_smaller_than_need(self) -> None:
        groups = self.create_unprocessed_groups(3)
        group_ids = sorted(group.id for group in groups)
        for group_id in group_ids:
            process_group_log(group_id)

        with patch("sentry.issues.derived.heal.random.randint", return_value=group_ids[-1]):
            result = _pick_random_fresh_group_ranges(
                PIPELINE.pipeline_hash, batch_size=2, task_count=2
            )

        assert result == [
            (group_ids[0], group_ids[1] + 1),
            (group_ids[2], group_ids[2] + 1),
        ]

    def test_returns_empty_without_fresh_rows(self) -> None:
        assert (
            _pick_random_fresh_group_ranges(PIPELINE.pipeline_hash, batch_size=1000, task_count=5)
            == []
        )

    def test_caps_total_groups(self) -> None:
        groups = self.create_unprocessed_groups(3)
        group_ids = sorted(group.id for group in groups)
        for group_id in group_ids:
            process_group_log(group_id)

        with (
            patch("sentry.issues.derived.heal._MAX_CHECK_GROUPS", 2),
            patch("sentry.issues.derived.heal.random.randint", return_value=group_ids[0]),
        ):
            result = _pick_random_fresh_group_ranges(
                PIPELINE.pipeline_hash, batch_size=1000, task_count=5
            )

        assert result == [(group_ids[0], group_ids[1] + 1)]


class TestGroupIdRangeMath:
    def test_exact_ranges_use_lookahead_to_close_final_range(self) -> None:
        assert _exact_group_id_ranges([10, 20, 30, 40, 50], range_size=2, max_ranges=2) == [
            (10, 30),
            (30, 50),
        ]

    def test_exact_ranges_close_short_tail_after_last_group(self) -> None:
        assert _exact_group_id_ranges([10, 20, 30], range_size=2, max_ranges=5) == [
            (10, 30),
            (30, 31),
        ]

    def test_estimated_ranges_use_sample_density(self) -> None:
        assert _estimate_group_id_ranges([10, 20, 31], range_size=2, range_count=2) == [
            (10, 25),
            (25, 40),
        ]


@with_feature("projects:issue-action-log-write-to-db")
class GroupIdRangesForHashTest(DerivedDataHealTestBase):
    HASH = "a" * 16
    OTHER_HASH = "b" * 16

    def _seed(self, count: int, pipeline_hash: str | None) -> list[int]:
        groups = self.create_unprocessed_groups(count)
        for group in groups:
            GroupDerivedData.objects.create(group_id=group.id, pipeline_hash=pipeline_hash)
        return sorted(group.id for group in groups)

    def test_no_matching_rows(self) -> None:
        self._seed(2, self.OTHER_HASH)

        assert group_id_ranges_for_hash(
            self.HASH, range_size=2, max_ranges=5
        ) == GroupIdRangeResult(ranges=[], drained=True)
        assert group_id_ranges_for_hash(None, range_size=2, max_ranges=5) == GroupIdRangeResult(
            ranges=[], drained=True
        )

    def test_query_has_statement_timeout(self) -> None:
        with patch("sentry.issues.derived.heal.statement_timeout") as timeout:
            group_id_ranges_for_hash(self.HASH, range_size=2, max_ranges=5)

        query_timeout = timeout.call_args.args[1]
        assert timedelta(0) < query_timeout <= timedelta(seconds=40)

    def test_short_tail_is_one_range(self) -> None:
        null_ids = self._seed(3, None)
        hash_ids = self._seed(3, self.HASH)

        # Fewer rows than range_size, for both the NULL and the concrete-hash
        # predicate, and neither picks up the other's rows.
        assert group_id_ranges_for_hash(None, range_size=10, max_ranges=5).ranges == [
            (null_ids[0], null_ids[-1] + 1)
        ]
        assert group_id_ranges_for_hash(self.HASH, range_size=10, max_ranges=5).ranges == [
            (hash_ids[0], hash_ids[-1] + 1)
        ]

    def test_exact_chunk_boundaries(self) -> None:
        group_ids = self._seed(5, self.HASH)

        assert group_id_ranges_for_hash(self.HASH, range_size=2, max_ranges=5).ranges == [
            (group_ids[0], group_ids[2]),
            (group_ids[2], group_ids[4]),
            (group_ids[4], group_ids[4] + 1),
        ]

    def test_truncates_to_max_ranges(self) -> None:
        group_ids = self._seed(5, self.HASH)

        # The 5th group is left out rather than folded into an oversized last range.
        assert group_id_ranges_for_hash(self.HASH, range_size=2, max_ranges=2).ranges == [
            (group_ids[0], group_ids[2]),
            (group_ids[2], group_ids[4]),
        ]

    def test_truncates_full_tail_to_max_ranges(self) -> None:
        group_ids = self._seed(6, self.HASH)

        assert group_id_ranges_for_hash(self.HASH, range_size=2, max_ranges=2).ranges == [
            (group_ids[0], group_ids[2]),
            (group_ids[2], group_ids[4]),
        ]

    def test_invalid_chunking(self) -> None:
        self._seed(2, self.HASH)

        assert group_id_ranges_for_hash(
            self.HASH, range_size=0, max_ranges=5
        ) == GroupIdRangeResult(ranges=[], drained=False)
        assert group_id_ranges_for_hash(
            self.HASH, range_size=2, max_ranges=0
        ) == GroupIdRangeResult(ranges=[], drained=False)

    def test_density_probes_share_one_query_budget(self) -> None:
        with (
            patch(
                "sentry.issues.derived.heal.time.monotonic",
                # Query deadline, metrics timer start, budget check, metrics timer end.
                side_effect=[0.0, 0.0, 41.0, 41.0],
            ),
            pytest.raises(OperationalError, match="query budget exceeded"),
        ):
            group_id_ranges_for_hash(self.HASH, range_size=2, max_ranges=5)

    def test_samples_local_density_for_approximate_ranges(self) -> None:
        groups = self.create_unprocessed_groups(81)
        all_group_ids = sorted(group.id for group in groups)
        matching_group_ids = [all_group_ids[index] for index in range(0, 81, 10)]
        for group_id in all_group_ids:
            GroupDerivedData.objects.create(
                group_id=group_id,
                pipeline_hash=self.HASH if group_id in matching_group_ids else self.OTHER_HASH,
            )

        with (
            patch("sentry.issues.derived.heal._MAX_EXACT_RANGE_ROWS", 0),
            patch("sentry.issues.derived.heal._RANGE_DENSITY_SAMPLE_SIZE", 2),
            patch("sentry.issues.derived.heal._RANGES_PER_DENSITY_SAMPLE", 2),
            patch("sentry.issues.derived.heal._MAX_RANGE_DENSITY_SAMPLES", 2),
        ):
            result = group_id_ranges_for_hash(self.HASH, range_size=4, max_ranges=4)

        assert len(result.ranges) == 4
        assert result.ranges == sorted(result.ranges)
        assert all(
            any(start <= group_id < end for start, end in result.ranges)
            for group_id in matching_group_ids
        )

    def test_low_volume_request_uses_exact_boundaries(self) -> None:
        group_ids = self._seed(5, self.HASH)

        with (
            patch("sentry.issues.derived.heal._MAX_EXACT_RANGE_ROWS", 10),
            patch("sentry.issues.derived.heal._RANGE_DENSITY_SAMPLE_SIZE", 2),
        ):
            result = group_id_ranges_for_hash(self.HASH, range_size=2, max_ranges=5)

        assert result.ranges == [
            (group_ids[0], group_ids[2]),
            (group_ids[2], group_ids[4]),
            (group_ids[4], group_ids[4] + 1),
        ]

    def test_lower_bound(self) -> None:
        group_ids = self._seed(3, self.HASH)

        result = group_id_ranges_for_hash(
            self.HASH,
            range_size=2,
            max_ranges=5,
            group_id_lower_bound=group_ids[1],
        )

        assert result.ranges == [(group_ids[1], group_ids[2] + 1)]


@with_feature("projects:issue-action-log-write-to-db")
class RegenerateStaleDerivedDataBatchTest(DerivedDataHealTestBase):
    @staticmethod
    def _regenerate(
        *,
        group_id_start: int,
        group_id_end: int,
        target_hash: str | None,
        resume_generated_at: str | None = None,
        resume_pipeline_hash: str | None = None,
        rows_found_before: int = 0,
        range_overflowed: bool = False,
    ) -> RegenerationResult:
        return regenerate_stale_derived_data_batch(
            group_id_start=group_id_start,
            group_id_end=group_id_end,
            target_hash=target_hash,
            timeout=REGENERATION_TIMEOUT,
            resume_generated_at=resume_generated_at,
            resume_pipeline_hash=resume_pipeline_hash,
            rows_found_before=rows_found_before,
            range_overflowed=range_overflowed,
        )

    @staticmethod
    def _stale() -> str:
        return "0" * 16 if PIPELINE.pipeline_hash != "0" * 16 else "z" * 16

    def test_rebuilds_stale_rows(self) -> None:
        groups = self.create_unprocessed_groups(2)
        group_ids = sorted(g.id for g in groups)

        for gid in group_ids:
            process_group_log(gid)

        stale = self._stale()
        GroupDerivedData.objects.filter(group_id__in=group_ids).update(pipeline_hash=stale)

        self._regenerate(
            target_hash=stale,
            group_id_start=group_ids[0],
            group_id_end=group_ids[-1] + 1,
        )

        for gid in group_ids:
            gdd = GroupDerivedData.objects.get(group_id=gid)
            assert gdd.pipeline_hash == PIPELINE.pipeline_hash

    def test_rebuilds_null_hash_rows_when_target_is_none(self) -> None:
        groups = self.create_unprocessed_groups(1)
        gid = groups[0].id
        process_group_log(gid)
        GroupDerivedData.objects.filter(group_id=gid).update(pipeline_hash=None)

        self._regenerate(
            target_hash=None,
            group_id_start=gid,
            group_id_end=gid + 1,
        )

        gdd = GroupDerivedData.objects.get(group_id=gid)
        assert gdd.pipeline_hash == PIPELINE.pipeline_hash

    def test_targets_only_the_given_hash(self) -> None:
        groups = self.create_unprocessed_groups(2)
        group_ids = sorted(g.id for g in groups)
        for gid in group_ids:
            process_group_log(gid)

        stale = self._stale()
        GroupDerivedData.objects.filter(group_id=group_ids[0]).update(pipeline_hash=None)
        GroupDerivedData.objects.filter(group_id=group_ids[1]).update(pipeline_hash=stale)

        self._regenerate(
            target_hash=stale,
            group_id_start=group_ids[0],
            group_id_end=group_ids[-1] + 1,
        )

        # The NULL row belongs to the task targeting None, not this one.
        assert GroupDerivedData.objects.get(group_id=group_ids[0]).pipeline_hash is None
        assert (
            GroupDerivedData.objects.get(group_id=group_ids[1]).pipeline_hash
            == PIPELINE.pipeline_hash
        )

    def test_skips_rows_no_longer_stale(self) -> None:
        # Row now has the current hash — the range query should return
        # nothing so build_and_promote is never called.
        groups = self.create_unprocessed_groups(1)
        gid = groups[0].id
        process_group_log(gid)

        with patch("sentry.issues.derived.promote.build_and_promote_derived_data") as mock_build:
            self._regenerate(
                target_hash=self._stale(),
                group_id_start=gid,
                group_id_end=gid + 1,
            )
        mock_build.assert_not_called()

    def test_reschedules_on_batch_timeout(self) -> None:
        groups = self.create_unprocessed_groups(3)
        group_ids = sorted(g.id for g in groups)
        for gid in group_ids:
            process_group_log(gid)

        stale = self._stale()
        GroupDerivedData.objects.filter(group_id__in=group_ids).update(pipeline_hash=stale)

        with (
            patch("sentry.issues.derived.promote.time") as mock_time,
            patch("sentry.issues.derived.promote.build_and_promote_derived_data") as mock_build,
        ):
            expired = REGENERATION_TIMEOUT.total_seconds() + 1
            # Helper start(), iter 1 remaining, iter 1 deadline check
            # (triggers reschedule after the first group).
            mock_time.monotonic.side_effect = [0.0, 0.0, expired]

            result = self._regenerate(
                target_hash=stale,
                group_id_start=group_ids[0],
                group_id_end=group_ids[-1] + 1,
            )

        mock_build.assert_called_once()
        assert result.continuation_reason == "batch_timeout"
        assert result.continuation is not None
        assert result.continuation.target_hash == stale
        assert result.continuation.group_id_start == group_ids[0] + 1
        assert result.continuation.group_id_end == group_ids[-1] + 1

    def test_reschedules_when_estimated_range_is_overfull(self) -> None:
        groups = self.create_unprocessed_groups(3)
        group_ids = sorted(g.id for g in groups)
        for gid in group_ids:
            process_group_log(gid)

        stale = self._stale()
        GroupDerivedData.objects.filter(group_id__in=group_ids).update(pipeline_hash=stale)

        with override_options({"issues.derived.heal-batch-size": 2}):
            result = self._regenerate(
                target_hash=stale,
                group_id_start=group_ids[0],
                group_id_end=group_ids[-1] + 1,
            )

        assert (
            GroupDerivedData.objects.get(group_id=group_ids[0]).pipeline_hash
            == PIPELINE.pipeline_hash
        )
        assert (
            GroupDerivedData.objects.get(group_id=group_ids[1]).pipeline_hash
            == PIPELINE.pipeline_hash
        )
        assert GroupDerivedData.objects.get(group_id=group_ids[2]).pipeline_hash == stale
        assert result.continuation_reason == "range_overflow"
        assert result.continuation == RegenerationRequest(
            target_hash=stale,
            group_id_start=group_ids[1] + 1,
            group_id_end=group_ids[-1] + 1,
            rows_found_before=2,
            range_overflowed=True,
        )

    def test_records_rows_found_across_overflow_retriggers(self) -> None:
        groups = self.create_unprocessed_groups(3)
        group_ids = sorted(g.id for g in groups)
        for gid in group_ids:
            process_group_log(gid)

        stale = self._stale()
        GroupDerivedData.objects.filter(group_id__in=group_ids).update(pipeline_hash=stale)

        with (
            override_options({"issues.derived.heal-batch-size": 2}),
            patch("sentry.issues.derived.heal.metrics.distribution") as distribution,
        ):
            result = self._regenerate(
                target_hash=stale,
                group_id_start=group_ids[0],
                group_id_end=group_ids[-1] + 1,
            )

            distribution.assert_not_called()
            assert result.continuation is not None
            self._regenerate(**vars(result.continuation))

        distribution.assert_called_once_with(
            "issues.derived.heal_range_rows_found",
            3,
            sample_rate=1.0,
            tags={"hash_kind": "stale", "range_overflowed": "true"},
        )

    def test_reschedules_on_group_log_timeout(self) -> None:
        groups = self.create_unprocessed_groups(2)
        group_ids = sorted(g.id for g in groups)
        for gid in group_ids:
            process_group_log(gid)

        stale = self._stale()
        GroupDerivedData.objects.filter(group_id__in=group_ids).update(pipeline_hash=stale)

        with (
            patch(
                "sentry.issues.derived.promote.build_and_promote_derived_data",
                side_effect=GroupLogTimeout(0),
            ),
        ):
            result = self._regenerate(
                target_hash=stale,
                group_id_start=group_ids[0],
                group_id_end=group_ids[-1] + 1,
            )

        assert result.continuation is not None
        # Resume from the SAME group on a per-group timeout.
        assert result.continuation.group_id_start == group_ids[0]
        assert result.continuation.target_hash == stale


@with_feature("projects:issue-action-log-write-to-db")
class DiscoverStalePipelineHashesTest(DerivedDataHealTestBase):
    def _seed_hashes(self, hashes: Sequence[str | None], per_hash: int = 1) -> None:
        for h in hashes:
            groups = self.create_unprocessed_groups(per_hash)
            for group in groups:
                GroupDerivedData.objects.create(group_id=group.id, pipeline_hash=h)

    def test_returns_empty_when_only_current_hash_present(self) -> None:
        current = PIPELINE.pipeline_hash
        self._seed_hashes([current, current, current])

        assert _discover_stale_pipeline_hashes(current, limit=5) == []

    def test_returns_empty_when_table_empty(self) -> None:
        assert _discover_stale_pipeline_hashes(PIPELINE.pipeline_hash, limit=5) == []

    def test_query_has_statement_timeout(self) -> None:
        with patch("sentry.issues.derived.heal.statement_timeout") as timeout:
            _discover_stale_pipeline_hashes(PIPELINE.pipeline_hash, limit=5)

        assert timeout.call_args.args[1] == timedelta(seconds=15)

    def test_excludes_null_pipeline_hash(self) -> None:
        current = PIPELINE.pipeline_hash
        self._seed_hashes([None, None])

        assert _discover_stale_pipeline_hashes(current, limit=5) == []

    def test_excludes_current_hash(self) -> None:
        current = PIPELINE.pipeline_hash
        stale_low = "0" * 16
        stale_high = "z" * 16
        self._seed_hashes([stale_low, current, stale_high])

        result = _discover_stale_pipeline_hashes(current, limit=5)
        assert current not in result
        assert set(result) == {stale_low, stale_high}

    def test_returns_distinct_hashes_across_many_duplicate_rows(self) -> None:
        current = PIPELINE.pipeline_hash
        stale = "0" * 16 if current != "0" * 16 else "1" * 16
        self._seed_hashes([stale], per_hash=25)

        assert _discover_stale_pipeline_hashes(current, limit=5) == [stale]

    def test_respects_limit(self) -> None:
        current = PIPELINE.pipeline_hash
        stale_hashes = [f"stale-{i:02d}" for i in range(5)]
        assert current not in stale_hashes
        self._seed_hashes(stale_hashes)

        result = _discover_stale_pipeline_hashes(current, limit=3)
        assert len(result) == 3
        assert result == sorted(result)
        assert set(result).issubset(set(stale_hashes))

    def test_returns_hashes_in_ascending_order(self) -> None:
        current = PIPELINE.pipeline_hash
        stale_hashes = ["c-hash", "a-hash", "b-hash"]
        assert current not in stale_hashes
        self._seed_hashes(stale_hashes)

        result = _discover_stale_pipeline_hashes(current, limit=10)
        assert result == ["a-hash", "b-hash", "c-hash"]

    def test_limit_honored_when_current_hash_appears_mid_walk(self) -> None:
        current = "m-current"
        stale_hashes = ["a-hash", "b-hash", "y-hash", "z-hash"]
        self._seed_hashes(stale_hashes + [current])

        result = _discover_stale_pipeline_hashes(current, limit=3)
        assert result == ["a-hash", "b-hash", "y-hash"]
