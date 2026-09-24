from collections.abc import Sequence
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock, call, patch

from sentry.issues.action_log.publish import publish_action
from sentry.issues.action_log.types import ActionSource, GroupActionActor, ViewAction
from sentry.issues.derived.check import CheckId, CheckTimeout
from sentry.issues.derived.gate import GROUP_ACTION_LOG_BACKFILL_COMPLETED_OPTION
from sentry.issues.derived.heal import RegenerationRequest, RegenerationResult
from sentry.issues.derived.processing import process_group_log
from sentry.issues.derived.tasks import (
    BATCH_RETRIGGER_TIMEOUT,
    check_fresh_derived_data_batch,
    generate_project_derived_data,
    generate_project_derived_data_batch,
    heal_stale_derived_data,
    regenerate_stale_derived_data_batch,
)
from sentry.issues.derived.tasks_util import SpawnState
from sentry.issues.models.groupderiveddata import GroupDerivedData
from sentry.models.group import Group, GroupStatus
from sentry.taskworker.selfchain_idempotency import already_spawned, mark_spawned
from sentry.testutils.cases import TestCase
from sentry.testutils.helpers.features import with_feature
from sentry.testutils.helpers.options import override_options
from sentry.testutils.outbox import outbox_runner


class DerivedDataTaskTestBase(TestCase):
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
            # Delete the derived data created by publish so the task sees them as unprocessed
            GroupDerivedData.objects.filter(group_id=group.id).delete()
            groups.append(group)
        return groups


@with_feature("projects:issue-action-log-write-to-db")
class GenerateProjectDerivedDataStaleOnlyTest(DerivedDataTaskTestBase):
    def test_only_includes_stale_groups(self) -> None:
        groups = self.create_unprocessed_groups(3)
        group_ids = sorted(g.id for g in groups)

        for gid in group_ids:
            process_group_log(gid)

        # Make one group stale
        GroupDerivedData.objects.filter(group_id=group_ids[0]).update(pipeline_hash="stale")

        with patch.object(generate_project_derived_data_batch, "delay") as mock_delay:
            generate_project_derived_data(project_id=self.project.id, stale_only=True)

        mock_delay.assert_called_once()
        assert mock_delay.call_args[1]["group_id_start"] == group_ids[0]
        assert mock_delay.call_args[1]["group_id_end"] == group_ids[0] + 1

    def test_includes_null_hash_groups(self) -> None:
        groups = self.create_unprocessed_groups(2)
        group_ids = sorted(g.id for g in groups)

        for gid in group_ids:
            process_group_log(gid)

        GroupDerivedData.objects.filter(group_id=group_ids[0]).update(pipeline_hash=None)

        with patch.object(generate_project_derived_data_batch, "delay") as mock_delay:
            generate_project_derived_data(project_id=self.project.id, stale_only=True)

        mock_delay.assert_called_once()
        assert mock_delay.call_args[1]["group_id_start"] == group_ids[0]

    def test_excludes_current_hash_groups(self) -> None:
        groups = self.create_unprocessed_groups(2)
        group_ids = sorted(g.id for g in groups)

        for gid in group_ids:
            process_group_log(gid)

        # All groups have the current hash — nothing to do
        with patch.object(generate_project_derived_data_batch, "delay") as mock_delay:
            generate_project_derived_data(project_id=self.project.id, stale_only=True)

        mock_delay.assert_not_called()

    def test_excludes_groups_without_gdd(self) -> None:
        groups = self.create_unprocessed_groups(3)
        group_ids = sorted(g.id for g in groups)

        # Only process two groups — group_ids[2] has no GDD at all
        process_group_log(group_ids[0])
        process_group_log(group_ids[1])

        # Make one stale
        GroupDerivedData.objects.filter(group_id=group_ids[0]).update(pipeline_hash="stale")

        with patch.object(generate_project_derived_data_batch, "delay") as mock_delay:
            generate_project_derived_data(project_id=self.project.id, stale_only=True)

        # Only the stale group should be included, not the one missing GDD
        mock_delay.assert_called_once()
        assert mock_delay.call_args[1]["group_id_start"] == group_ids[0]
        assert mock_delay.call_args[1]["group_id_end"] == group_ids[0] + 1


@with_feature("projects:issue-action-log-write-to-db")
class GenerateProjectDerivedDataBatchResumeTest(DerivedDataTaskTestBase):
    def test_resume_generation_id_not_applied_when_start_group_filtered_out(self) -> None:
        # A resume ``GenerationId`` identifies a specific group. If that
        # group is no longer in the batch queryset (e.g. under stale_only
        # it was already rebuilt to the current hash), the resume must
        # be dropped — it must NOT get applied to whichever group happens
        # to be first, because the cached partial progress belongs to a
        # different group.
        groups = self.create_unprocessed_groups(2)
        group_ids = sorted(g.id for g in groups)
        group_a, group_b = group_ids

        for gid in group_ids:
            process_group_log(gid)

        # A is at the current hash (not stale); B is stale.
        GroupDerivedData.objects.filter(group_id=group_b).update(pipeline_hash="stale")

        resume_generated_at = datetime(2024, 1, 1, tzinfo=timezone.utc).isoformat()
        resume_pipeline_hash = "prevhash"

        with patch("sentry.issues.derived.promote.build_and_promote_derived_data") as mock_build:
            generate_project_derived_data_batch(
                project_id=self.project.id,
                group_id_start=group_a,
                group_id_end=group_b + 1,
                resume_generated_at=resume_generated_at,
                resume_pipeline_hash=resume_pipeline_hash,
                stale_only=True,
            )

        # Only B is processed (A is filtered by stale_only).
        mock_build.assert_called_once()
        call_kwargs = mock_build.call_args.kwargs
        assert mock_build.call_args.args[0] == group_b
        # And critically, B does NOT inherit the resume generation_id
        # that was built for A.
        assert call_kwargs["generation_id"] is None


@with_feature("projects:issue-action-log-write-to-db")
class GenerateProjectDerivedDataPaginationTest(DerivedDataTaskTestBase):
    def test_limits_page_to_max_tasks(self) -> None:
        groups = self.create_unprocessed_groups(3)
        group_ids = sorted(group.id for group in groups)

        with (
            override_options(
                {
                    "issues.derived.project-batch-size": 2,
                    "issues.derived.project-max-tasks": 1,
                }
            ),
            patch.object(generate_project_derived_data_batch, "delay") as mock_batch_delay,
            patch.object(generate_project_derived_data, "apply_async") as mock_project_delay,
        ):
            generate_project_derived_data(project_id=self.project.id)

        mock_batch_delay.assert_called_once_with(
            project_id=self.project.id,
            group_id_start=group_ids[0],
            group_id_end=group_ids[1] + 1,
            stale_only=False,
        )
        mock_project_delay.assert_called_once_with(
            kwargs={
                "project_id": self.project.id,
                "cursor_group_id": group_ids[1],
                "stale_only": False,
            },
            headers={"sentry-propagate-traces": False},
        )

    def test_schedules_the_next_page(self) -> None:
        groups = self.create_unprocessed_groups(3)
        group_ids = sorted(group.id for group in groups)

        with (
            patch("sentry.issues.derived.tasks._MAX_PROJECT_GROUPS", 2),
            patch.object(generate_project_derived_data_batch, "delay") as mock_batch_delay,
            patch.object(generate_project_derived_data, "apply_async") as mock_project_delay,
        ):
            generate_project_derived_data(project_id=self.project.id)

        mock_batch_delay.assert_called_once_with(
            project_id=self.project.id,
            group_id_start=group_ids[0],
            group_id_end=group_ids[1] + 1,
            stale_only=False,
        )
        mock_project_delay.assert_called_once_with(
            kwargs={
                "project_id": self.project.id,
                "cursor_group_id": group_ids[1],
                "stale_only": False,
            },
            headers={"sentry-propagate-traces": False},
        )

    def test_resumes_after_the_cursor(self) -> None:
        groups = self.create_unprocessed_groups(3)
        group_ids = sorted(group.id for group in groups)

        with (
            patch.object(generate_project_derived_data_batch, "delay") as mock_batch_delay,
            patch.object(generate_project_derived_data, "apply_async") as mock_project_delay,
        ):
            generate_project_derived_data(
                project_id=self.project.id,
                cursor_group_id=group_ids[1],
            )

        mock_batch_delay.assert_called_once_with(
            project_id=self.project.id,
            group_id_start=group_ids[2],
            group_id_end=group_ids[2] + 1,
            stale_only=False,
        )
        mock_project_delay.assert_not_called()

    @patch("taskbroker_client.state.current_task")
    def test_selfchain_skips_self_schedule_when_marked_during_work(
        self, mock_current_task: MagicMock
    ) -> None:
        # Entry guard passes; a concurrent delivery marks before we self-schedule the next page.
        groups = self.create_unprocessed_groups(3)
        group_ids = sorted(group.id for group in groups)
        mock_current_task.return_value = SimpleNamespace(id="proj-act-race")

        def mark_during_chunk(
            chunk_group_ids: Sequence[int], batch_size: int
        ) -> list[tuple[int, int]]:
            mark_spawned("generate_project_derived_data", "proj-act-race")
            return [(chunk_group_ids[0], chunk_group_ids[-1] + 1)]

        with (
            override_options(
                {
                    "issues.derived.project-batch-size": 2,
                    "issues.derived.project-max-tasks": 1,
                }
            ),
            patch(
                "sentry.issues.derived.tasks._chunk_group_ids_into_ranges",
                side_effect=mark_during_chunk,
            ),
            patch.object(generate_project_derived_data_batch, "delay") as mock_batch_delay,
            patch.object(generate_project_derived_data, "apply_async") as mock_project_delay,
        ):
            generate_project_derived_data(project_id=self.project.id)

        mock_batch_delay.assert_called_once_with(
            project_id=self.project.id,
            group_id_start=group_ids[0],
            group_id_end=group_ids[1] + 1,
            stale_only=False,
        )
        mock_project_delay.assert_not_called()

    @patch("taskbroker_client.state.current_task")
    def test_selfchain_marks_after_self_schedule(self, mock_current_task: MagicMock) -> None:
        groups = self.create_unprocessed_groups(3)
        group_ids = sorted(group.id for group in groups)
        mock_current_task.return_value = SimpleNamespace(id="proj-act-mark")

        with (
            override_options(
                {
                    "issues.derived.project-batch-size": 2,
                    "issues.derived.project-max-tasks": 1,
                }
            ),
            patch.object(generate_project_derived_data_batch, "delay"),
            patch.object(generate_project_derived_data, "apply_async") as mock_project_delay,
        ):
            generate_project_derived_data(project_id=self.project.id)

        mock_project_delay.assert_called_once()
        assert already_spawned("generate_project_derived_data", "proj-act-mark") is True
        call_kwargs: dict[str, Any] = mock_project_delay.call_args.kwargs["kwargs"]
        assert group_ids[1] == call_kwargs["cursor_group_id"]


class SpawnStateTest(TestCase):
    def test_roundtrip(self) -> None:
        spawn = SpawnState(SimpleNamespace(id="act-spawn-state"), "merge_groups")
        assert spawn.task_key == "merge_groups"
        assert spawn.activation_id == "act-spawn-state"
        assert spawn.already_spawned() is False

        spawn.mark_spawned()

        assert spawn.already_spawned() is True
        assert already_spawned(spawn.task_key, "act-spawn-state") is True

    def test_noop_without_activation(self) -> None:
        spawn = SpawnState(None, "merge_groups")
        assert spawn.task_key == "merge_groups"
        assert spawn.activation_id is None
        assert spawn.already_spawned() is False
        spawn.mark_spawned()
        assert already_spawned(spawn.task_key, "act-none") is False


class HealTaskAdapterTest(TestCase):
    def test_delegates_with_task_enqueue_callbacks(self) -> None:
        with patch("sentry.issues.derived.heal.heal_stale_derived_data") as heal:
            heal_stale_derived_data()

        heal.assert_called_once()
        assert heal.call_args.kwargs["enqueue_regeneration"].__name__ == "_enqueue_regeneration"
        assert heal.call_args.kwargs["enqueue_check"].__name__ == "_enqueue_fresh_check"


class RegenerateStaleDerivedDataBatchAdapterTest(TestCase):
    @patch("taskbroker_client.state.current_task")
    def test_enqueues_continuation_and_marks_activation(self, mock_current_task: MagicMock) -> None:
        mock_current_task.return_value = SimpleNamespace(id="regenerate-adapter")
        continuation = RegenerationRequest(
            target_hash="stale",
            group_id_start=2,
            group_id_end=3,
            rows_found_before=1,
        )
        result = RegenerationResult(
            processed={},
            total=1,
            continuation=continuation,
            continuation_reason="range_overflow",
        )

        with (
            patch(
                "sentry.issues.derived.heal.regenerate_stale_derived_data_batch",
                return_value=result,
            ) as regenerate,
            patch("sentry.issues.derived.tasks._enqueue_regeneration") as enqueue,
        ):
            regenerate_stale_derived_data_batch(
                target_hash="stale", group_id_start=1, group_id_end=3
            )

        regenerate.assert_called_once_with(
            target_hash="stale",
            group_id_start=1,
            group_id_end=3,
            timeout=BATCH_RETRIGGER_TIMEOUT,
            resume_generated_at=None,
            resume_pipeline_hash=None,
            rows_found_before=0,
            range_overflowed=False,
        )
        enqueue.assert_called_once_with(continuation)
        assert already_spawned("regenerate_stale_derived_data_batch", "regenerate-adapter")

    @patch("taskbroker_client.state.current_task")
    def test_duplicate_activation_skips_implementation(self, mock_current_task: MagicMock) -> None:
        mock_current_task.return_value = SimpleNamespace(id="regenerate-duplicate")
        mark_spawned("regenerate_stale_derived_data_batch", "regenerate-duplicate")

        with patch("sentry.issues.derived.heal.regenerate_stale_derived_data_batch") as regenerate:
            regenerate_stale_derived_data_batch(group_id_start=1, group_id_end=2)

        regenerate.assert_not_called()


@with_feature("projects:issue-action-log-write-to-db")
class CheckFreshDerivedDataBatchTest(DerivedDataTaskTestBase):
    def test_checks_only_fresh_rows_inline(self) -> None:
        groups = self.create_unprocessed_groups(3)
        group_ids = sorted(group.id for group in groups)
        for group_id in group_ids:
            process_group_log(group_id)
        GroupDerivedData.objects.filter(group_id=group_ids[1]).update(pipeline_hash="stale")

        with patch("sentry.issues.derived.tasks_util.metrics.incr") as mock_incr:
            check_fresh_derived_data_batch(
                group_id_start=group_ids[0],
                group_id_end=group_ids[-1] + 1,
            )

        assert mock_incr.call_args_list == [
            call(
                "issues.status_reconciliation.checked",
                sample_rate=1.0,
                tags={"result": "aligned", "source": "batch_check"},
            ),
            call("issues.derived.check_group", sample_rate=1.0, tags={"result": "success"}),
            call(
                "issues.status_reconciliation.checked",
                sample_rate=1.0,
                tags={"result": "aligned", "source": "batch_check"},
            ),
            call("issues.derived.check_group", sample_rate=1.0, tags={"result": "success"}),
        ]

    def test_reschedules_timed_out_group_with_check_id(self) -> None:
        group = self.create_unprocessed_groups(1)[0]
        derived = process_group_log(group.id)
        assert derived.pipeline_hash is not None
        check_id = CheckId(
            "invocation-id",
            group.id,
            derived.generated_at,
            derived.cursor_date,
            derived.cursor_id,
            derived.pipeline_hash,
        )

        with (
            patch(
                "sentry.issues.derived.check.check_derived_data",
                side_effect=CheckTimeout(check_id),
            ),
            patch.object(check_fresh_derived_data_batch, "delay") as mock_delay,
        ):
            check_fresh_derived_data_batch(
                group_id_start=group.id,
                group_id_end=group.id + 1,
            )

        mock_delay.assert_called_once_with(
            group_id_start=group.id,
            group_id_end=group.id + 1,
            resume_check_id="invocation-id",
            resume_generated_at=derived.generated_at.isoformat(),
            resume_cursor_date=derived.cursor_date.isoformat(),
            resume_cursor_id=derived.cursor_id,
            resume_pipeline_hash=derived.pipeline_hash,
            prior_runs=1,
        )

    def test_advances_after_check_retry_limit(self) -> None:
        group = self.create_unprocessed_groups(1)[0]
        derived = process_group_log(group.id)
        assert derived.pipeline_hash is not None
        check_id = CheckId(
            "invocation-id",
            group.id,
            derived.generated_at,
            derived.cursor_date,
            derived.cursor_id,
            derived.pipeline_hash,
        )

        with (
            patch(
                "sentry.issues.derived.check.check_derived_data",
                side_effect=CheckTimeout(check_id),
            ),
            patch("sentry.issues.derived.tasks._MAX_CHECK_RUNS", 1),
            patch.object(check_fresh_derived_data_batch, "delay") as mock_delay,
            patch("sentry.issues.derived.tasks_util.metrics.incr") as mock_incr,
        ):
            check_fresh_derived_data_batch(
                group_id_start=group.id,
                group_id_end=group.id + 2,
            )

        mock_delay.assert_called_once_with(
            group_id_start=group.id + 1,
            group_id_end=group.id + 2,
        )
        assert mock_incr.call_args_list == [
            call(
                "issues.status_reconciliation.checked",
                sample_rate=1.0,
                tags={"result": "aligned", "source": "batch_check"},
            ),
            call(
                "issues.derived.check_group",
                sample_rate=1.0,
                tags={"result": "no_result"},
            ),
        ]

    def test_records_status_inconsistency_for_backfilled_project(self) -> None:
        group = self.create_unprocessed_groups(1)[0]
        process_group_log(group.id)
        group.update(status=GroupStatus.IGNORED)
        self.project.update_option(GROUP_ACTION_LOG_BACKFILL_COMPLETED_OPTION, True)
        GroupDerivedData.objects.filter(group_id=group.id).update(
            data={"status": "open"},
        )

        with patch("sentry.issues.derived.check.metrics.incr") as mock_incr:
            check_fresh_derived_data_batch(
                group_id_start=group.id,
                group_id_end=group.id + 1,
            )

        assert (
            call(
                "issues.status_reconciliation.checked",
                sample_rate=1.0,
                tags={
                    "result": "diverged",
                    "derived_status": "open",
                    "actual_status": "closed",
                    "source": "batch_check",
                },
            )
            in mock_incr.call_args_list
        )

    def test_skips_status_check_when_not_derived_should_be_correct(self) -> None:
        group = self.create_unprocessed_groups(1)[0]
        process_group_log(group.id)
        group.update(status=GroupStatus.IGNORED)
        self.project.update_option(GROUP_ACTION_LOG_BACKFILL_COMPLETED_OPTION, False)
        GroupDerivedData.objects.filter(group_id=group.id).update(data={"status": "open"})

        with patch("sentry.issues.derived.check.record_status_consistency") as mock_record_status:
            check_fresh_derived_data_batch(
                group_id_start=group.id,
                group_id_end=group.id + 1,
            )

        mock_record_status.assert_not_called()

    @override_options({"issues.derived.status-consistency-check-enabled": False})
    def test_skips_status_check_when_option_disabled(self) -> None:
        group = self.create_unprocessed_groups(1)[0]
        process_group_log(group.id)
        group.update(status=GroupStatus.IGNORED)
        self.project.update_option(GROUP_ACTION_LOG_BACKFILL_COMPLETED_OPTION, True)
        GroupDerivedData.objects.filter(group_id=group.id).update(data={"status": "open"})

        with patch("sentry.issues.derived.check.record_status_consistency") as mock_record_status:
            check_fresh_derived_data_batch(
                group_id_start=group.id,
                group_id_end=group.id + 1,
            )

        mock_record_status.assert_not_called()

    def test_status_check_runs_before_derived_check_timeout(self) -> None:
        group = self.create_unprocessed_groups(1)[0]
        derived = process_group_log(group.id)
        assert derived.pipeline_hash is not None
        group.update(status=GroupStatus.IGNORED)
        self.project.update_option(GROUP_ACTION_LOG_BACKFILL_COMPLETED_OPTION, True)
        GroupDerivedData.objects.filter(group_id=group.id).update(data={"status": "open"})
        check_id = CheckId(
            "invocation-id",
            group.id,
            derived.generated_at,
            derived.cursor_date,
            derived.cursor_id,
            derived.pipeline_hash,
        )

        with (
            patch(
                "sentry.issues.derived.check.check_derived_data",
                side_effect=CheckTimeout(check_id),
            ),
            patch.object(check_fresh_derived_data_batch, "delay"),
            patch("sentry.issues.derived.check.record_status_consistency") as mock_record_status,
        ):
            check_fresh_derived_data_batch(
                group_id_start=group.id,
                group_id_end=group.id + 1,
            )

        mock_record_status.assert_called_once()
