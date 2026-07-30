"""Unit tests for the checkpoint store — the read-merge-write.

This is where the guarantee that a checkpoint write cannot destroy a HITL edit
lives: checkpoint_data is a shared column, and the store is the only thing that
knows which keys belong to whom.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.services.execution.checkpointing import store
from src.services.execution.checkpointing.record import (
    CHECKPOINT_KEY,
    KIND_CREW,
    KIND_FLOW,
    LEGACY_CREW_KEY,
    build_unit,
)


class FakeRepo:
    """Stands in for ExecutionHistoryRepository around one column value."""

    def __init__(self, column=None):
        self.column = column
        self.status = "untouched"
        self.written = False

    async def get_checkpoint_data(self, job_id, group_ids=None):
        return self.column

    async def set_checkpoint_data(
        self, job_id, checkpoint_data, checkpoint_status=None
    ):
        self.column = checkpoint_data
        self.status = checkpoint_status
        self.written = True
        return True


@pytest.fixture
def repo():
    return FakeRepo()


def install(repo):
    """Patch the repository class and the session helper the store uses."""
    session = MagicMock()
    session.commit = AsyncMock()

    async def run_op(op):
        return await op(session)

    return (
        patch(
            "src.repositories.execution_history_repository.ExecutionHistoryRepository",
            return_value=repo,
        ),
        patch("src.utils.asyncio_utils.execute_db_operation_smart", side_effect=run_op),
        session,
    )


class TestRecordUnit:
    @pytest.mark.asyncio
    async def test_writes_the_record_under_its_own_key(self, repo):
        repo_patch, db_patch, session = install(repo)
        with repo_patch, db_patch:
            ok = await store.record_unit(
                "job-1", KIND_CREW, build_unit(0, "task", "out"), unit_count=2
            )

        assert ok is True
        record = repo.column[CHECKPOINT_KEY]
        assert record["kind"] == KIND_CREW
        assert record["unit_count"] == 2
        assert record["units"]["0"]["output_raw"] == "out"
        session.commit.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_marks_the_checkpoint_active(self, repo):
        repo_patch, db_patch, _ = install(repo)
        with repo_patch, db_patch:
            await store.record_unit("job-1", KIND_CREW, build_unit(0, "t", "o"))

        # A checkpoint the list endpoint cannot see is one nobody can resume.
        assert repo.status == "active"

    @pytest.mark.asyncio
    async def test_preserves_hitl_keys_in_the_shared_column(self):
        repo = FakeRepo({"edited_config": {"a": 1}, "ucmv_yaml_edits": "yaml"})
        repo_patch, db_patch, _ = install(repo)
        with repo_patch, db_patch:
            await store.record_unit("job-1", KIND_CREW, build_unit(0, "t", "o"))

        assert repo.column["edited_config"] == {"a": 1}
        assert repo.column["ucmv_yaml_edits"] == "yaml"
        assert CHECKPOINT_KEY in repo.column

    @pytest.mark.asyncio
    async def test_merges_onto_a_pre_unification_payload(self):
        repo = FakeRepo(
            {
                LEGACY_CREW_KEY: {
                    "task_count": 3,
                    "process": "sequential",
                    "completed": {"0": {"index": 0, "output_raw": "a"}},
                }
            }
        )
        repo_patch, db_patch, _ = install(repo)
        with repo_patch, db_patch:
            await store.record_unit(
                "job-1", KIND_CREW, build_unit(1, "t1", "b"), unit_count=3
            )

        record = repo.column[CHECKPOINT_KEY]
        # The migrated unit and the new one coexist; nothing was lost.
        assert set(record["units"]) == {"0", "1"}

    @pytest.mark.asyncio
    async def test_a_failure_is_swallowed(self, repo):
        repo_patch, _, _ = install(repo)
        with (
            repo_patch,
            patch(
                "src.utils.asyncio_utils.execute_db_operation_smart",
                side_effect=RuntimeError("db down"),
            ),
        ):
            ok = await store.record_unit("job-1", KIND_CREW, build_unit(0, "t", "o"))

        # Fail-open: a checkpoint failure must never fail the run.
        assert ok is False


class TestClear:
    @pytest.mark.asyncio
    async def test_removes_both_record_keys_and_the_status(self):
        repo = FakeRepo(
            {CHECKPOINT_KEY: {"units": {}}, LEGACY_CREW_KEY: {"completed": {}}}
        )
        repo_patch, db_patch, _ = install(repo)
        with repo_patch, db_patch:
            ok = await store.clear("job-1")

        assert ok is True
        assert repo.column is None
        assert repo.status is None

    @pytest.mark.asyncio
    async def test_keeps_hitl_keys(self):
        repo = FakeRepo({CHECKPOINT_KEY: {"units": {}}, "edited_config": {"a": 1}})
        repo_patch, db_patch, _ = install(repo)
        with repo_patch, db_patch:
            await store.clear("job-1")

        assert repo.column == {"edited_config": {"a": 1}}

    @pytest.mark.asyncio
    async def test_a_failure_is_swallowed(self, repo):
        repo_patch, _, _ = install(repo)
        with (
            repo_patch,
            patch(
                "src.utils.asyncio_utils.execute_db_operation_smart",
                side_effect=RuntimeError("db down"),
            ),
        ):
            assert await store.clear("job-1") is False


class TestReadRecord:
    @pytest.mark.asyncio
    async def test_normalises_on_read(self):
        repo = FakeRepo(
            {
                LEGACY_CREW_KEY: {
                    "task_count": 2,
                    "completed": {"0": {"index": 0, "output_raw": "a"}},
                }
            }
        )
        with patch(
            "src.repositories.execution_history_repository.ExecutionHistoryRepository",
            return_value=repo,
        ):
            record = await store.read_record(MagicMock(), "job-1")

        assert record["kind"] == KIND_CREW
        assert record["migrated_from_version"] == 0

    @pytest.mark.asyncio
    async def test_no_checkpoint_reads_as_none(self):
        repo = FakeRepo({"edited_config": {"a": 1}})
        with patch(
            "src.repositories.execution_history_repository.ExecutionHistoryRepository",
            return_value=repo,
        ):
            assert await store.read_record(MagicMock(), "job-1") is None


class TestWriteRecord:
    @pytest.mark.asyncio
    async def test_seeds_a_record_wholesale(self):
        repo = FakeRepo({"edited_config": {"a": 1}})
        record = {"version": 1, "kind": KIND_FLOW, "units": {"1": {}}}
        with patch(
            "src.repositories.execution_history_repository.ExecutionHistoryRepository",
            return_value=repo,
        ):
            ok = await store.write_record(
                MagicMock(), "job-2", record, checkpoint_status="active"
            )

        assert ok is True
        assert repo.column[CHECKPOINT_KEY] == record
        assert repo.column["edited_config"] == {"a": 1}
        assert repo.status == "active"
