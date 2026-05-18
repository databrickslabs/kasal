"""
Unit tests for GroupToolRepository.

Tests all CRUD methods, find operations, update operations, and error cases.
"""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from src.repositories.group_tool_repository import GroupToolRepository
from src.models.group_tool import GroupTool


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_session():
    """Return a mock AsyncSession with the helpers we need."""
    session = AsyncMock()
    session.execute = AsyncMock()
    session.add = MagicMock()
    session.flush = AsyncMock()
    session.refresh = AsyncMock()
    return session


def make_scalars_result(items):
    """Return a mock result whose .scalars().first() / .all() returns the given items."""
    scalars_mock = MagicMock()
    if isinstance(items, list):
        scalars_mock.all.return_value = items
        scalars_mock.first.return_value = items[0] if items else None
    else:
        scalars_mock.first.return_value = items
        scalars_mock.all.return_value = [items] if items is not None else []
    result_mock = MagicMock()
    result_mock.scalars.return_value = scalars_mock
    return result_mock


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def session():
    return make_session()


@pytest.fixture
def repo(session):
    return GroupToolRepository(session)


@pytest.fixture
def sample_group_tool():
    gt = MagicMock(spec=GroupTool)
    gt.id = 1
    gt.tool_id = 10
    gt.group_id = "group-abc"
    gt.enabled = True
    gt.config = {"key": "value"}
    return gt


# ---------------------------------------------------------------------------
# __init__
# ---------------------------------------------------------------------------

class TestInit:
    def test_stores_session(self, session):
        repo = GroupToolRepository(session)
        assert repo.session is session


# ---------------------------------------------------------------------------
# get_by_id
# ---------------------------------------------------------------------------

class TestGetById:
    @pytest.mark.asyncio
    async def test_returns_mapping_when_found(self, repo, session, sample_group_tool):
        session.execute.return_value = make_scalars_result(sample_group_tool)
        result = await repo.get_by_id(1)
        assert result is sample_group_tool
        session.execute.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_returns_none_when_not_found(self, repo, session):
        session.execute.return_value = make_scalars_result(None)
        result = await repo.get_by_id(999)
        assert result is None

    @pytest.mark.asyncio
    async def test_calls_execute_with_query(self, repo, session):
        session.execute.return_value = make_scalars_result(None)
        await repo.get_by_id(42)
        session.execute.assert_awaited_once()


# ---------------------------------------------------------------------------
# find_by_tool_and_group
# ---------------------------------------------------------------------------

class TestFindByToolAndGroup:
    @pytest.mark.asyncio
    async def test_returns_mapping_when_found(self, repo, session, sample_group_tool):
        session.execute.return_value = make_scalars_result(sample_group_tool)
        result = await repo.find_by_tool_and_group(10, "group-abc")
        assert result is sample_group_tool

    @pytest.mark.asyncio
    async def test_returns_none_when_not_found(self, repo, session):
        session.execute.return_value = make_scalars_result(None)
        result = await repo.find_by_tool_and_group(99, "no-such-group")
        assert result is None

    @pytest.mark.asyncio
    async def test_executes_query(self, repo, session):
        session.execute.return_value = make_scalars_result(None)
        await repo.find_by_tool_and_group(1, "g")
        session.execute.assert_awaited_once()


# ---------------------------------------------------------------------------
# list_for_group
# ---------------------------------------------------------------------------

class TestListForGroup:
    @pytest.mark.asyncio
    async def test_returns_all_mappings(self, repo, session, sample_group_tool):
        gt2 = MagicMock(spec=GroupTool)
        session.execute.return_value = make_scalars_result([sample_group_tool, gt2])
        result = await repo.list_for_group("group-abc")
        assert len(result) == 2

    @pytest.mark.asyncio
    async def test_returns_empty_list_when_none(self, repo, session):
        session.execute.return_value = make_scalars_result([])
        result = await repo.list_for_group("no-group")
        assert result == []

    @pytest.mark.asyncio
    async def test_returns_list_type(self, repo, session):
        session.execute.return_value = make_scalars_result([])
        result = await repo.list_for_group("g")
        assert isinstance(result, list)


# ---------------------------------------------------------------------------
# list_enabled_for_group
# ---------------------------------------------------------------------------

class TestListEnabledForGroup:
    @pytest.mark.asyncio
    async def test_returns_enabled_mappings(self, repo, session, sample_group_tool):
        session.execute.return_value = make_scalars_result([sample_group_tool])
        result = await repo.list_enabled_for_group("group-abc")
        assert len(result) == 1

    @pytest.mark.asyncio
    async def test_returns_empty_when_none_enabled(self, repo, session):
        session.execute.return_value = make_scalars_result([])
        result = await repo.list_enabled_for_group("group-abc")
        assert result == []

    @pytest.mark.asyncio
    async def test_executes_query(self, repo, session):
        session.execute.return_value = make_scalars_result([])
        await repo.list_enabled_for_group("g")
        session.execute.assert_awaited_once()


# ---------------------------------------------------------------------------
# create
# ---------------------------------------------------------------------------

class TestCreate:
    @pytest.mark.asyncio
    async def test_creates_and_returns_mapping(self, repo, session, sample_group_tool):
        session.refresh = AsyncMock()
        # Make add/flush set up the returned object
        with patch("src.repositories.group_tool_repository.GroupTool", return_value=sample_group_tool):
            result = await repo.create({"tool_id": 10, "group_id": "g"})
        assert result is sample_group_tool
        session.add.assert_called_once_with(sample_group_tool)
        session.flush.assert_awaited_once()
        session.refresh.assert_awaited_once_with(sample_group_tool)

    @pytest.mark.asyncio
    async def test_passes_payload_to_model(self, repo, session):
        captured = {}
        session.refresh = AsyncMock()

        def fake_init(**kwargs):
            captured.update(kwargs)
            gt = MagicMock(spec=GroupTool)
            return gt

        with patch("src.repositories.group_tool_repository.GroupTool", side_effect=fake_init):
            await repo.create({"tool_id": 5, "group_id": "grp", "enabled": True})

        assert captured["tool_id"] == 5
        assert captured["group_id"] == "grp"
        assert captured["enabled"] is True


# ---------------------------------------------------------------------------
# upsert
# ---------------------------------------------------------------------------

class TestUpsert:
    @pytest.mark.asyncio
    async def test_returns_existing_when_found(self, repo, session, sample_group_tool):
        """When the mapping already exists, upsert returns it without creating."""
        with patch.object(repo, "find_by_tool_and_group", new_callable=AsyncMock) as mock_find:
            mock_find.return_value = sample_group_tool
            result = await repo.upsert(10, "group-abc")
        assert result is sample_group_tool

    @pytest.mark.asyncio
    async def test_creates_when_not_found(self, repo, session, sample_group_tool):
        """When mapping does not exist, upsert creates it."""
        with patch.object(repo, "find_by_tool_and_group", new_callable=AsyncMock) as mock_find, \
             patch.object(repo, "create", new_callable=AsyncMock) as mock_create:
            mock_find.return_value = None
            mock_create.return_value = sample_group_tool
            result = await repo.upsert(10, "group-abc")
        assert result is sample_group_tool
        mock_create.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_merges_defaults_on_create(self, repo, session, sample_group_tool):
        """Defaults dict is merged into the create payload."""
        captured = {}

        async def fake_create(payload):
            captured.update(payload)
            return sample_group_tool

        with patch.object(repo, "find_by_tool_and_group", new_callable=AsyncMock) as mock_find, \
             patch.object(repo, "create", side_effect=fake_create):
            mock_find.return_value = None
            await repo.upsert(7, "g", defaults={"enabled": True, "config": {"k": "v"}})

        assert captured["tool_id"] == 7
        assert captured["group_id"] == "g"
        assert captured["enabled"] is True


# ---------------------------------------------------------------------------
# update_config
# ---------------------------------------------------------------------------

class TestUpdateConfig:
    @pytest.mark.asyncio
    async def test_returns_none_when_not_found(self, repo, session):
        with patch.object(repo, "find_by_tool_and_group", new_callable=AsyncMock) as mock_find:
            mock_find.return_value = None
            result = await repo.update_config(99, "ghost", {"k": "v"})
        assert result is None

    @pytest.mark.asyncio
    async def test_executes_update_and_returns_mapping(self, repo, session, sample_group_tool):
        session.execute.return_value = MagicMock()
        session.flush = AsyncMock()
        session.refresh = AsyncMock()
        with patch.object(repo, "find_by_tool_and_group", new_callable=AsyncMock) as mock_find:
            mock_find.return_value = sample_group_tool
            result = await repo.update_config(10, "group-abc", {"new_key": "new_val"})
        assert result is sample_group_tool
        session.execute.assert_awaited_once()
        session.flush.assert_awaited_once()
        session.refresh.assert_awaited_once_with(sample_group_tool)


# ---------------------------------------------------------------------------
# set_enabled
# ---------------------------------------------------------------------------

class TestSetEnabled:
    @pytest.mark.asyncio
    async def test_returns_none_when_not_found(self, repo, session):
        with patch.object(repo, "find_by_tool_and_group", new_callable=AsyncMock) as mock_find:
            mock_find.return_value = None
            result = await repo.set_enabled(99, "ghost", True)
        assert result is None

    @pytest.mark.asyncio
    async def test_updates_enabled_flag(self, repo, session, sample_group_tool):
        session.execute.return_value = MagicMock()
        session.flush = AsyncMock()
        session.refresh = AsyncMock()
        with patch.object(repo, "find_by_tool_and_group", new_callable=AsyncMock) as mock_find:
            mock_find.return_value = sample_group_tool
            result = await repo.set_enabled(10, "group-abc", False)
        assert result is sample_group_tool
        session.flush.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_can_set_enabled_true(self, repo, session, sample_group_tool):
        session.execute.return_value = MagicMock()
        session.flush = AsyncMock()
        session.refresh = AsyncMock()
        with patch.object(repo, "find_by_tool_and_group", new_callable=AsyncMock) as mock_find:
            mock_find.return_value = sample_group_tool
            result = await repo.set_enabled(10, "group-abc", True)
        assert result is sample_group_tool


# ---------------------------------------------------------------------------
# delete_mapping
# ---------------------------------------------------------------------------

class TestDeleteMapping:
    @pytest.mark.asyncio
    async def test_returns_rowcount_on_delete(self, repo, session):
        delete_result = MagicMock()
        delete_result.rowcount = 1
        session.execute.return_value = delete_result
        session.flush = AsyncMock()
        count = await repo.delete_mapping(10, "group-abc")
        assert count == 1
        session.flush.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_returns_zero_when_not_found(self, repo, session):
        delete_result = MagicMock()
        delete_result.rowcount = 0
        session.execute.return_value = delete_result
        session.flush = AsyncMock()
        count = await repo.delete_mapping(99, "no-group")
        assert count == 0

    @pytest.mark.asyncio
    async def test_executes_delete_statement(self, repo, session):
        delete_result = MagicMock()
        delete_result.rowcount = 1
        session.execute.return_value = delete_result
        session.flush = AsyncMock()
        await repo.delete_mapping(1, "g")
        session.execute.assert_awaited_once()
