"""
Additional coverage tests for repositories/mcp_repository.py
Covers missing lines: 59-64, 76-84, 89-91, 97-99, 109-122, 130-147, 187-202, 266-270, 283-293, 360, 375-378
"""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Session

from src.repositories.mcp_repository import (
    MCPServerRepository,
    MCPSettingsRepository,
    SyncMCPServerRepository,
)
from src.models.mcp_server import MCPServer
from src.models.mcp_settings import MCPSettings


class MockServer:
    def __init__(self, id=1, name="srv1", enabled=True, global_enabled=True, group_id=None):
        self.id = id
        self.name = name
        self.enabled = enabled
        self.global_enabled = global_enabled
        self.group_id = group_id


class MockSettings:
    def __init__(self, id=1, global_enabled=False, individual_enabled=True):
        self.id = id
        self.global_enabled = global_enabled
        self.individual_enabled = individual_enabled


class MockScalars:
    def __init__(self, results):
        self._results = results

    def first(self):
        return self._results[0] if self._results else None

    def all(self):
        return self._results


class MockResult:
    def __init__(self, results):
        self._scalars = MockScalars(results)

    def scalars(self):
        return self._scalars


@pytest.fixture
def async_session():
    s = AsyncMock(spec=AsyncSession)
    s.execute = AsyncMock()
    s.flush = AsyncMock()
    s.refresh = AsyncMock()
    s.rollback = AsyncMock()
    s.add = MagicMock()
    return s


@pytest.fixture
def server_repo(async_session):
    return MCPServerRepository(session=async_session)


@pytest.fixture
def settings_repo(async_session):
    return MCPSettingsRepository(session=async_session)


# --- find_global_enabled ---

@pytest.mark.asyncio
async def test_find_global_enabled_returns_results(server_repo, async_session):
    servers = [MockServer(id=1, enabled=True, global_enabled=True)]
    async_session.execute.return_value = MockResult(servers)
    result = await server_repo.find_global_enabled()
    assert result == servers
    async_session.execute.assert_called_once()


@pytest.mark.asyncio
async def test_find_global_enabled_empty(server_repo, async_session):
    async_session.execute.return_value = MockResult([])
    result = await server_repo.find_global_enabled()
    assert result == []


# --- find_by_names ---

@pytest.mark.asyncio
async def test_find_by_names_empty_list(server_repo, async_session):
    result = await server_repo.find_by_names([])
    assert result == []
    async_session.execute.assert_not_called()


@pytest.mark.asyncio
async def test_find_by_names_returns_matches(server_repo, async_session):
    servers = [MockServer(name="s1"), MockServer(name="s2")]
    async_session.execute.return_value = MockResult(servers)
    result = await server_repo.find_by_names(["s1", "s2"])
    assert result == servers


# --- find_by_name_and_group ---

@pytest.mark.asyncio
async def test_find_by_name_and_group_found(server_repo, async_session):
    srv = MockServer(name="srv", group_id="g1")
    async_session.execute.return_value = MockResult([srv])
    result = await server_repo.find_by_name_and_group("srv", "g1")
    assert result is srv


@pytest.mark.asyncio
async def test_find_by_name_and_group_not_found(server_repo, async_session):
    async_session.execute.return_value = MockResult([])
    result = await server_repo.find_by_name_and_group("noname", "g1")
    assert result is None


# --- find_base_by_name ---

@pytest.mark.asyncio
async def test_find_base_by_name_found(server_repo, async_session):
    srv = MockServer(name="base_srv", group_id=None)
    async_session.execute.return_value = MockResult([srv])
    result = await server_repo.find_base_by_name("base_srv")
    assert result is srv


@pytest.mark.asyncio
async def test_find_base_by_name_not_found(server_repo, async_session):
    async_session.execute.return_value = MockResult([])
    result = await server_repo.find_base_by_name("nobody")
    assert result is None


# --- list_for_group_scope ---

@pytest.mark.asyncio
async def test_list_for_group_scope_with_group(server_repo, async_session):
    servers = [MockServer(group_id="g1")]
    async_session.execute.return_value = MockResult(servers)
    result = await server_repo.list_for_group_scope("g1")
    assert result == servers


@pytest.mark.asyncio
async def test_list_for_group_scope_no_group(server_repo, async_session):
    servers = [MockServer(group_id=None)]
    async_session.execute.return_value = MockResult(servers)
    result = await server_repo.list_for_group_scope(None)
    assert result == servers


# --- find_by_names_group_scope ---

@pytest.mark.asyncio
async def test_find_by_names_group_scope_empty_names(server_repo, async_session):
    result = await server_repo.find_by_names_group_scope([], "g1")
    assert result == []
    async_session.execute.assert_not_called()


@pytest.mark.asyncio
async def test_find_by_names_group_scope_with_group(server_repo, async_session):
    servers = [MockServer(name="srv", group_id="g1")]
    async_session.execute.return_value = MockResult(servers)
    result = await server_repo.find_by_names_group_scope(["srv"], "g1")
    assert result == servers


@pytest.mark.asyncio
async def test_find_by_names_group_scope_no_group(server_repo, async_session):
    servers = [MockServer(name="base", group_id=None)]
    async_session.execute.return_value = MockResult(servers)
    result = await server_repo.find_by_names_group_scope(["base"], None)
    assert result == servers


# --- toggle_global_enabled ---

@pytest.mark.asyncio
async def test_toggle_global_enabled_not_found(server_repo):
    with patch.object(server_repo, 'get', new_callable=AsyncMock, return_value=None):
        result = await server_repo.toggle_global_enabled(999)
    assert result is None


@pytest.mark.asyncio
async def test_toggle_global_enabled_success(server_repo, async_session):
    srv = MockServer(global_enabled=True)
    with patch.object(server_repo, 'get', new_callable=AsyncMock, return_value=srv):
        result = await server_repo.toggle_global_enabled(1)
    assert result is srv
    assert srv.global_enabled is False
    async_session.flush.assert_called_once()
    async_session.refresh.assert_called_once_with(srv)


@pytest.mark.asyncio
async def test_toggle_global_enabled_error(server_repo, async_session):
    srv = MockServer(global_enabled=False)
    with patch.object(server_repo, 'get', new_callable=AsyncMock, return_value=srv):
        async_session.flush.side_effect = Exception("flush error")
        with patch("logging.error"):
            with pytest.raises(Exception, match="flush error"):
                await server_repo.toggle_global_enabled(1)
    async_session.rollback.assert_called_once()


# --- MCPSettings: update_individual_enabled ---

@pytest.mark.asyncio
async def test_update_individual_enabled_true(settings_repo, async_session):
    s = MockSettings(individual_enabled=False)
    with patch.object(settings_repo, 'get_settings', new_callable=AsyncMock, return_value=s):
        result = await settings_repo.update_individual_enabled(True)
    assert result is s
    assert s.individual_enabled is True
    async_session.flush.assert_called_once()
    async_session.refresh.assert_called_once_with(s)


@pytest.mark.asyncio
async def test_update_individual_enabled_false(settings_repo, async_session):
    s = MockSettings(individual_enabled=True)
    with patch.object(settings_repo, 'get_settings', new_callable=AsyncMock, return_value=s):
        result = await settings_repo.update_individual_enabled(False)
    assert s.individual_enabled is False


# --- MCPSettings: update_settings ---

@pytest.mark.asyncio
async def test_update_settings_both_none(settings_repo, async_session):
    s = MockSettings(global_enabled=False, individual_enabled=True)
    with patch.object(settings_repo, 'get_settings', new_callable=AsyncMock, return_value=s):
        result = await settings_repo.update_settings()
    # Nothing changed
    assert result is s
    assert s.global_enabled is False
    assert s.individual_enabled is True


@pytest.mark.asyncio
async def test_update_settings_global_only(settings_repo, async_session):
    s = MockSettings(global_enabled=False)
    with patch.object(settings_repo, 'get_settings', new_callable=AsyncMock, return_value=s):
        result = await settings_repo.update_settings(global_enabled=True)
    assert s.global_enabled is True


@pytest.mark.asyncio
async def test_update_settings_individual_only(settings_repo, async_session):
    s = MockSettings(individual_enabled=True)
    with patch.object(settings_repo, 'get_settings', new_callable=AsyncMock, return_value=s):
        result = await settings_repo.update_settings(individual_enabled=False)
    assert s.individual_enabled is False


@pytest.mark.asyncio
async def test_update_settings_both(settings_repo, async_session):
    s = MockSettings(global_enabled=False, individual_enabled=False)
    with patch.object(settings_repo, 'get_settings', new_callable=AsyncMock, return_value=s):
        result = await settings_repo.update_settings(global_enabled=True, individual_enabled=True)
    assert s.global_enabled is True
    assert s.individual_enabled is True


# --- SyncMCPServerRepository: find_global_enabled and find_by_names ---

def test_sync_find_global_enabled():
    db = MagicMock(spec=Session)
    query_chain = MagicMock()
    db.query.return_value = query_chain
    query_chain.filter.return_value = query_chain
    servers = [MockServer()]
    query_chain.all.return_value = servers
    repo = SyncMCPServerRepository(db=db)
    result = repo.find_global_enabled()
    assert result == servers


def test_sync_find_by_names_empty():
    db = MagicMock(spec=Session)
    repo = SyncMCPServerRepository(db=db)
    result = repo.find_by_names([])
    assert result == []
    db.query.assert_not_called()


def test_sync_find_by_names_with_names():
    db = MagicMock(spec=Session)
    query_chain = MagicMock()
    db.query.return_value = query_chain
    query_chain.filter.return_value = query_chain
    servers = [MockServer(name="s1")]
    query_chain.all.return_value = servers
    repo = SyncMCPServerRepository(db=db)
    result = repo.find_by_names(["s1"])
    assert result == servers
