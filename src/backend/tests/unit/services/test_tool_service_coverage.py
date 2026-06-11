"""
Coverage tests for services/tool_service.py
Covers missing lines: find_enabled, get_enabled_tools_for_group branches, get_tool_by_id, etc.
"""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch




@pytest.fixture(autouse=True)
def _clear_tool_list_cache():
    """The enabled-tools cache is module-global (PERF: burst polling);
    clear it around every test so suites stay independent."""
    from src.core.cache import tool_list_cache
    tool_list_cache._cache.clear()
    yield
    tool_list_cache._cache.clear()

def make_service():
    session = AsyncMock()
    with patch('src.services.tool_service.ToolRepository') as MockRepo:
        mock_repo = AsyncMock()
        MockRepo.return_value = mock_repo
        from src.services.tool_service import ToolService
        svc = ToolService(session)
        svc.repository = mock_repo
    return svc


def make_tool(id=1, name="search_tool", enabled=True, group_id=None):
    tool = MagicMock()
    tool.id = id
    tool.name = name
    tool.enabled = enabled
    tool.group_id = group_id
    tool.config = {"api_key": "test"}
    return tool


def make_group_ctx(primary_group_id=None):
    ctx = MagicMock()
    ctx.primary_group_id = primary_group_id
    return ctx


# ---- get_enabled_tools ----

@pytest.mark.asyncio
async def test_get_enabled_tools():
    svc = make_service()
    tools = [make_tool(id=1), make_tool(id=2)]
    svc.repository.find_enabled = AsyncMock(return_value=tools)
    with patch('src.services.tool_service.ToolResponse') as MockToolResp, \
         patch('src.services.tool_service.ToolListResponse') as MockListResp:
        MockToolResp.model_validate = MagicMock(side_effect=lambda t: MagicMock())
        MockListResp.return_value = MagicMock()
        result = await svc.get_enabled_tools()
    assert result is not None


# ---- get_enabled_tools_for_group ----

@pytest.mark.asyncio
async def test_get_enabled_tools_for_group_no_primary_group():
    svc = make_service()
    base_tools = [make_tool(id=1, group_id=None)]
    svc.repository.find_enabled = AsyncMock(return_value=base_tools)
    ctx = make_group_ctx(primary_group_id=None)

    with patch('src.services.tool_service.ToolResponse') as MockResp, \
         patch('src.services.tool_service.ToolListResponse') as MockList:
        MockResp.model_validate = MagicMock(side_effect=lambda t: MagicMock())
        MockList.return_value = MagicMock()
        result = await svc.get_enabled_tools_for_group(ctx)
    assert result is not None


@pytest.mark.asyncio
async def test_get_enabled_tools_for_group_with_primary_group():
    svc = make_service()
    base_tools = [make_tool(id=1, group_id=None), make_tool(id=2, group_id=None)]
    svc.repository.find_enabled = AsyncMock(return_value=base_tools)
    ctx = make_group_ctx(primary_group_id="g1")

    mapping1 = MagicMock()
    mapping1.tool_id = 1
    mapping1.config = {"extra": "value"}

    with patch('src.services.tool_service.GroupToolRepository') as MockGroupRepo, \
         patch('src.services.tool_service.ToolResponse') as MockResp, \
         patch('src.services.tool_service.ToolListResponse') as MockList:
        mock_group_repo = AsyncMock()
        mock_group_repo.list_enabled_for_group = AsyncMock(return_value=[mapping1])
        MockGroupRepo.return_value = mock_group_repo

        mock_tool_resp = MagicMock()
        mock_tool_resp.config = {}
        MockResp.model_validate = MagicMock(return_value=mock_tool_resp)
        MockList.return_value = MagicMock()

        result = await svc.get_enabled_tools_for_group(ctx)
    assert result is not None


@pytest.mark.asyncio
async def test_get_enabled_tools_for_group_merge_exception():
    """Test exception in config merge falls back gracefully."""
    svc = make_service()
    base_tools = [make_tool(id=1, group_id=None)]
    svc.repository.find_enabled = AsyncMock(return_value=base_tools)
    ctx = make_group_ctx(primary_group_id="g1")

    mapping1 = MagicMock()
    mapping1.tool_id = 1
    mapping1.config = None

    with patch('src.services.tool_service.GroupToolRepository') as MockGroupRepo, \
         patch('src.services.tool_service.ToolResponse') as MockResp, \
         patch('src.services.tool_service.ToolListResponse') as MockList:
        mock_group_repo = AsyncMock()
        mock_group_repo.list_enabled_for_group = AsyncMock(return_value=[mapping1])
        MockGroupRepo.return_value = mock_group_repo

        # Make ToolResponse.model_validate raise on first call (triggers fallback)
        call_count = [0]
        def side_validate(t):
            call_count[0] += 1
            if call_count[0] == 1:
                raise Exception("merge failed")
            return MagicMock()
        MockResp.model_validate = MagicMock(side_effect=side_validate)
        MockList.return_value = MagicMock()

        result = await svc.get_enabled_tools_for_group(ctx)
    assert result is not None


@pytest.mark.asyncio
async def test_get_enabled_tools_for_group_no_context():
    """Test with None group context."""
    svc = make_service()
    base_tools = [make_tool(id=1, group_id=None)]
    svc.repository.find_enabled = AsyncMock(return_value=base_tools)

    with patch('src.services.tool_service.ToolResponse') as MockResp, \
         patch('src.services.tool_service.ToolListResponse') as MockList:
        MockResp.model_validate = MagicMock(return_value=MagicMock())
        MockList.return_value = MagicMock()
        result = await svc.get_enabled_tools_for_group(None)
    assert result is not None
