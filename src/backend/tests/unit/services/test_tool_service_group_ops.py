"""
Coverage tests for src/services/tool_service.py targeting group-scoped operations
and uncovered branches.
Missing lines targeted: 42, 173, 195-196, 222-228, 244-257, 284-286, 306-307,
320-322, 347-349, 368-369, 381-383, 412-414, 441-442, 446-447, 488-491, 497-499,
523-525, 549-551, 563-564
"""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from src.services.tool_service import ToolService
from src.core.exceptions import NotFoundError, ForbiddenError, KasalError
from src.schemas.tool import ToolCreate, ToolUpdate, ToolResponse, ToggleResponse


def make_service():
    session = AsyncMock()
    with patch("src.services.tool_service.ToolRepository") as MockRepo:
        mock_repo = AsyncMock()
        MockRepo.return_value = mock_repo
        svc = ToolService(session)
        svc.repository = mock_repo
    return svc


def make_tool(id=1, title="TestTool", group_id=None, enabled=True, config=None):
    tool = MagicMock()
    tool.id = id
    tool.title = title
    tool.group_id = group_id
    tool.enabled = enabled
    tool.description = "A test tool"
    tool.icon = "icon"
    tool.config = config or {}
    return tool


def make_group_context(group_ids=None, primary_group_id="g1", group_email="group@example.com", valid=True):
    ctx = MagicMock()
    ctx.group_ids = group_ids or [primary_group_id]
    ctx.primary_group_id = primary_group_id
    ctx.group_email = group_email
    ctx.is_valid.return_value = valid
    return ctx


# ─── get_all_tools (line 42 - empty list) ────────────────────────────────────

@pytest.mark.asyncio
async def test_get_all_tools_empty():
    svc = make_service()
    svc.repository.list = AsyncMock(return_value=[])
    result = await svc.get_all_tools()
    assert result.count == 0
    assert result.tools == []


# ─── get_tool_by_id ─────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_get_tool_by_id_not_found(  ):
    svc = make_service()
    svc.repository.get = AsyncMock(return_value=None)
    with pytest.raises(NotFoundError):
        await svc.get_tool_by_id(999)


# ─── get_tool_with_group_check ───────────────────────────────────────────────

@pytest.mark.asyncio
async def test_get_tool_with_group_check_tool_not_found():
    svc = make_service()
    svc.repository.get = AsyncMock(return_value=None)
    ctx = make_group_context()
    with pytest.raises(NotFoundError):
        await svc.get_tool_with_group_check(999, ctx)


@pytest.mark.asyncio
async def test_get_tool_with_group_check_unauthorized_group():
    svc = make_service()
    tool = make_tool(id=1, group_id="other-group")
    svc.repository.get = AsyncMock(return_value=tool)
    ctx = make_group_context(group_ids=["my-group"])
    with pytest.raises(NotFoundError):
        await svc.get_tool_with_group_check(1, ctx)


@pytest.mark.asyncio
async def test_get_tool_with_group_check_no_group_context():
    svc = make_service()
    tool = make_tool(id=1, group_id="g1")
    svc.repository.get = AsyncMock(return_value=tool)
    with pytest.raises(NotFoundError):
        await svc.get_tool_with_group_check(1, None)


@pytest.mark.asyncio
async def test_get_tool_with_group_check_default_tool_always_accessible():
    svc = make_service()
    tool = make_tool(id=1, group_id=None)
    svc.repository.get = AsyncMock(return_value=tool)
    ctx = make_group_context()
    with patch("src.services.tool_service.ToolResponse") as mock_resp:
        mock_resp.model_validate.return_value = MagicMock(id=1)
        result = await svc.get_tool_with_group_check(1, ctx)
    mock_resp.model_validate.assert_called_once_with(tool)


# ─── create_tool ─────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_create_tool_success():
    svc = make_service()
    tool = make_tool()
    svc.repository.create = AsyncMock(return_value=tool)
    tool_data = MagicMock()
    tool_data.model_dump.return_value = {"title": "TestTool", "enabled": True}
    with patch("src.services.tool_service.ToolResponse") as mock_resp:
        mock_resp.model_validate.return_value = MagicMock(id=1)
        result = await svc.create_tool(tool_data)
    assert result is not None


@pytest.mark.asyncio
async def test_create_tool_raises_on_error():
    svc = make_service()
    svc.repository.create = AsyncMock(side_effect=Exception("DB error"))
    tool_data = MagicMock()
    tool_data.model_dump.return_value = {"title": "TestTool"}
    with pytest.raises(KasalError):
        await svc.create_tool(tool_data)


# ─── create_tool_with_group ───────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_create_tool_with_group_adds_group_info():
    svc = make_service()
    tool = make_tool(group_id="g1")
    svc.repository.create = AsyncMock(return_value=tool)
    tool_data = MagicMock()
    tool_data.model_dump.return_value = {"title": "TestTool", "enabled": True}
    ctx = make_group_context()
    with patch("src.services.tool_service.ToolResponse") as mock_resp:
        mock_resp.model_validate.return_value = MagicMock(id=1)
        result = await svc.create_tool_with_group(tool_data, ctx)
    call_dict = svc.repository.create.call_args[0][0]
    assert call_dict.get("group_id") == "g1"


@pytest.mark.asyncio
async def test_create_tool_with_group_raises_on_error():
    svc = make_service()
    svc.repository.create = AsyncMock(side_effect=Exception("DB error"))
    tool_data = MagicMock()
    tool_data.model_dump.return_value = {"title": "TestTool"}
    ctx = make_group_context()
    with pytest.raises(KasalError):
        await svc.create_tool_with_group(tool_data, ctx)


# ─── update_tool ─────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_update_tool_not_found():
    svc = make_service()
    svc.repository.get = AsyncMock(return_value=None)
    tool_data = MagicMock()
    with pytest.raises(NotFoundError):
        await svc.update_tool(999, tool_data)


@pytest.mark.asyncio
async def test_update_tool_update_error():
    svc = make_service()
    tool = make_tool()
    svc.repository.get = AsyncMock(return_value=tool)
    svc.repository.update = AsyncMock(side_effect=Exception("DB error"))
    tool_data = MagicMock()
    tool_data.model_dump.return_value = {"title": "Updated"}
    with pytest.raises(KasalError):
        await svc.update_tool(1, tool_data)


# ─── update_tool_with_group_check ─────────────────────────────────────────────

@pytest.mark.asyncio
async def test_update_tool_with_group_check_not_found():
    svc = make_service()
    svc.repository.get = AsyncMock(return_value=None)
    ctx = make_group_context()
    with pytest.raises(NotFoundError):
        await svc.update_tool_with_group_check(999, MagicMock(), ctx)


@pytest.mark.asyncio
async def test_update_tool_with_group_check_unauthorized():
    svc = make_service()
    tool = make_tool(group_id="other-group")
    svc.repository.get = AsyncMock(return_value=tool)
    ctx = make_group_context(group_ids=["my-group"])
    with pytest.raises(NotFoundError):
        await svc.update_tool_with_group_check(1, MagicMock(), ctx)


@pytest.mark.asyncio
async def test_update_tool_with_group_check_update_error():
    svc = make_service()
    tool = make_tool(group_id="g1")
    svc.repository.get = AsyncMock(return_value=tool)
    svc.repository.update = AsyncMock(side_effect=Exception("DB error"))
    ctx = make_group_context()
    tool_data = MagicMock()
    tool_data.model_dump.return_value = {"title": "Updated"}
    with pytest.raises(KasalError):
        await svc.update_tool_with_group_check(1, tool_data, ctx)


# ─── delete_tool ─────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_delete_tool_not_found():
    svc = make_service()
    svc.repository.get = AsyncMock(return_value=None)
    with pytest.raises(NotFoundError):
        await svc.delete_tool(999)


@pytest.mark.asyncio
async def test_delete_tool_delete_error():
    svc = make_service()
    tool = make_tool()
    svc.repository.get = AsyncMock(return_value=tool)
    svc.repository.delete = AsyncMock(side_effect=Exception("DB error"))
    with pytest.raises(KasalError):
        await svc.delete_tool(1)


# ─── delete_tool_with_group_check ─────────────────────────────────────────────

@pytest.mark.asyncio
async def test_delete_tool_with_group_check_not_found():
    svc = make_service()
    svc.repository.get = AsyncMock(return_value=None)
    ctx = make_group_context()
    with pytest.raises(NotFoundError):
        await svc.delete_tool_with_group_check(999, ctx)


@pytest.mark.asyncio
async def test_delete_tool_with_group_check_unauthorized():
    svc = make_service()
    tool = make_tool(group_id="other-group")
    svc.repository.get = AsyncMock(return_value=tool)
    ctx = make_group_context(group_ids=["my-group"])
    with pytest.raises(NotFoundError):
        await svc.delete_tool_with_group_check(1, ctx)


@pytest.mark.asyncio
async def test_delete_tool_with_group_check_delete_error():
    svc = make_service()
    tool = make_tool(group_id="g1")
    svc.repository.get = AsyncMock(return_value=tool)
    svc.repository.delete = AsyncMock(side_effect=Exception("DB error"))
    ctx = make_group_context()
    with pytest.raises(KasalError):
        await svc.delete_tool_with_group_check(1, ctx)


# ─── toggle_tool_enabled ──────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_toggle_tool_enabled_not_found():
    svc = make_service()
    svc.repository.toggle_enabled = AsyncMock(return_value=None)
    with pytest.raises(NotFoundError):
        await svc.toggle_tool_enabled(999)


@pytest.mark.asyncio
async def test_toggle_tool_enabled_error():
    svc = make_service()
    svc.repository.toggle_enabled = AsyncMock(side_effect=Exception("DB error"))
    with pytest.raises(KasalError):
        await svc.toggle_tool_enabled(1)


# ─── toggle_tool_enabled_with_group_check ─────────────────────────────────────

@pytest.mark.asyncio
async def test_toggle_group_tool_no_group_context():
    svc = make_service()
    tool = make_tool()
    svc.repository.get = AsyncMock(return_value=tool)
    with pytest.raises(ForbiddenError):
        await svc.toggle_tool_enabled_with_group_check(1, None)


@pytest.mark.asyncio
async def test_toggle_group_tool_default_creates_group_copy():
    svc = make_service()
    tool = make_tool(group_id=None, enabled=True)
    svc.repository.get = AsyncMock(return_value=tool)
    svc.repository.find_by_title_and_group = AsyncMock(return_value=None)
    created_tool = make_tool(id=2, group_id="g1", enabled=False)
    svc.repository.create = AsyncMock(return_value=created_tool)
    ctx = make_group_context()
    result = await svc.toggle_tool_enabled_with_group_check(1, ctx)
    assert result.enabled is False


@pytest.mark.asyncio
async def test_toggle_group_tool_toggles_existing_group_copy():
    svc = make_service()
    tool = make_tool(group_id=None, enabled=True)
    svc.repository.get = AsyncMock(return_value=tool)
    existing_group_tool = make_tool(id=2, group_id="g1", enabled=True)
    svc.repository.find_by_title_and_group = AsyncMock(return_value=existing_group_tool)
    toggled_tool = make_tool(id=2, group_id="g1", enabled=False)
    svc.repository.toggle_enabled = AsyncMock(return_value=toggled_tool)
    ctx = make_group_context()
    result = await svc.toggle_tool_enabled_with_group_check(1, ctx)
    assert result.enabled is False


@pytest.mark.asyncio
async def test_toggle_group_specific_tool_unauthorized():
    svc = make_service()
    tool = make_tool(id=1, group_id="other-group")
    svc.repository.get = AsyncMock(return_value=tool)
    ctx = make_group_context(group_ids=["my-group"])
    with pytest.raises(NotFoundError):
        await svc.toggle_tool_enabled_with_group_check(1, ctx)


@pytest.mark.asyncio
async def test_toggle_group_tool_error():
    svc = make_service()
    svc.repository.get = AsyncMock(side_effect=Exception("DB error"))
    ctx = make_group_context()
    with pytest.raises(KasalError):
        await svc.toggle_tool_enabled_with_group_check(1, ctx)


# ─── get_tool_config_by_name ──────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_get_tool_config_by_name_not_found():
    svc = make_service()
    svc.repository.find_by_title = AsyncMock(return_value=None)
    result = await svc.get_tool_config_by_name("UnknownTool")
    assert result is None


@pytest.mark.asyncio
async def test_get_tool_config_by_name_exception():
    svc = make_service()
    svc.repository.find_by_title = AsyncMock(side_effect=Exception("DB error"))
    result = await svc.get_tool_config_by_name("SomeTool")
    assert result is None


# ─── update_tool_configuration_by_title ──────────────────────────────────────

@pytest.mark.asyncio
async def test_update_tool_config_by_title_not_found():
    svc = make_service()
    svc.repository.update_configuration_by_title = AsyncMock(return_value=None)
    with pytest.raises(NotFoundError):
        await svc.update_tool_configuration_by_title("UnknownTool", {})


@pytest.mark.asyncio
async def test_update_tool_config_by_title_error():
    svc = make_service()
    svc.repository.update_configuration_by_title = AsyncMock(side_effect=Exception("DB error"))
    with pytest.raises(KasalError):
        await svc.update_tool_configuration_by_title("SomeTool", {})


# ─── get_all_tool_configurations_for_group ────────────────────────────────────

@pytest.mark.asyncio
async def test_get_all_tool_configurations_for_group():
    svc = make_service()
    tool_resp = MagicMock()
    tool_resp.title = "Tool1"
    tool_resp.config = {"key": "val"}
    tools_response = MagicMock()
    tools_response.tools = [tool_resp]
    with patch.object(svc, "get_all_tools_for_group", new=AsyncMock(return_value=tools_response)):
        ctx = make_group_context()
        result = await svc.get_all_tool_configurations_for_group(ctx)
    assert result == {"Tool1": {"key": "val"}}


# ─── get_tool_configuration_with_group_check ─────────────────────────────────

@pytest.mark.asyncio
async def test_get_tool_configuration_with_group_check_returns_group_config():
    svc = make_service()
    group_tool = make_tool(config={"group_key": "group_val"})
    svc.repository.find_by_title_and_group = AsyncMock(return_value=group_tool)
    ctx = make_group_context()
    result = await svc.get_tool_configuration_with_group_check("MyTool", ctx)
    assert result == {"group_key": "group_val"}


@pytest.mark.asyncio
async def test_get_tool_configuration_with_group_check_falls_back_to_base():
    svc = make_service()
    svc.repository.find_by_title_and_group = AsyncMock(return_value=None)
    base_tool = make_tool(config={"base_key": "base_val"})
    svc.repository.find_base_by_title = AsyncMock(return_value=base_tool)
    ctx = make_group_context()
    result = await svc.get_tool_configuration_with_group_check("MyTool", ctx)
    assert result == {"base_key": "base_val"}


# ─── update_tool_configuration_group_scoped ───────────────────────────────────

@pytest.mark.asyncio
async def test_update_tool_config_group_scoped_no_context():
    svc = make_service()
    with pytest.raises(ForbiddenError):
        await svc.update_tool_configuration_group_scoped("MyTool", {}, None)


@pytest.mark.asyncio
async def test_update_tool_config_group_scoped_updates_existing():
    svc = make_service()
    existing = make_tool(group_id="g1", config={"old": "val"})
    svc.repository.find_by_title_and_group = AsyncMock(return_value=existing)
    updated = make_tool(config={"new": "val"})
    svc.repository.update_configuration_for_title_and_group = AsyncMock(return_value=updated)
    ctx = make_group_context()
    with patch("src.services.tool_service.ToolResponse") as mock_resp:
        mock_resp.model_validate.return_value = MagicMock()
        await svc.update_tool_configuration_group_scoped("MyTool", {"new": "val"}, ctx)
    svc.repository.update_configuration_for_title_and_group.assert_called_once()


@pytest.mark.asyncio
async def test_update_tool_config_group_scoped_creates_new_from_base():
    svc = make_service()
    svc.repository.find_by_title_and_group = AsyncMock(return_value=None)
    base_tool = make_tool(config={"base": "val"}, enabled=True)
    svc.repository.find_base_by_title = AsyncMock(return_value=base_tool)
    created = make_tool(group_id="g1", config={"new": "val"})
    svc.repository.create = AsyncMock(return_value=created)
    ctx = make_group_context()
    with patch("src.services.tool_service.ToolResponse") as mock_resp:
        mock_resp.model_validate.return_value = MagicMock()
        await svc.update_tool_configuration_group_scoped("MyTool", {"new": "val"}, ctx)
    svc.repository.create.assert_called_once()


@pytest.mark.asyncio
async def test_update_tool_config_group_scoped_creates_new_no_base():
    svc = make_service()
    svc.repository.find_by_title_and_group = AsyncMock(return_value=None)
    svc.repository.find_base_by_title = AsyncMock(return_value=None)
    created = make_tool(group_id="g1", config={"new": "val"})
    svc.repository.create = AsyncMock(return_value=created)
    ctx = make_group_context()
    with patch("src.services.tool_service.ToolResponse") as mock_resp:
        mock_resp.model_validate.return_value = MagicMock()
        await svc.update_tool_configuration_group_scoped("NewTool", {"new": "val"}, ctx)
    svc.repository.create.assert_called_once()


# ─── personal-workspace-only tools (Gmail) ───────────────────────────────────

@pytest.mark.asyncio
async def test_gmail_hidden_in_shared_workspace():
    """Gmail must not appear in get_all_tools_for_group for a shared workspace."""
    svc = make_service()
    svc.repository.list = AsyncMock(return_value=[
        make_tool(id=1, title="Gmail", group_id=None),
        make_tool(id=2, title="PerplexityTool", group_id=None),
    ])
    # Shared workspace: primary_group_id is NOT user_<email>.
    ctx = make_group_context(
        group_ids=["bi-specialist"], primary_group_id="bi-specialist",
        group_email="alice@x.com",
    )
    result = await svc.get_all_tools_for_group(ctx)
    titles = {t.title for t in result.tools}
    assert "Gmail" not in titles
    assert "PerplexityTool" in titles


@pytest.mark.asyncio
async def test_gmail_visible_in_personal_workspace():
    """Gmail appears when the active group IS the caller's personal workspace."""
    svc = make_service()
    svc.repository.list = AsyncMock(return_value=[
        make_tool(id=1, title="Gmail", group_id=None),
        make_tool(id=2, title="PerplexityTool", group_id=None),
    ])
    # generate_individual_group_id("alice@x.com") == "user_alice_x_com"
    ctx = make_group_context(
        group_ids=["user_alice_x_com"], primary_group_id="user_alice_x_com",
        group_email="alice@x.com",
    )
    result = await svc.get_all_tools_for_group(ctx)
    titles = {t.title for t in result.tools}
    assert "Gmail" in titles
    assert "PerplexityTool" in titles


@pytest.mark.asyncio
async def test_gmail_hidden_in_enabled_tools_for_shared_workspace():
    """The same filter applies to the /enabled path used by ChatMode/generation."""
    from src.core.cache import tool_list_cache
    await tool_list_cache.clear()

    svc = make_service()
    svc.repository.find_enabled = AsyncMock(return_value=[
        make_tool(id=1, title="Gmail", group_id=None),
        make_tool(id=2, title="PerplexityTool", group_id=None),
    ])
    mapping1 = MagicMock(tool_id=1, config={})
    mapping2 = MagicMock(tool_id=2, config={})
    with patch("src.services.tool_service.GroupToolRepository") as MockGroupRepo:
        group_repo = AsyncMock()
        group_repo.list_enabled_for_group = AsyncMock(return_value=[mapping1, mapping2])
        MockGroupRepo.return_value = group_repo
        ctx = make_group_context(
            group_ids=["bi-specialist"], primary_group_id="bi-specialist",
            group_email="alice@x.com",
        )
        result = await svc._build_enabled_tools_for_group(ctx)

    titles = {t.title for t in result.tools}
    assert "Gmail" not in titles
    assert "PerplexityTool" in titles
