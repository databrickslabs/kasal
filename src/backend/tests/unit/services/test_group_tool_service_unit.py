"""
Unit tests for GroupToolService.

Tests the functionality of group tool management service including
listing, adding, enabling/disabling, configuring, and removing
group-tool mappings with group context isolation.
"""
import pytest
from unittest.mock import AsyncMock, patch
from datetime import datetime
from types import SimpleNamespace

from sqlalchemy.ext.asyncio import AsyncSession

from src.services.group_tool_service import GroupToolService
from src.core.exceptions import NotFoundError, ForbiddenError, BadRequestError
from src.utils.user_context import GroupContext


# ---------------------------------------------------------------------------
# Helpers to build lightweight mock domain objects
# ---------------------------------------------------------------------------

def _make_tool(
    id: int = 1,
    title: str = "WebSearch",
    description: str = "Search the web",
    icon: str = "search",
    config: dict = None,
    enabled: bool = True,
    group_id: str = None,
    created_at: datetime = None,
    updated_at: datetime = None,
) -> SimpleNamespace:
    """Return a lightweight object that looks like a Tool ORM model."""
    now = datetime.utcnow()
    return SimpleNamespace(
        id=id,
        title=title,
        description=description,
        icon=icon,
        config=config or {},
        enabled=enabled,
        group_id=group_id,
        created_at=created_at or now,
        updated_at=updated_at or now,
    )


def _make_group_tool(
    id: int = 100,
    tool_id: int = 1,
    group_id: str = "group-abc",
    enabled: bool = False,
    config: dict = None,
    credentials_status: str = "unknown",
    created_at: datetime = None,
    updated_at: datetime = None,
) -> SimpleNamespace:
    """Return a lightweight object that looks like a GroupTool ORM model."""
    now = datetime.utcnow()
    return SimpleNamespace(
        id=id,
        tool_id=tool_id,
        group_id=group_id,
        enabled=enabled,
        config=config or {},
        credentials_status=credentials_status,
        created_at=created_at or now,
        updated_at=updated_at or now,
    )


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def mock_session():
    """Create a mock async database session."""
    return AsyncMock(spec=AsyncSession)


@pytest.fixture
def mock_tool_repo():
    """Create a mock ToolRepository with all async methods."""
    repo = AsyncMock()
    return repo


@pytest.fixture
def mock_group_tool_repo():
    """Create a mock GroupToolRepository with all async methods."""
    repo = AsyncMock()
    return repo


@pytest.fixture
def service(mock_session, mock_tool_repo, mock_group_tool_repo):
    """Create a GroupToolService with mocked repository dependencies."""
    with patch(
        "src.services.group_tool_service.ToolRepository",
        return_value=mock_tool_repo,
    ), patch(
        "src.services.group_tool_service.GroupToolRepository",
        return_value=mock_group_tool_repo,
    ):
        svc = GroupToolService(session=mock_session)
    return svc


@pytest.fixture
def group_context():
    """Create a valid GroupContext with a primary group ID."""
    return GroupContext(
        group_ids=["group-abc", "group-def"],
        group_email="user@example.com",
        email_domain="example.com",
        user_id="user-1",
    )


@pytest.fixture
def empty_group_context():
    """Create a GroupContext with no group IDs (no primary_group_id)."""
    return GroupContext(
        group_ids=[],
        group_email="user@example.com",
        email_domain="example.com",
        user_id="user-1",
    )


# ===========================================================================
# Tests for list_added_for_group
# ===========================================================================

class TestListAddedForGroup:
    """Tests for GroupToolService.list_added_for_group."""

    @pytest.mark.asyncio
    async def test_happy_path_returns_group_tool_list(
        self, service, mock_group_tool_repo, group_context
    ):
        """Listing added tools returns a GroupToolListResponse with correct items and count."""
        mapping1 = _make_group_tool(id=100, tool_id=1, group_id="group-abc")
        mapping2 = _make_group_tool(id=101, tool_id=2, group_id="group-abc")
        mock_group_tool_repo.list_for_group.return_value = [mapping1, mapping2]

        result = await service.list_added_for_group(group_context)

        mock_group_tool_repo.list_for_group.assert_called_once_with("group-abc")
        assert result.count == 2
        assert len(result.items) == 2
        assert result.items[0].tool_id == 1
        assert result.items[1].tool_id == 2

    @pytest.mark.asyncio
    async def test_forbidden_when_no_primary_group_id(
        self, service, empty_group_context
    ):
        """Raises ForbiddenError when group context has no primary group ID."""
        with pytest.raises(ForbiddenError, match="Group context required"):
            await service.list_added_for_group(empty_group_context)

    @pytest.mark.asyncio
    async def test_forbidden_when_group_context_is_none(self, service):
        """Raises ForbiddenError when group context is None."""
        with pytest.raises(ForbiddenError, match="Group context required"):
            await service.list_added_for_group(None)


# ===========================================================================
# Tests for list_available_to_add_for_group
# ===========================================================================

class TestListAvailableToAddForGroup:
    """Tests for GroupToolService.list_available_to_add_for_group."""

    @pytest.mark.asyncio
    async def test_happy_path_returns_available_tools(
        self, service, mock_tool_repo, mock_group_tool_repo, group_context
    ):
        """Available tools are global, enabled, and not yet mapped to the group."""
        global_enabled = _make_tool(id=1, enabled=True, group_id=None)
        global_disabled = _make_tool(id=2, enabled=False, group_id=None)
        non_global = _make_tool(id=3, enabled=True, group_id="other-group")
        already_mapped = _make_tool(id=4, enabled=True, group_id=None)

        mock_tool_repo.list.return_value = [
            global_enabled,
            global_disabled,
            non_global,
            already_mapped,
        ]

        # Tool 4 is already mapped to the group
        existing_mapping = _make_group_tool(tool_id=4, group_id="group-abc")
        mock_group_tool_repo.list_for_group.return_value = [existing_mapping]

        result = await service.list_available_to_add_for_group(group_context)

        # Only tool 1 should appear: global + enabled + not mapped
        assert result.count == 1
        assert len(result.tools) == 1
        assert result.tools[0].id == 1

    @pytest.mark.asyncio
    async def test_forbidden_when_no_primary_group_id(
        self, service, empty_group_context
    ):
        """Raises ForbiddenError when group context has no primary group ID."""
        with pytest.raises(ForbiddenError, match="Group context required"):
            await service.list_available_to_add_for_group(empty_group_context)


# ===========================================================================
# Tests for add_tool_to_group
# ===========================================================================

class TestAddToolToGroup:
    """Tests for GroupToolService.add_tool_to_group."""

    @pytest.mark.asyncio
    async def test_happy_path_adds_tool(
        self, service, mock_tool_repo, mock_group_tool_repo, group_context
    ):
        """Successfully adds a global enabled tool to a group and returns GroupToolResponse."""
        tool = _make_tool(id=5, enabled=True, group_id=None)
        mock_tool_repo.get.return_value = tool

        created_mapping = _make_group_tool(
            id=200, tool_id=5, group_id="group-abc", enabled=False
        )
        mock_group_tool_repo.upsert.return_value = created_mapping

        result = await service.add_tool_to_group(5, group_context)

        mock_tool_repo.get.assert_called_once_with(5)
        mock_group_tool_repo.upsert.assert_called_once_with(
            tool_id=5, group_id="group-abc", defaults=None
        )
        assert result.tool_id == 5
        assert result.group_id == "group-abc"

    @pytest.mark.asyncio
    async def test_happy_path_with_defaults(
        self, service, mock_tool_repo, mock_group_tool_repo, group_context
    ):
        """Passes optional defaults dict through to the repository upsert."""
        tool = _make_tool(id=6, enabled=True, group_id=None)
        mock_tool_repo.get.return_value = tool

        defaults = {"enabled": True, "config": {"api_key": "secret"}}
        mapping = _make_group_tool(
            id=201, tool_id=6, group_id="group-abc", enabled=True,
            config={"api_key": "secret"},
        )
        mock_group_tool_repo.upsert.return_value = mapping

        result = await service.add_tool_to_group(6, group_context, defaults=defaults)

        mock_group_tool_repo.upsert.assert_called_once_with(
            tool_id=6, group_id="group-abc", defaults=defaults
        )
        assert result.tool_id == 6

    @pytest.mark.asyncio
    async def test_forbidden_when_no_primary_group_id(
        self, service, empty_group_context
    ):
        """Raises ForbiddenError when group context has no primary group ID."""
        with pytest.raises(ForbiddenError, match="Group context required"):
            await service.add_tool_to_group(1, empty_group_context)

    @pytest.mark.asyncio
    async def test_not_found_when_tool_does_not_exist(
        self, service, mock_tool_repo, group_context
    ):
        """Raises NotFoundError when the tool ID does not exist in the catalog."""
        mock_tool_repo.get.return_value = None

        with pytest.raises(NotFoundError, match="Tool not found"):
            await service.add_tool_to_group(999, group_context)

    @pytest.mark.asyncio
    async def test_bad_request_when_tool_is_not_global(
        self, service, mock_tool_repo, group_context
    ):
        """Raises BadRequestError when the tool is group-specific (group_id is not None)."""
        non_global_tool = _make_tool(id=7, enabled=True, group_id="other-group")
        mock_tool_repo.get.return_value = non_global_tool

        with pytest.raises(BadRequestError, match="Only global tools"):
            await service.add_tool_to_group(7, group_context)

    @pytest.mark.asyncio
    async def test_bad_request_when_tool_is_not_enabled(
        self, service, mock_tool_repo, group_context
    ):
        """Raises BadRequestError when the global tool is disabled."""
        disabled_tool = _make_tool(id=8, enabled=False, group_id=None)
        mock_tool_repo.get.return_value = disabled_tool

        with pytest.raises(BadRequestError, match="not globally available"):
            await service.add_tool_to_group(8, group_context)


# ===========================================================================
# Tests for set_group_tool_enabled
# ===========================================================================

class TestSetGroupToolEnabled:
    """Tests for GroupToolService.set_group_tool_enabled."""

    @pytest.mark.asyncio
    async def test_happy_path_enables_tool(
        self, service, mock_group_tool_repo, group_context
    ):
        """Successfully enables a group tool mapping and returns the updated response."""
        updated_mapping = _make_group_tool(
            id=300, tool_id=10, group_id="group-abc", enabled=True
        )
        mock_group_tool_repo.set_enabled.return_value = updated_mapping

        result = await service.set_group_tool_enabled(10, True, group_context)

        mock_group_tool_repo.set_enabled.assert_called_once_with(
            tool_id=10, group_id="group-abc", enabled=True
        )
        assert result.enabled is True
        assert result.tool_id == 10

    @pytest.mark.asyncio
    async def test_happy_path_disables_tool(
        self, service, mock_group_tool_repo, group_context
    ):
        """Successfully disables a group tool mapping."""
        disabled_mapping = _make_group_tool(
            id=301, tool_id=11, group_id="group-abc", enabled=False
        )
        mock_group_tool_repo.set_enabled.return_value = disabled_mapping

        result = await service.set_group_tool_enabled(11, False, group_context)

        mock_group_tool_repo.set_enabled.assert_called_once_with(
            tool_id=11, group_id="group-abc", enabled=False
        )
        assert result.enabled is False

    @pytest.mark.asyncio
    async def test_forbidden_when_no_primary_group_id(
        self, service, empty_group_context
    ):
        """Raises ForbiddenError when group context has no primary group ID."""
        with pytest.raises(ForbiddenError, match="Group context required"):
            await service.set_group_tool_enabled(1, True, empty_group_context)

    @pytest.mark.asyncio
    async def test_not_found_when_mapping_does_not_exist(
        self, service, mock_group_tool_repo, group_context
    ):
        """Raises NotFoundError when no group tool mapping exists for the tool+group pair."""
        mock_group_tool_repo.set_enabled.return_value = None

        with pytest.raises(NotFoundError, match="Group tool mapping not found"):
            await service.set_group_tool_enabled(999, True, group_context)


# ===========================================================================
# Tests for update_group_tool_config
# ===========================================================================

class TestUpdateGroupToolConfig:
    """Tests for GroupToolService.update_group_tool_config."""

    @pytest.mark.asyncio
    async def test_happy_path_updates_config(
        self, service, mock_group_tool_repo, group_context
    ):
        """Successfully updates group tool configuration and returns updated response."""
        new_config = {"api_key": "new-key", "timeout": 30}
        updated_mapping = _make_group_tool(
            id=400, tool_id=20, group_id="group-abc", config=new_config
        )
        mock_group_tool_repo.update_config.return_value = updated_mapping

        result = await service.update_group_tool_config(20, new_config, group_context)

        mock_group_tool_repo.update_config.assert_called_once_with(
            tool_id=20, group_id="group-abc", config=new_config
        )
        assert result.config == new_config
        assert result.tool_id == 20

    @pytest.mark.asyncio
    async def test_forbidden_when_no_primary_group_id(
        self, service, empty_group_context
    ):
        """Raises ForbiddenError when group context has no primary group ID."""
        with pytest.raises(ForbiddenError, match="Group context required"):
            await service.update_group_tool_config(1, {"key": "val"}, empty_group_context)

    @pytest.mark.asyncio
    async def test_not_found_when_mapping_does_not_exist(
        self, service, mock_group_tool_repo, group_context
    ):
        """Raises NotFoundError when no group tool mapping exists for the tool+group pair."""
        mock_group_tool_repo.update_config.return_value = None

        with pytest.raises(NotFoundError, match="Group tool mapping not found"):
            await service.update_group_tool_config(999, {"k": "v"}, group_context)


# ===========================================================================
# Tests for remove_tool_from_group
# ===========================================================================

class TestRemoveToolFromGroup:
    """Tests for GroupToolService.remove_tool_from_group."""

    @pytest.mark.asyncio
    async def test_happy_path_returns_true(
        self, service, mock_group_tool_repo, group_context
    ):
        """Returns True when a mapping is successfully deleted (rowcount > 0)."""
        mock_group_tool_repo.delete_mapping.return_value = 1

        result = await service.remove_tool_from_group(30, group_context)

        mock_group_tool_repo.delete_mapping.assert_called_once_with(
            tool_id=30, group_id="group-abc"
        )
        assert result is True

    @pytest.mark.asyncio
    async def test_returns_false_when_nothing_deleted(
        self, service, mock_group_tool_repo, group_context
    ):
        """Returns False when no mapping existed (rowcount == 0)."""
        mock_group_tool_repo.delete_mapping.return_value = 0

        result = await service.remove_tool_from_group(999, group_context)

        mock_group_tool_repo.delete_mapping.assert_called_once_with(
            tool_id=999, group_id="group-abc"
        )
        assert result is False

    @pytest.mark.asyncio
    async def test_forbidden_when_no_primary_group_id(
        self, service, empty_group_context
    ):
        """Raises ForbiddenError when group context has no primary group ID."""
        with pytest.raises(ForbiddenError, match="Group context required"):
            await service.remove_tool_from_group(1, empty_group_context)
