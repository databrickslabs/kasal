"""Only a system admin may create or edit a template converter config (audit N2).

A saved config with ``is_template=True`` is visible to every tenant
(``get_visible_to_groups``, ``find_templates``, ``use_config``), so letting any
user set the flag let them plant a configuration in every other workspace, and
keep editing it afterwards.
"""

from datetime import datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from src.core.exceptions import ForbiddenError
from src.schemas.conversion import SavedConfigurationCreate, SavedConfigurationUpdate
from src.services.powerbi.conversions import ConverterService
from src.utils.user_context import GroupContext

EMAIL = "user@example.com"


def _context(system_admin):
    return GroupContext(
        group_ids=["group-1"],
        group_email=EMAIL,
        email_domain="example.com",
        user_role="admin",  # a WORKSPACE admin is not enough
        current_user=SimpleNamespace(is_system_admin=system_admin),
    )


def _service(context):
    service = ConverterService(AsyncMock(), group_context=context)
    service.config_repo = AsyncMock()
    return service


def _create(is_template):
    return SavedConfigurationCreate(
        name="c",
        source_format="powerbi",
        target_format="dax",
        configuration={},
        is_template=is_template,
    )


def _row(is_template):
    return SimpleNamespace(
        id=1,
        name="c",
        description=None,
        source_format="powerbi",
        target_format="dax",
        configuration={},
        is_public=False,
        is_template=is_template,
        tags=None,
        extra_metadata=None,
        use_count=0,
        last_used_at=None,
        group_id="group-1",
        created_by_email=EMAIL,
        created_at=datetime.utcnow(),
        updated_at=datetime.utcnow(),
    )


@pytest.mark.asyncio
async def test_non_admin_cannot_create_a_template():
    service = _service(_context(False))
    with pytest.raises(ForbiddenError):
        await service.create_saved_config(_create(True))
    service.config_repo.create.assert_not_called()


@pytest.mark.asyncio
async def test_truthy_non_bool_is_not_treated_as_system_admin():
    context = _context(False)
    context.current_user = MagicMock()  # attribute access yields a truthy mock
    service = _service(context)
    with pytest.raises(ForbiddenError):
        await service.create_saved_config(_create(True))


@pytest.mark.asyncio
async def test_non_admin_can_create_an_ordinary_config():
    service = _service(_context(False))
    service.config_repo.create.return_value = _row(False)
    await service.create_saved_config(_create(False))
    assert service.config_repo.create.call_args.args[0]["is_template"] is False


@pytest.mark.asyncio
async def test_system_admin_can_create_a_template():
    service = _service(_context(True))
    service.config_repo.create.return_value = _row(True)
    result = await service.create_saved_config(_create(True))
    assert result.is_template is True
    assert service.config_repo.create.call_args.args[0]["is_template"] is True


@pytest.mark.asyncio
@pytest.mark.parametrize("action", ["update", "delete"])
async def test_creator_who_is_not_system_admin_cannot_change_a_template(action):
    service = _service(_context(False))
    service.config_repo.get_visible_to_groups.return_value = _row(True)
    with pytest.raises(ForbiddenError):
        if action == "update":
            await service.update_saved_config(1, SavedConfigurationUpdate(name="x"))
        else:
            await service.delete_saved_config(1)
    service.config_repo.update.assert_not_called()
    service.config_repo.delete.assert_not_called()


@pytest.mark.asyncio
async def test_system_admin_can_edit_their_template():
    service = _service(_context(True))
    service.config_repo.get_visible_to_groups.return_value = _row(True)
    service.config_repo.update.return_value = _row(True)
    await service.update_saved_config(1, SavedConfigurationUpdate(name="x"))
    service.config_repo.update.assert_awaited_once()


@pytest.mark.asyncio
async def test_non_admin_can_still_edit_their_own_ordinary_config():
    service = _service(_context(False))
    service.config_repo.get_visible_to_groups.return_value = _row(False)
    service.config_repo.update.return_value = _row(False)
    await service.update_saved_config(1, SavedConfigurationUpdate(name="x"))
    service.config_repo.update.assert_awaited_once()
