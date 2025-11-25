import pytest
from unittest.mock import AsyncMock
from types import SimpleNamespace
from datetime import datetime

from src.api.templates_router import (
    health_check,
    list_templates,
    get_template,
    get_template_by_name,
    create_template,
    update_template,
    delete_template,
    delete_all_templates,
    reset_templates,
)
from src.schemas.template import PromptTemplateCreate, PromptTemplateUpdate


class Ctx:
    def __init__(self, primary_group_id="g1"):
        self.primary_group_id = primary_group_id
        self.group_ids = [primary_group_id]
        self.group_email = "u@x"


def make_tpl(i=1, name="t1"):
    now = datetime.utcnow()
    return SimpleNamespace(id=i, name=name, description=None, template="hi", is_active=True, created_at=now, updated_at=now)


@pytest.mark.asyncio
async def test_health_and_list_and_get_404():
    assert (await health_check())["status"] == "healthy"

    svc = AsyncMock()
    ctx = Ctx()

    # list returns list of objects; router returns as-is
    svc.find_all_templates_for_group = AsyncMock(return_value=[make_tpl()])
    out = await list_templates(service=svc, group_context=ctx)
    assert isinstance(out, list) and out[0].name == "t1"

    # get 404
    svc.get_template_with_group_check = AsyncMock(return_value=None)
    with pytest.raises(Exception):
        await get_template(123, service=svc, group_context=ctx)

    # get by name 404
    svc.find_template_by_name_with_group = AsyncMock(return_value=None)
    with pytest.raises(Exception):
        await get_template_by_name("missing", service=svc, group_context=ctx)


@pytest.mark.asyncio
async def test_create_update_delete_and_reset_paths():
    svc = AsyncMock()
    ctx = Ctx()

    # create success
    svc.create_template_with_group = AsyncMock(return_value=make_tpl(2, "t2"))
    out = await create_template(PromptTemplateCreate(name="t2", template="x"), service=svc, group_context=ctx)
    assert out.name == "t2"

    # create conflict -> 400
    svc.create_template_with_group = AsyncMock(side_effect=ValueError("exists"))
    with pytest.raises(Exception):
        await create_template(PromptTemplateCreate(name="t2", template="x"), service=svc, group_context=ctx)

    # update not found -> 404
    svc.update_with_group_check = AsyncMock(return_value=None)
    with pytest.raises(Exception):
        await update_template(5, PromptTemplateUpdate(name="t3"), service=svc, group_context=ctx)

    # update conflict -> 400
    svc.update_with_group_check = AsyncMock(side_effect=ValueError("conflict"))
    with pytest.raises(Exception):
        await update_template(5, PromptTemplateUpdate(name="t3"), service=svc, group_context=ctx)

    # delete not found -> 404
    svc.delete_with_group_check = AsyncMock(return_value=False)
    with pytest.raises(Exception):
        await delete_template(5, service=svc, group_context=ctx)

    # delete all and reset
    svc.delete_all_for_group_internal = AsyncMock(return_value=3)
    da = await delete_all_templates(service=svc, group_context=ctx)
    assert da["deleted_count"] == 3

    svc.reset_templates_with_group = AsyncMock(return_value=2)
    ra = await reset_templates(service=svc, group_context=ctx)
    assert ra["reset_count"] == 2

