"""
Coverage-focused tests for TemplateService.
Targets uncovered branches to push coverage to 85%+.
"""
import pytest
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

from src.services.template_service import TemplateService
from src.schemas.template import PromptTemplateCreate, PromptTemplateUpdate
from src.utils.user_context import GroupContext


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_template(id=1, name="test-template", group_id=None, template="content", description="desc",
                  is_active=True):
    return SimpleNamespace(id=id, name=name, group_id=group_id, template=template,
                           description=description, is_active=is_active)


class FakeRepo:
    def __init__(self):
        self._get_return = None
        self._find_by_name_return = None
        self._find_by_name_and_group_return = None
        self.active_templates = []
        self.created = []
        self.updated = []
        self.deleted = []

    async def get(self, id):
        return self._get_return

    async def create(self, data):
        ns = SimpleNamespace(id=len(self.created) + 1, **data)
        self.created.append(ns)
        return ns

    async def update_template(self, id, data):
        result = SimpleNamespace(id=id, **data)
        self.updated.append(result)
        return result

    async def delete(self, id):
        self.deleted.append(id)
        return True

    async def delete_all(self):
        return len(self.active_templates)

    async def find_by_name(self, name):
        return self._find_by_name_return

    async def find_by_name_and_group(self, name, group_id):
        return self._find_by_name_and_group_return

    async def find_active_templates(self):
        return self.active_templates


def make_svc():
    svc = TemplateService(session=MagicMock())
    svc.repository = FakeRepo()
    return svc


def make_gc(group_ids=None, primary=None, email=None):
    gc = MagicMock(spec=GroupContext)
    gc.group_ids = group_ids or ["g1"]
    gc.primary_group_id = primary or (group_ids[0] if group_ids else "g1")
    gc.group_email = email or "u@e.com"
    gc.is_valid = MagicMock(return_value=True)
    return gc


# ---------------------------------------------------------------------------
# find_all_templates / find_all
# ---------------------------------------------------------------------------

class TestFindAll:
    @pytest.mark.asyncio
    async def test_find_all_templates_delegates(self):
        svc = make_svc()
        t = make_template()
        svc.repository.active_templates = [t]
        result = await svc.find_all_templates()
        assert result == [t]

    @pytest.mark.asyncio
    async def test_find_all_returns_active_templates(self):
        svc = make_svc()
        t = make_template(id=2, name="other")
        svc.repository.active_templates = [t]
        result = await svc.find_all()
        assert result == [t]


# ---------------------------------------------------------------------------
# find_all_templates_for_group
# ---------------------------------------------------------------------------

class TestFindAllTemplatesForGroup:
    @pytest.mark.asyncio
    async def test_returns_group_template_over_global(self):
        svc = make_svc()
        global_t = make_template(id=1, name="prompt", group_id=None)
        group_t = make_template(id=2, name="prompt", group_id="g1")
        svc.repository.active_templates = [global_t, group_t]
        # No DEFAULT_TEMPLATES seeds needed: both keys exist
        svc.repository._find_by_name_and_group_return = global_t

        gc = make_gc(group_ids=["g1"])
        with patch("src.services.template_service.DEFAULT_TEMPLATES", []):
            result = await svc.find_all_templates_for_group(gc)
        names = [t.name for t in result]
        assert "prompt" in names
        # The group-scoped one should take precedence
        ids = [t.id for t in result]
        assert 2 in ids

    @pytest.mark.asyncio
    async def test_creates_base_row_if_missing(self):
        svc = make_svc()
        svc.repository.active_templates = []
        gc = make_gc(group_ids=["g1"])
        seed = {"name": "new-seed", "description": "d", "template": "t", "is_active": True}
        with patch("src.services.template_service.DEFAULT_TEMPLATES", [seed]):
            result = await svc.find_all_templates_for_group(gc)
        # Should have tried to create the base template
        assert len(svc.repository.created) >= 1

    @pytest.mark.asyncio
    async def test_handles_create_exception_gracefully(self):
        svc = make_svc()
        svc.repository.active_templates = []
        gc = make_gc(group_ids=["g1"])
        seed = {"name": "failing-seed", "description": "d", "template": "t", "is_active": True}

        original_create = svc.repository.create

        async def failing_create(data):
            raise Exception("unique constraint violated")

        svc.repository.create = failing_create
        with patch("src.services.template_service.DEFAULT_TEMPLATES", [seed]):
            # Should not raise
            result = await svc.find_all_templates_for_group(gc)


# ---------------------------------------------------------------------------
# find_by_group
# ---------------------------------------------------------------------------

class TestFindByGroup:
    @pytest.mark.asyncio
    async def test_returns_empty_when_no_group_context(self):
        svc = make_svc()
        assert await svc.find_by_group(None) == []

    @pytest.mark.asyncio
    async def test_returns_empty_when_no_group_ids(self):
        svc = make_svc()
        gc = make_gc(group_ids=[])
        gc.group_ids = []
        assert await svc.find_by_group(gc) == []

    @pytest.mark.asyncio
    async def test_filters_by_group_id(self):
        svc = make_svc()
        t_g1 = make_template(id=1, name="t1", group_id="g1")
        t_g2 = make_template(id=2, name="t2", group_id="g2")
        svc.repository.active_templates = [t_g1, t_g2]
        gc = make_gc(group_ids=["g1"])
        result = await svc.find_by_group(gc)
        assert result == [t_g1]


# ---------------------------------------------------------------------------
# get / get_template_by_id / get_with_group_check
# ---------------------------------------------------------------------------

class TestGet:
    @pytest.mark.asyncio
    async def test_get_returns_template(self):
        svc = make_svc()
        t = make_template()
        svc.repository._get_return = t
        result = await svc.get(1)
        assert result == t

    @pytest.mark.asyncio
    async def test_get_template_by_id_delegates(self):
        svc = make_svc()
        t = make_template(id=5)
        svc.repository._get_return = t
        result = await svc.get_template_by_id(5)
        assert result == t

    @pytest.mark.asyncio
    async def test_get_with_group_check_global_visible(self):
        svc = make_svc()
        t = make_template(group_id=None)
        svc.repository._get_return = t
        result = await svc.get_with_group_check(1, make_gc())
        assert result == t

    @pytest.mark.asyncio
    async def test_get_with_group_check_matching_group(self):
        svc = make_svc()
        t = make_template(group_id="g1")
        svc.repository._get_return = t
        result = await svc.get_with_group_check(1, make_gc(group_ids=["g1"]))
        assert result == t

    @pytest.mark.asyncio
    async def test_get_with_group_check_non_matching_group(self):
        svc = make_svc()
        t = make_template(group_id="g2")
        svc.repository._get_return = t
        result = await svc.get_with_group_check(1, make_gc(group_ids=["g1"]))
        assert result is None

    @pytest.mark.asyncio
    async def test_get_with_group_check_not_found(self):
        svc = make_svc()
        svc.repository._get_return = None
        result = await svc.get_with_group_check(99, make_gc())
        assert result is None


# ---------------------------------------------------------------------------
# find_by_name / find_template_by_name
# ---------------------------------------------------------------------------

class TestFindByName:
    @pytest.mark.asyncio
    async def test_find_by_name_returns_template(self):
        svc = make_svc()
        t = make_template(name="my-template")
        svc.repository._find_by_name_return = t
        result = await svc.find_by_name("my-template")
        assert result == t

    @pytest.mark.asyncio
    async def test_find_template_by_name_delegates(self):
        svc = make_svc()
        t = make_template(name="x")
        svc.repository._find_by_name_return = t
        result = await svc.find_template_by_name("x")
        assert result == t


# ---------------------------------------------------------------------------
# find_by_name_with_group_check
# ---------------------------------------------------------------------------

class TestFindByNameWithGroupCheck:
    @pytest.mark.asyncio
    async def test_returns_group_row_when_exists(self):
        svc = make_svc()
        group_t = make_template(name="prompt", group_id="g1")
        svc.repository._find_by_name_and_group_return = group_t
        gc = make_gc(primary="g1")
        result = await svc.find_by_name_with_group_check("prompt", gc)
        assert result == group_t

    @pytest.mark.asyncio
    async def test_falls_back_to_base_row(self):
        svc = make_svc()
        base_t = make_template(name="prompt", group_id=None)
        call_count = 0

        async def find_by_name_and_group(name, gid):
            nonlocal call_count
            call_count += 1
            if gid is None:
                return base_t
            return None

        svc.repository.find_by_name_and_group = find_by_name_and_group
        gc = make_gc(primary="g1")
        result = await svc.find_by_name_with_group_check("prompt", gc)
        assert result == base_t

    @pytest.mark.asyncio
    async def test_lazy_seeds_from_defaults_when_not_found(self):
        svc = make_svc()
        svc.repository._find_by_name_and_group_return = None

        created_t = make_template(name="seed-template")
        original_create = svc.repository.create

        async def mock_create(data):
            return created_t

        svc.repository.create = mock_create

        gc = make_gc(primary="g1")
        seed = {"name": "seed-template", "description": "d", "template": "t", "is_active": True}
        with patch("src.services.template_service.DEFAULT_TEMPLATES", [seed]):
            result = await svc.find_by_name_with_group_check("seed-template", gc)
        assert result == created_t

    @pytest.mark.asyncio
    async def test_returns_none_when_not_in_defaults(self):
        svc = make_svc()
        svc.repository._find_by_name_and_group_return = None
        gc = make_gc(primary="g1")
        with patch("src.services.template_service.DEFAULT_TEMPLATES", []):
            result = await svc.find_by_name_with_group_check("unknown", gc)
        assert result is None

    @pytest.mark.asyncio
    async def test_seed_creation_exception_returns_none(self):
        svc = make_svc()
        svc.repository._find_by_name_and_group_return = None

        async def failing_create(data):
            raise Exception("DB error")

        svc.repository.create = failing_create
        gc = make_gc(primary="g1")
        seed = {"name": "seed-fail", "description": "d", "template": "t", "is_active": True}
        with patch("src.services.template_service.DEFAULT_TEMPLATES", [seed]):
            result = await svc.find_by_name_with_group_check("seed-fail", gc)
        assert result is None


# ---------------------------------------------------------------------------
# create_template / create_with_group / create_new_template / create_template_with_group
# ---------------------------------------------------------------------------

class TestCreateTemplate:
    @pytest.mark.asyncio
    async def test_create_template(self):
        svc = make_svc()
        data = PromptTemplateCreate(name="t1", description="d", template="content", is_active=True)
        result = await svc.create_template(data)
        assert result.name == "t1"

    @pytest.mark.asyncio
    async def test_create_new_template_delegates(self):
        svc = make_svc()
        data = PromptTemplateCreate(name="t2", description="d", template="c", is_active=True)
        result = await svc.create_new_template(data)
        assert result.name == "t2"

    @pytest.mark.asyncio
    async def test_create_with_group_assigns_group(self):
        svc = make_svc()
        data = PromptTemplateCreate(name="t3", description="d", template="c", is_active=True)
        gc = make_gc(primary="g1", email="u@test.com")
        result = await svc.create_with_group(data, gc)
        assert result.group_id == "g1"
        assert result.created_by_email == "u@test.com"

    @pytest.mark.asyncio
    async def test_create_template_with_group_delegates(self):
        svc = make_svc()
        data = PromptTemplateCreate(name="t4", description="d", template="c", is_active=True)
        gc = make_gc(primary="g1")
        result = await svc.create_template_with_group(data, gc)
        assert result is not None


# ---------------------------------------------------------------------------
# update_template / update_with_group_check
# ---------------------------------------------------------------------------

class TestUpdateTemplate:
    @pytest.mark.asyncio
    async def test_update_template_calls_repo(self):
        svc = make_svc()
        data = PromptTemplateUpdate(template="new content")
        result = await svc.update_template(1, data)
        assert result.template == "new content"

    @pytest.mark.asyncio
    async def test_update_with_group_check_returns_none_when_not_found(self):
        svc = make_svc()
        svc.repository._get_return = None
        data = PromptTemplateUpdate(template="x")
        result = await svc.update_with_group_check(1, data, make_gc())
        assert result is None

    @pytest.mark.asyncio
    async def test_update_with_group_check_own_group(self):
        svc = make_svc()
        t = make_template(group_id="g1")
        svc.repository._get_return = t
        data = PromptTemplateUpdate(template="updated")
        gc = make_gc(primary="g1")
        result = await svc.update_with_group_check(1, data, gc)
        assert result is not None

    @pytest.mark.asyncio
    async def test_update_with_group_check_no_current_group(self):
        svc = make_svc()
        t = make_template(group_id="g2")
        svc.repository._get_return = t
        data = PromptTemplateUpdate(template="x")
        gc = make_gc(primary=None)
        gc.primary_group_id = None
        result = await svc.update_with_group_check(1, data, gc)
        assert result is None

    @pytest.mark.asyncio
    async def test_update_with_group_check_upserts_new_group_row(self):
        svc = make_svc()
        original = make_template(id=1, name="t", group_id=None, template="old")
        svc.repository._get_return = original
        svc.repository._find_by_name_and_group_return = None  # no existing group row
        data = PromptTemplateUpdate(template="new")
        gc = make_gc(primary="g1", email="u@test.com")
        result = await svc.update_with_group_check(1, data, gc)
        assert result is not None

    @pytest.mark.asyncio
    async def test_update_with_group_check_updates_existing_group_row(self):
        svc = make_svc()
        original = make_template(id=1, name="t", group_id=None)
        existing_group = make_template(id=2, name="t", group_id="g1")
        svc.repository._get_return = original
        svc.repository._find_by_name_and_group_return = existing_group
        data = PromptTemplateUpdate(template="newer")
        gc = make_gc(primary="g1")
        result = await svc.update_with_group_check(1, data, gc)
        assert result is not None


# ---------------------------------------------------------------------------
# delete_template / delete_with_group_check / delete_all_templates
# ---------------------------------------------------------------------------

class TestDeleteTemplate:
    @pytest.mark.asyncio
    async def test_delete_template_returns_true(self):
        svc = make_svc()
        result = await svc.delete_template(1)
        assert result is True

    @pytest.mark.asyncio
    async def test_delete_with_group_check_returns_false_when_not_found(self):
        svc = make_svc()
        svc.repository._get_return = None
        result = await svc.delete_with_group_check(1, make_gc())
        assert result is False

    @pytest.mark.asyncio
    async def test_delete_with_group_check_authorized(self):
        svc = make_svc()
        t = make_template(group_id="g1")
        svc.repository._get_return = t
        result = await svc.delete_with_group_check(1, make_gc(group_ids=["g1"]))
        assert result is True

    @pytest.mark.asyncio
    async def test_delete_all_templates_returns_count(self):
        svc = make_svc()
        svc.repository.active_templates = [make_template(), make_template(id=2)]
        result = await svc.delete_all_templates()
        assert isinstance(result, int)


# ---------------------------------------------------------------------------
# delete_all_for_group_internal
# ---------------------------------------------------------------------------

class TestDeleteAllForGroup:
    @pytest.mark.asyncio
    async def test_returns_zero_when_no_context(self):
        svc = make_svc()
        assert await svc.delete_all_for_group_internal(None) == 0

    @pytest.mark.asyncio
    async def test_returns_zero_when_no_group_ids(self):
        svc = make_svc()
        gc = make_gc(group_ids=[])
        gc.group_ids = []
        assert await svc.delete_all_for_group_internal(gc) == 0

    @pytest.mark.asyncio
    async def test_deletes_group_templates(self):
        svc = make_svc()
        t = make_template(id=1, group_id="g1")
        svc.repository.active_templates = [t]
        gc = make_gc(group_ids=["g1"])
        count = await svc.delete_all_for_group_internal(gc)
        assert count == 1


# ---------------------------------------------------------------------------
# reset_templates / reset_templates_with_group
# ---------------------------------------------------------------------------

class TestResetTemplates:
    @pytest.mark.asyncio
    async def test_reset_templates_creates_defaults(self):
        svc = make_svc()
        seeds = [
            {"name": "s1", "description": "d", "template": "t", "is_active": True},
            {"name": "s2", "description": "d", "template": "t", "is_active": True},
        ]
        with patch("src.services.template_service.DEFAULT_TEMPLATES", seeds):
            count = await svc.reset_templates()
        assert count == 2

    @pytest.mark.asyncio
    async def test_reset_with_group_updates_existing(self):
        svc = make_svc()
        existing_t = make_template(name="s1", group_id=None)
        svc.repository._find_by_name_and_group_return = existing_t
        seeds = [
            {"name": "s1", "description": "new desc", "template": "new tmpl", "is_active": True},
        ]
        with patch("src.services.template_service.DEFAULT_TEMPLATES", seeds):
            gc = make_gc(primary="g1")
            count = await svc.reset_templates_with_group(gc)
        assert count == 1

    @pytest.mark.asyncio
    async def test_reset_with_group_creates_new_base(self):
        svc = make_svc()
        svc.repository._find_by_name_and_group_return = None
        seeds = [
            {"name": "brand-new", "description": "d", "template": "t", "is_active": True},
        ]
        with patch("src.services.template_service.DEFAULT_TEMPLATES", seeds):
            gc = make_gc(primary="g1")
            count = await svc.reset_templates_with_group(gc)
        assert count == 1

    @pytest.mark.asyncio
    async def test_reset_with_group_handles_exception(self):
        svc = make_svc()
        svc.repository._find_by_name_and_group_return = None

        async def failing_create(data):
            raise Exception("DB error")

        svc.repository.create = failing_create
        seeds = [{"name": "err-seed", "description": "d", "template": "t", "is_active": True}]
        with patch("src.services.template_service.DEFAULT_TEMPLATES", seeds):
            gc = make_gc(primary="g1")
            count = await svc.reset_templates_with_group(gc)
        assert count == 0


# ---------------------------------------------------------------------------
# get_template_content / _get_template_content_instance
# ---------------------------------------------------------------------------

class TestGetTemplateContent:
    @pytest.mark.asyncio
    async def test_returns_template_content(self):
        svc = make_svc()
        t = make_template(template="hello world")
        svc.repository._find_by_name_return = t
        result = await svc.get_template_content("my-template")
        assert result == "hello world"

    @pytest.mark.asyncio
    async def test_returns_default_when_not_found(self):
        svc = make_svc()
        svc.repository._find_by_name_return = None
        result = await svc.get_template_content("missing", default_template="default")
        assert result == "default"

    @pytest.mark.asyncio
    async def test_returns_empty_string_when_not_found_no_default(self):
        svc = make_svc()
        svc.repository._find_by_name_return = None
        result = await svc.get_template_content("missing")
        assert result == ""

    @pytest.mark.asyncio
    async def test_returns_default_on_exception(self):
        svc = make_svc()

        async def raise_find(name):
            raise Exception("db error")

        svc.repository.find_by_name = raise_find
        result = await svc.get_template_content("x", default_template="fallback")
        assert result == "fallback"

    @pytest.mark.asyncio
    async def test_returns_empty_on_exception_no_default(self):
        svc = make_svc()

        async def raise_find(name):
            raise Exception("db error")

        svc.repository.find_by_name = raise_find
        result = await svc.get_template_content("x")
        assert result == ""


# ---------------------------------------------------------------------------
# _get_effective_template_content_instance
# ---------------------------------------------------------------------------

class TestGetEffectiveTemplateContent:
    @pytest.mark.asyncio
    async def test_returns_group_template_content(self):
        svc = make_svc()
        group_t = make_template(template="group content", group_id="g1")

        async def find_by_name_and_group(name, gid):
            if gid == "g1":
                return group_t
            return None

        svc.repository.find_by_name_and_group = find_by_name_and_group
        gc = make_gc(primary="g1")
        result = await svc._get_effective_template_content_instance("t1", gc)
        assert result == "group content"

    @pytest.mark.asyncio
    async def test_falls_back_to_base_template(self):
        svc = make_svc()
        base_t = make_template(template="base content", group_id=None)

        async def find_by_name_and_group(name, gid):
            if gid is None:
                return base_t
            return None

        svc.repository.find_by_name_and_group = find_by_name_and_group
        gc = make_gc(primary="g1")
        result = await svc._get_effective_template_content_instance("t1", gc)
        assert result == "base content"

    @pytest.mark.asyncio
    async def test_returns_empty_string_when_no_context(self):
        svc = make_svc()
        svc.repository._find_by_name_and_group_return = None
        result = await svc._get_effective_template_content_instance("t1", None)
        assert result == ""

    @pytest.mark.asyncio
    async def test_returns_empty_string_on_exception(self):
        svc = make_svc()

        async def failing(name, gid):
            raise Exception("db error")

        svc.repository.find_by_name_and_group = failing
        gc = make_gc(primary="g1")
        result = await svc._get_effective_template_content_instance("t1", gc)
        assert result == ""


# ---------------------------------------------------------------------------
# find_template_by_name_with_group delegates to find_by_name_with_group_check
# ---------------------------------------------------------------------------

class TestFindTemplateByNameWithGroup:
    @pytest.mark.asyncio
    async def test_delegates(self):
        svc = make_svc()
        t = make_template(name="specific")
        svc.repository._find_by_name_and_group_return = t
        gc = make_gc(primary="g1")
        result = await svc.find_template_by_name_with_group("specific", gc)
        assert result == t

    @pytest.mark.asyncio
    async def test_get_template_with_group_check_delegates(self):
        svc = make_svc()
        t = make_template(group_id=None)
        svc.repository._get_return = t
        result = await svc.get_template_with_group_check(1, make_gc())
        assert result == t
