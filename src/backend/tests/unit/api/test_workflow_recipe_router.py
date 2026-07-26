"""Tests for the workflow-recipe endpoints.

Handlers are called directly (the repo's router-test style) — the concern here
is the HTTP boundary's own logic: who is allowed in, and that the service result
passes through unchanged.

Permissions are not incidental. ``/by-job`` and ``/suggest`` are readable by
anyone who can start a run, while curating and reading the effectiveness report
change (or describe) what the whole workspace gets offered, so they are
admin/editor. Getting that backwards would let an operator silently reshape
every future generation.
"""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from src.api.workflow_recipe_router import (
    curate_recipe,
    recipe_effectiveness,
    recipes_by_job,
)
from src.core.exceptions import ForbiddenError


def _ctx(role="admin"):
    return SimpleNamespace(
        user_role=role,
        group_ids=["g1"],
        primary_group_id="g1",
        group_email="dev@localhost",
    )


class TestByJob:
    @pytest.mark.asyncio
    async def test_returns_the_service_index_unchanged(self):
        service = AsyncMock()
        service.recipes_by_job.return_value = {
            "job-1": {
                "recipe_id": 7,
                "curation": "good",
                "intent_text": "Load US and EU",
                "run_count": 3,
                "times_reused": 0,
            }
        }
        result = await recipes_by_job(service, _ctx("operator"))

        assert result["job-1"]["recipe_id"] == 7
        service.recipes_by_job.assert_awaited_once_with(["g1"])

    @pytest.mark.asyncio
    async def test_unknown_role_is_refused(self):
        with pytest.raises(ForbiddenError):
            await recipes_by_job(AsyncMock(), _ctx("viewer"))


class TestEffectiveness:
    @pytest.mark.asyncio
    async def test_passes_the_window_through(self):
        service = AsyncMock()
        service.effectiveness.return_value = {"generations": 0}
        await recipe_effectiveness(service, _ctx("editor"), days=7)
        service.effectiveness.assert_awaited_once_with(["g1"], days=7)

    @pytest.mark.asyncio
    async def test_operator_cannot_read_it(self):
        """It describes how the workspace's generation behaviour is being
        shaped — a configuration concern, not a run-level one."""
        with pytest.raises(ForbiddenError):
            await recipe_effectiveness(AsyncMock(), _ctx("operator"))


class TestCurationPermissions:
    @pytest.mark.asyncio
    async def test_operator_cannot_curate(self):
        """Curating changes what every later generation is offered, so it is
        deliberately not an operator-level action."""
        request = SimpleNamespace(curation="good")
        with pytest.raises(ForbiddenError):
            await curate_recipe(1, request, AsyncMock(), _ctx("operator"))
