"""A run of a published capability executes in the publication's teamspace.

An unpinned MCP/A2A caller in several teamspaces resolves to the UNION of them;
a run created under that context was saved to whichever group came first, so a
crew published in ``user_dev_localhost`` ran (and was recorded) in
``bi-specialist``.
"""

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.services.external.identity import (
    ExternalAuthError,
    ExternalCaller,
    narrow_to_group,
)
from src.services.external.invocation import start_run

_UNION = ["bi-specialist", "test_befd4be5", "user_dev_localhost"]
_PUB_GROUP = "user_dev_localhost"


class _Ctx:
    def __init__(self, group_ids, role="operator"):
        self.group_ids = list(group_ids)
        self.group_email = "dev@localhost"
        self.access_token = "tok-123"
        self.user_role = role

    @property
    def primary_group_id(self):
        return self.group_ids[0] if self.group_ids else None


def _caller(group_ids):
    return ExternalCaller(
        group_context=_Ctx(group_ids), protocol="mcp", identifier="dev@localhost"
    )


def _publication(entity_type="crew", group_id=_PUB_GROUP):
    return SimpleNamespace(
        entity_type=entity_type,
        entity_id="00000000-0000-0000-0000-000000000001",
        external_name="gather_best_rtx_3090_ti_models",
        group_id=group_id,
    )


def _patch_from_email():
    """from_email as it behaves for a pinned group: that group only."""

    async def _resolve(email, access_token=None, group_id=None, **_):
        return _Ctx([group_id], role="editor")

    return patch(
        "src.services.external.identity.GroupContext.from_email",
        new=AsyncMock(side_effect=_resolve),
    )


class TestNarrowToGroup:
    @pytest.mark.asyncio
    async def test_union_caller_is_narrowed_to_the_one_group(self):
        with _patch_from_email() as from_email:
            narrowed = await narrow_to_group(_caller(_UNION), _PUB_GROUP)
        assert narrowed.group_ids == [_PUB_GROUP]
        assert narrowed.group_context.user_role == "editor"
        kwargs = from_email.await_args.kwargs
        assert kwargs["group_id"] == _PUB_GROUP
        assert kwargs["access_token"] == "tok-123"  # OBO kept
        assert narrowed.origin == "mcp:dev@localhost"

    @pytest.mark.asyncio
    async def test_pinned_caller_is_unchanged(self):
        caller = _caller([_PUB_GROUP])
        with _patch_from_email() as from_email:
            narrowed = await narrow_to_group(caller, _PUB_GROUP)
        assert narrowed is caller
        from_email.assert_not_awaited()

    @pytest.mark.asyncio
    @pytest.mark.parametrize("group_id", ["other_team", None, ""])
    async def test_a_group_the_caller_is_not_in_is_refused(self, group_id):
        with _patch_from_email() as from_email:
            with pytest.raises(ExternalAuthError):
                await narrow_to_group(_caller(_UNION), group_id)
        from_email.assert_not_awaited()


class TestStartRunUsesThePublicationsGroup:
    @pytest.mark.asyncio
    async def test_crew_run_is_created_in_the_publication_group(self):
        service = MagicMock()
        service.create_execution = AsyncMock(
            return_value={"execution_id": "run-1", "status": "pending"}
        )
        crews = MagicMock()
        crews.get = AsyncMock(return_value=SimpleNamespace(reasoning_config=None))
        yaml = ({"a": {"role": "r", "llm": "m"}}, {"t": {"description": "d"}})
        with (
            _patch_from_email(),
            patch("src.services.catalog.crews.CrewService", return_value=crews),
            patch(
                "src.services.external.invocation._crew_to_yaml",
                new=AsyncMock(return_value=yaml),
            ),
            patch(
                "src.services.execution.service.ExecutionService", return_value=service
            ),
        ):
            result = await start_run(_caller(_UNION), _publication())
        assert result.run_id == "run-1"
        context = service.create_execution.await_args.kwargs["group_context"]
        assert context.group_ids == [_PUB_GROUP]

    @pytest.mark.asyncio
    async def test_flow_run_is_created_in_the_publication_group(self):
        flows = MagicMock()
        flows.run_flow = AsyncMock(return_value={"job_id": "run-2"})
        with (
            _patch_from_email(),
            patch(
                "src.services.flow_builder.kasal_flow_service.KasalFlowService",
                return_value=flows,
            ),
        ):
            result = await start_run(_caller(_UNION), _publication("flow"))
        assert result.run_id == "run-2"
        kwargs = flows.run_flow.await_args.kwargs
        assert kwargs["group_context"].group_ids == [_PUB_GROUP]
        assert kwargs["user_token"] == "tok-123"

    @pytest.mark.asyncio
    async def test_a_publication_outside_the_callers_groups_is_refused(self):
        flows = MagicMock()
        flows.run_flow = AsyncMock()
        with (
            _patch_from_email(),
            patch(
                "src.services.flow_builder.kasal_flow_service.KasalFlowService",
                return_value=flows,
            ),
        ):
            with pytest.raises(ExternalAuthError):
                await start_run(
                    _caller(_UNION), _publication("flow", group_id="other_team")
                )
        flows.run_flow.assert_not_awaited()
