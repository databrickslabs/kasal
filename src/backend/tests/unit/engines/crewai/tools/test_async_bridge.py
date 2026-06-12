"""Tests for the context-preserving sync→async bridge.

Regression coverage for the bug where CrewAI tools offloaded
``LLMManager.completion`` to a fresh thread via a bare
``ThreadPoolExecutor.submit(asyncio.run, coro)`` — new threads start with an
EMPTY contextvars Context, so the UserContext group_id (and OBO token) were
silently dropped and every completion raised
``ValueError: group_id is required``.
"""
import asyncio

import pytest
from unittest.mock import AsyncMock, patch

from src.engines.crewai.tools.async_bridge import (
    run_async_with_context,
    run_sync_with_context,
)
from src.utils.user_context import GroupContext, UserContext


def _set_group(group_id: str) -> None:
    UserContext.set_group_context(
        GroupContext(
            group_ids=[group_id],
            group_email=f"{group_id}@example.com",
            email_domain="example.com",
        )
    )


async def _read_group_id():
    gc = UserContext.get_group_context()
    return gc.primary_group_id if gc else None


class TestRunAsyncWithContext:
    def test_no_running_loop_runs_inline(self):
        _set_group("grp_inline")
        try:
            assert run_async_with_context(_read_group_id()) == "grp_inline"
        finally:
            UserContext.clear_context()

    def test_running_loop_offloads_and_preserves_context(self):
        """The offloaded worker thread must see the caller's ContextVars."""
        async def _caller():
            _set_group("grp_offload")
            # We're inside a running loop → bridge offloads to a thread
            return run_async_with_context(_read_group_id())

        try:
            assert asyncio.run(_caller()) == "grp_offload"
        finally:
            UserContext.clear_context()

    def test_returns_coroutine_result(self):
        async def _compute():
            return 41 + 1

        assert run_async_with_context(_compute()) == 42

    def test_propagates_exceptions(self):
        async def _boom():
            raise RuntimeError("boom")

        with pytest.raises(RuntimeError, match="boom"):
            run_async_with_context(_boom())


class TestRunSyncWithContext:
    def test_no_running_loop_runs_inline(self):
        assert run_sync_with_context(lambda: "ok") == "ok"

    def test_running_loop_offloads_and_preserves_context(self):
        def _blocking_read():
            gc = UserContext.get_group_context()
            return gc.primary_group_id if gc else None

        async def _caller():
            _set_group("grp_sync")
            return run_sync_with_context(_blocking_read)

        try:
            assert asyncio.run(_caller()) == "grp_sync"
        finally:
            UserContext.clear_context()


class TestToolLLMBridgeRegression:
    """The two UCMV tools must propagate group_id into LLMManager.completion."""

    @pytest.mark.parametrize(
        "tool_module,tool_class",
        [
            (
                "src.engines.crewai.tools.custom.pbi_visual_ucmv_mapper_tool",
                "PBIVisualUCMVMapperTool",
            ),
            (
                "src.engines.crewai.tools.custom.ucmv_genie_config_generator_tool",
                "UCMVGenieConfigGeneratorTool",
            ),
        ],
    )
    def test_call_llm_preserves_group_context(self, tool_module, tool_class):
        import importlib

        module = importlib.import_module(tool_module)
        tool = getattr(module, tool_class)()

        seen_group_ids = []

        async def _capturing_completion(*args, **kwargs):
            seen_group_ids.append(await _read_group_id())
            return "{}"

        _set_group("grp_tool_bridge")
        try:
            with patch(
                "src.core.llm_manager.LLMManager.completion",
                AsyncMock(side_effect=_capturing_completion),
            ):
                result = tool._call_llm("test prompt", "databricks/test-model")
            assert result == "{}"
            assert seen_group_ids == ["grp_tool_bridge"]
        finally:
            UserContext.clear_context()
