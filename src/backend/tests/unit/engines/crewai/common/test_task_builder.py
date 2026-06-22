"""Shared Task-args assembly (common/task_builder.build_task_args) used by BOTH
the crew path (task_helpers.create_task) and the flow path
(flow.modules.task_adapter.configure_task). Pins the unified behavior: base fields,
markdown, Genie formatting, code/LLM guardrails, and output_pydantic."""

import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.engines.crewai.common.task_builder import build_task_args

GENIE = {"MCP_SERVERS": {"servers": ["Databricks Genie: space"]}}
GENIE_MCP_URL = (
    "https://ws.databricks.com/api/2.0/mcp/genie/01f16bcd318214ec8ef983b7627e0221"
)
GENIE_SPACE_ID = "01f16bcd318214ec8ef983b7627e0221"


def _agent():
    a = MagicMock()
    a.llm.model = "databricks/databricks-claude-sonnet-4-5"
    return a


class TestBaseAssembly:
    @pytest.mark.asyncio
    async def test_defaults(self):
        args = await build_task_args(
            {"description": "D", "expected_output": "E"}, _agent(), []
        )
        assert args["description"] == "D"
        assert args["expected_output"] == "E"
        assert args["tools"] == []
        assert args["async_execution"] is False
        assert args["retry_on_fail"] is False
        assert args["max_retries"] == 3
        assert args["markdown"] is False

    @pytest.mark.asyncio
    async def test_markdown_appended(self):
        args = await build_task_args(
            {"description": "D", "expected_output": "E", "markdown": True}, _agent(), []
        )
        assert "markdown syntax" in args["description"]
        assert "markdown" in args["expected_output"].lower()

    @pytest.mark.asyncio
    async def test_genie_formatting_applied(self):
        args = await build_task_args(
            {"description": "D", "expected_output": "E", "tool_configs": GENIE},
            _agent(),
            [],
        )
        assert "Genie Tool output structure" in args["expected_output"]

    @pytest.mark.asyncio
    async def test_optional_fields_passed_through(self):
        args = await build_task_args(
            {
                "description": "D",
                "expected_output": "E",
                "human_input": True,
                "context": ["t1"],
            },
            _agent(),
            [],
        )
        assert args["human_input"] is True
        assert args["context"] == ["t1"]


class TestCodeGuardrail:
    @pytest.mark.asyncio
    async def test_factory_guardrail_set_with_retry(self):
        with (
            patch(
                "src.engines.crewai.guardrails.guardrail_factory.GuardrailFactory"
            ) as MockGF,
            patch("src.engines.crewai.common.task_builder.GuardrailWrapper") as MockGW,
        ):
            MockGF.create_guardrail.return_value = MagicMock()
            MockGW.return_value = MagicMock()
            args = await build_task_args(
                {
                    "description": "D",
                    "expected_output": "E",
                    "guardrail": {"type": "company_count"},
                },
                _agent(),
                [],
            )
        assert "guardrail" in args
        assert args["retry_on_fail"] is True

    @pytest.mark.asyncio
    async def test_self_reflection_stamps_run_model(self):
        captured = {}

        def _capture(cfg):
            captured["cfg"] = cfg
            return MagicMock()

        with (
            patch(
                "src.engines.crewai.guardrails.guardrail_factory.GuardrailFactory"
            ) as MockGF,
            patch(
                "src.engines.crewai.common.task_builder.GuardrailWrapper",
                return_value=MagicMock(),
            ),
        ):
            MockGF.create_guardrail.side_effect = _capture
            await build_task_args(
                {
                    "description": "D",
                    "expected_output": "E",
                    "guardrail": {"type": "self_reflection"},
                },
                _agent(),
                [],
            )
        sent = json.loads(captured["cfg"])
        assert sent["type"] == "self_reflection"
        assert sent["llm_model"] == "databricks-claude-sonnet-4-5"  # prefix stripped

    @pytest.mark.asyncio
    async def test_guardrail_without_type_reroutes_to_llm(self):
        # A 'guardrail' with description/llm_model but no 'type' is actually an LLM
        # guardrail and must be re-routed.
        with (
            patch("crewai.tasks.llm_guardrail.LLMGuardrail") as MockLLMG,
            patch(
                "src.core.llm_manager.LLMManager.configure_crewai_llm",
                new_callable=AsyncMock,
                return_value=MagicMock(),
            ),
        ):
            MockLLMG.return_value = MagicMock()
            args = await build_task_args(
                {
                    "description": "D",
                    "expected_output": "E",
                    "guardrail": {"description": "validate"},
                },
                _agent(),
                [],
                config={"group_id": "g1"},
            )
        assert "guardrail" in args  # LLM guardrail attached
        MockLLMG.assert_called_once()


class TestLlmGuardrail:
    @pytest.mark.asyncio
    async def test_llm_guardrail_set(self):
        with (
            patch("crewai.tasks.llm_guardrail.LLMGuardrail") as MockLLMG,
            patch(
                "src.core.llm_manager.LLMManager.configure_crewai_llm",
                new_callable=AsyncMock,
                return_value=MagicMock(),
            ),
        ):
            MockLLMG.return_value = MagicMock()
            args = await build_task_args(
                {
                    "description": "D",
                    "expected_output": "E",
                    "llm_guardrail": {"description": "Validate output"},
                },
                _agent(),
                [],
                config={"group_id": "g1"},
            )
        assert "guardrail" in args
        assert args["retry_on_fail"] is True

    @pytest.mark.asyncio
    async def test_missing_group_id_raises(self):
        with patch(
            "src.utils.user_context.UserContext.get_group_context", return_value=None
        ):
            with pytest.raises(ValueError):
                await build_task_args(
                    {
                        "description": "D",
                        "expected_output": "E",
                        "llm_guardrail": {"description": "Validate"},
                    },
                    _agent(),
                    [],
                    config=None,
                )


class TestGenieMcpSpaceId:
    """build_task_args must hand the selected Genie MCP server's space id to a
    GenieTool the generator also assigned (so it doesn't error 'not configured')."""

    @staticmethod
    def _genie_mcp_tool():
        adapter = SimpleNamespace(server_url=GENIE_MCP_URL)
        return SimpleNamespace(
            name="genie_query_space",
            _mcp_tool_wrapper=SimpleNamespace(adapter=adapter),
        )

    @staticmethod
    def _genie_tool():
        # Matched by apply_genie_mcp_space_id via .name == "GenieTool".
        return SimpleNamespace(name="GenieTool", _space_id=None)

    @pytest.mark.asyncio
    async def test_genie_tool_space_id_filled_from_mcp(self):
        genie_tool = self._genie_tool()
        tools = [self._genie_mcp_tool(), genie_tool]
        await build_task_args(
            {"description": "D", "expected_output": "E", "tool_configs": GENIE},
            _agent(),
            tools,
        )
        assert genie_tool._space_id == GENIE_SPACE_ID

    @pytest.mark.asyncio
    async def test_noop_without_mcp_genie_tool(self):
        genie_tool = self._genie_tool()
        await build_task_args(
            {"description": "D", "expected_output": "E"}, _agent(), [genie_tool]
        )
        assert genie_tool._space_id is None


class TestOutputPydantic:
    @pytest.mark.asyncio
    async def test_output_pydantic_resolved(self):
        class FakeModel:
            __name__ = "FakeModel"

        with (
            patch(
                "src.engines.crewai.helpers.task_adapter.get_pydantic_class_from_name",
                new_callable=AsyncMock,
                return_value=FakeModel,
            ),
            patch(
                "src.engines.crewai.helpers.model_conversion_handler.get_compatible_converter_for_model",
                return_value=(None, FakeModel, False, False),
            ),
        ):
            args = await build_task_args(
                {
                    "description": "D",
                    "expected_output": "E",
                    "output_pydantic": "FakeModel",
                },
                _agent(),
                [],
            )
        assert args["output_pydantic"] is FakeModel

    @pytest.mark.asyncio
    async def test_unresolvable_output_pydantic_dropped(self):
        with patch(
            "src.engines.crewai.helpers.task_adapter.get_pydantic_class_from_name",
            new_callable=AsyncMock,
            return_value=None,
        ):
            args = await build_task_args(
                {
                    "description": "D",
                    "expected_output": "E",
                    "output_pydantic": "Missing",
                },
                _agent(),
                [],
            )
        assert "output_pydantic" not in args
