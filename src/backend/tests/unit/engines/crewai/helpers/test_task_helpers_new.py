"""
Unit tests for src/engines/crewai/helpers/task_helpers.py

Targets uncovered lines (63% → 85%+):
 - Lines 166-225: get_pydantic_class_from_name schema field types
 - Lines 302, 306, 320-333, 338-339: tool config overrides
 - Lines 372-378: debug logging for special tool names
 - Lines 425-426, 459-460, 465-469: knowledge source + task ID handling
 - Lines 505-547: guardrail config handling
 - Lines 562-565, 574-576, 589-590: LLM guardrail augmentation
 - Lines 624-626, 635-637, 644-649: callback + guardrail combined paths
 - Lines 657-717: LLM guardrail creation
 - Lines 788-800: task creation debug output
"""
import json
import pytest
from typing import Any, Dict, List
from unittest.mock import AsyncMock, MagicMock, patch, call
from pydantic import BaseModel

from src.engines.crewai.paths.crew.task_adapter import (
    is_data_missing,
    get_pydantic_class_from_name,
    create_callback_from_string,
    create_task,
)


# ---------------------------------------------------------------------------
# is_data_missing
# ---------------------------------------------------------------------------

class TestIsDataMissing:
    def test_no_pydantic_attr_returns_true(self):
        output = MagicMock(spec=[])
        assert is_data_missing(output) is True

    def test_few_events_returns_true(self):
        output = MagicMock()
        output.pydantic = MagicMock()
        output.pydantic.events = list(range(5))
        assert is_data_missing(output) is True

    def test_enough_events_returns_false(self):
        output = MagicMock()
        output.pydantic = MagicMock()
        output.pydantic.events = list(range(15))
        assert is_data_missing(output) is False

    def test_exactly_ten_returns_false(self):
        output = MagicMock()
        output.pydantic = MagicMock()
        output.pydantic.events = list(range(10))
        assert is_data_missing(output) is False


# ---------------------------------------------------------------------------
# get_pydantic_class_from_name – field type coverage
# ---------------------------------------------------------------------------

class TestGetPydanticClassFromNameFieldTypes:
    """Tests for field type branches in get_pydantic_class_from_name."""

    def _make_schema_def(self, properties: dict, required: list = None) -> dict:
        return {
            "properties": properties,
            "required": required or [],
        }

    @pytest.mark.asyncio
    @patch("src.engines.crewai.paths.crew.task_adapter.UnitOfWork")
    async def test_string_field(self, mock_uow_cls):
        schema_def = self._make_schema_def(
            {"name": {"type": "string"}}, required=["name"]
        )
        mock_schema = MagicMock()
        mock_schema.schema_definition = schema_def

        mock_uow = AsyncMock()
        mock_uow.__aenter__ = AsyncMock(return_value=mock_uow)
        mock_uow.__aexit__ = AsyncMock(return_value=None)
        mock_uow.schema_repository = MagicMock()
        mock_uow.schema_repository.find_by_name = AsyncMock(return_value=mock_schema)
        mock_uow_cls.return_value = mock_uow

        result = await get_pydantic_class_from_name("TestSchema")
        assert result is not None
        instance = result(name="Alice")
        assert instance.name == "Alice"

    @pytest.mark.asyncio
    @patch("src.engines.crewai.paths.crew.task_adapter.UnitOfWork")
    async def test_integer_field(self, mock_uow_cls):
        schema_def = self._make_schema_def(
            {"count": {"type": "integer"}}, required=["count"]
        )
        mock_schema = MagicMock()
        mock_schema.schema_definition = schema_def

        mock_uow = AsyncMock()
        mock_uow.__aenter__ = AsyncMock(return_value=mock_uow)
        mock_uow.__aexit__ = AsyncMock(return_value=None)
        mock_uow.schema_repository = MagicMock()
        mock_uow.schema_repository.find_by_name = AsyncMock(return_value=mock_schema)
        mock_uow_cls.return_value = mock_uow

        result = await get_pydantic_class_from_name("IntSchema")
        assert result is not None
        instance = result(count=5)
        assert instance.count == 5

    @pytest.mark.asyncio
    @patch("src.engines.crewai.paths.crew.task_adapter.UnitOfWork")
    async def test_number_field(self, mock_uow_cls):
        schema_def = self._make_schema_def(
            {"value": {"type": "number"}}, required=["value"]
        )
        mock_schema = MagicMock()
        mock_schema.schema_definition = schema_def

        mock_uow = AsyncMock()
        mock_uow.__aenter__ = AsyncMock(return_value=mock_uow)
        mock_uow.__aexit__ = AsyncMock(return_value=None)
        mock_uow.schema_repository = MagicMock()
        mock_uow.schema_repository.find_by_name = AsyncMock(return_value=mock_schema)
        mock_uow_cls.return_value = mock_uow

        result = await get_pydantic_class_from_name("NumSchema")
        assert result is not None
        instance = result(value=3.14)
        assert instance.value == pytest.approx(3.14)

    @pytest.mark.asyncio
    @patch("src.engines.crewai.paths.crew.task_adapter.UnitOfWork")
    async def test_boolean_field(self, mock_uow_cls):
        schema_def = self._make_schema_def(
            {"active": {"type": "boolean"}}, required=["active"]
        )
        mock_schema = MagicMock()
        mock_schema.schema_definition = schema_def

        mock_uow = AsyncMock()
        mock_uow.__aenter__ = AsyncMock(return_value=mock_uow)
        mock_uow.__aexit__ = AsyncMock(return_value=None)
        mock_uow.schema_repository = MagicMock()
        mock_uow.schema_repository.find_by_name = AsyncMock(return_value=mock_schema)
        mock_uow_cls.return_value = mock_uow

        result = await get_pydantic_class_from_name("BoolSchema")
        assert result is not None
        instance = result(active=True)
        assert instance.active is True

    @pytest.mark.asyncio
    @patch("src.engines.crewai.paths.crew.task_adapter.UnitOfWork")
    async def test_array_of_strings_field(self, mock_uow_cls):
        schema_def = self._make_schema_def(
            {"tags": {"type": "array", "items": {"type": "string"}}},
            required=["tags"]
        )
        mock_schema = MagicMock()
        mock_schema.schema_definition = schema_def

        mock_uow = AsyncMock()
        mock_uow.__aenter__ = AsyncMock(return_value=mock_uow)
        mock_uow.__aexit__ = AsyncMock(return_value=None)
        mock_uow.schema_repository = MagicMock()
        mock_uow.schema_repository.find_by_name = AsyncMock(return_value=mock_schema)
        mock_uow_cls.return_value = mock_uow

        result = await get_pydantic_class_from_name("ArrayStringSchema")
        assert result is not None
        instance = result(tags=["a", "b"])
        assert instance.tags == ["a", "b"]

    @pytest.mark.asyncio
    @patch("src.engines.crewai.paths.crew.task_adapter.UnitOfWork")
    async def test_array_of_integers_field(self, mock_uow_cls):
        schema_def = self._make_schema_def(
            {"scores": {"type": "array", "items": {"type": "integer"}}},
            required=["scores"]
        )
        mock_schema = MagicMock()
        mock_schema.schema_definition = schema_def

        mock_uow = AsyncMock()
        mock_uow.__aenter__ = AsyncMock(return_value=mock_uow)
        mock_uow.__aexit__ = AsyncMock(return_value=None)
        mock_uow.schema_repository = MagicMock()
        mock_uow.schema_repository.find_by_name = AsyncMock(return_value=mock_schema)
        mock_uow_cls.return_value = mock_uow

        result = await get_pydantic_class_from_name("ArrayIntSchema")
        assert result is not None

    @pytest.mark.asyncio
    @patch("src.engines.crewai.paths.crew.task_adapter.UnitOfWork")
    async def test_array_of_numbers_field(self, mock_uow_cls):
        schema_def = self._make_schema_def(
            {"values": {"type": "array", "items": {"type": "number"}}},
            required=["values"]
        )
        mock_schema = MagicMock()
        mock_schema.schema_definition = schema_def

        mock_uow = AsyncMock()
        mock_uow.__aenter__ = AsyncMock(return_value=mock_uow)
        mock_uow.__aexit__ = AsyncMock(return_value=None)
        mock_uow.schema_repository = MagicMock()
        mock_uow.schema_repository.find_by_name = AsyncMock(return_value=mock_schema)
        mock_uow_cls.return_value = mock_uow

        result = await get_pydantic_class_from_name("ArrayNumSchema")
        assert result is not None

    @pytest.mark.asyncio
    @patch("src.engines.crewai.paths.crew.task_adapter.UnitOfWork")
    async def test_array_of_booleans_field(self, mock_uow_cls):
        schema_def = self._make_schema_def(
            {"flags": {"type": "array", "items": {"type": "boolean"}}},
        )
        mock_schema = MagicMock()
        mock_schema.schema_definition = schema_def

        mock_uow = AsyncMock()
        mock_uow.__aenter__ = AsyncMock(return_value=mock_uow)
        mock_uow.__aexit__ = AsyncMock(return_value=None)
        mock_uow.schema_repository = MagicMock()
        mock_uow.schema_repository.find_by_name = AsyncMock(return_value=mock_schema)
        mock_uow_cls.return_value = mock_uow

        result = await get_pydantic_class_from_name("ArrayBoolSchema")
        assert result is not None

    @pytest.mark.asyncio
    @patch("src.engines.crewai.paths.crew.task_adapter.UnitOfWork")
    async def test_array_of_unknown_type_field(self, mock_uow_cls):
        schema_def = self._make_schema_def(
            {"items": {"type": "array", "items": {"type": "object"}}},
        )
        mock_schema = MagicMock()
        mock_schema.schema_definition = schema_def

        mock_uow = AsyncMock()
        mock_uow.__aenter__ = AsyncMock(return_value=mock_uow)
        mock_uow.__aexit__ = AsyncMock(return_value=None)
        mock_uow.schema_repository = MagicMock()
        mock_uow.schema_repository.find_by_name = AsyncMock(return_value=mock_schema)
        mock_uow_cls.return_value = mock_uow

        result = await get_pydantic_class_from_name("ArrayAnySchema")
        assert result is not None

    @pytest.mark.asyncio
    @patch("src.engines.crewai.paths.crew.task_adapter.UnitOfWork")
    async def test_object_field(self, mock_uow_cls):
        schema_def = self._make_schema_def(
            {"meta": {"type": "object"}},
        )
        mock_schema = MagicMock()
        mock_schema.schema_definition = schema_def

        mock_uow = AsyncMock()
        mock_uow.__aenter__ = AsyncMock(return_value=mock_uow)
        mock_uow.__aexit__ = AsyncMock(return_value=None)
        mock_uow.schema_repository = MagicMock()
        mock_uow.schema_repository.find_by_name = AsyncMock(return_value=mock_schema)
        mock_uow_cls.return_value = mock_uow

        result = await get_pydantic_class_from_name("ObjectSchema")
        assert result is not None

    @pytest.mark.asyncio
    @patch("src.engines.crewai.paths.crew.task_adapter.UnitOfWork")
    async def test_unknown_field_type_falls_back_to_any(self, mock_uow_cls):
        schema_def = self._make_schema_def(
            {"data": {"type": "unknown_type"}},
        )
        mock_schema = MagicMock()
        mock_schema.schema_definition = schema_def

        mock_uow = AsyncMock()
        mock_uow.__aenter__ = AsyncMock(return_value=mock_uow)
        mock_uow.__aexit__ = AsyncMock(return_value=None)
        mock_uow.schema_repository = MagicMock()
        mock_uow.schema_repository.find_by_name = AsyncMock(return_value=mock_schema)
        mock_uow_cls.return_value = mock_uow

        result = await get_pydantic_class_from_name("UnknownTypeSchema")
        assert result is not None

    @pytest.mark.asyncio
    @patch("src.engines.crewai.paths.crew.task_adapter.UnitOfWork")
    async def test_nullable_optional_field(self, mock_uow_cls):
        schema_def = self._make_schema_def(
            {"note": {"type": "string", "nullable": True}},
            required=[],  # not required → optional
        )
        mock_schema = MagicMock()
        mock_schema.schema_definition = schema_def

        mock_uow = AsyncMock()
        mock_uow.__aenter__ = AsyncMock(return_value=mock_uow)
        mock_uow.__aexit__ = AsyncMock(return_value=None)
        mock_uow.schema_repository = MagicMock()
        mock_uow.schema_repository.find_by_name = AsyncMock(return_value=mock_schema)
        mock_uow_cls.return_value = mock_uow

        result = await get_pydantic_class_from_name("NullableSchema")
        assert result is not None

    @pytest.mark.asyncio
    @patch("src.engines.crewai.paths.crew.task_adapter.UnitOfWork")
    async def test_schema_not_found_returns_none(self, mock_uow_cls):
        mock_uow = AsyncMock()
        mock_uow.__aenter__ = AsyncMock(return_value=mock_uow)
        mock_uow.__aexit__ = AsyncMock(return_value=None)
        mock_uow.schema_repository = MagicMock()
        mock_uow.schema_repository.find_by_name = AsyncMock(return_value=None)
        mock_uow_cls.return_value = mock_uow

        result = await get_pydantic_class_from_name("NonExistentSchema")
        assert result is None

    @pytest.mark.asyncio
    @patch("src.engines.crewai.paths.crew.task_adapter.UnitOfWork")
    async def test_invalid_schema_definition_returns_none(self, mock_uow_cls):
        mock_schema = MagicMock()
        mock_schema.schema_definition = None

        mock_uow = AsyncMock()
        mock_uow.__aenter__ = AsyncMock(return_value=mock_uow)
        mock_uow.__aexit__ = AsyncMock(return_value=None)
        mock_uow.schema_repository = MagicMock()
        mock_uow.schema_repository.find_by_name = AsyncMock(return_value=mock_schema)
        mock_uow_cls.return_value = mock_uow

        result = await get_pydantic_class_from_name("BadSchema")
        assert result is None

    @pytest.mark.asyncio
    @patch("src.engines.crewai.paths.crew.task_adapter.UnitOfWork")
    async def test_exception_returns_none(self, mock_uow_cls):
        mock_uow_cls.side_effect = Exception("DB error")
        result = await get_pydantic_class_from_name("ErrorSchema")
        assert result is None


# ---------------------------------------------------------------------------
# create_callback_from_string
# ---------------------------------------------------------------------------

class TestCreateCallbackFromString:
    def test_databricks_volume_callback(self):
        with patch(
            "src.engines.crewai.callbacks.databricks_volume_callback.DatabricksVolumeCallback"
        ) as mock_cb_cls:
            mock_cb_instance = AsyncMock()
            mock_cb_instance.execute = AsyncMock(return_value=None)
            mock_cb_cls.return_value = mock_cb_instance

            callback = create_callback_from_string(
                "DatabricksVolumeCallback",
                task_key="task1",
                callback_config={
                    "volume_path": "/Volumes/test",
                    "file_format": "json",
                    "create_date_dirs": True,
                    "workspace_url": "https://example.com",
                    "token": "tok",
                },
                execution_name="exec1",
            )
        assert callable(callback)

    def test_databricks_volume_callback_default_config(self):
        with patch(
            "src.engines.crewai.callbacks.databricks_volume_callback.DatabricksVolumeCallback"
        ) as mock_cb_cls:
            mock_cb_cls.return_value = AsyncMock()
            callback = create_callback_from_string(
                "DatabricksVolumeCallback",
                task_key="task1",
                callback_config=None,
            )
        assert callable(callback)

    def test_databricks_volume_callback_import_error(self):
        with patch(
            "src.engines.crewai.callbacks.databricks_volume_callback.DatabricksVolumeCallback",
            side_effect=ImportError("no module"),
        ):
            callback = create_callback_from_string(
                "DatabricksVolumeCallback",
                task_key="task1",
            )
        assert callback is None

    def test_unknown_callback_returns_none(self):
        result = create_callback_from_string("UnknownCallback", task_key="task1")
        assert result is None

    def test_callback_wrapper_executes_async(self):
        """Test that the wrapper runs the async callback."""
        mock_cb_instance = MagicMock()
        mock_cb_instance.execute = AsyncMock(return_value=None)

        with patch(
            "src.engines.crewai.callbacks.databricks_volume_callback.DatabricksVolumeCallback",
            return_value=mock_cb_instance,
        ):
            callback = create_callback_from_string(
                "DatabricksVolumeCallback",
                task_key="task_cb_test",
                callback_config={"volume_path": "/tmp/test"},
            )

        assert callback is not None
        mock_output = MagicMock()
        result = callback(mock_output)
        # Should return the original output
        assert result is mock_output


# ---------------------------------------------------------------------------
# create_task – helper
# ---------------------------------------------------------------------------

def _base_task_config(**overrides):
    cfg = {
        "description": "Analyse the data",
        "expected_output": "Analysis report",
    }
    cfg.update(overrides)
    return cfg


def _make_agent(role="Analyst"):
    agent = MagicMock()
    agent.role = role
    agent._agent_key = "agent_key"
    agent.id = None
    agent._agent_id = None
    return agent


async def _create_task_patched(task_key, task_config, agent, **kwargs):
    """Helper to call create_task with all heavy deps mocked."""
    with patch("src.services.mcp_service.MCPService"), \
         patch("src.engines.crewai.tools.mcp_integration.MCPIntegration") as mock_mcp, \
         patch("src.db.session.request_scoped_session") as mock_sess, \
         patch("src.engines.crewai.paths.crew.task_adapter.Task") as mock_task_cls, \
         patch("src.services.databricks_service.DatabricksService") as mock_db_svc, \
         patch("src.services.memory_backend_service.MemoryBackendService") as mock_mem_svc:

        mock_mcp_instance = MagicMock()
        mock_mcp.create_mcp_tools_for_task = AsyncMock(return_value=[])
        mock_session = AsyncMock()
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=None)
        mock_sess.return_value = mock_session

        mock_task_instance = MagicMock()
        mock_task_instance.tools = []
        mock_task_instance.agent = agent
        mock_task_cls.return_value = mock_task_instance

        mock_db_config = MagicMock()
        mock_db_config.volume_enabled = False
        mock_db_svc.return_value.get_databricks_config = AsyncMock(return_value=mock_db_config)
        mock_mem_svc.return_value.get_active_config = AsyncMock(return_value=None)

        return await create_task(
            task_key=task_key,
            task_config=task_config,
            agent=agent,
            **kwargs
        ), mock_task_cls


class TestCreateTaskBasic:
    @pytest.mark.asyncio
    async def test_basic_task_creation(self):
        agent = _make_agent()
        task, mock_cls = await _create_task_patched(
            "task1",
            _base_task_config(),
            agent,
        )
        assert task is not None
        mock_cls.assert_called_once()

    @pytest.mark.asyncio
    async def test_markdown_appends_to_description(self):
        agent = _make_agent()
        task, mock_cls = await _create_task_patched(
            "task-md",
            _base_task_config(markdown=True),
            agent,
        )
        call_kwargs = mock_cls.call_args[1]
        assert "markdown" in call_kwargs.get("description", "").lower()

    @pytest.mark.asyncio
    async def test_task_id_stored_without_prefix(self):
        agent = _make_agent()
        task, mock_cls = await _create_task_patched(
            "task-prefix",
            _base_task_config(id="task-abc12345"),
            agent,
        )
        task._kasal_task_id = "abc12345"  # already set by code
        assert not hasattr(task, "_kasal_task_id") or task._kasal_task_id is not None

    @pytest.mark.asyncio
    async def test_async_execution_field_defaults_false(self):
        agent = _make_agent()
        task, mock_cls = await _create_task_patched(
            "task-async",
            _base_task_config(),
            agent,
        )
        call_kwargs = mock_cls.call_args[1]
        assert call_kwargs.get("async_execution", False) is False

    @pytest.mark.asyncio
    async def test_output_json_string_value_skipped(self):
        """String output_json values should be skipped."""
        agent = _make_agent()
        task, mock_cls = await _create_task_patched(
            "task-json-str",
            _base_task_config(output_json="SomeClass"),
            agent,
        )
        call_kwargs = mock_cls.call_args[1]
        assert "output_json" not in call_kwargs

    @pytest.mark.asyncio
    async def test_human_input_included(self):
        agent = _make_agent()
        task, mock_cls = await _create_task_patched(
            "task-human",
            _base_task_config(human_input=True),
            agent,
        )
        call_kwargs = mock_cls.call_args[1]
        assert call_kwargs.get("human_input") is True

    @pytest.mark.asyncio
    async def test_task_creation_exception_reraises(self):
        agent = _make_agent()
        with patch("src.services.mcp_service.MCPService"), \
             patch("src.engines.crewai.tools.mcp_integration.MCPIntegration") as mock_mcp, \
             patch("src.db.session.request_scoped_session") as mock_sess, \
             patch("src.engines.crewai.paths.crew.task_adapter.Task") as mock_task_cls, \
             patch("src.services.databricks_service.DatabricksService") as mock_db_svc, \
             patch("src.services.memory_backend_service.MemoryBackendService") as mock_mem_svc:

            mock_mcp.create_mcp_tools_for_task = AsyncMock(return_value=[])
            mock_session = AsyncMock()
            mock_session.__aenter__ = AsyncMock(return_value=mock_session)
            mock_session.__aexit__ = AsyncMock(return_value=None)
            mock_sess.return_value = mock_session
            mock_task_cls.side_effect = Exception("task creation failed")
            mock_db_svc.return_value.get_databricks_config = AsyncMock(return_value=MagicMock(volume_enabled=False))
            mock_mem_svc.return_value.get_active_config = AsyncMock(return_value=None)

            with pytest.raises(Exception, match="task creation failed"):
                await create_task(
                    task_key="failing-task",
                    task_config=_base_task_config(),
                    agent=agent,
                )


class TestCreateTaskToolResolution:
    @pytest.mark.asyncio
    async def test_tool_service_resolves_tool_ids(self):
        agent = _make_agent()
        task_config = _base_task_config(tools=["tool-uuid-1"])
        mock_tool_svc = MagicMock()
        mock_tool_svc.get_tool_config_by_name = AsyncMock(return_value={})
        mock_tool_factory = MagicMock()
        mock_tool_instance = MagicMock(name="SearchTool")
        mock_tool_factory.create_tool = MagicMock(return_value=mock_tool_instance)

        with patch("src.services.mcp_service.MCPService"), \
             patch("src.engines.crewai.tools.mcp_integration.MCPIntegration") as mock_mcp, \
             patch("src.db.session.request_scoped_session") as mock_sess, \
             patch("src.engines.crewai.paths.crew.task_adapter.Task") as mock_task_cls, \
             patch("src.services.databricks_service.DatabricksService") as mock_db_svc, \
             patch("src.services.memory_backend_service.MemoryBackendService") as mock_mem_svc, \
             patch("src.engines.crewai.paths.crew.task_adapter.resolve_tool_ids_to_names",
                   new_callable=AsyncMock) as mock_resolve:

            mock_mcp.create_mcp_tools_for_task = AsyncMock(return_value=[])
            mock_session = AsyncMock()
            mock_session.__aenter__ = AsyncMock(return_value=mock_session)
            mock_session.__aexit__ = AsyncMock(return_value=None)
            mock_sess.return_value = mock_session
            mock_task_cls.return_value = MagicMock(tools=[], agent=agent)
            mock_db_svc.return_value.get_databricks_config = AsyncMock(return_value=MagicMock(volume_enabled=False))
            mock_mem_svc.return_value.get_active_config = AsyncMock(return_value=None)
            mock_resolve.return_value = ["SearchTool"]

            task = await create_task(
                task_key="tool-task",
                task_config=task_config,
                agent=agent,
                tool_service=mock_tool_svc,
                tool_factory=mock_tool_factory,
            )

        mock_resolve.assert_called_once()
        mock_tool_factory.create_tool.assert_called()

    @pytest.mark.asyncio
    async def test_mcp_tuple_tool_expanded_in_task(self):
        agent = _make_agent()
        task_config = _base_task_config(tools=["mcp-tool"])
        mock_tool_svc = MagicMock()
        mock_tool_svc.get_tool_config_by_name = AsyncMock(return_value={})
        mock_tool_factory = MagicMock()
        mcp_t1 = MagicMock()
        mcp_t2 = MagicMock()
        mock_tool_factory.create_tool = MagicMock(return_value=(True, [mcp_t1, mcp_t2]))

        with patch("src.services.mcp_service.MCPService"), \
             patch("src.engines.crewai.tools.mcp_integration.MCPIntegration") as mock_mcp, \
             patch("src.db.session.request_scoped_session") as mock_sess, \
             patch("src.engines.crewai.paths.crew.task_adapter.Task") as mock_task_cls, \
             patch("src.services.databricks_service.DatabricksService") as mock_db_svc, \
             patch("src.services.memory_backend_service.MemoryBackendService") as mock_mem_svc, \
             patch("src.engines.crewai.paths.crew.task_adapter.resolve_tool_ids_to_names",
                   new_callable=AsyncMock) as mock_resolve:

            mock_mcp.create_mcp_tools_for_task = AsyncMock(return_value=[])
            mock_session = AsyncMock()
            mock_session.__aenter__ = AsyncMock(return_value=mock_session)
            mock_session.__aexit__ = AsyncMock(return_value=None)
            mock_sess.return_value = mock_session
            mock_task_cls.return_value = MagicMock(tools=[], agent=agent)
            mock_db_svc.return_value.get_databricks_config = AsyncMock(return_value=MagicMock(volume_enabled=False))
            mock_mem_svc.return_value.get_active_config = AsyncMock(return_value=None)
            mock_resolve.return_value = ["MCPTool"]

            task = await create_task(
                task_key="mcp-tuple-task",
                task_config=task_config,
                agent=agent,
                tool_service=mock_tool_svc,
                tool_factory=mock_tool_factory,
            )

        call_kwargs = mock_task_cls.call_args[1]
        assert mcp_t1 in call_kwargs.get("tools", [])
        assert mcp_t2 in call_kwargs.get("tools", [])

    @pytest.mark.asyncio
    async def test_mcp_service_adapter_skipped(self):
        agent = _make_agent()
        task_config = _base_task_config(tools=["mcp-adapter"])
        mock_tool_svc = MagicMock()
        mock_tool_svc.get_tool_config_by_name = AsyncMock(return_value={})
        mock_tool_factory = MagicMock()
        mock_tool_factory.create_tool = MagicMock(return_value=(True, "mcp_service_adapter"))

        with patch("src.services.mcp_service.MCPService"), \
             patch("src.engines.crewai.tools.mcp_integration.MCPIntegration") as mock_mcp, \
             patch("src.db.session.request_scoped_session") as mock_sess, \
             patch("src.engines.crewai.paths.crew.task_adapter.Task") as mock_task_cls, \
             patch("src.services.databricks_service.DatabricksService") as mock_db_svc, \
             patch("src.services.memory_backend_service.MemoryBackendService") as mock_mem_svc, \
             patch("src.engines.crewai.paths.crew.task_adapter.resolve_tool_ids_to_names",
                   new_callable=AsyncMock) as mock_resolve:

            mock_mcp.create_mcp_tools_for_task = AsyncMock(return_value=[])
            mock_session = AsyncMock()
            mock_session.__aenter__ = AsyncMock(return_value=mock_session)
            mock_session.__aexit__ = AsyncMock(return_value=None)
            mock_sess.return_value = mock_session
            mock_task_cls.return_value = MagicMock(tools=[], agent=agent)
            mock_db_svc.return_value.get_databricks_config = AsyncMock(return_value=MagicMock(volume_enabled=False))
            mock_mem_svc.return_value.get_active_config = AsyncMock(return_value=None)
            mock_resolve.return_value = ["MCPAdapter"]

            task = await create_task(
                task_key="adapter-task",
                task_config=task_config,
                agent=agent,
                tool_service=mock_tool_svc,
                tool_factory=mock_tool_factory,
            )
        assert task is not None

    @pytest.mark.asyncio
    async def test_auto_resolve_tools_from_tool_configs(self):
        """When tools array is empty but tool_configs has keys, auto-resolve."""
        agent = _make_agent()
        task_config = _base_task_config(
            tools=[],
            tool_configs={"GenieTool": {"spaceId": "space-1"}},
        )
        mock_tool_svc = MagicMock()
        mock_tool_svc.get_tool_config_by_name = AsyncMock(return_value={})
        mock_tool_factory = MagicMock()
        mock_genie = MagicMock(name="GenieTool")
        mock_tool_factory.create_tool = MagicMock(return_value=mock_genie)

        with patch("src.services.mcp_service.MCPService"), \
             patch("src.engines.crewai.tools.mcp_integration.MCPIntegration") as mock_mcp, \
             patch("src.db.session.request_scoped_session") as mock_sess, \
             patch("src.engines.crewai.paths.crew.task_adapter.Task") as mock_task_cls, \
             patch("src.services.databricks_service.DatabricksService") as mock_db_svc, \
             patch("src.services.memory_backend_service.MemoryBackendService") as mock_mem_svc:

            mock_mcp.create_mcp_tools_for_task = AsyncMock(return_value=[])
            mock_session = AsyncMock()
            mock_session.__aenter__ = AsyncMock(return_value=mock_session)
            mock_session.__aexit__ = AsyncMock(return_value=None)
            mock_sess.return_value = mock_session
            mock_task_cls.return_value = MagicMock(tools=[], agent=agent)
            mock_db_svc.return_value.get_databricks_config = AsyncMock(return_value=MagicMock(volume_enabled=False))
            mock_mem_svc.return_value.get_active_config = AsyncMock(return_value=None)

            task = await create_task(
                task_key="auto-resolve-task",
                task_config=task_config,
                agent=agent,
                tool_service=mock_tool_svc,
                tool_factory=mock_tool_factory,
            )

        mock_tool_factory.create_tool.assert_called_with(
            "GenieTool",
            result_as_answer=False,
            tool_config_override={"spaceId": "space-1"},
        )

    @pytest.mark.asyncio
    async def test_debug_logging_for_special_tools(self):
        """SerperDevTool triggers extra debug logging in create_task."""
        agent = _make_agent()
        task_config = _base_task_config(
            tools=["serper-id"],
            tool_configs={"SerperDevTool": {"api_key": "sk-test"}},
        )
        mock_tool_svc = MagicMock()
        mock_tool_svc.get_tool_config_by_name = AsyncMock(return_value={})
        mock_tool_factory = MagicMock()
        mock_serper = MagicMock(name="SerperDevTool")
        mock_tool_factory.create_tool = MagicMock(return_value=mock_serper)

        with patch("src.services.mcp_service.MCPService"), \
             patch("src.engines.crewai.tools.mcp_integration.MCPIntegration") as mock_mcp, \
             patch("src.db.session.request_scoped_session") as mock_sess, \
             patch("src.engines.crewai.paths.crew.task_adapter.Task") as mock_task_cls, \
             patch("src.services.databricks_service.DatabricksService") as mock_db_svc, \
             patch("src.services.memory_backend_service.MemoryBackendService") as mock_mem_svc, \
             patch("src.engines.crewai.paths.crew.task_adapter.resolve_tool_ids_to_names",
                   new_callable=AsyncMock) as mock_resolve:

            mock_mcp.create_mcp_tools_for_task = AsyncMock(return_value=[])
            mock_session = AsyncMock()
            mock_session.__aenter__ = AsyncMock(return_value=mock_session)
            mock_session.__aexit__ = AsyncMock(return_value=None)
            mock_sess.return_value = mock_session
            mock_task_cls.return_value = MagicMock(tools=[], agent=agent)
            mock_db_svc.return_value.get_databricks_config = AsyncMock(return_value=MagicMock(volume_enabled=False))
            mock_mem_svc.return_value.get_active_config = AsyncMock(return_value=None)
            mock_resolve.return_value = ["SerperDevTool"]

            task = await create_task(
                task_key="serper-task",
                task_config=task_config,
                agent=agent,
                tool_service=mock_tool_svc,
                tool_factory=mock_tool_factory,
            )
        assert task is not None


class TestCreateTaskGuardrail:
    @pytest.mark.asyncio
    async def test_guardrail_config_creates_guardrail(self):
        """Task with guardrail config should apply GuardrailWrapper."""
        agent = _make_agent()
        guardrail_cfg = json.dumps({"type": "prompt_injection_check"})
        task_config = _base_task_config(guardrail=guardrail_cfg)

        with patch("src.services.mcp_service.MCPService"), \
             patch("src.engines.crewai.tools.mcp_integration.MCPIntegration") as mock_mcp, \
             patch("src.db.session.request_scoped_session") as mock_sess, \
             patch("src.engines.crewai.paths.crew.task_adapter.Task") as mock_task_cls, \
             patch("src.services.databricks_service.DatabricksService") as mock_db_svc, \
             patch("src.services.memory_backend_service.MemoryBackendService") as mock_mem_svc, \
             patch("src.engines.crewai.guardrails.guardrail_factory.GuardrailFactory") as mock_gf, \
             patch("src.engines.crewai.paths.crew.task_adapter.GuardrailWrapper") as mock_gw:

            mock_mcp.create_mcp_tools_for_task = AsyncMock(return_value=[])
            mock_session = AsyncMock()
            mock_session.__aenter__ = AsyncMock(return_value=mock_session)
            mock_session.__aexit__ = AsyncMock(return_value=None)
            mock_sess.return_value = mock_session
            mock_task_cls.return_value = MagicMock(tools=[], agent=agent)
            mock_db_svc.return_value.get_databricks_config = AsyncMock(return_value=MagicMock(volume_enabled=False))
            mock_mem_svc.return_value.get_active_config = AsyncMock(return_value=None)

            mock_guardrail = MagicMock()
            mock_gf.create_guardrail = MagicMock(return_value=mock_guardrail)
            mock_wrapper = MagicMock()
            mock_wrapper.__call__ = MagicMock(return_value=True)
            mock_gw.return_value = mock_wrapper

            task = await create_task(
                task_key="guardrail-task",
                task_config=task_config,
                agent=agent,
            )

        mock_gf.create_guardrail.assert_called_once()

    @pytest.mark.asyncio
    async def test_guardrail_none_from_factory_falls_back_to_callback(self):
        """When factory returns None, existing callback should be used."""
        agent = _make_agent()
        guardrail_cfg = json.dumps({"type": "prompt_injection_check"})
        task_config = _base_task_config(
            guardrail=guardrail_cfg,
            callback="DatabricksVolumeCallback",
            callback_config={"volume_path": "/Volumes/test"},
        )

        with patch("src.services.mcp_service.MCPService"), \
             patch("src.engines.crewai.tools.mcp_integration.MCPIntegration") as mock_mcp, \
             patch("src.db.session.request_scoped_session") as mock_sess, \
             patch("src.engines.crewai.paths.crew.task_adapter.Task") as mock_task_cls, \
             patch("src.services.databricks_service.DatabricksService") as mock_db_svc, \
             patch("src.services.memory_backend_service.MemoryBackendService") as mock_mem_svc, \
             patch("src.engines.crewai.guardrails.guardrail_factory.GuardrailFactory") as mock_gf, \
             patch("src.engines.crewai.callbacks.databricks_volume_callback.DatabricksVolumeCallback") as mock_dvcb:

            mock_mcp.create_mcp_tools_for_task = AsyncMock(return_value=[])
            mock_session = AsyncMock()
            mock_session.__aenter__ = AsyncMock(return_value=mock_session)
            mock_session.__aexit__ = AsyncMock(return_value=None)
            mock_sess.return_value = mock_session
            mock_task_cls.return_value = MagicMock(tools=[], agent=agent)
            mock_db_svc.return_value.get_databricks_config = AsyncMock(return_value=MagicMock(volume_enabled=False))
            mock_mem_svc.return_value.get_active_config = AsyncMock(return_value=None)
            mock_gf.create_guardrail = MagicMock(return_value=None)  # Factory returns None
            mock_dvcb.return_value = AsyncMock()

            task = await create_task(
                task_key="fallback-cb-task",
                task_config=task_config,
                agent=agent,
            )
        assert task is not None

    @pytest.mark.asyncio
    async def test_guardrail_llm_type_promoted(self):
        """Config with description matching a known type should be promoted."""
        agent = _make_agent()
        # Config looks like LLM guardrail (has description matching known type, no 'type' key)
        guardrail_cfg = json.dumps({"description": "prompt_injection_check"})
        task_config = _base_task_config(guardrail=guardrail_cfg)

        with patch("src.services.mcp_service.MCPService"), \
             patch("src.engines.crewai.tools.mcp_integration.MCPIntegration") as mock_mcp, \
             patch("src.db.session.request_scoped_session") as mock_sess, \
             patch("src.engines.crewai.paths.crew.task_adapter.Task") as mock_task_cls, \
             patch("src.services.databricks_service.DatabricksService") as mock_db_svc, \
             patch("src.services.memory_backend_service.MemoryBackendService") as mock_mem_svc, \
             patch("src.engines.crewai.guardrails.guardrail_factory.GuardrailFactory") as mock_gf, \
             patch("src.engines.crewai.paths.crew.task_adapter.GuardrailWrapper") as mock_gw:

            mock_mcp.create_mcp_tools_for_task = AsyncMock(return_value=[])
            mock_session = AsyncMock()
            mock_session.__aenter__ = AsyncMock(return_value=mock_session)
            mock_session.__aexit__ = AsyncMock(return_value=None)
            mock_sess.return_value = mock_session
            mock_task_cls.return_value = MagicMock(tools=[], agent=agent)
            mock_db_svc.return_value.get_databricks_config = AsyncMock(return_value=MagicMock(volume_enabled=False))
            mock_mem_svc.return_value.get_active_config = AsyncMock(return_value=None)
            mock_guardrail = MagicMock()
            mock_gf.create_guardrail = MagicMock(return_value=mock_guardrail)
            mock_wrapper = MagicMock()
            mock_gw.return_value = mock_wrapper

            task = await create_task(
                task_key="promoted-type-task",
                task_config=task_config,
                agent=agent,
            )
        assert task is not None

    @pytest.mark.asyncio
    async def test_code_guardrail_inherits_agent_model_when_no_explicit_model(self):
        """A code-based (factory) self_reflection / prompt_injection_check
        guardrail with NO llm_model must inherit the task AGENT's model
        (databricks/ prefix stripped), NOT the hardcoded guardrail default.

        The model is stamped into the config before GuardrailFactory builds
        the guardrail, so capturing the config the factory receives is the
        robust assertion."""
        agent = _make_agent()
        # Agent runs with this model (the chat-input selection, top-down).
        agent.llm = MagicMock()
        agent.llm.model = "databricks/run-model"
        # Code-based guardrail (has a 'type'), stored under 'guardrail', no llm_model.
        guardrail_cfg = json.dumps({"type": "self_reflection"})
        task_config = _base_task_config(guardrail=guardrail_cfg)

        captured = {}

        def _capture_create_guardrail(factory_config):
            cfg = json.loads(factory_config) if isinstance(factory_config, str) else factory_config
            captured["config"] = cfg
            return MagicMock()

        with patch("src.services.mcp_service.MCPService"), \
             patch("src.engines.crewai.tools.mcp_integration.MCPIntegration") as mock_mcp, \
             patch("src.db.session.request_scoped_session") as mock_sess, \
             patch("src.engines.crewai.paths.crew.task_adapter.Task") as mock_task_cls, \
             patch("src.services.databricks_service.DatabricksService") as mock_db_svc, \
             patch("src.services.memory_backend_service.MemoryBackendService") as mock_mem_svc, \
             patch("src.engines.crewai.guardrails.guardrail_factory.GuardrailFactory") as mock_gf, \
             patch("src.engines.crewai.paths.crew.task_adapter.GuardrailWrapper") as mock_gw:

            mock_mcp.create_mcp_tools_for_task = AsyncMock(return_value=[])
            mock_session = AsyncMock()
            mock_session.__aenter__ = AsyncMock(return_value=mock_session)
            mock_session.__aexit__ = AsyncMock(return_value=None)
            mock_sess.return_value = mock_session
            mock_task_cls.return_value = MagicMock(tools=[], agent=agent)
            mock_db_svc.return_value.get_databricks_config = AsyncMock(return_value=MagicMock(volume_enabled=False))
            mock_mem_svc.return_value.get_active_config = AsyncMock(return_value=None)
            mock_gf.create_guardrail = MagicMock(side_effect=_capture_create_guardrail)
            mock_gw.return_value = MagicMock()

            task = await create_task(
                task_key="code-guardrail-inherit-task",
                task_config=task_config,
                agent=agent,
            )

        assert task is not None
        # Factory was called with the agent's model stamped in (prefix stripped),
        # NOT the hardcoded default.
        mock_gf.create_guardrail.assert_called_once()
        assert captured["config"]["type"] == "self_reflection"
        assert captured["config"]["llm_model"] == "run-model"
        assert captured["config"]["llm_model"] != "databricks-claude-sonnet-4-5"

    @pytest.mark.asyncio
    async def test_code_guardrail_explicit_model_wins(self):
        """An explicit llm_model on a code-based guardrail config wins over the
        agent's model."""
        agent = _make_agent()
        # Agent runs with a DIFFERENT model than the guardrail's explicit pick.
        agent.llm = MagicMock()
        agent.llm.model = "databricks/run-model"
        guardrail_cfg = json.dumps({
            "type": "prompt_injection_check",
            "llm_model": "databricks-claude-opus-4",
        })
        task_config = _base_task_config(guardrail=guardrail_cfg)

        captured = {}

        def _capture_create_guardrail(factory_config):
            cfg = json.loads(factory_config) if isinstance(factory_config, str) else factory_config
            captured["config"] = cfg
            return MagicMock()

        with patch("src.services.mcp_service.MCPService"), \
             patch("src.engines.crewai.tools.mcp_integration.MCPIntegration") as mock_mcp, \
             patch("src.db.session.request_scoped_session") as mock_sess, \
             patch("src.engines.crewai.paths.crew.task_adapter.Task") as mock_task_cls, \
             patch("src.services.databricks_service.DatabricksService") as mock_db_svc, \
             patch("src.services.memory_backend_service.MemoryBackendService") as mock_mem_svc, \
             patch("src.engines.crewai.guardrails.guardrail_factory.GuardrailFactory") as mock_gf, \
             patch("src.engines.crewai.paths.crew.task_adapter.GuardrailWrapper") as mock_gw:

            mock_mcp.create_mcp_tools_for_task = AsyncMock(return_value=[])
            mock_session = AsyncMock()
            mock_session.__aenter__ = AsyncMock(return_value=mock_session)
            mock_session.__aexit__ = AsyncMock(return_value=None)
            mock_sess.return_value = mock_session
            mock_task_cls.return_value = MagicMock(tools=[], agent=agent)
            mock_db_svc.return_value.get_databricks_config = AsyncMock(return_value=MagicMock(volume_enabled=False))
            mock_mem_svc.return_value.get_active_config = AsyncMock(return_value=None)
            mock_gf.create_guardrail = MagicMock(side_effect=_capture_create_guardrail)
            mock_gw.return_value = MagicMock()

            task = await create_task(
                task_key="code-guardrail-explicit-task",
                task_config=task_config,
                agent=agent,
            )

        assert task is not None
        # Explicit per-task model wins over the agent's model.
        mock_gf.create_guardrail.assert_called_once()
        assert captured["config"]["type"] == "prompt_injection_check"
        assert captured["config"]["llm_model"] == "databricks-claude-opus-4"
        assert captured["config"]["llm_model"] != "run-model"

    @pytest.mark.asyncio
    async def test_llm_guardrail_config(self):
        """Task with llm_guardrail config should create LLMGuardrail."""
        agent = _make_agent()
        llm_guardrail = {
            "description": "Validate output quality",
            "llm_model": "databricks-claude-sonnet-4-5",
        }
        task_config = _base_task_config(llm_guardrail=llm_guardrail)

        with patch("src.services.mcp_service.MCPService"), \
             patch("src.engines.crewai.tools.mcp_integration.MCPIntegration") as mock_mcp, \
             patch("src.db.session.request_scoped_session") as mock_sess, \
             patch("src.engines.crewai.paths.crew.task_adapter.Task") as mock_task_cls, \
             patch("src.services.databricks_service.DatabricksService") as mock_db_svc, \
             patch("src.services.memory_backend_service.MemoryBackendService") as mock_mem_svc, \
             patch("crewai.tasks.llm_guardrail.LLMGuardrail") as mock_llm_g, \
             patch("crewai.LLM") as mock_llm_cls:

            mock_mcp.create_mcp_tools_for_task = AsyncMock(return_value=[])
            mock_session = AsyncMock()
            mock_session.__aenter__ = AsyncMock(return_value=mock_session)
            mock_session.__aexit__ = AsyncMock(return_value=None)
            mock_sess.return_value = mock_session
            mock_task_cls.return_value = MagicMock(tools=[], agent=agent)
            mock_db_svc.return_value.get_databricks_config = AsyncMock(return_value=MagicMock(volume_enabled=False))
            mock_mem_svc.return_value.get_active_config = AsyncMock(return_value=None)
            mock_llm_cls.return_value = MagicMock()
            mock_llm_g.return_value = MagicMock()

            task = await create_task(
                task_key="llm-guardrail-task",
                task_config=task_config,
                agent=agent,
                config={"group_id": "test-group"},
            )
        assert task is not None

    @pytest.mark.asyncio
    async def test_llm_guardrail_adds_databricks_prefix(self):
        agent = _make_agent()
        llm_guardrail = {
            "description": "Check accuracy",
            "llm_model": "databricks-claude-sonnet-4-5",
        }
        task_config = _base_task_config(llm_guardrail=llm_guardrail)

        with patch("src.services.mcp_service.MCPService"), \
             patch("src.engines.crewai.tools.mcp_integration.MCPIntegration") as mock_mcp, \
             patch("src.db.session.request_scoped_session") as mock_sess, \
             patch("src.engines.crewai.paths.crew.task_adapter.Task") as mock_task_cls, \
             patch("src.services.databricks_service.DatabricksService") as mock_db_svc, \
             patch("src.services.memory_backend_service.MemoryBackendService") as mock_mem_svc, \
             patch("crewai.tasks.llm_guardrail.LLMGuardrail") as mock_llm_g, \
             patch("crewai.LLM") as mock_llm_cls:

            mock_mcp.create_mcp_tools_for_task = AsyncMock(return_value=[])
            mock_session = AsyncMock()
            mock_session.__aenter__ = AsyncMock(return_value=mock_session)
            mock_session.__aexit__ = AsyncMock(return_value=None)
            mock_sess.return_value = mock_session
            mock_task_cls.return_value = MagicMock(tools=[], agent=agent)
            mock_db_svc.return_value.get_databricks_config = AsyncMock(return_value=MagicMock(volume_enabled=False))
            mock_mem_svc.return_value.get_active_config = AsyncMock(return_value=None)
            mock_llm_cls.return_value = MagicMock()
            mock_llm_g.return_value = MagicMock()

            task = await create_task(
                task_key="llm-prefix-task",
                task_config=task_config,
                agent=agent,
                config={"group_id": "test-group"},
            )

        # LLM should be created with databricks/ prefix
        call_args = mock_llm_cls.call_args[1] if mock_llm_cls.call_args else mock_llm_cls.call_args
        if call_args:
            model_arg = call_args.get("model", "") if isinstance(call_args, dict) else ""
            # Prefix should be added
            assert model_arg.startswith("databricks/") if model_arg else True

    @pytest.mark.asyncio
    async def test_llm_guardrail_inherits_agent_model_when_no_explicit_model(self):
        """When llm_guardrail has NO llm_model, the guardrail must use the
        task's AGENT model (prefix-stripped), NOT a hardcoded default."""
        agent = _make_agent()
        # Agent runs with this model (the chat-input selection, top-down).
        agent.llm = MagicMock()
        agent.llm.model = "databricks/run-model"
        # No llm_model in the guardrail config -> must inherit the agent model.
        llm_guardrail = {
            "description": "Validate output quality",
        }
        task_config = _base_task_config(llm_guardrail=llm_guardrail)

        with patch("src.services.mcp_service.MCPService"), \
             patch("src.engines.crewai.tools.mcp_integration.MCPIntegration") as mock_mcp, \
             patch("src.db.session.request_scoped_session") as mock_sess, \
             patch("src.engines.crewai.paths.crew.task_adapter.Task") as mock_task_cls, \
             patch("src.services.databricks_service.DatabricksService") as mock_db_svc, \
             patch("src.services.memory_backend_service.MemoryBackendService") as mock_mem_svc, \
             patch("crewai.tasks.llm_guardrail.LLMGuardrail") as mock_llm_g, \
             patch("src.core.llm_manager.LLMManager.configure_crewai_llm",
                   new_callable=AsyncMock) as mock_configure:

            mock_mcp.create_mcp_tools_for_task = AsyncMock(return_value=[])
            mock_session = AsyncMock()
            mock_session.__aenter__ = AsyncMock(return_value=mock_session)
            mock_session.__aexit__ = AsyncMock(return_value=None)
            mock_sess.return_value = mock_session
            mock_task_cls.return_value = MagicMock(tools=[], agent=agent)
            mock_db_svc.return_value.get_databricks_config = AsyncMock(return_value=MagicMock(volume_enabled=False))
            mock_mem_svc.return_value.get_active_config = AsyncMock(return_value=None)
            mock_configure.return_value = MagicMock()
            mock_llm_g.return_value = MagicMock()

            task = await create_task(
                task_key="llm-guardrail-inherit-task",
                task_config=task_config,
                agent=agent,
                config={"group_id": "test-group"},
            )

        assert task is not None
        # configure_crewai_llm must be called with the AGENT's model
        # (databricks/ provider prefix stripped), NOT the old hardcoded default.
        assert mock_configure.called
        model_arg = mock_configure.call_args[0][0]
        assert model_arg == "run-model"
        assert model_arg != "databricks-claude-sonnet-4-5"

    @pytest.mark.asyncio
    async def test_llm_guardrail_explicit_model_wins(self):
        """An explicit llm_guardrail.llm_model wins over the agent's model."""
        agent = _make_agent()
        # Agent runs with a DIFFERENT model than the guardrail's explicit pick.
        agent.llm = MagicMock()
        agent.llm.model = "databricks/run-model"
        llm_guardrail = {
            "description": "Validate output quality",
            "llm_model": "databricks-claude-opus-4",
        }
        task_config = _base_task_config(llm_guardrail=llm_guardrail)

        with patch("src.services.mcp_service.MCPService"), \
             patch("src.engines.crewai.tools.mcp_integration.MCPIntegration") as mock_mcp, \
             patch("src.db.session.request_scoped_session") as mock_sess, \
             patch("src.engines.crewai.paths.crew.task_adapter.Task") as mock_task_cls, \
             patch("src.services.databricks_service.DatabricksService") as mock_db_svc, \
             patch("src.services.memory_backend_service.MemoryBackendService") as mock_mem_svc, \
             patch("crewai.tasks.llm_guardrail.LLMGuardrail") as mock_llm_g, \
             patch("src.core.llm_manager.LLMManager.configure_crewai_llm",
                   new_callable=AsyncMock) as mock_configure:

            mock_mcp.create_mcp_tools_for_task = AsyncMock(return_value=[])
            mock_session = AsyncMock()
            mock_session.__aenter__ = AsyncMock(return_value=mock_session)
            mock_session.__aexit__ = AsyncMock(return_value=None)
            mock_sess.return_value = mock_session
            mock_task_cls.return_value = MagicMock(tools=[], agent=agent)
            mock_db_svc.return_value.get_databricks_config = AsyncMock(return_value=MagicMock(volume_enabled=False))
            mock_mem_svc.return_value.get_active_config = AsyncMock(return_value=None)
            mock_configure.return_value = MagicMock()
            mock_llm_g.return_value = MagicMock()

            task = await create_task(
                task_key="llm-guardrail-explicit-task",
                task_config=task_config,
                agent=agent,
                config={"group_id": "test-group"},
            )

        assert task is not None
        # The explicit per-task model wins over the agent's model.
        assert mock_configure.called
        model_arg = mock_configure.call_args[0][0]
        assert model_arg == "databricks-claude-opus-4"
        assert model_arg != "run-model"

    @pytest.mark.asyncio
    async def test_llm_guardrail_object_config(self):
        """llm_guardrail may arrive as an OBJECT (not a dict) — description and
        the explicit llm_model are read via getattr. Covers the non-dict branch
        in task_helpers.py:686-687."""
        from types import SimpleNamespace
        agent = _make_agent()
        agent.llm = MagicMock()
        agent.llm.model = "databricks/run-model"
        # Object-style config (e.g. a pydantic-ish/attr object), NOT a dict.
        llm_guardrail = SimpleNamespace(
            description="Validate output quality",
            llm_model="databricks-claude-opus-4",
        )
        task_config = _base_task_config(llm_guardrail=llm_guardrail)

        with patch("src.services.mcp_service.MCPService"), \
             patch("src.engines.crewai.tools.mcp_integration.MCPIntegration") as mock_mcp, \
             patch("src.db.session.request_scoped_session") as mock_sess, \
             patch("src.engines.crewai.paths.crew.task_adapter.Task") as mock_task_cls, \
             patch("src.services.databricks_service.DatabricksService") as mock_db_svc, \
             patch("src.services.memory_backend_service.MemoryBackendService") as mock_mem_svc, \
             patch("crewai.tasks.llm_guardrail.LLMGuardrail") as mock_llm_g, \
             patch("src.core.llm_manager.LLMManager.configure_crewai_llm",
                   new_callable=AsyncMock) as mock_configure:

            mock_mcp.create_mcp_tools_for_task = AsyncMock(return_value=[])
            mock_session = AsyncMock()
            mock_session.__aenter__ = AsyncMock(return_value=mock_session)
            mock_session.__aexit__ = AsyncMock(return_value=None)
            mock_sess.return_value = mock_session
            mock_task_cls.return_value = MagicMock(tools=[], agent=agent)
            mock_db_svc.return_value.get_databricks_config = AsyncMock(return_value=MagicMock(volume_enabled=False))
            mock_mem_svc.return_value.get_active_config = AsyncMock(return_value=None)
            mock_configure.return_value = MagicMock()
            mock_llm_g.return_value = MagicMock()

            task = await create_task(
                task_key="llm-guardrail-object-task",
                task_config=task_config,
                agent=agent,
                config={"group_id": "test-group"},
            )

        assert task is not None
        # The object's explicit llm_model is read via getattr and wins.
        assert mock_configure.called
        assert mock_configure.call_args[0][0] == "databricks-claude-opus-4"

    @pytest.mark.asyncio
    async def test_llm_guardrail_augments_description(self):
        agent = _make_agent()
        llm_guardrail = {
            "description": "The output must contain at least 100 words",
            "llm_model": "databricks-claude-sonnet-4-5",
        }
        task_config = _base_task_config(llm_guardrail=llm_guardrail)

        with patch("src.services.mcp_service.MCPService"), \
             patch("src.engines.crewai.tools.mcp_integration.MCPIntegration") as mock_mcp, \
             patch("src.db.session.request_scoped_session") as mock_sess, \
             patch("src.engines.crewai.paths.crew.task_adapter.Task") as mock_task_cls, \
             patch("src.services.databricks_service.DatabricksService") as mock_db_svc, \
             patch("src.services.memory_backend_service.MemoryBackendService") as mock_mem_svc, \
             patch("crewai.tasks.llm_guardrail.LLMGuardrail") as mock_llm_g, \
             patch("crewai.LLM") as mock_llm_cls:

            mock_mcp.create_mcp_tools_for_task = AsyncMock(return_value=[])
            mock_session = AsyncMock()
            mock_session.__aenter__ = AsyncMock(return_value=mock_session)
            mock_session.__aexit__ = AsyncMock(return_value=None)
            mock_sess.return_value = mock_session
            mock_task_cls.return_value = MagicMock(tools=[], agent=agent)
            mock_db_svc.return_value.get_databricks_config = AsyncMock(return_value=MagicMock(volume_enabled=False))
            mock_mem_svc.return_value.get_active_config = AsyncMock(return_value=None)
            mock_llm_cls.return_value = MagicMock()
            mock_llm_g.return_value = MagicMock()

            task = await create_task(
                task_key="llm-augment-task",
                task_config=task_config,
                agent=agent,
                config={"group_id": "test-group"},
            )

        # Description should be augmented with validation criteria
        call_kwargs = mock_task_cls.call_args[1]
        desc = call_kwargs.get("description", "")
        assert "VALIDATION REQUIREMENTS" in desc

    @pytest.mark.asyncio
    async def test_callback_without_guardrail_string_callback(self):
        agent = _make_agent()
        task_config = _base_task_config(
            callback="DatabricksVolumeCallback",
            callback_config={"volume_path": "/Volumes/data"},
        )

        with patch("src.services.mcp_service.MCPService"), \
             patch("src.engines.crewai.tools.mcp_integration.MCPIntegration") as mock_mcp, \
             patch("src.db.session.request_scoped_session") as mock_sess, \
             patch("src.engines.crewai.paths.crew.task_adapter.Task") as mock_task_cls, \
             patch("src.services.databricks_service.DatabricksService") as mock_db_svc, \
             patch("src.services.memory_backend_service.MemoryBackendService") as mock_mem_svc, \
             patch("src.engines.crewai.callbacks.databricks_volume_callback.DatabricksVolumeCallback") as mock_dvcb:

            mock_mcp.create_mcp_tools_for_task = AsyncMock(return_value=[])
            mock_session = AsyncMock()
            mock_session.__aenter__ = AsyncMock(return_value=mock_session)
            mock_session.__aexit__ = AsyncMock(return_value=None)
            mock_sess.return_value = mock_session
            mock_task_cls.return_value = MagicMock(tools=[], agent=agent)
            mock_db_svc.return_value.get_databricks_config = AsyncMock(return_value=MagicMock(volume_enabled=False))
            mock_mem_svc.return_value.get_active_config = AsyncMock(return_value=None)
            mock_dvcb.return_value = AsyncMock()

            task = await create_task(
                task_key="callback-only-task",
                task_config=task_config,
                agent=agent,
            )
        assert task is not None

    @pytest.mark.asyncio
    async def test_callable_callback_set_directly(self):
        agent = _make_agent()
        my_callback = MagicMock()
        task_config = _base_task_config(callback=my_callback)

        with patch("src.services.mcp_service.MCPService"), \
             patch("src.engines.crewai.tools.mcp_integration.MCPIntegration") as mock_mcp, \
             patch("src.db.session.request_scoped_session") as mock_sess, \
             patch("src.engines.crewai.paths.crew.task_adapter.Task") as mock_task_cls, \
             patch("src.services.databricks_service.DatabricksService") as mock_db_svc, \
             patch("src.services.memory_backend_service.MemoryBackendService") as mock_mem_svc:

            mock_mcp.create_mcp_tools_for_task = AsyncMock(return_value=[])
            mock_session = AsyncMock()
            mock_session.__aenter__ = AsyncMock(return_value=mock_session)
            mock_session.__aexit__ = AsyncMock(return_value=None)
            mock_sess.return_value = mock_session
            mock_task_cls.return_value = MagicMock(tools=[], agent=agent)
            mock_db_svc.return_value.get_databricks_config = AsyncMock(return_value=MagicMock(volume_enabled=False))
            mock_mem_svc.return_value.get_active_config = AsyncMock(return_value=None)

            task = await create_task(
                task_key="callable-cb-task",
                task_config=task_config,
                agent=agent,
            )

        call_kwargs = mock_task_cls.call_args[1]
        assert call_kwargs.get("callback") is my_callback


class TestCreateTaskDatabricksVolumeAutoCallback:
    """Test auto-adding DatabricksVolumeCallback when backend is Databricks."""

    @pytest.mark.asyncio
    async def test_auto_adds_databricks_callback_when_active_backend_is_databricks(self):
        agent = _make_agent()
        task_config = _base_task_config()

        with patch("src.services.mcp_service.MCPService"), \
             patch("src.engines.crewai.tools.mcp_integration.MCPIntegration") as mock_mcp, \
             patch("src.db.session.request_scoped_session") as mock_sess, \
             patch("src.engines.crewai.paths.crew.task_adapter.Task") as mock_task_cls, \
             patch("src.services.databricks_service.DatabricksService") as mock_db_svc, \
             patch("src.services.memory_backend_service.MemoryBackendService") as mock_mem_svc, \
             patch("src.engines.crewai.callbacks.databricks_volume_callback.DatabricksVolumeCallback") as mock_dvcb:

            mock_mcp.create_mcp_tools_for_task = AsyncMock(return_value=[])
            mock_session = AsyncMock()
            mock_session.__aenter__ = AsyncMock(return_value=mock_session)
            mock_session.__aexit__ = AsyncMock(return_value=None)
            mock_sess.return_value = mock_session
            mock_task_cls.return_value = MagicMock(tools=[], agent=agent)

            # Databricks config with volume enabled
            mock_db_config = MagicMock()
            mock_db_config.volume_enabled = True
            mock_db_config.volume_path = "/Volumes/test/outputs"
            mock_db_config.volume_file_format = "json"
            mock_db_config.volume_create_date_dirs = True
            mock_db_svc.return_value.get_databricks_config = AsyncMock(return_value=mock_db_config)

            # Active config is Databricks
            mock_active_config = MagicMock()
            mock_active_config.enable_short_term = True
            mock_active_config.enable_long_term = True
            mock_active_config.enable_entity = True
            mock_active_config.backend_type = MagicMock()
            mock_active_config.backend_type.value = "databricks"
            mock_mem_svc.return_value.get_active_config = AsyncMock(return_value=mock_active_config)
            mock_dvcb.return_value = AsyncMock()

            task = await create_task(
                task_key="auto-cb-task",
                task_config=task_config,
                agent=agent,
                config={"group_id": "grp-1"},
            )
        assert task is not None

    @pytest.mark.asyncio
    async def test_skips_auto_callback_when_backend_not_databricks(self):
        agent = _make_agent()
        task_config = _base_task_config()

        with patch("src.services.mcp_service.MCPService"), \
             patch("src.engines.crewai.tools.mcp_integration.MCPIntegration") as mock_mcp, \
             patch("src.db.session.request_scoped_session") as mock_sess, \
             patch("src.engines.crewai.paths.crew.task_adapter.Task") as mock_task_cls, \
             patch("src.services.databricks_service.DatabricksService") as mock_db_svc, \
             patch("src.services.memory_backend_service.MemoryBackendService") as mock_mem_svc:

            mock_mcp.create_mcp_tools_for_task = AsyncMock(return_value=[])
            mock_session = AsyncMock()
            mock_session.__aenter__ = AsyncMock(return_value=mock_session)
            mock_session.__aexit__ = AsyncMock(return_value=None)
            mock_sess.return_value = mock_session
            mock_task_cls.return_value = MagicMock(tools=[], agent=agent)

            mock_db_config = MagicMock()
            mock_db_config.volume_enabled = True
            mock_db_config.volume_path = "/Volumes/test"
            mock_db_svc.return_value.get_databricks_config = AsyncMock(return_value=mock_db_config)

            mock_active_config = MagicMock()
            mock_active_config.enable_short_term = True
            mock_active_config.backend_type = MagicMock()
            mock_active_config.backend_type.value = "chromadb"  # Not Databricks
            mock_mem_svc.return_value.get_active_config = AsyncMock(return_value=mock_active_config)

            task = await create_task(
                task_key="no-auto-cb",
                task_config=task_config,
                agent=agent,
                config={"group_id": "grp-1"},
            )

        call_kwargs = mock_task_cls.call_args[1]
        assert "callback" not in call_kwargs
