"""
Comprehensive unit tests for ConnectionService.

Tests cover:
- __init__ / constructor
- _log_llm_interaction (success, error, exception paths)
- _format_agents_and_tasks (various agent/task shapes)
- _validate_response (valid, missing keys, unassigned tasks, malformed data)
- generate_connections (happy path, session vs no-session template lookup,
    model defaulting, auth context branches, LLM failure, JSON parsing branches,
    validation failure, outer exception propagation)
- validate_api_key (valid, invalid, empty, network error)
- test_api_keys (env vars present and absent, openai validation delegation)
"""

import json
import os
import sys
import pytest
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

from src.services.connection_service import ConnectionService
from src.schemas.connection import (
    ConnectionRequest,
    ConnectionResponse,
    Agent,
    Task,
    TaskContext,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_SENTINEL = object()

def _make_agent(name="Agent1", role="Researcher", goal="Research", backstory=None, tools=None):
    return Agent(name=name, role=role, goal=goal, backstory=backstory, tools=tools)


def _make_task(name="Task1", description="Do stuff", expected_output=None, tools=None, context=None):
    return Task(name=name, description=description, expected_output=expected_output, tools=tools, context=context)


def _make_request(agents=None, tasks=None, model=_SENTINEL, instructions=None):
    """Build a ConnectionRequest. Pass model="" explicitly to get an empty model."""
    if model is _SENTINEL:
        model = "gpt-4-turbo"
    return ConnectionRequest(
        agents=agents or [_make_agent()],
        tasks=tasks or [_make_task()],
        model=model,
        instructions=instructions,
    )


def _valid_response_data(task_names=None, agent_name="Agent1"):
    """Return a well-formed response dict that passes validation."""
    task_names = task_names or ["Task1"]
    return {
        "assignments": [
            {
                "agent_name": agent_name,
                "tasks": [{"task_name": tn, "reasoning": "fits"} for tn in task_names],
            }
        ],
        "dependencies": [],
        "explanation": "All tasks assigned.",
    }


def _mock_llm_response(content):
    """Create a mock LLM response dict."""
    return {
        "choices": [{"message": {"content": content}}]
    }


# Patch paths -- local imports in generate_connections use these modules
_AUTH_PATCH = "src.utils.databricks_auth.get_auth_context"
_LLM_MANAGER_PATCH = "src.services.connection_service.LLMManager"
_JSON_PARSER_PATCH = "src.services.connection_service.robust_json_parser"
_TEMPLATE_SVC_PATCH = "src.services.connection_service.TemplateService"
_TEMPLATE_REPO_PATCH = "src.repositories.template_repository.TemplateRepository"
_AIOHTTP_SESSION_PATCH = "aiohttp.ClientSession"


# ---------------------------------------------------------------------------
# TestInit
# ---------------------------------------------------------------------------

class TestInit:
    def test_init_with_session(self):
        session = MagicMock()
        svc = ConnectionService(session=session)
        assert svc.session is session

    def test_init_without_session(self):
        svc = ConnectionService()
        assert svc.session is None


# ---------------------------------------------------------------------------
# TestLogLLMInteraction
# ---------------------------------------------------------------------------

class TestLogLLMInteraction:

    @pytest.mark.asyncio
    async def test_success_log(self):
        svc = ConnectionService()
        # Should complete without raising
        await svc._log_llm_interaction(
            endpoint="test-endpoint",
            prompt="test prompt",
            response="test response",
            model="gpt-4",
            status="success",
        )

    @pytest.mark.asyncio
    async def test_error_log_with_error_message(self):
        svc = ConnectionService()
        await svc._log_llm_interaction(
            endpoint="test-endpoint",
            prompt="test prompt",
            response="err",
            model="gpt-4",
            status="error",
            error_message="something broke",
        )

    @pytest.mark.asyncio
    async def test_exception_in_logging_does_not_propagate(self):
        svc = ConnectionService()
        with patch("src.services.connection_service.logger") as mock_logger:
            mock_logger.info.side_effect = RuntimeError("logging failed")
            # Should not raise, error is caught internally
            await svc._log_llm_interaction(
                endpoint="ep", prompt="p", response="r", model="m"
            )
            mock_logger.error.assert_called()


# ---------------------------------------------------------------------------
# TestFormatAgentsAndTasks
# ---------------------------------------------------------------------------

class TestFormatAgentsAndTasks:

    @pytest.mark.asyncio
    async def test_basic_agent_and_task(self):
        svc = ConnectionService()
        request = _make_request()
        agents_info, tasks_info = await svc._format_agents_and_tasks(request)

        assert "Agent1" in agents_info
        assert "Researcher" in agents_info
        assert "Research" in agents_info
        assert "Not provided" in agents_info  # backstory is None
        assert "Task1" in tasks_info
        assert "Do stuff" in tasks_info

    @pytest.mark.asyncio
    async def test_agent_with_tools(self):
        svc = ConnectionService()
        agent = _make_agent(tools=["scraper", "browser"])
        request = _make_request(agents=[agent])
        agents_info, _ = await svc._format_agents_and_tasks(request)

        assert "scraper" in agents_info
        assert "browser" in agents_info

    @pytest.mark.asyncio
    async def test_agent_with_backstory(self):
        svc = ConnectionService()
        agent = _make_agent(backstory="Expert in AI")
        request = _make_request(agents=[agent])
        agents_info, _ = await svc._format_agents_and_tasks(request)

        assert "Expert in AI" in agents_info
        assert "Not provided" not in agents_info

    @pytest.mark.asyncio
    async def test_task_with_expected_output_and_tools(self):
        svc = ConnectionService()
        task = _make_task(expected_output="A report", tools=["tool_a", "tool_b"])
        request = _make_request(tasks=[task])
        _, tasks_info = await svc._format_agents_and_tasks(request)

        assert "A report" in tasks_info
        assert "tool_a" in tasks_info
        assert "tool_b" in tasks_info

    @pytest.mark.asyncio
    async def test_task_with_context(self):
        svc = ConnectionService()
        ctx = TaskContext(
            type="analysis",
            priority="high",
            complexity="complex",
            required_skills=["python", "ml"],
        )
        task = _make_task(context=ctx)
        request = _make_request(tasks=[task])
        _, tasks_info = await svc._format_agents_and_tasks(request)

        assert "analysis" in tasks_info
        assert "high" in tasks_info
        assert "complex" in tasks_info
        assert "python" in tasks_info
        assert "ml" in tasks_info

    @pytest.mark.asyncio
    async def test_task_context_without_required_skills(self):
        svc = ConnectionService()
        ctx = TaskContext(type="general", priority="low", complexity="simple")
        task = _make_task(context=ctx)
        request = _make_request(tasks=[task])
        _, tasks_info = await svc._format_agents_and_tasks(request)

        assert "general" in tasks_info
        assert "Required Skills" not in tasks_info

    @pytest.mark.asyncio
    async def test_multiple_agents_and_tasks(self):
        svc = ConnectionService()
        agents = [_make_agent(name="A1"), _make_agent(name="A2")]
        tasks = [_make_task(name="T1"), _make_task(name="T2")]
        request = _make_request(agents=agents, tasks=tasks)
        agents_info, tasks_info = await svc._format_agents_and_tasks(request)

        assert "A1" in agents_info
        assert "A2" in agents_info
        assert "T1" in tasks_info
        assert "T2" in tasks_info


# ---------------------------------------------------------------------------
# TestValidateResponse
# ---------------------------------------------------------------------------

class TestValidateResponse:

    @pytest.mark.asyncio
    async def test_valid_response_passes(self):
        svc = ConnectionService()
        request = _make_request()
        data = _valid_response_data()
        # Should not raise
        await svc._validate_response(data, request)

    @pytest.mark.asyncio
    async def test_missing_assignments_key(self):
        svc = ConnectionService()
        request = _make_request()
        with pytest.raises(ValueError, match="Invalid response structure"):
            await svc._validate_response({"dependencies": []}, request)

    @pytest.mark.asyncio
    async def test_missing_dependencies_key(self):
        svc = ConnectionService()
        request = _make_request()
        with pytest.raises(ValueError, match="Invalid response structure"):
            await svc._validate_response({"assignments": []}, request)

    @pytest.mark.asyncio
    async def test_not_a_dict(self):
        svc = ConnectionService()
        request = _make_request()
        with pytest.raises(ValueError, match="Invalid response structure"):
            await svc._validate_response("not a dict", request)

    @pytest.mark.asyncio
    async def test_assignment_missing_agent_name(self):
        svc = ConnectionService()
        request = _make_request()
        data = {
            "assignments": [{"tasks": [{"task_name": "Task1", "reasoning": "ok"}]}],
            "dependencies": [],
        }
        with pytest.raises(ValueError, match="Invalid assignment structure"):
            await svc._validate_response(data, request)

    @pytest.mark.asyncio
    async def test_assignment_missing_tasks(self):
        svc = ConnectionService()
        request = _make_request()
        data = {
            "assignments": [{"agent_name": "Agent1"}],
            "dependencies": [],
        }
        with pytest.raises(ValueError, match="Invalid assignment structure"):
            await svc._validate_response(data, request)

    @pytest.mark.asyncio
    async def test_task_missing_task_name(self):
        svc = ConnectionService()
        request = _make_request()
        data = {
            "assignments": [
                {"agent_name": "Agent1", "tasks": [{"reasoning": "ok"}]}
            ],
            "dependencies": [],
        }
        with pytest.raises(ValueError, match="Invalid task structure"):
            await svc._validate_response(data, request)

    @pytest.mark.asyncio
    async def test_task_missing_reasoning(self):
        svc = ConnectionService()
        request = _make_request()
        data = {
            "assignments": [
                {"agent_name": "Agent1", "tasks": [{"task_name": "Task1"}]}
            ],
            "dependencies": [],
        }
        with pytest.raises(ValueError, match="Invalid task structure"):
            await svc._validate_response(data, request)

    @pytest.mark.asyncio
    async def test_unassigned_tasks(self):
        svc = ConnectionService()
        tasks = [_make_task(name="T1"), _make_task(name="T2")]
        request = _make_request(tasks=tasks)
        # Only T1 assigned, T2 missing
        data = {
            "assignments": [
                {"agent_name": "Agent1", "tasks": [{"task_name": "T1", "reasoning": "ok"}]}
            ],
            "dependencies": [],
        }
        with pytest.raises(ValueError, match="failed to assign"):
            await svc._validate_response(data, request)

    @pytest.mark.asyncio
    async def test_all_tasks_assigned_multiple_agents(self):
        svc = ConnectionService()
        tasks = [_make_task(name="T1"), _make_task(name="T2")]
        request = _make_request(tasks=tasks)
        data = {
            "assignments": [
                {"agent_name": "Agent1", "tasks": [{"task_name": "T1", "reasoning": "ok"}]},
                {"agent_name": "Agent2", "tasks": [{"task_name": "T2", "reasoning": "ok"}]},
            ],
            "dependencies": [],
        }
        # Should not raise
        await svc._validate_response(data, request)

    @pytest.mark.asyncio
    async def test_task_in_assignment_not_a_dict(self):
        svc = ConnectionService()
        request = _make_request()
        data = {
            "assignments": [
                {"agent_name": "Agent1", "tasks": ["not a dict"]}
            ],
            "dependencies": [],
        }
        with pytest.raises(ValueError, match="Invalid task structure"):
            await svc._validate_response(data, request)

    @pytest.mark.asyncio
    async def test_assignment_not_a_dict(self):
        svc = ConnectionService()
        request = _make_request()
        data = {
            "assignments": ["not a dict"],
            "dependencies": [],
        }
        with pytest.raises(ValueError, match="Invalid assignment structure"):
            await svc._validate_response(data, request)


# ---------------------------------------------------------------------------
# TestGenerateConnections
# ---------------------------------------------------------------------------

class TestGenerateConnections:

    @pytest.mark.asyncio
    @patch(_LLM_MANAGER_PATCH)
    @patch(_JSON_PARSER_PATCH)
    @patch(_TEMPLATE_SVC_PATCH)
    @patch(_AUTH_PATCH, new_callable=AsyncMock, return_value=None)
    async def test_happy_path_no_session(self, mock_auth, MockTemplateService, mock_parser, MockLLMManager):
        """Test generate_connections with no session (static fallback)."""
        svc = ConnectionService(session=None)
        request = _make_request()

        # Template static method -- no session means TemplateService.get_template_content is called as static
        MockTemplateService.get_template_content = AsyncMock(return_value="system prompt")

        # LLM
        response_data = _valid_response_data()
        MockLLMManager.configure_litellm = AsyncMock(return_value={"model": "gpt-4-turbo"})
        MockLLMManager.acompletion = AsyncMock(
            return_value=_mock_llm_response(json.dumps(response_data))
        )

        # Parser
        mock_parser.return_value = response_data

        result = await svc.generate_connections(request)

        assert isinstance(result, ConnectionResponse)
        assert len(result.assignments) == 1

    @pytest.mark.asyncio
    @patch(_LLM_MANAGER_PATCH)
    @patch(_JSON_PARSER_PATCH)
    @patch(_AUTH_PATCH, new_callable=AsyncMock, return_value=None)
    async def test_happy_path_with_session(self, mock_auth, mock_parser, MockLLMManager):
        """Test generate_connections with a session (instance template lookup)."""
        svc = ConnectionService(session=MagicMock())
        request = _make_request()

        response_data = _valid_response_data()

        # Patch the template repository and service at their real import locations
        with patch(_TEMPLATE_REPO_PATCH) as MockRepo, \
             patch(_TEMPLATE_SVC_PATCH) as MockTplSvc:

            mock_tpl_instance = MagicMock()
            mock_tpl_instance.get_template_content = AsyncMock(return_value="system prompt")
            MockTplSvc.return_value = mock_tpl_instance

            MockLLMManager.configure_litellm = AsyncMock(return_value={"model": "gpt-4-turbo"})
            MockLLMManager.acompletion = AsyncMock(
                return_value=_mock_llm_response(json.dumps(response_data))
            )
            mock_parser.return_value = response_data

            result = await svc.generate_connections(request)

        assert isinstance(result, ConnectionResponse)

    @pytest.mark.asyncio
    @patch(_LLM_MANAGER_PATCH)
    @patch(_JSON_PARSER_PATCH)
    @patch(_TEMPLATE_SVC_PATCH)
    @patch(_AUTH_PATCH, new_callable=AsyncMock, return_value=None)
    async def test_template_not_found_raises(self, mock_auth, MockTemplateService, mock_parser, MockLLMManager):
        """Raises ValueError when template is not found."""
        svc = ConnectionService(session=None)
        request = _make_request()

        MockTemplateService.get_template_content = AsyncMock(return_value=None)

        with pytest.raises(ValueError, match="template.*not found"):
            await svc.generate_connections(request)

    @pytest.mark.asyncio
    @patch(_LLM_MANAGER_PATCH)
    @patch(_JSON_PARSER_PATCH)
    @patch(_TEMPLATE_SVC_PATCH)
    @patch(_AUTH_PATCH, new_callable=AsyncMock, return_value=None)
    async def test_empty_template_raises(self, mock_auth, MockTemplateService, mock_parser, MockLLMManager):
        """Raises ValueError when template content is empty string."""
        svc = ConnectionService(session=None)
        request = _make_request()

        MockTemplateService.get_template_content = AsyncMock(return_value="")

        with pytest.raises(ValueError, match="template.*not found"):
            await svc.generate_connections(request)

    @pytest.mark.asyncio
    @patch(_LLM_MANAGER_PATCH)
    @patch(_JSON_PARSER_PATCH)
    @patch(_TEMPLATE_SVC_PATCH)
    @patch(_AUTH_PATCH, new_callable=AsyncMock, return_value=None)
    async def test_additional_instructions_appended(self, mock_auth, MockTemplateService, mock_parser, MockLLMManager):
        """When request has instructions, they are included in user message."""
        svc = ConnectionService(session=None)
        request = _make_request(instructions="Focus on performance")

        MockTemplateService.get_template_content = AsyncMock(return_value="system prompt")

        response_data = _valid_response_data()
        MockLLMManager.configure_litellm = AsyncMock(return_value={"model": "gpt-4-turbo"})

        captured_kwargs = {}
        async def capture_acompletion(**kwargs):
            captured_kwargs.update(kwargs)
            return _mock_llm_response(json.dumps(response_data))

        MockLLMManager.acompletion = capture_acompletion
        mock_parser.return_value = response_data

        await svc.generate_connections(request)

        # Verify user message includes additional instructions
        messages = captured_kwargs.get("messages", [])
        user_msg = messages[-1]["content"]
        assert "Focus on performance" in user_msg
        assert "ADDITIONAL INSTRUCTIONS" in user_msg

    @pytest.mark.asyncio
    @patch(_LLM_MANAGER_PATCH)
    @patch(_TEMPLATE_SVC_PATCH)
    @patch(_AUTH_PATCH, new_callable=AsyncMock, return_value=None)
    async def test_llm_completion_error_raises_valueerror(self, mock_auth, MockTemplateService, MockLLMManager):
        """When LLM acompletion raises, it becomes a ValueError."""
        svc = ConnectionService(session=None)
        request = _make_request()

        MockTemplateService.get_template_content = AsyncMock(return_value="system prompt")
        MockLLMManager.configure_litellm = AsyncMock(return_value={"model": "gpt-4-turbo"})
        MockLLMManager.acompletion = AsyncMock(side_effect=RuntimeError("API down"))

        with pytest.raises(ValueError, match="Failed to generate connections"):
            await svc.generate_connections(request)

    @pytest.mark.asyncio
    @patch(_LLM_MANAGER_PATCH)
    @patch(_JSON_PARSER_PATCH)
    @patch(_TEMPLATE_SVC_PATCH)
    @patch(_AUTH_PATCH, new_callable=AsyncMock, return_value=None)
    async def test_json_parsing_error_raises_valueerror(self, mock_auth, MockTemplateService, mock_parser, MockLLMManager):
        """When JSON parsing fails, a ValueError is raised."""
        svc = ConnectionService(session=None)
        request = _make_request()

        MockTemplateService.get_template_content = AsyncMock(return_value="system prompt")
        MockLLMManager.configure_litellm = AsyncMock(return_value={"model": "gpt-4-turbo"})
        MockLLMManager.acompletion = AsyncMock(
            return_value=_mock_llm_response("not json at all")
        )
        mock_parser.side_effect = ValueError("Cannot parse")

        with pytest.raises(ValueError, match="Error processing connection response"):
            await svc.generate_connections(request)

    @pytest.mark.asyncio
    @patch(_LLM_MANAGER_PATCH)
    @patch(_JSON_PARSER_PATCH)
    @patch(_TEMPLATE_SVC_PATCH)
    @patch(_AUTH_PATCH, new_callable=AsyncMock, return_value=None)
    async def test_validation_failure_raises_valueerror(self, mock_auth, MockTemplateService, mock_parser, MockLLMManager):
        """When _validate_response raises ValueError, it is re-raised."""
        svc = ConnectionService(session=None)
        tasks = [_make_task(name="T1"), _make_task(name="T2")]
        request = _make_request(tasks=tasks)

        MockTemplateService.get_template_content = AsyncMock(return_value="system prompt")
        MockLLMManager.configure_litellm = AsyncMock(return_value={"model": "gpt-4-turbo"})

        # Only assign T1, leave T2 unassigned
        bad_data = {
            "assignments": [
                {"agent_name": "Agent1", "tasks": [{"task_name": "T1", "reasoning": "ok"}]}
            ],
            "dependencies": [],
        }
        MockLLMManager.acompletion = AsyncMock(
            return_value=_mock_llm_response(json.dumps(bad_data))
        )
        mock_parser.return_value = bad_data

        with pytest.raises(ValueError, match="Error processing connection response"):
            await svc.generate_connections(request)

    @pytest.mark.asyncio
    @patch(_LLM_MANAGER_PATCH)
    @patch(_JSON_PARSER_PATCH)
    @patch(_TEMPLATE_SVC_PATCH)
    @patch(_AUTH_PATCH, new_callable=AsyncMock, return_value=None)
    async def test_markdown_json_extraction(self, mock_auth, MockTemplateService, mock_parser, MockLLMManager):
        """Content wrapped in ```json ``` is stripped before parsing."""
        svc = ConnectionService(session=None)
        request = _make_request()

        MockTemplateService.get_template_content = AsyncMock(return_value="system prompt")
        MockLLMManager.configure_litellm = AsyncMock(return_value={"model": "gpt-4-turbo"})

        response_data = _valid_response_data()
        markdown_content = f"```json\n{json.dumps(response_data)}\n```"
        MockLLMManager.acompletion = AsyncMock(
            return_value=_mock_llm_response(markdown_content)
        )
        mock_parser.return_value = response_data

        result = await svc.generate_connections(request)

        assert isinstance(result, ConnectionResponse)
        # Verify the parser was called with the stripped content, not the raw markdown
        call_arg = mock_parser.call_args[0][0]
        assert "```" not in call_arg

    @pytest.mark.asyncio
    @patch(_LLM_MANAGER_PATCH)
    @patch(_JSON_PARSER_PATCH)
    @patch(_TEMPLATE_SVC_PATCH)
    @patch(_AUTH_PATCH, new_callable=AsyncMock, return_value=None)
    async def test_generic_code_block_extraction(self, mock_auth, MockTemplateService, mock_parser, MockLLMManager):
        """Content wrapped in generic ``` ``` blocks is also stripped."""
        svc = ConnectionService(session=None)
        request = _make_request()

        MockTemplateService.get_template_content = AsyncMock(return_value="system prompt")
        MockLLMManager.configure_litellm = AsyncMock(return_value={"model": "gpt-4-turbo"})

        response_data = _valid_response_data()
        # generic code block (no 'json' keyword)
        code_content = f"```\n{json.dumps(response_data)}\n```"
        MockLLMManager.acompletion = AsyncMock(
            return_value=_mock_llm_response(code_content)
        )
        mock_parser.return_value = response_data

        result = await svc.generate_connections(request)

        assert isinstance(result, ConnectionResponse)

    @pytest.mark.asyncio
    @patch(_LLM_MANAGER_PATCH)
    @patch(_JSON_PARSER_PATCH)
    @patch(_TEMPLATE_SVC_PATCH)
    @patch(_AUTH_PATCH, new_callable=AsyncMock, return_value=None)
    async def test_model_default_no_env_no_request(self, mock_auth, MockTemplateService, mock_parser, MockLLMManager):
        """When request.model is empty and no env var, defaults to gpt-4o-mini."""
        svc = ConnectionService(session=None)
        # Use empty string model to trigger default logic
        request = _make_request(model="")

        MockTemplateService.get_template_content = AsyncMock(return_value="system prompt")

        response_data = _valid_response_data()
        captured_model = []

        async def capture_configure(model):
            captured_model.append(model)
            return {"model": model}

        MockLLMManager.configure_litellm = capture_configure
        MockLLMManager.acompletion = AsyncMock(
            return_value=_mock_llm_response(json.dumps(response_data))
        )
        mock_parser.return_value = response_data

        # Clear the CONNECTION_MODEL env var if present
        env_patch = {k: v for k, v in os.environ.items() if k != "CONNECTION_MODEL"}
        with patch.dict(os.environ, env_patch, clear=True):
            result = await svc.generate_connections(request)

        assert isinstance(result, ConnectionResponse)
        # Default should be gpt-4o-mini when auth returns None
        assert "gpt-4o-mini" in captured_model

    @pytest.mark.asyncio
    @patch(_LLM_MANAGER_PATCH)
    @patch(_JSON_PARSER_PATCH)
    @patch(_TEMPLATE_SVC_PATCH)
    @patch(_AUTH_PATCH, new_callable=AsyncMock, return_value=None)
    async def test_model_from_env_var(self, mock_auth, MockTemplateService, mock_parser, MockLLMManager):
        """When request.model is empty, CONNECTION_MODEL env var is used."""
        svc = ConnectionService(session=None)
        # Empty string model triggers fallback to env var
        request = _make_request(model="")

        MockTemplateService.get_template_content = AsyncMock(return_value="system prompt")

        response_data = _valid_response_data()
        captured_model = []

        async def capture_configure(model):
            captured_model.append(model)
            return {"model": model}

        MockLLMManager.configure_litellm = capture_configure
        MockLLMManager.acompletion = AsyncMock(
            return_value=_mock_llm_response(json.dumps(response_data))
        )
        mock_parser.return_value = response_data

        with patch.dict(os.environ, {"CONNECTION_MODEL": "custom-model"}):
            result = await svc.generate_connections(request)

        assert "custom-model" in captured_model

    @pytest.mark.asyncio
    @patch(_LLM_MANAGER_PATCH)
    @patch(_JSON_PARSER_PATCH)
    @patch(_TEMPLATE_SVC_PATCH)
    async def test_service_principal_auth_uses_databricks_model(self, MockTemplateService, mock_parser, MockLLMManager):
        """When auth context says service_principal, default model is databricks."""
        svc = ConnectionService(session=None)
        # Empty string model triggers default logic
        request = _make_request(model="")

        MockTemplateService.get_template_content = AsyncMock(return_value="system prompt")

        response_data = _valid_response_data()
        captured_model = []

        async def capture_configure(model):
            captured_model.append(model)
            return {"model": model}

        MockLLMManager.configure_litellm = capture_configure
        MockLLMManager.acompletion = AsyncMock(
            return_value=_mock_llm_response(json.dumps(response_data))
        )
        mock_parser.return_value = response_data

        auth_ctx = SimpleNamespace(auth_method="service_principal")
        with patch(_AUTH_PATCH, new_callable=AsyncMock, return_value=auth_ctx):
            # Remove CONNECTION_MODEL if present
            os.environ.pop("CONNECTION_MODEL", None)
            result = await svc.generate_connections(request)

        assert any("databricks" in m for m in captured_model)

    @pytest.mark.asyncio
    @patch(_LLM_MANAGER_PATCH)
    @patch(_JSON_PARSER_PATCH)
    @patch(_TEMPLATE_SVC_PATCH)
    async def test_auth_context_exception_falls_back(self, MockTemplateService, mock_parser, MockLLMManager):
        """When get_auth_context raises, the default model is still gpt-4o-mini."""
        svc = ConnectionService(session=None)
        # Empty string model triggers default logic
        request = _make_request(model="")

        MockTemplateService.get_template_content = AsyncMock(return_value="system prompt")

        response_data = _valid_response_data()
        captured_model = []

        async def capture_configure(model):
            captured_model.append(model)
            return {"model": model}

        MockLLMManager.configure_litellm = capture_configure
        MockLLMManager.acompletion = AsyncMock(
            return_value=_mock_llm_response(json.dumps(response_data))
        )
        mock_parser.return_value = response_data

        with patch(_AUTH_PATCH, new_callable=AsyncMock, side_effect=ImportError("no module")):
            os.environ.pop("CONNECTION_MODEL", None)
            result = await svc.generate_connections(request)

        assert any("gpt-4o-mini" in m for m in captured_model)

    @pytest.mark.asyncio
    @patch(_LLM_MANAGER_PATCH)
    @patch(_JSON_PARSER_PATCH)
    @patch(_TEMPLATE_SVC_PATCH)
    @patch(_AUTH_PATCH, new_callable=AsyncMock, return_value=None)
    async def test_request_model_overrides_default(self, mock_auth, MockTemplateService, mock_parser, MockLLMManager):
        """When request.model is specified, it overrides all defaults."""
        svc = ConnectionService(session=None)
        request = _make_request(model="claude-3-opus")

        MockTemplateService.get_template_content = AsyncMock(return_value="system prompt")

        response_data = _valid_response_data()
        captured_model = []

        async def capture_configure(model):
            captured_model.append(model)
            return {"model": model}

        MockLLMManager.configure_litellm = capture_configure
        MockLLMManager.acompletion = AsyncMock(
            return_value=_mock_llm_response(json.dumps(response_data))
        )
        mock_parser.return_value = response_data

        result = await svc.generate_connections(request)

        assert "claude-3-opus" in captured_model

    @pytest.mark.asyncio
    @patch(_LLM_MANAGER_PATCH)
    @patch(_JSON_PARSER_PATCH)
    @patch(_TEMPLATE_SVC_PATCH)
    @patch(_AUTH_PATCH, new_callable=AsyncMock, return_value=None)
    async def test_response_without_explanation(self, mock_auth, MockTemplateService, mock_parser, MockLLMManager):
        """Response data without 'explanation' key still works (defaults to empty)."""
        svc = ConnectionService(session=None)
        request = _make_request()

        MockTemplateService.get_template_content = AsyncMock(return_value="system prompt")
        MockLLMManager.configure_litellm = AsyncMock(return_value={"model": "gpt-4-turbo"})

        response_data = {
            "assignments": [
                {"agent_name": "Agent1", "tasks": [{"task_name": "Task1", "reasoning": "ok"}]}
            ],
            "dependencies": [],
        }
        MockLLMManager.acompletion = AsyncMock(
            return_value=_mock_llm_response(json.dumps(response_data))
        )
        mock_parser.return_value = response_data

        result = await svc.generate_connections(request)

        assert isinstance(result, ConnectionResponse)


# ---------------------------------------------------------------------------
# TestValidateApiKey
# ---------------------------------------------------------------------------

class TestValidateApiKey:

    @pytest.mark.asyncio
    async def test_empty_key(self):
        svc = ConnectionService()
        valid, msg = await svc.validate_api_key("")
        assert valid is False
        assert "No API key" in msg

    @pytest.mark.asyncio
    async def test_none_key(self):
        svc = ConnectionService()
        valid, msg = await svc.validate_api_key(None)
        assert valid is False
        assert "No API key" in msg

    @pytest.mark.asyncio
    async def test_valid_key(self):
        svc = ConnectionService()

        mock_response = AsyncMock()
        mock_response.status = 200

        mock_session_ctx = AsyncMock()
        mock_session_ctx.__aenter__ = AsyncMock(return_value=mock_response)
        mock_session_ctx.__aexit__ = AsyncMock(return_value=False)

        mock_session = AsyncMock()
        mock_session.get = MagicMock(return_value=mock_session_ctx)

        mock_client_ctx = AsyncMock()
        mock_client_ctx.__aenter__ = AsyncMock(return_value=mock_session)
        mock_client_ctx.__aexit__ = AsyncMock(return_value=False)

        with patch(_AIOHTTP_SESSION_PATCH, return_value=mock_client_ctx):
            valid, msg = await svc.validate_api_key("sk-test-key-12345")

        assert valid is True
        assert "valid" in msg.lower()

    @pytest.mark.asyncio
    async def test_invalid_key_status_401(self):
        svc = ConnectionService()

        mock_response = AsyncMock()
        mock_response.status = 401
        mock_response.text = AsyncMock(return_value="Unauthorized")

        mock_session_ctx = AsyncMock()
        mock_session_ctx.__aenter__ = AsyncMock(return_value=mock_response)
        mock_session_ctx.__aexit__ = AsyncMock(return_value=False)

        mock_session = AsyncMock()
        mock_session.get = MagicMock(return_value=mock_session_ctx)

        mock_client_ctx = AsyncMock()
        mock_client_ctx.__aenter__ = AsyncMock(return_value=mock_session)
        mock_client_ctx.__aexit__ = AsyncMock(return_value=False)

        with patch(_AIOHTTP_SESSION_PATCH, return_value=mock_client_ctx):
            valid, msg = await svc.validate_api_key("sk-bad-key")

        assert valid is False
        assert "401" in msg

    @pytest.mark.asyncio
    async def test_network_error(self):
        svc = ConnectionService()

        mock_client_ctx = AsyncMock()
        mock_client_ctx.__aenter__ = AsyncMock(side_effect=ConnectionError("no network"))
        mock_client_ctx.__aexit__ = AsyncMock(return_value=False)

        with patch(_AIOHTTP_SESSION_PATCH, return_value=mock_client_ctx):
            valid, msg = await svc.validate_api_key("sk-some-key")

        assert valid is False
        assert "error" in msg.lower()


# ---------------------------------------------------------------------------
# TestTestApiKeys
# ---------------------------------------------------------------------------

class TestTestApiKeys:

    @pytest.mark.asyncio
    async def test_all_keys_present(self):
        svc = ConnectionService()
        svc.validate_api_key = AsyncMock(return_value=(True, "valid"))

        env = {
            "OPENAI_API_KEY": "sk-1234567890",
            "ANTHROPIC_API_KEY": "ant-abc",
            "DEEPSEEK_API_KEY": "ds-xyz",
        }
        with patch.dict(os.environ, env, clear=False):
            results = await svc.test_api_keys()

        # OpenAI
        assert results["openai"]["has_key"] is True
        assert results["openai"]["valid"] is True
        assert results["openai"]["key_prefix"] == "sk-1..."

        # Anthropic
        assert results["anthropic"]["has_key"] is True
        assert results["anthropic"]["key_prefix"] == "ant-..."

        # DeepSeek
        assert results["deepseek"]["has_key"] is True
        assert results["deepseek"]["key_prefix"] == "ds-x..."

        # Python info
        assert "version" in results["python_info"]
        assert "executable" in results["python_info"]
        assert "platform" in results["python_info"]

    @pytest.mark.asyncio
    async def test_no_keys_present(self):
        svc = ConnectionService()

        # Remove all relevant keys
        env_clean = {k: v for k, v in os.environ.items()
                     if k not in ("OPENAI_API_KEY", "ANTHROPIC_API_KEY", "DEEPSEEK_API_KEY")}
        with patch.dict(os.environ, env_clean, clear=True):
            results = await svc.test_api_keys()

        assert results["openai"]["has_key"] is False
        assert results["openai"]["valid"] is False
        assert results["anthropic"]["has_key"] is False
        assert results["deepseek"]["has_key"] is False

    @pytest.mark.asyncio
    async def test_openai_key_invalid(self):
        svc = ConnectionService()
        svc.validate_api_key = AsyncMock(return_value=(False, "invalid key"))

        env = {"OPENAI_API_KEY": "sk-bad"}
        env_clean = {k: v for k, v in os.environ.items()
                     if k not in ("ANTHROPIC_API_KEY", "DEEPSEEK_API_KEY")}
        env_clean.update(env)
        with patch.dict(os.environ, env_clean, clear=True):
            results = await svc.test_api_keys()

        assert results["openai"]["has_key"] is True
        assert results["openai"]["valid"] is False
        assert results["openai"]["message"] == "invalid key"

    @pytest.mark.asyncio
    async def test_python_info_populated(self):
        svc = ConnectionService()

        env_clean = {k: v for k, v in os.environ.items()
                     if k not in ("OPENAI_API_KEY", "ANTHROPIC_API_KEY", "DEEPSEEK_API_KEY")}
        with patch.dict(os.environ, env_clean, clear=True):
            results = await svc.test_api_keys()

        assert results["python_info"]["version"]
        assert results["python_info"]["executable"]
        assert results["python_info"]["platform"]


# ---------------------------------------------------------------------------
# Edge cases and integration-like scenarios
# ---------------------------------------------------------------------------

class TestEdgeCases:

    @pytest.mark.asyncio
    async def test_format_agents_no_optional_fields(self):
        """Agent and task with only required fields."""
        svc = ConnectionService()
        agent = Agent(name="A", role="R", goal="G")
        task = Task(name="T", description="D")
        request = ConnectionRequest(agents=[agent], tasks=[task], model="m")
        agents_info, tasks_info = await svc._format_agents_and_tasks(request)

        assert "A" in agents_info
        assert "T" in tasks_info
        assert "Not provided" in agents_info  # no backstory

    @pytest.mark.asyncio
    async def test_validate_response_empty_assignments_with_no_tasks(self):
        """If request has no tasks and assignments are empty, validation passes."""
        svc = ConnectionService()
        request = ConnectionRequest(agents=[], tasks=[], model="m")
        data = {"assignments": [], "dependencies": []}
        # No tasks to assign -- should not raise
        await svc._validate_response(data, request)

    @pytest.mark.asyncio
    @patch(_LLM_MANAGER_PATCH)
    @patch(_JSON_PARSER_PATCH)
    @patch(_TEMPLATE_SVC_PATCH)
    @patch(_AUTH_PATCH, new_callable=AsyncMock, return_value=None)
    async def test_generate_connections_no_instructions(self, mock_auth, MockTemplateService, mock_parser, MockLLMManager):
        """When request has no instructions, ADDITIONAL INSTRUCTIONS is not in the prompt."""
        svc = ConnectionService(session=None)
        request = _make_request(instructions=None)

        MockTemplateService.get_template_content = AsyncMock(return_value="system prompt")

        response_data = _valid_response_data()
        captured_kwargs = {}

        async def capture_acompletion(**kwargs):
            captured_kwargs.update(kwargs)
            return {"choices": [{"message": {"content": json.dumps(response_data)}}]}

        MockLLMManager.configure_litellm = AsyncMock(return_value={"model": "gpt-4-turbo"})
        MockLLMManager.acompletion = capture_acompletion
        mock_parser.return_value = response_data

        await svc.generate_connections(request)

        user_msg = captured_kwargs["messages"][-1]["content"]
        assert "ADDITIONAL INSTRUCTIONS" not in user_msg

    @pytest.mark.asyncio
    @patch(_LLM_MANAGER_PATCH)
    @patch(_JSON_PARSER_PATCH)
    @patch(_TEMPLATE_SVC_PATCH)
    @patch(_AUTH_PATCH, new_callable=AsyncMock, return_value=None)
    async def test_generate_connections_calls_configure_litellm(self, mock_auth, MockTemplateService, mock_parser, MockLLMManager):
        """Verify configure_litellm is called with the correct model."""
        svc = ConnectionService(session=None)
        request = _make_request(model="my-model")

        MockTemplateService.get_template_content = AsyncMock(return_value="sys")

        response_data = _valid_response_data()
        MockLLMManager.configure_litellm = AsyncMock(return_value={"model": "my-model"})
        MockLLMManager.acompletion = AsyncMock(
            return_value={"choices": [{"message": {"content": json.dumps(response_data)}}]}
        )
        mock_parser.return_value = response_data

        await svc.generate_connections(request)

        MockLLMManager.configure_litellm.assert_awaited_once_with("my-model")

    @pytest.mark.asyncio
    @patch(_LLM_MANAGER_PATCH)
    @patch(_JSON_PARSER_PATCH)
    @patch(_TEMPLATE_SVC_PATCH)
    @patch(_AUTH_PATCH, new_callable=AsyncMock, return_value=None)
    async def test_generate_connections_acompletion_receives_expected_params(self, mock_auth, MockTemplateService, mock_parser, MockLLMManager):
        """Verify acompletion is called with temperature and max_tokens."""
        svc = ConnectionService(session=None)
        request = _make_request()

        MockTemplateService.get_template_content = AsyncMock(return_value="sys")

        response_data = _valid_response_data()
        captured = {}

        async def capture(**kwargs):
            captured.update(kwargs)
            return {"choices": [{"message": {"content": json.dumps(response_data)}}]}

        MockLLMManager.configure_litellm = AsyncMock(return_value={"model": "gpt-4-turbo"})
        MockLLMManager.acompletion = capture
        mock_parser.return_value = response_data

        await svc.generate_connections(request)

        assert captured["temperature"] == 0.7
        assert captured["max_tokens"] == 4000
        assert "messages" in captured

    @pytest.mark.asyncio
    async def test_validate_response_invalid_structure_not_dict(self):
        """Test validation with a list instead of dict."""
        svc = ConnectionService()
        request = _make_request()
        with pytest.raises(ValueError, match="Invalid response structure"):
            await svc._validate_response([], request)

    @pytest.mark.asyncio
    async def test_validate_response_none_input(self):
        """Test validation with None."""
        svc = ConnectionService()
        request = _make_request()
        with pytest.raises((ValueError, TypeError)):
            await svc._validate_response(None, request)

    @pytest.mark.asyncio
    @patch(_LLM_MANAGER_PATCH)
    @patch(_JSON_PARSER_PATCH)
    @patch(_TEMPLATE_SVC_PATCH)
    @patch(_AUTH_PATCH, new_callable=AsyncMock, return_value=None)
    async def test_generate_connections_messages_structure(self, mock_auth, MockTemplateService, mock_parser, MockLLMManager):
        """Verify messages contain system and user roles."""
        svc = ConnectionService(session=None)
        request = _make_request()

        MockTemplateService.get_template_content = AsyncMock(return_value="sys prompt text")

        response_data = _valid_response_data()
        captured = {}

        async def capture(**kwargs):
            captured.update(kwargs)
            return _mock_llm_response(json.dumps(response_data))

        MockLLMManager.configure_litellm = AsyncMock(return_value={"model": "gpt-4-turbo"})
        MockLLMManager.acompletion = capture
        mock_parser.return_value = response_data

        await svc.generate_connections(request)

        messages = captured["messages"]
        assert len(messages) == 2
        assert messages[0]["role"] == "system"
        assert messages[0]["content"] == "sys prompt text"
        assert messages[1]["role"] == "user"
        assert "AVAILABLE AGENTS" in messages[1]["content"]
        assert "TASKS TO ASSIGN" in messages[1]["content"]

    @pytest.mark.asyncio
    @patch(_LLM_MANAGER_PATCH)
    @patch(_JSON_PARSER_PATCH)
    @patch(_TEMPLATE_SVC_PATCH)
    @patch(_AUTH_PATCH, new_callable=AsyncMock, return_value=None)
    async def test_generate_connections_model_params_merged(self, mock_auth, MockTemplateService, mock_parser, MockLLMManager):
        """Verify model_params from configure_litellm are spread into acompletion."""
        svc = ConnectionService(session=None)
        request = _make_request()

        MockTemplateService.get_template_content = AsyncMock(return_value="sys")

        response_data = _valid_response_data()
        captured = {}

        async def capture(**kwargs):
            captured.update(kwargs)
            return _mock_llm_response(json.dumps(response_data))

        MockLLMManager.configure_litellm = AsyncMock(
            return_value={"model": "gpt-4-turbo", "api_key": "test-key", "timeout": 120}
        )
        MockLLMManager.acompletion = capture
        mock_parser.return_value = response_data

        await svc.generate_connections(request)

        # model_params are spread into the call
        assert captured["model"] == "gpt-4-turbo"
        assert captured["api_key"] == "test-key"
        assert captured["timeout"] == 120

    @pytest.mark.asyncio
    async def test_validate_response_unassigned_tasks_message_content(self):
        """Verify the error message for unassigned tasks includes suggestions."""
        svc = ConnectionService()
        tasks = [_make_task(name="MissingTask")]
        request = _make_request(tasks=tasks)
        data = {
            "assignments": [
                {"agent_name": "Agent1", "tasks": []}
            ],
            "dependencies": [],
        }
        with pytest.raises(ValueError) as exc_info:
            await svc._validate_response(data, request)

        error_msg = str(exc_info.value)
        assert "MissingTask" in error_msg
        assert "Suggestions" in error_msg
        assert "different model" in error_msg
