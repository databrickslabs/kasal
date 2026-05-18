"""
Comprehensive unit tests for crew deployment service.
"""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

from src.services.crew_deployment_service import CrewDeploymentService, CrewAIModelWrapper
from src.schemas.crew_export import (
    ModelServingConfig,
    DeploymentResponse,
    DeploymentStatus,
)


class TestCrewDeploymentService:
    """Tests for CrewDeploymentService."""

    @pytest.fixture
    def mock_session(self):
        return AsyncMock()

    @pytest.fixture
    def service(self, mock_session):
        return CrewDeploymentService(session=mock_session)

    def test_init_creates_repositories(self, service, mock_session):
        assert service.session is mock_session
        assert service.crew_repository is not None
        assert service.agent_repository is not None
        assert service.task_repository is not None
        assert service.tool_repository is not None


class TestDeployToModelServing:
    """Tests for deploy_to_model_serving method."""

    @pytest.fixture
    def mock_session(self):
        return AsyncMock()

    @pytest.fixture
    def service(self, mock_session):
        return CrewDeploymentService(session=mock_session)

    @pytest.fixture
    def mock_group_context(self):
        context = MagicMock()
        context.group_ids = ['test-group']
        context.is_valid.return_value = True
        return context

    @pytest.fixture
    def model_config(self):
        return ModelServingConfig(
            model_name="test-model",
            endpoint_name="test-endpoint",
            workload_size="Small",
            scale_to_zero_enabled=True,
        )

    @pytest.mark.asyncio
    async def test_deploy_success(self, service, model_config, mock_group_context):
        crew_id = str(uuid4())
        mock_crew = MagicMock()
        mock_crew.id = crew_id
        mock_crew.name = 'Test Crew'
        mock_crew.agent_ids = []
        mock_crew.task_ids = []
        mock_crew.group_id = 'test-group'

        service.crew_repository.get = AsyncMock(return_value=mock_crew)

        with patch.object(service, '_create_mlflow_model', new_callable=AsyncMock) as mock_create:
            mock_create.return_value = ('model-uri', '1')
            with patch.object(service, '_deploy_to_endpoint', new_callable=AsyncMock) as mock_deploy:
                mock_deploy.return_value = (
                    'https://example.com/serving-endpoints/test-endpoint',
                    DeploymentStatus.PENDING,
                )
                result = await service.deploy_to_model_serving(
                    crew_id=crew_id,
                    config=model_config,
                    group_context=mock_group_context,
                )

        assert result.crew_id == crew_id
        assert result.model_name == 'test-model'
        assert result.endpoint_status == DeploymentStatus.PENDING

    @pytest.mark.asyncio
    async def test_deploy_crew_not_found(self, service, model_config, mock_group_context):
        crew_id = str(uuid4())
        service.crew_repository.get = AsyncMock(return_value=None)

        with pytest.raises(ValueError, match="not found"):
            await service.deploy_to_model_serving(
                crew_id=crew_id,
                config=model_config,
                group_context=mock_group_context,
            )

    @pytest.mark.asyncio
    async def test_deploy_group_not_authorized(self, service, model_config):
        crew_id = str(uuid4())
        mock_crew = MagicMock()
        mock_crew.id = crew_id
        mock_crew.name = 'Test Crew'
        mock_crew.agent_ids = []
        mock_crew.task_ids = []
        mock_crew.group_id = 'different-group'

        service.crew_repository.get = AsyncMock(return_value=mock_crew)

        context = MagicMock()
        context.group_ids = ['my-group']
        context.is_valid.return_value = True

        with pytest.raises(ValueError, match="not found"):
            await service.deploy_to_model_serving(
                crew_id=crew_id,
                config=model_config,
                group_context=context,
            )

    @pytest.mark.asyncio
    async def test_deploy_no_group_context(self, service, model_config):
        crew_id = str(uuid4())
        mock_crew = MagicMock()
        mock_crew.id = crew_id
        mock_crew.name = 'Test Crew'
        mock_crew.agent_ids = []
        mock_crew.task_ids = []
        mock_crew.group_id = 'any-group'

        service.crew_repository.get = AsyncMock(return_value=mock_crew)

        with patch.object(service, '_create_mlflow_model', new_callable=AsyncMock) as mock_create:
            mock_create.return_value = ('uri', '1')
            with patch.object(service, '_deploy_to_endpoint', new_callable=AsyncMock) as mock_deploy:
                mock_deploy.return_value = ('https://example.com/ep', DeploymentStatus.PENDING)
                result = await service.deploy_to_model_serving(
                    crew_id=crew_id,
                    config=model_config,
                    group_context=None,
                )

        assert result is not None

    @pytest.mark.asyncio
    async def test_deploy_with_agents_and_tasks(self, service, model_config, mock_group_context):
        crew_id = str(uuid4())
        mock_crew = MagicMock()
        mock_crew.id = crew_id
        mock_crew.name = 'Complex Crew'
        mock_crew.agent_ids = ['agent-1']
        mock_crew.task_ids = ['task-1']
        mock_crew.group_id = 'test-group'

        mock_agent = MagicMock()
        mock_agent.id = 'agent-1'
        mock_agent.name = 'Agent'
        mock_agent.role = 'Role'
        mock_agent.goal = 'Goal'
        mock_agent.backstory = 'BS'
        mock_agent.llm = 'gpt-4'
        mock_agent.tools = []
        mock_agent.max_iter = 25
        mock_agent.verbose = False
        mock_agent.allow_delegation = False

        mock_task = MagicMock()
        mock_task.id = 'task-1'
        mock_task.name = 'Task'
        mock_task.description = 'Desc'
        mock_task.expected_output = 'Output'
        mock_task.agent_id = 'agent-1'
        mock_task.tools = []
        mock_task.async_execution = False
        mock_task.context = []

        service.crew_repository.get = AsyncMock(return_value=mock_crew)
        service.agent_repository.get = AsyncMock(return_value=mock_agent)
        service.task_repository.get = AsyncMock(return_value=mock_task)

        with patch.object(service, '_create_mlflow_model', new_callable=AsyncMock) as mock_create:
            mock_create.return_value = ('uri', '2')
            with patch.object(service, '_deploy_to_endpoint', new_callable=AsyncMock) as mock_deploy:
                mock_deploy.return_value = ('https://example.com/ep', DeploymentStatus.PENDING)
                result = await service.deploy_to_model_serving(
                    crew_id=crew_id,
                    config=model_config,
                    group_context=mock_group_context,
                )

        assert result.metadata['agents_count'] == 1
        assert result.metadata['tasks_count'] == 1


class TestConvertToolIdsToNames:
    """Tests for _convert_tool_ids_to_names."""

    @pytest.fixture
    def service(self):
        return CrewDeploymentService(session=AsyncMock())

    @pytest.mark.asyncio
    async def test_empty_list(self, service):
        result = await service._convert_tool_ids_to_names([])
        assert result == []

    @pytest.mark.asyncio
    async def test_string_tool_name_passthrough(self, service):
        result = await service._convert_tool_ids_to_names(["WebSearch", "Calculator"])
        assert result == ["WebSearch", "Calculator"]

    @pytest.mark.asyncio
    async def test_integer_tool_id_lookup(self, service):
        mock_tool = MagicMock()
        mock_tool.title = "WebSearch"
        service.tool_repository.get = AsyncMock(return_value=mock_tool)

        result = await service._convert_tool_ids_to_names([42])
        assert result == ["WebSearch"]

    @pytest.mark.asyncio
    async def test_numeric_string_converted_to_int(self, service):
        mock_tool = MagicMock()
        mock_tool.title = "Calculator"
        service.tool_repository.get = AsyncMock(return_value=mock_tool)

        result = await service._convert_tool_ids_to_names(["10"])
        assert result == ["Calculator"]

    @pytest.mark.asyncio
    async def test_tool_not_found_returns_id_as_string(self, service):
        service.tool_repository.get = AsyncMock(return_value=None)
        result = await service._convert_tool_ids_to_names([99])
        assert result == ["99"]

    @pytest.mark.asyncio
    async def test_unknown_type_returns_string(self, service):
        result = await service._convert_tool_ids_to_names([3.14])
        assert result == ["3.14"]


class TestAgentToDict:
    """Tests for _agent_to_dict."""

    @pytest.fixture
    def service(self):
        return CrewDeploymentService(session=AsyncMock())

    @pytest.mark.asyncio
    async def test_converts_agent_to_dict(self, service):
        mock_agent = MagicMock()
        mock_agent.id = 'a-1'
        mock_agent.name = 'Agent'
        mock_agent.role = 'Researcher'
        mock_agent.goal = 'Research'
        mock_agent.backstory = 'Expert'
        mock_agent.llm = 'gpt-4'
        mock_agent.tools = []
        mock_agent.max_iter = 25
        mock_agent.verbose = False
        mock_agent.allow_delegation = False

        with patch.object(service, '_convert_tool_ids_to_names', new_callable=AsyncMock, return_value=[]):
            result = await service._agent_to_dict(mock_agent)

        assert result['role'] == 'Researcher'
        assert result['id'] == 'a-1'
        assert 'tools' in result


class TestTaskToDict:
    """Tests for _task_to_dict."""

    @pytest.fixture
    def service(self):
        return CrewDeploymentService(session=AsyncMock())

    @pytest.mark.asyncio
    async def test_converts_task_to_dict(self, service):
        mock_task = MagicMock()
        mock_task.id = 't-1'
        mock_task.name = 'Task'
        mock_task.description = 'Do stuff'
        mock_task.expected_output = 'Result'
        mock_task.agent_id = 'a-1'
        mock_task.tools = []
        mock_task.async_execution = False
        mock_task.context = []

        with patch.object(service, '_convert_tool_ids_to_names', new_callable=AsyncMock, return_value=[]):
            result = await service._task_to_dict(mock_task)

        assert result['name'] == 'Task'
        assert result['description'] == 'Do stuff'
        assert 'context' in result


class TestGenerateUsageExample:
    """Tests for _generate_usage_example."""

    @pytest.fixture
    def service(self):
        return CrewDeploymentService(session=AsyncMock())

    def test_returns_string(self, service):
        result = service._generate_usage_example(
            "https://example.com/serving-endpoints/my-ep/invocations",
            "my-ep"
        )
        assert isinstance(result, str)

    def test_contains_endpoint_url(self, service):
        url = "https://example.com/serving-endpoints/test/invocations"
        result = service._generate_usage_example(url, "test")
        assert url in result

    def test_contains_endpoint_name(self, service):
        result = service._generate_usage_example(
            "https://example.com/ep",
            "my-endpoint-name"
        )
        assert "my-endpoint-name" in result


class TestDeployToEndpoint:
    """Tests for _deploy_to_endpoint."""

    @pytest.fixture
    def service(self):
        return CrewDeploymentService(session=AsyncMock())

    @pytest.mark.asyncio
    async def test_creates_new_endpoint(self, service):
        config = ModelServingConfig(
            model_name="model",
            endpoint_name="new-endpoint",
            workload_size="Small",
            scale_to_zero_enabled=True,
        )

        mock_ws = MagicMock()
        # Simulate endpoint not existing on first get, exists on second
        mock_ws.serving_endpoints.get.side_effect = [
            Exception("not found"),
            MagicMock()  # second call after creation
        ]
        mock_ws.config.host = "my-workspace.azuredatabricks.net"

        mock_wc_module = MagicMock()
        mock_wc_module.WorkspaceClient.return_value = mock_ws

        with patch.dict("sys.modules", {
            "databricks.sdk": mock_wc_module,
            "databricks.sdk.service.serving": MagicMock(),
        }):
            url, status = await service._deploy_to_endpoint("model", "1", config)

        assert status == DeploymentStatus.PENDING
        mock_ws.serving_endpoints.create.assert_called_once()

    @pytest.mark.asyncio
    async def test_updates_existing_endpoint(self, service):
        config = ModelServingConfig(
            model_name="model",
            endpoint_name="existing-endpoint",
            workload_size="Small",
            scale_to_zero_enabled=True,
        )

        mock_ws = MagicMock()
        mock_ws.serving_endpoints.get.return_value = MagicMock()
        mock_ws.config.host = "my-workspace.azuredatabricks.net"

        mock_wc_module = MagicMock()
        mock_wc_module.WorkspaceClient.return_value = mock_ws

        with patch.dict("sys.modules", {
            "databricks.sdk": mock_wc_module,
            "databricks.sdk.service.serving": MagicMock(),
        }):
            url, status = await service._deploy_to_endpoint("model", "2", config)

        assert status == DeploymentStatus.UPDATING
        mock_ws.serving_endpoints.update_config.assert_called_once()


class TestDeploymentStatusEnum:
    """Tests for DeploymentStatus enum values."""

    def test_ready(self):
        assert DeploymentStatus.READY == "ready"

    def test_pending(self):
        assert DeploymentStatus.PENDING == "pending"

    def test_failed(self):
        assert DeploymentStatus.FAILED == "failed"

    def test_in_progress(self):
        assert DeploymentStatus.IN_PROGRESS == "in_progress"

    def test_updating(self):
        assert DeploymentStatus.UPDATING == "updating"


class TestCrewAIModelWrapper:
    """Tests for CrewAIModelWrapper."""

    def test_init_stores_config(self):
        config = {"id": "1", "name": "Test Crew", "agents": [], "tasks": []}
        wrapper = CrewAIModelWrapper(config)
        assert wrapper.crew_config is config

    def test_load_context_reads_config(self, tmp_path):
        import json
        config_file = tmp_path / "crew_config.json"
        config_data = {"id": "1", "name": "From File", "agents": [], "tasks": []}
        config_file.write_text(json.dumps(config_data))

        wrapper = CrewAIModelWrapper({})
        mock_context = MagicMock()
        mock_context.artifacts = {"crew_config": str(config_file)}
        wrapper.load_context(mock_context)

        assert wrapper.crew_config["name"] == "From File"

    def test_load_context_no_config_path(self):
        wrapper = CrewAIModelWrapper({"name": "Original"})
        mock_context = MagicMock()
        mock_context.artifacts = {}
        wrapper.load_context(mock_context)
        # Config should remain unchanged
        assert wrapper.crew_config["name"] == "Original"

    def test_predict_handles_exception(self):
        import pandas as pd

        config = {"id": "1", "name": "Test", "agents": [], "tasks": []}
        wrapper = CrewAIModelWrapper(config)

        # Patch _create_crew_from_config to raise
        with patch.object(wrapper, "_create_crew_from_config", side_effect=RuntimeError("crew error")):
            input_df = pd.DataFrame([{"inputs": '{"topic": "AI"}'}])
            result = wrapper.predict(MagicMock(), input_df)

        assert result["status"][0] == "failed"
        assert "crew error" in result["error"][0]
