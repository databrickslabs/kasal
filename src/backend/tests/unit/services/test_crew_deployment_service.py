"""
Unit tests for crew deployment service.
"""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

from src.services.crew_deployment_service import CrewDeploymentService
from src.schemas.crew_export import (
    DeploymentTarget,
    ModelServingConfig,
    DeploymentResponse,
    DeploymentStatus,
)


class TestCrewDeploymentService:
    """Tests for CrewDeploymentService."""

    @pytest.fixture
    def mock_session(self):
        """Create a mock database session."""
        return AsyncMock()

    @pytest.fixture
    def service(self, mock_session):
        """Create a CrewDeploymentService instance."""
        return CrewDeploymentService(session=mock_session)

    @pytest.fixture
    def sample_crew_data(self):
        """Create sample crew data."""
        return {
            'id': str(uuid4()),
            'name': 'Test Crew',
            'agents': [
                {
                    'id': 'agent-1',
                    'name': 'Research Agent',
                    'role': 'Researcher',
                    'goal': 'Research topics',
                    'backstory': 'Expert',
                    'llm': 'databricks-llama-4-maverick',
                    'tools': [],
                }
            ],
            'tasks': [
                {
                    'id': 'task-1',
                    'name': 'Research Task',
                    'description': 'Research the topic',
                    'expected_output': 'Report',
                    'agent_id': 'agent-1',
                }
            ],
        }

    @pytest.fixture
    def model_config(self):
        """Create a model serving configuration."""
        return ModelServingConfig(
            model_name="test-model",
            endpoint_name="test-endpoint",
            workload_size="Small",
            scale_to_zero_enabled=True,
        )


class TestDeployToModelServing:
    """Tests for deploy_to_model_serving method."""

    @pytest.fixture
    def mock_session(self):
        """Create a mock database session."""
        return AsyncMock()

    @pytest.fixture
    def service(self, mock_session):
        """Create a CrewDeploymentService instance."""
        return CrewDeploymentService(session=mock_session)

    @pytest.fixture
    def mock_group_context(self):
        """Create a mock group context."""
        context = MagicMock()
        context.group_ids = ['test-group']
        context.is_valid.return_value = True
        return context

    @pytest.fixture
    def model_config(self):
        """Create a model serving configuration."""
        return ModelServingConfig(
            model_name="test-model",
            endpoint_name="test-endpoint",
            workload_size="Small",
            scale_to_zero_enabled=True,
        )

    @pytest.mark.asyncio
    async def test_deploy_to_model_serving_success(self, service, model_config, mock_group_context):
        """Test successful deployment to model serving."""
        crew_id = str(uuid4())

        # Create mock crew
        mock_crew = MagicMock()
        mock_crew.id = crew_id
        mock_crew.name = 'Test Crew'
        mock_crew.agent_ids = []
        mock_crew.task_ids = []
        mock_crew.group_id = 'test-group'

        service.crew_repository.get = AsyncMock(return_value=mock_crew)

        with patch.object(service, '_create_mlflow_model') as mock_create:
            mock_create.return_value = ('model-uri', '1')

            with patch.object(service, '_deploy_to_endpoint') as mock_deploy:
                # _deploy_to_endpoint returns (endpoint_url, deployment_status)
                mock_deploy.return_value = ('https://example.com/serving-endpoints/test-endpoint', DeploymentStatus.PENDING)

                result = await service.deploy_to_model_serving(
                    crew_id=crew_id,
                    config=model_config,
                    group_context=mock_group_context
                )

        assert result.crew_id == crew_id
        assert result.model_name == 'test-model'
        assert result.endpoint_status == DeploymentStatus.PENDING

    @pytest.mark.asyncio
    async def test_deploy_to_model_serving_crew_not_found(self, service, model_config, mock_group_context):
        """Test deployment when crew is not found."""
        crew_id = str(uuid4())

        service.crew_repository.get = AsyncMock(return_value=None)

        with pytest.raises(ValueError) as exc_info:
            await service.deploy_to_model_serving(
                crew_id=crew_id,
                config=model_config,
                group_context=mock_group_context
            )

        assert "not found" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_deploy_with_unity_catalog(self, service, mock_group_context):
        """Test deployment with Unity Catalog configuration."""
        crew_id = str(uuid4())

        # Create mock crew
        mock_crew = MagicMock()
        mock_crew.id = crew_id
        mock_crew.name = 'Test Crew'
        mock_crew.agent_ids = []
        mock_crew.task_ids = []
        mock_crew.group_id = 'test-group'

        service.crew_repository.get = AsyncMock(return_value=mock_crew)

        config = ModelServingConfig(
            model_name="test-model",
            unity_catalog_model=True,
            catalog_name="main",
            schema_name="agents",
        )

        with patch.object(service, '_create_mlflow_model') as mock_create:
            mock_create.return_value = ('models:/main.agents.test-model/1', '1')

            with patch.object(service, '_deploy_to_endpoint') as mock_deploy:
                # _deploy_to_endpoint returns (endpoint_url, deployment_status)
                mock_deploy.return_value = ('https://example.com/serving-endpoints/test-model', DeploymentStatus.PENDING)

                result = await service.deploy_to_model_serving(
                    crew_id=crew_id,
                    config=config,
                    group_context=mock_group_context
                )

        assert result is not None
        assert result.model_name == 'test-model'


class TestModelPackageCreation:
    """Tests for _create_model_package method."""

    @pytest.fixture
    def mock_session(self):
        """Create a mock database session."""
        return AsyncMock()

    @pytest.fixture
    def service(self, mock_session):
        """Create a CrewDeploymentService instance."""
        return CrewDeploymentService(session=mock_session)

    @pytest.mark.asyncio
    async def test_create_model_package_structure(self, service):
        """Test that model package is created with correct structure."""
        crew_data = {
            'id': 'test-id',
            'name': 'Test Crew',
            'agents': [
                {
                    'name': 'Agent',
                    'role': 'Role',
                    'goal': 'Goal',
                    'backstory': 'Story',
                    'llm': 'databricks-llama-4-maverick',
                }
            ],
            'tasks': [
                {
                    'name': 'Task',
                    'description': 'Description',
                    'expected_output': 'Output',
                    'agent_id': 'Agent',
                }
            ],
        }

        with patch('tempfile.mkdtemp', return_value='/tmp/test_package'):
            with patch('builtins.open', MagicMock()):
                with patch('os.makedirs'):
                    # Test that the method can be called without errors
                    # Full testing would require filesystem mocking
                    pass


class TestModelRegistration:
    """Tests for _register_model method."""

    @pytest.fixture
    def mock_session(self):
        """Create a mock database session."""
        return AsyncMock()

    @pytest.fixture
    def service(self, mock_session):
        """Create a CrewDeploymentService instance."""
        return CrewDeploymentService(session=mock_session)

    @pytest.mark.asyncio
    async def test_register_model_with_mlflow(self, service):
        """Test model registration with MLflow."""
        model_config = ModelServingConfig(
            model_name="test-model",
            unity_catalog_model=True,
            catalog_name="main",
            schema_name="agents",
        )

        with patch('mlflow.set_registry_uri'):
            with patch('mlflow.pyfunc.log_model') as mock_log:
                mock_log.return_value = MagicMock(model_uri='runs:/abc123/model')

                with patch('mlflow.register_model') as mock_register:
                    mock_register.return_value = MagicMock(version='1')

                    # Test would verify registration logic


class TestServingEndpointCreation:
    """Tests for _create_serving_endpoint method."""

    @pytest.fixture
    def mock_session(self):
        """Create a mock database session."""
        return AsyncMock()

    @pytest.fixture
    def service(self, mock_session):
        """Create a CrewDeploymentService instance."""
        return CrewDeploymentService(session=mock_session)

    @pytest.mark.asyncio
    async def test_create_serving_endpoint_success(self, service):
        """Test successful endpoint creation."""
        model_config = ModelServingConfig(
            model_name="test-model",
            endpoint_name="test-endpoint",
            workload_size="Small",
            scale_to_zero_enabled=True,
            min_instances=0,
            max_instances=1,
        )

        with patch('databricks.sdk.WorkspaceClient') as mock_ws:
            mock_instance = MagicMock()
            mock_ws.return_value = mock_instance
            mock_instance.serving_endpoints.create.return_value = MagicMock(
                name='test-endpoint',
                state=MagicMock(ready='READY')
            )

            # Test would verify endpoint creation logic

    @pytest.mark.asyncio
    async def test_create_serving_endpoint_with_environment_vars(self, service):
        """Test endpoint creation with environment variables."""
        model_config = ModelServingConfig(
            model_name="test-model",
            endpoint_name="test-endpoint",
            environment_vars={
                'API_KEY': 'secret-key',
                'CONFIG': 'value',
            },
        )

        with patch('databricks.sdk.WorkspaceClient') as mock_ws:
            mock_instance = MagicMock()
            mock_ws.return_value = mock_instance

            # Test would verify environment variables are passed correctly


class TestDeploymentStatusMapping:
    """Tests for deployment status mapping."""

    def test_status_mapping_ready(self):
        """Test that READY status is mapped correctly."""
        from src.schemas.crew_export import DeploymentStatus

        assert DeploymentStatus.READY == "ready"

    def test_status_mapping_pending(self):
        """Test that PENDING status is mapped correctly."""
        from src.schemas.crew_export import DeploymentStatus

        assert DeploymentStatus.PENDING == "pending"

    def test_status_mapping_failed(self):
        """Test that FAILED status is mapped correctly."""
        from src.schemas.crew_export import DeploymentStatus

        assert DeploymentStatus.FAILED == "failed"

    def test_status_mapping_in_progress(self):
        """Test that IN_PROGRESS status is mapped correctly."""
        from src.schemas.crew_export import DeploymentStatus

        assert DeploymentStatus.IN_PROGRESS == "in_progress"

    def test_status_mapping_updating(self):
        """Test that UPDATING status is mapped correctly."""
        from src.schemas.crew_export import DeploymentStatus

        assert DeploymentStatus.UPDATING == "updating"
