"""
Unit tests for crew export schemas.
"""

import pytest
from pydantic import ValidationError

from src.schemas.crew_export import (
    ExportFormat,
    DeploymentTarget,
    ExportOptions,
    CrewExportRequest,
    ExportFile,
    CrewExportResponse,
    ModelServingConfig,
    DeploymentRequest,
    DeploymentStatus,
    DeploymentResponse,
    DeploymentStatusResponse,
    EndpointInvokeRequest,
    EndpointInvokeResponse,
)


class TestExportFormat:
    """Tests for ExportFormat enum."""

    def test_python_project_value(self):
        """Test PYTHON_PROJECT enum value."""
        assert ExportFormat.PYTHON_PROJECT == "python_project"

    def test_databricks_notebook_value(self):
        """Test DATABRICKS_NOTEBOOK enum value."""
        assert ExportFormat.DATABRICKS_NOTEBOOK == "databricks_notebook"

    def test_export_format_is_string_enum(self):
        """Test that ExportFormat is a string enum."""
        assert isinstance(ExportFormat.PYTHON_PROJECT, str)


class TestDeploymentTarget:
    """Tests for DeploymentTarget enum."""

    def test_databricks_model_serving_value(self):
        """Test DATABRICKS_MODEL_SERVING enum value."""
        assert DeploymentTarget.DATABRICKS_MODEL_SERVING == "databricks_model_serving"

    def test_deployment_target_is_string_enum(self):
        """Test that DeploymentTarget is a string enum."""
        assert isinstance(DeploymentTarget.DATABRICKS_MODEL_SERVING, str)


class TestDeploymentStatus:
    """Tests for DeploymentStatus enum."""

    def test_pending_value(self):
        """Test PENDING enum value."""
        assert DeploymentStatus.PENDING == "pending"

    def test_in_progress_value(self):
        """Test IN_PROGRESS enum value."""
        assert DeploymentStatus.IN_PROGRESS == "in_progress"

    def test_ready_value(self):
        """Test READY enum value."""
        assert DeploymentStatus.READY == "ready"

    def test_failed_value(self):
        """Test FAILED enum value."""
        assert DeploymentStatus.FAILED == "failed"

    def test_updating_value(self):
        """Test UPDATING enum value."""
        assert DeploymentStatus.UPDATING == "updating"


class TestExportOptions:
    """Tests for ExportOptions schema."""

    def test_default_values(self):
        """Test default values for ExportOptions."""
        options = ExportOptions()

        assert options.include_custom_tools is True
        assert options.include_comments is True
        assert options.include_tests is True
        assert options.model_override is None
        assert options.include_memory_config is True
        assert options.include_tracing is True
        assert options.include_evaluation is True
        assert options.include_deployment is True

    def test_custom_values(self):
        """Test custom values for ExportOptions."""
        options = ExportOptions(
            include_custom_tools=False,
            include_comments=False,
            include_tests=False,
            model_override='custom-model',
            include_tracing=False,
        )

        assert options.include_custom_tools is False
        assert options.include_comments is False
        assert options.include_tests is False
        assert options.model_override == 'custom-model'
        assert options.include_tracing is False


class TestCrewExportRequest:
    """Tests for CrewExportRequest schema."""

    def test_required_export_format(self):
        """Test that export_format is required."""
        with pytest.raises(ValidationError):
            CrewExportRequest()

    def test_valid_request(self):
        """Test valid request creation."""
        request = CrewExportRequest(
            export_format=ExportFormat.DATABRICKS_NOTEBOOK
        )

        assert request.export_format == ExportFormat.DATABRICKS_NOTEBOOK
        assert request.options is not None

    def test_request_with_options(self):
        """Test request with custom options."""
        request = CrewExportRequest(
            export_format=ExportFormat.PYTHON_PROJECT,
            options=ExportOptions(include_tests=False)
        )

        assert request.options.include_tests is False


class TestExportFile:
    """Tests for ExportFile schema."""

    def test_required_fields(self):
        """Test that all fields are required."""
        with pytest.raises(ValidationError):
            ExportFile()

    def test_valid_export_file(self):
        """Test valid ExportFile creation."""
        file = ExportFile(
            path='README.md',
            content='# Test',
            type='markdown'
        )

        assert file.path == 'README.md'
        assert file.content == '# Test'
        assert file.type == 'markdown'


class TestCrewExportResponse:
    """Tests for CrewExportResponse schema."""

    def test_required_fields(self):
        """Test that required fields are enforced."""
        with pytest.raises(ValidationError):
            CrewExportResponse()

    def test_valid_response(self):
        """Test valid response creation."""
        response = CrewExportResponse(
            crew_id='test-id',
            crew_name='Test Crew',
            export_format=ExportFormat.PYTHON_PROJECT,
            generated_at='2025-01-01 00:00:00 UTC'
        )

        assert response.crew_id == 'test-id'
        assert response.crew_name == 'Test Crew'
        assert response.export_format == ExportFormat.PYTHON_PROJECT

    def test_response_with_files(self):
        """Test response with files list."""
        files = [
            ExportFile(path='README.md', content='# Test', type='markdown')
        ]

        response = CrewExportResponse(
            crew_id='test-id',
            crew_name='Test',
            export_format=ExportFormat.PYTHON_PROJECT,
            files=files,
            generated_at='2025-01-01 00:00:00 UTC'
        )

        assert len(response.files) == 1

    def test_response_with_notebook(self):
        """Test response with notebook."""
        response = CrewExportResponse(
            crew_id='test-id',
            crew_name='Test',
            export_format=ExportFormat.DATABRICKS_NOTEBOOK,
            notebook={'cells': []},
            notebook_content='{}',
            generated_at='2025-01-01 00:00:00 UTC'
        )

        assert response.notebook is not None
        assert response.notebook_content == '{}'


class TestModelServingConfig:
    """Tests for ModelServingConfig schema."""

    def test_required_model_name(self):
        """Test that model_name is required."""
        with pytest.raises(ValidationError):
            ModelServingConfig()

    def test_default_values(self):
        """Test default values."""
        config = ModelServingConfig(model_name='test-model')

        assert config.model_name == 'test-model'
        assert config.endpoint_name is None
        assert config.workload_size == 'Small'
        assert config.scale_to_zero_enabled is True
        assert config.min_instances == 0
        assert config.max_instances == 1
        assert config.unity_catalog_model is True

    def test_custom_values(self):
        """Test custom values."""
        config = ModelServingConfig(
            model_name='test-model',
            endpoint_name='custom-endpoint',
            workload_size='Large',
            scale_to_zero_enabled=False,
            min_instances=1,
            max_instances=5,
        )

        assert config.endpoint_name == 'custom-endpoint'
        assert config.workload_size == 'Large'
        assert config.scale_to_zero_enabled is False
        assert config.min_instances == 1
        assert config.max_instances == 5

    def test_unity_catalog_config(self):
        """Test Unity Catalog configuration."""
        config = ModelServingConfig(
            model_name='test-model',
            unity_catalog_model=True,
            catalog_name='main',
            schema_name='agents',
        )

        assert config.unity_catalog_model is True
        assert config.catalog_name == 'main'
        assert config.schema_name == 'agents'

    def test_environment_vars(self):
        """Test environment variables."""
        config = ModelServingConfig(
            model_name='test-model',
            environment_vars={'API_KEY': 'secret'}
        )

        assert config.environment_vars == {'API_KEY': 'secret'}

    def test_tags(self):
        """Test tags."""
        config = ModelServingConfig(
            model_name='test-model',
            tags={'team': 'ml', 'version': '1.0'}
        )

        assert config.tags == {'team': 'ml', 'version': '1.0'}


class TestDeploymentRequest:
    """Tests for DeploymentRequest schema."""

    def test_required_fields(self):
        """Test that required fields are enforced."""
        with pytest.raises(ValidationError):
            DeploymentRequest()

    def test_valid_request(self):
        """Test valid request creation."""
        request = DeploymentRequest(
            deployment_target=DeploymentTarget.DATABRICKS_MODEL_SERVING,
            config=ModelServingConfig(model_name='test-model')
        )

        assert request.deployment_target == DeploymentTarget.DATABRICKS_MODEL_SERVING
        assert request.config.model_name == 'test-model'


class TestDeploymentResponse:
    """Tests for DeploymentResponse schema."""

    def test_required_fields(self):
        """Test that required fields are enforced."""
        with pytest.raises(ValidationError):
            DeploymentResponse()

    def test_valid_response(self):
        """Test valid response creation."""
        response = DeploymentResponse(
            crew_id='test-id',
            crew_name='Test Crew',
            deployment_target=DeploymentTarget.DATABRICKS_MODEL_SERVING,
            model_name='test-model',
            endpoint_name='test-endpoint',
            endpoint_status=DeploymentStatus.PENDING,
        )

        assert response.crew_id == 'test-id'
        assert response.model_name == 'test-model'
        assert response.endpoint_status == DeploymentStatus.PENDING

    def test_response_with_optional_fields(self):
        """Test response with optional fields."""
        response = DeploymentResponse(
            crew_id='test-id',
            crew_name='Test',
            deployment_target=DeploymentTarget.DATABRICKS_MODEL_SERVING,
            model_name='test-model',
            model_version='1',
            model_uri='models:/test-model/1',
            endpoint_name='test-endpoint',
            endpoint_url='https://example.com/endpoint',
            endpoint_status=DeploymentStatus.READY,
            deployment_id='deploy-123',
            deployed_at='2025-01-01 00:00:00 UTC',
            usage_example='curl ...',
        )

        assert response.model_version == '1'
        assert response.endpoint_url == 'https://example.com/endpoint'


class TestDeploymentStatusResponse:
    """Tests for DeploymentStatusResponse schema."""

    def test_required_fields(self):
        """Test that required fields are enforced."""
        with pytest.raises(ValidationError):
            DeploymentStatusResponse()

    def test_valid_response(self):
        """Test valid response creation."""
        response = DeploymentStatusResponse(
            deployment_id='deploy-123',
            endpoint_name='test-endpoint',
            status=DeploymentStatus.READY,
        )

        assert response.deployment_id == 'deploy-123'
        assert response.status == DeploymentStatus.READY

    def test_response_with_replicas(self):
        """Test response with replica counts."""
        response = DeploymentStatusResponse(
            deployment_id='deploy-123',
            endpoint_name='test-endpoint',
            status=DeploymentStatus.IN_PROGRESS,
            ready_replicas=1,
            target_replicas=3,
        )

        assert response.ready_replicas == 1
        assert response.target_replicas == 3


class TestEndpointInvokeRequest:
    """Tests for EndpointInvokeRequest schema."""

    def test_required_inputs(self):
        """Test that inputs is required."""
        with pytest.raises(ValidationError):
            EndpointInvokeRequest()

    def test_valid_request(self):
        """Test valid request creation."""
        request = EndpointInvokeRequest(
            inputs={'topic': 'AI trends'}
        )

        assert request.inputs == {'topic': 'AI trends'}
        assert request.stream is False
        assert request.timeout is None

    def test_request_with_options(self):
        """Test request with streaming and timeout."""
        request = EndpointInvokeRequest(
            inputs={'topic': 'AI'},
            stream=True,
            timeout=60,
        )

        assert request.stream is True
        assert request.timeout == 60


class TestEndpointInvokeResponse:
    """Tests for EndpointInvokeResponse schema."""

    def test_required_result(self):
        """Test that result is required."""
        with pytest.raises(ValidationError):
            EndpointInvokeResponse()

    def test_valid_response(self):
        """Test valid response creation."""
        response = EndpointInvokeResponse(
            result='Analysis complete'
        )

        assert response.result == 'Analysis complete'

    def test_response_with_metadata(self):
        """Test response with metadata."""
        response = EndpointInvokeResponse(
            result='Done',
            execution_time_seconds=12.5,
            tokens_used=1500,
            task_outputs=[
                {'task': 'research', 'output': 'Research done'}
            ],
            metadata={'model': 'llama'}
        )

        assert response.execution_time_seconds == 12.5
        assert response.tokens_used == 1500
        assert len(response.task_outputs) == 1
