"""
Comprehensive unit tests for MLflow Pydantic schemas.

Tests all schemas in mlflow.py including validation and serialization.
"""
import pytest
from pydantic import BaseModel, ValidationError

from src.schemas.mlflow import (
    MLflowConfigUpdate, MLflowConfigResponse,
    MLflowEvaluateRequest, MLflowEvaluateResponse
)


class TestMLflowConfigUpdate:
    """Test MLflowConfigUpdate schema."""

    def test_mlflow_config_update_required_field(self):
        """Test MLflowConfigUpdate with required field."""
        config = MLflowConfigUpdate(enabled=True)
        
        assert config.enabled is True

    def test_mlflow_config_update_enabled_false(self):
        """Test MLflowConfigUpdate with enabled=False."""
        config = MLflowConfigUpdate(enabled=False)
        
        assert config.enabled is False

    def test_mlflow_config_update_missing_required_field(self):
        """Test MLflowConfigUpdate validation with missing required field."""
        with pytest.raises(ValidationError) as exc_info:
            MLflowConfigUpdate()
        assert "Field required" in str(exc_info.value)

    def test_mlflow_config_update_invalid_type(self):
        """Test MLflowConfigUpdate validation with invalid type."""
        with pytest.raises(ValidationError) as exc_info:
            MLflowConfigUpdate(enabled="not_a_boolean")
        assert "Input should be a valid boolean" in str(exc_info.value)

    def test_mlflow_config_update_boolean_coercion(self):
        """Test MLflowConfigUpdate boolean coercion."""
        # Test truthy values
        config1 = MLflowConfigUpdate(enabled=1)
        assert config1.enabled is True

        config2 = MLflowConfigUpdate(enabled="true")
        assert config2.enabled is True

        # Test falsy values
        config3 = MLflowConfigUpdate(enabled=0)
        assert config3.enabled is False

        config4 = MLflowConfigUpdate(enabled="false")
        assert config4.enabled is False

    def test_mlflow_config_update_serialization(self):
        """Test MLflowConfigUpdate serialization."""
        config = MLflowConfigUpdate(enabled=True)
        
        serialized = config.model_dump()
        assert serialized == {"enabled": True}

    def test_mlflow_config_update_json_serialization(self):
        """Test MLflowConfigUpdate JSON serialization."""
        config = MLflowConfigUpdate(enabled=True)
        
        json_str = config.model_dump_json()
        assert json_str == '{"enabled":true}'


class TestMLflowConfigResponse:
    """Test MLflowConfigResponse schema."""

    def test_mlflow_config_response_required_field(self):
        """Test MLflowConfigResponse with required field."""
        response = MLflowConfigResponse(enabled=True)
        
        assert response.enabled is True

    def test_mlflow_config_response_enabled_false(self):
        """Test MLflowConfigResponse with enabled=False."""
        response = MLflowConfigResponse(enabled=False)
        
        assert response.enabled is False

    def test_mlflow_config_response_missing_required_field(self):
        """Test MLflowConfigResponse validation with missing required field."""
        with pytest.raises(ValidationError) as exc_info:
            MLflowConfigResponse()
        assert "Field required" in str(exc_info.value)

    def test_mlflow_config_response_invalid_type(self):
        """Test MLflowConfigResponse validation with invalid type."""
        with pytest.raises(ValidationError) as exc_info:
            MLflowConfigResponse(enabled="not_a_boolean")
        assert "Input should be a valid boolean" in str(exc_info.value)

    def test_mlflow_config_response_boolean_coercion(self):
        """Test MLflowConfigResponse boolean coercion."""
        # Test truthy values
        response1 = MLflowConfigResponse(enabled=1)
        assert response1.enabled is True

        response2 = MLflowConfigResponse(enabled="true")
        assert response2.enabled is True

        # Test falsy values
        response3 = MLflowConfigResponse(enabled=0)
        assert response3.enabled is False

        response4 = MLflowConfigResponse(enabled="false")
        assert response4.enabled is False

    def test_mlflow_config_response_serialization(self):
        """Test MLflowConfigResponse serialization."""
        response = MLflowConfigResponse(enabled=True)
        
        serialized = response.model_dump()
        assert serialized == {"enabled": True}

    def test_mlflow_config_response_json_serialization(self):
        """Test MLflowConfigResponse JSON serialization."""
        response = MLflowConfigResponse(enabled=False)
        
        json_str = response.model_dump_json()
        assert json_str == '{"enabled":false}'


class TestMLflowEvaluateRequest:
    """Test MLflowEvaluateRequest schema."""

    def test_mlflow_evaluate_request_required_field(self):
        """Test MLflowEvaluateRequest with required field."""
        request = MLflowEvaluateRequest(job_id="test-job-123")
        
        assert request.job_id == "test-job-123"

    def test_mlflow_evaluate_request_missing_required_field(self):
        """Test MLflowEvaluateRequest validation with missing required field."""
        with pytest.raises(ValidationError) as exc_info:
            MLflowEvaluateRequest()
        assert "Field required" in str(exc_info.value)

    def test_mlflow_evaluate_request_empty_job_id(self):
        """Test MLflowEvaluateRequest with empty job_id."""
        request = MLflowEvaluateRequest(job_id="")
        
        assert request.job_id == ""

    def test_mlflow_evaluate_request_whitespace_job_id(self):
        """Test MLflowEvaluateRequest with whitespace job_id."""
        request = MLflowEvaluateRequest(job_id="   ")
        
        assert request.job_id == "   "

    def test_mlflow_evaluate_request_numeric_job_id(self):
        """Test MLflowEvaluateRequest with numeric job_id."""
        request = MLflowEvaluateRequest(job_id="12345")
        
        assert request.job_id == "12345"

    def test_mlflow_evaluate_request_uuid_job_id(self):
        """Test MLflowEvaluateRequest with UUID job_id."""
        uuid_job_id = "550e8400-e29b-41d4-a716-446655440000"
        request = MLflowEvaluateRequest(job_id=uuid_job_id)
        
        assert request.job_id == uuid_job_id

    def test_mlflow_evaluate_request_serialization(self):
        """Test MLflowEvaluateRequest serialization."""
        request = MLflowEvaluateRequest(job_id="test-job-123")
        
        serialized = request.model_dump()
        assert serialized == {"job_id": "test-job-123"}

    def test_mlflow_evaluate_request_json_serialization(self):
        """Test MLflowEvaluateRequest JSON serialization."""
        request = MLflowEvaluateRequest(job_id="test-job-123")
        
        json_str = request.model_dump_json()
        assert json_str == '{"job_id":"test-job-123"}'


class TestMLflowEvaluateResponse:
    """Test MLflowEvaluateResponse schema."""

    def test_mlflow_evaluate_response_defaults(self):
        """Test MLflowEvaluateResponse with default values."""
        response = MLflowEvaluateResponse()
        
        assert response.experiment_id is None
        assert response.run_id is None
        assert response.experiment_name is None

    def test_mlflow_evaluate_response_all_fields(self):
        """Test MLflowEvaluateResponse with all fields."""
        response = MLflowEvaluateResponse(
            experiment_id="exp-123",
            run_id="run-456",
            experiment_name="test_experiment"
        )
        
        assert response.experiment_id == "exp-123"
        assert response.run_id == "run-456"
        assert response.experiment_name == "test_experiment"

    def test_mlflow_evaluate_response_partial_fields(self):
        """Test MLflowEvaluateResponse with partial fields."""
        response = MLflowEvaluateResponse(experiment_id="exp-123")
        
        assert response.experiment_id == "exp-123"
        assert response.run_id is None
        assert response.experiment_name is None

    def test_mlflow_evaluate_response_empty_strings(self):
        """Test MLflowEvaluateResponse with empty strings."""
        response = MLflowEvaluateResponse(
            experiment_id="",
            run_id="",
            experiment_name=""
        )
        
        assert response.experiment_id == ""
        assert response.run_id == ""
        assert response.experiment_name == ""

    def test_mlflow_evaluate_response_numeric_ids(self):
        """Test MLflowEvaluateResponse with numeric IDs."""
        response = MLflowEvaluateResponse(
            experiment_id="123",
            run_id="456"
        )
        
        assert response.experiment_id == "123"
        assert response.run_id == "456"

    def test_mlflow_evaluate_response_uuid_ids(self):
        """Test MLflowEvaluateResponse with UUID IDs."""
        exp_uuid = "550e8400-e29b-41d4-a716-446655440000"
        run_uuid = "6ba7b810-9dad-11d1-80b4-00c04fd430c8"
        
        response = MLflowEvaluateResponse(
            experiment_id=exp_uuid,
            run_id=run_uuid
        )
        
        assert response.experiment_id == exp_uuid
        assert response.run_id == run_uuid

    def test_mlflow_evaluate_response_serialization_defaults(self):
        """Test MLflowEvaluateResponse serialization with defaults."""
        response = MLflowEvaluateResponse()
        
        serialized = response.model_dump()
        assert serialized == {
            "experiment_id": None,
            "run_id": None,
            "experiment_name": None
        }

    def test_mlflow_evaluate_response_serialization_all_fields(self):
        """Test MLflowEvaluateResponse serialization with all fields."""
        response = MLflowEvaluateResponse(
            experiment_id="exp-123",
            run_id="run-456",
            experiment_name="test_experiment"
        )
        
        serialized = response.model_dump()
        assert serialized == {
            "experiment_id": "exp-123",
            "run_id": "run-456",
            "experiment_name": "test_experiment"
        }

    def test_mlflow_evaluate_response_json_serialization(self):
        """Test MLflowEvaluateResponse JSON serialization."""
        response = MLflowEvaluateResponse(
            experiment_id="exp-123",
            run_id="run-456"
        )
        
        json_str = response.model_dump_json()
        assert '"experiment_id":"exp-123"' in json_str
        assert '"run_id":"run-456"' in json_str
        assert '"experiment_name":null' in json_str

    def test_mlflow_evaluate_response_exclude_none(self):
        """Test MLflowEvaluateResponse serialization excluding None values."""
        response = MLflowEvaluateResponse(experiment_id="exp-123")
        
        serialized = response.model_dump(exclude_none=True)
        assert serialized == {"experiment_id": "exp-123"}


class TestMLflowSchemaInteroperability:
    """Test MLflow schema interoperability and edge cases."""

    def test_config_update_response_compatibility(self):
        """Test MLflowConfigUpdate and MLflowConfigResponse compatibility."""
        update = MLflowConfigUpdate(enabled=True)
        response = MLflowConfigResponse(enabled=update.enabled)
        
        assert update.enabled == response.enabled

    def test_request_response_workflow(self):
        """Test typical request-response workflow."""
        # Create request
        request = MLflowEvaluateRequest(job_id="test-job-123")
        
        # Create response based on request
        response = MLflowEvaluateResponse(
            experiment_id="exp-for-" + request.job_id,
            run_id="run-for-" + request.job_id,
            experiment_name="experiment_" + request.job_id
        )
        
        assert response.experiment_id == "exp-for-test-job-123"
        assert response.run_id == "run-for-test-job-123"
        assert response.experiment_name == "experiment_test-job-123"

    def test_schema_inheritance(self):
        """Test schema inheritance from BaseModel."""
        assert issubclass(MLflowConfigUpdate, BaseModel)
        assert issubclass(MLflowConfigResponse, BaseModel)
        assert issubclass(MLflowEvaluateRequest, BaseModel)
        assert issubclass(MLflowEvaluateResponse, BaseModel)

    def test_schema_field_info(self):
        """Test schema field information."""
        # Test MLflowConfigUpdate fields
        update_fields = MLflowConfigUpdate.model_fields
        assert "enabled" in update_fields
        assert update_fields["enabled"].annotation == bool

        # Test MLflowEvaluateRequest fields
        request_fields = MLflowEvaluateRequest.model_fields
        assert "job_id" in request_fields
        assert request_fields["job_id"].annotation == str

        # Test MLflowEvaluateResponse fields
        response_fields = MLflowEvaluateResponse.model_fields
        assert "experiment_id" in response_fields
        assert "run_id" in response_fields
        assert "experiment_name" in response_fields
