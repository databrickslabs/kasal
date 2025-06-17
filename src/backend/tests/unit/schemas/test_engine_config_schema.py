"""
Unit tests for engine_config schemas.

Tests the functionality of Pydantic schemas for engine configuration operations
including validation, serialization, and field constraints.
"""
import pytest
from datetime import datetime
from pydantic import ValidationError
from typing import List

from src.schemas.engine_config import (
    EngineConfigBase, EngineConfigCreate, EngineConfigUpdate, EngineConfigResponse,
    EngineConfigToggleUpdate, EngineConfigValueUpdate, EngineConfigListResponse,
    CrewAIFlowConfigUpdate
)


class TestEngineConfigBase:
    """Test cases for EngineConfigBase schema."""
    
    def test_valid_engine_config_base_minimal(self):
        """Test EngineConfigBase with minimal required fields."""
        config_data = {
            "engine_name": "crewai",
            "engine_type": "workflow",
            "config_key": "flow_enabled",
            "config_value": "true"
        }
        config = EngineConfigBase(**config_data)
        assert config.engine_name == "crewai"
        assert config.engine_type == "workflow"
        assert config.config_key == "flow_enabled"
        assert config.config_value == "true"
        assert config.enabled is True  # Default value
        assert config.description is None
    
    def test_valid_engine_config_base_full(self):
        """Test EngineConfigBase with all fields."""
        config_data = {
            "engine_name": "databricks",
            "engine_type": "ai",
            "config_key": "model_endpoint",
            "config_value": "databricks-llama-4-maverick",
            "enabled": False,
            "description": "Configuration for Databricks LLM model endpoint"
        }
        config = EngineConfigBase(**config_data)
        assert config.engine_name == "databricks"
        assert config.engine_type == "ai"
        assert config.config_key == "model_endpoint"
        assert config.config_value == "databricks-llama-4-maverick"
        assert config.enabled is False
        assert config.description == "Configuration for Databricks LLM model endpoint"
    
    def test_engine_config_base_missing_required_fields(self):
        """Test EngineConfigBase validation with missing required fields."""
        with pytest.raises(ValidationError) as exc_info:
            EngineConfigBase(
                engine_name="test",
                engine_type="workflow"
            )
        
        errors = exc_info.value.errors()
        missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
        assert "config_key" in missing_fields
        assert "config_value" in missing_fields
    
    def test_engine_config_base_empty_strings(self):
        """Test EngineConfigBase with empty strings."""
        config_data = {
            "engine_name": "",
            "engine_type": "",
            "config_key": "",
            "config_value": ""
        }
        config = EngineConfigBase(**config_data)
        assert config.engine_name == ""
        assert config.engine_type == ""
        assert config.config_key == ""
        assert config.config_value == ""
    
    def test_engine_config_base_various_engines(self):
        """Test EngineConfigBase with various engine configurations."""
        engine_configs = [
            {
                "engine_name": "crewai",
                "engine_type": "workflow",
                "config_key": "parallel_execution",
                "config_value": "true",
                "description": "Enable parallel task execution"
            },
            {
                "engine_name": "langchain",
                "engine_type": "ai",
                "config_key": "temperature",
                "config_value": "0.7",
                "description": "Model temperature setting"
            },
            {
                "engine_name": "pandas",
                "engine_type": "processing",
                "config_key": "max_rows",
                "config_value": "10000",
                "description": "Maximum rows to process"
            },
            {
                "engine_name": "postgresql",
                "engine_type": "database",
                "config_key": "connection_pool_size",
                "config_value": "20",
                "description": "Database connection pool size"
            }
        ]
        
        for engine_config in engine_configs:
            config = EngineConfigBase(**engine_config)
            assert config.engine_name == engine_config["engine_name"]
            assert config.engine_type == engine_config["engine_type"]
            assert config.config_key == engine_config["config_key"]
            assert config.config_value == engine_config["config_value"]
    
    def test_engine_config_base_json_config_values(self):
        """Test EngineConfigBase with JSON string config values."""
        json_configs = [
            {
                "config_key": "model_params",
                "config_value": '{"temperature": 0.7, "max_tokens": 1000}'
            },
            {
                "config_key": "database_settings",
                "config_value": '{"host": "localhost", "port": 5432, "ssl": true}'
            },
            {
                "config_key": "feature_flags",
                "config_value": '["experimental_mode", "beta_features", "debug_logging"]'
            },
            {
                "config_key": "nested_config",
                "config_value": '{"api": {"timeout": 30, "retries": 3}, "cache": {"ttl": 3600}}'
            }
        ]
        
        for json_config in json_configs:
            config_data = {
                "engine_name": "test_engine",
                "engine_type": "test",
                "config_key": json_config["config_key"],
                "config_value": json_config["config_value"]
            }
            config = EngineConfigBase(**config_data)
            assert config.config_value == json_config["config_value"]
    
    def test_engine_config_base_boolean_conversion(self):
        """Test EngineConfigBase boolean field conversion."""
        config_data = {
            "engine_name": "test",
            "engine_type": "test",
            "config_key": "test_key",
            "config_value": "test_value",
            "enabled": "true"
        }
        config = EngineConfigBase(**config_data)
        assert config.enabled is True
        
        config_data["enabled"] = 0
        config = EngineConfigBase(**config_data)
        assert config.enabled is False
        
        config_data["enabled"] = 1
        config = EngineConfigBase(**config_data)
        assert config.enabled is True


class TestEngineConfigCreate:
    """Test cases for EngineConfigCreate schema."""
    
    def test_engine_config_create_inheritance(self):
        """Test that EngineConfigCreate inherits from EngineConfigBase."""
        create_data = {
            "engine_name": "new_engine",
            "engine_type": "new_type",
            "config_key": "new_config",
            "config_value": "new_value",
            "description": "New engine configuration"
        }
        create_config = EngineConfigCreate(**create_data)
        
        # Should have all base class attributes
        assert hasattr(create_config, 'engine_name')
        assert hasattr(create_config, 'engine_type')
        assert hasattr(create_config, 'config_key')
        assert hasattr(create_config, 'config_value')
        assert hasattr(create_config, 'enabled')
        assert hasattr(create_config, 'description')
        
        # Should behave like base class
        assert create_config.engine_name == "new_engine"
        assert create_config.engine_type == "new_type"
        assert create_config.config_key == "new_config"
        assert create_config.config_value == "new_value"
        assert create_config.enabled is True  # Default
        assert create_config.description == "New engine configuration"
    
    def test_engine_config_create_common_scenarios(self):
        """Test EngineConfigCreate with common configuration scenarios."""
        scenarios = [
            {
                "name": "crewai_flow_enable",
                "data": {
                    "engine_name": "crewai",
                    "engine_type": "workflow",
                    "config_key": "flow_enabled",
                    "config_value": "true",
                    "description": "Enable CrewAI flow functionality"
                }
            },
            {
                "name": "model_configuration",
                "data": {
                    "engine_name": "databricks",
                    "engine_type": "ai",
                    "config_key": "default_model",
                    "config_value": "databricks-llama-4-maverick",
                    "enabled": True,
                    "description": "Default AI model for Databricks"
                }
            },
            {
                "name": "processing_limits",
                "data": {
                    "engine_name": "data_processor",
                    "engine_type": "processing",
                    "config_key": "max_concurrent_jobs",
                    "config_value": "5",
                    "enabled": False,
                    "description": "Maximum concurrent processing jobs"
                }
            }
        ]
        
        for scenario in scenarios:
            create_config = EngineConfigCreate(**scenario["data"])
            assert create_config.engine_name == scenario["data"]["engine_name"]
            assert create_config.config_key == scenario["data"]["config_key"]


class TestEngineConfigUpdate:
    """Test cases for EngineConfigUpdate schema."""
    
    def test_engine_config_update_all_optional(self):
        """Test that all EngineConfigUpdate fields are optional."""
        update = EngineConfigUpdate()
        assert update.engine_type is None
        assert update.config_key is None
        assert update.config_value is None
        assert update.enabled is None
        assert update.description is None
    
    def test_engine_config_update_partial(self):
        """Test EngineConfigUpdate with partial fields."""
        update_data = {
            "config_value": "updated_value",
            "enabled": False
        }
        update = EngineConfigUpdate(**update_data)
        assert update.config_value == "updated_value"
        assert update.enabled is False
        assert update.engine_type is None
        assert update.config_key is None
        assert update.description is None
    
    def test_engine_config_update_full(self):
        """Test EngineConfigUpdate with all fields."""
        update_data = {
            "engine_type": "updated_ai",
            "config_key": "updated_model",
            "config_value": "new_model_endpoint",
            "enabled": True,
            "description": "Updated model configuration"
        }
        update = EngineConfigUpdate(**update_data)
        assert update.engine_type == "updated_ai"
        assert update.config_key == "updated_model"
        assert update.config_value == "new_model_endpoint"
        assert update.enabled is True
        assert update.description == "Updated model configuration"
    
    def test_engine_config_update_scenarios(self):
        """Test EngineConfigUpdate with different update scenarios."""
        scenarios = [
            {
                "name": "enable_only",
                "data": {"enabled": True},
                "description": "Enable a disabled configuration"
            },
            {
                "name": "value_only",
                "data": {"config_value": "new_endpoint_url"},
                "description": "Update configuration value only"
            },
            {
                "name": "description_only",
                "data": {"description": "Updated description"},
                "description": "Update description only"
            },
            {
                "name": "type_and_key",
                "data": {"engine_type": "ai_v2", "config_key": "model_v2"},
                "description": "Update type and key together"
            }
        ]
        
        for scenario in scenarios:
            update = EngineConfigUpdate(**scenario["data"])
            for key, value in scenario["data"].items():
                assert getattr(update, key) == value


class TestEngineConfigResponse:
    """Test cases for EngineConfigResponse schema."""
    
    def test_valid_engine_config_response(self):
        """Test EngineConfigResponse with all required fields."""
        now = datetime.now()
        response_data = {
            "id": 1,
            "engine_name": "crewai",
            "engine_type": "workflow",
            "config_key": "flow_enabled",
            "config_value": "true",
            "enabled": True,
            "description": "CrewAI flow configuration",
            "created_at": now,
            "updated_at": now
        }
        response = EngineConfigResponse(**response_data)
        assert response.id == 1
        assert response.engine_name == "crewai"
        assert response.engine_type == "workflow"
        assert response.config_key == "flow_enabled"
        assert response.config_value == "true"
        assert response.enabled is True
        assert response.description == "CrewAI flow configuration"
        assert response.created_at == now
        assert response.updated_at == now
    
    def test_engine_config_response_inheritance(self):
        """Test that EngineConfigResponse inherits from EngineConfigBase."""
        now = datetime.now()
        response_data = {
            "id": 2,
            "engine_name": "test_engine",
            "engine_type": "test",
            "config_key": "test_key",
            "config_value": "test_value",
            "created_at": now,
            "updated_at": now
        }
        response = EngineConfigResponse(**response_data)
        
        # Should have all base class attributes
        assert hasattr(response, 'engine_name')
        assert hasattr(response, 'engine_type')
        assert hasattr(response, 'config_key')
        assert hasattr(response, 'config_value')
        assert hasattr(response, 'enabled')
        assert hasattr(response, 'description')
        
        # Should have response-specific attributes
        assert hasattr(response, 'id')
        assert hasattr(response, 'created_at')
        assert hasattr(response, 'updated_at')
        
        # Should behave like base class with defaults
        assert response.enabled is True  # Default from base
        assert response.description is None  # Default from base
    
    def test_engine_config_response_missing_fields(self):
        """Test EngineConfigResponse validation with missing fields."""
        with pytest.raises(ValidationError) as exc_info:
            EngineConfigResponse(
                engine_name="test",
                engine_type="test",
                config_key="test_key",
                config_value="test_value"
            )
        
        errors = exc_info.value.errors()
        missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
        required_fields = {"id", "created_at", "updated_at"}
        assert required_fields.intersection(set(missing_fields)) == required_fields
    
    def test_engine_config_response_config(self):
        """Test EngineConfigResponse Config class."""
        assert hasattr(EngineConfigResponse, 'model_config')
        assert EngineConfigResponse.model_config.get('from_attributes') is True
    
    def test_engine_config_response_id_conversion(self):
        """Test EngineConfigResponse with different ID types."""
        now = datetime.now()
        response_data = {
            "id": "123",  # String that can be converted to int
            "engine_name": "test",
            "engine_type": "test",
            "config_key": "test_key",
            "config_value": "test_value",
            "created_at": now,
            "updated_at": now
        }
        response = EngineConfigResponse(**response_data)
        assert response.id == 123
        assert isinstance(response.id, int)
    
    def test_engine_config_response_datetime_conversion(self):
        """Test EngineConfigResponse with datetime string conversion."""
        response_data = {
            "id": 3,
            "engine_name": "datetime_test",
            "engine_type": "test",
            "config_key": "test_key",
            "config_value": "test_value",
            "created_at": "2023-01-01T12:00:00",
            "updated_at": "2023-01-01T12:30:00"
        }
        response = EngineConfigResponse(**response_data)
        assert isinstance(response.created_at, datetime)
        assert isinstance(response.updated_at, datetime)


class TestEngineConfigToggleUpdate:
    """Test cases for EngineConfigToggleUpdate schema."""
    
    def test_valid_engine_config_toggle_update(self):
        """Test EngineConfigToggleUpdate with valid data."""
        toggle_data = {"enabled": True}
        toggle = EngineConfigToggleUpdate(**toggle_data)
        assert toggle.enabled is True
        
        toggle_data = {"enabled": False}
        toggle = EngineConfigToggleUpdate(**toggle_data)
        assert toggle.enabled is False
    
    def test_engine_config_toggle_update_missing_field(self):
        """Test EngineConfigToggleUpdate validation with missing enabled field."""
        with pytest.raises(ValidationError) as exc_info:
            EngineConfigToggleUpdate()
        
        errors = exc_info.value.errors()
        missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
        assert "enabled" in missing_fields
    
    def test_engine_config_toggle_update_boolean_conversion(self):
        """Test EngineConfigToggleUpdate boolean field conversion."""
        toggle_data = {"enabled": "true"}
        toggle = EngineConfigToggleUpdate(**toggle_data)
        assert toggle.enabled is True
        
        toggle_data = {"enabled": 0}
        toggle = EngineConfigToggleUpdate(**toggle_data)
        assert toggle.enabled is False
        
        toggle_data = {"enabled": 1}
        toggle = EngineConfigToggleUpdate(**toggle_data)
        assert toggle.enabled is True


class TestEngineConfigValueUpdate:
    """Test cases for EngineConfigValueUpdate schema."""
    
    def test_valid_engine_config_value_update(self):
        """Test EngineConfigValueUpdate with valid data."""
        value_data = {"config_value": "new_configuration_value"}
        value_update = EngineConfigValueUpdate(**value_data)
        assert value_update.config_value == "new_configuration_value"
    
    def test_engine_config_value_update_missing_field(self):
        """Test EngineConfigValueUpdate validation with missing config_value field."""
        with pytest.raises(ValidationError) as exc_info:
            EngineConfigValueUpdate()
        
        errors = exc_info.value.errors()
        missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
        assert "config_value" in missing_fields
    
    def test_engine_config_value_update_empty_string(self):
        """Test EngineConfigValueUpdate with empty string."""
        value_data = {"config_value": ""}
        value_update = EngineConfigValueUpdate(**value_data)
        assert value_update.config_value == ""
    
    def test_engine_config_value_update_json_values(self):
        """Test EngineConfigValueUpdate with JSON string values."""
        json_values = [
            '{"temperature": 0.8, "max_tokens": 500}',
            '["feature_a", "feature_b", "feature_c"]',
            '{"nested": {"key": "value", "number": 42}}',
            'simple_string_value',
            '12345',
            'true'
        ]
        
        for json_value in json_values:
            value_data = {"config_value": json_value}
            value_update = EngineConfigValueUpdate(**value_data)
            assert value_update.config_value == json_value


class TestEngineConfigListResponse:
    """Test cases for EngineConfigListResponse schema."""
    
    def test_valid_engine_config_list_response(self):
        """Test EngineConfigListResponse with configs."""
        now = datetime.now()
        configs = [
            EngineConfigResponse(
                id=1,
                engine_name="crewai",
                engine_type="workflow",
                config_key="flow_enabled",
                config_value="true",
                created_at=now,
                updated_at=now
            ),
            EngineConfigResponse(
                id=2,
                engine_name="databricks",
                engine_type="ai",
                config_key="model_endpoint",
                config_value="databricks-llama-4-maverick",
                enabled=False,
                created_at=now,
                updated_at=now
            )
        ]
        
        list_response_data = {
            "configs": configs,
            "count": 2
        }
        list_response = EngineConfigListResponse(**list_response_data)
        assert len(list_response.configs) == 2
        assert list_response.count == 2
        assert list_response.configs[0].id == 1
        assert list_response.configs[1].id == 2
        assert list_response.configs[0].enabled is True
        assert list_response.configs[1].enabled is False
    
    def test_engine_config_list_response_empty(self):
        """Test EngineConfigListResponse with empty config list."""
        list_response_data = {
            "configs": [],
            "count": 0
        }
        list_response = EngineConfigListResponse(**list_response_data)
        assert len(list_response.configs) == 0
        assert list_response.count == 0
    
    def test_engine_config_list_response_count_mismatch(self):
        """Test EngineConfigListResponse with mismatched count and list length."""
        now = datetime.now()
        configs = [
            EngineConfigResponse(
                id=1,
                engine_name="single_config",
                engine_type="test",
                config_key="test_key",
                config_value="test_value",
                created_at=now,
                updated_at=now
            )
        ]
        
        # Count represents total available, not just current page
        list_response_data = {
            "configs": configs,
            "count": 50
        }
        list_response = EngineConfigListResponse(**list_response_data)
        assert len(list_response.configs) == 1
        assert list_response.count == 50
    
    def test_engine_config_list_response_missing_fields(self):
        """Test EngineConfigListResponse validation with missing fields."""
        with pytest.raises(ValidationError) as exc_info:
            EngineConfigListResponse(configs=[])
        
        errors = exc_info.value.errors()
        missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
        assert "count" in missing_fields
        
        with pytest.raises(ValidationError) as exc_info:
            EngineConfigListResponse(count=0)
        
        errors = exc_info.value.errors()
        missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
        assert "configs" in missing_fields


class TestCrewAIFlowConfigUpdate:
    """Test cases for CrewAIFlowConfigUpdate schema."""
    
    def test_valid_crewai_flow_config_update(self):
        """Test CrewAIFlowConfigUpdate with valid data."""
        flow_data = {"flow_enabled": True}
        flow_update = CrewAIFlowConfigUpdate(**flow_data)
        assert flow_update.flow_enabled is True
        
        flow_data = {"flow_enabled": False}
        flow_update = CrewAIFlowConfigUpdate(**flow_data)
        assert flow_update.flow_enabled is False
    
    def test_crewai_flow_config_update_missing_field(self):
        """Test CrewAIFlowConfigUpdate validation with missing flow_enabled field."""
        with pytest.raises(ValidationError) as exc_info:
            CrewAIFlowConfigUpdate()
        
        errors = exc_info.value.errors()
        missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
        assert "flow_enabled" in missing_fields
    
    def test_crewai_flow_config_update_boolean_conversion(self):
        """Test CrewAIFlowConfigUpdate boolean field conversion."""
        flow_data = {"flow_enabled": "true"}
        flow_update = CrewAIFlowConfigUpdate(**flow_data)
        assert flow_update.flow_enabled is True
        
        flow_data = {"flow_enabled": 0}
        flow_update = CrewAIFlowConfigUpdate(**flow_data)
        assert flow_update.flow_enabled is False
        
        flow_data = {"flow_enabled": 1}
        flow_update = CrewAIFlowConfigUpdate(**flow_data)
        assert flow_update.flow_enabled is True


class TestSchemaIntegration:
    """Integration tests for engine_config schema interactions."""
    
    def test_engine_config_lifecycle(self):
        """Test complete engine configuration lifecycle."""
        # Create config
        create_data = {
            "engine_name": "crewai",
            "engine_type": "workflow",
            "config_key": "flow_enabled",
            "config_value": "false",
            "description": "CrewAI flow feature toggle"
        }
        create_config = EngineConfigCreate(**create_data)
        
        # Update config value
        value_update = EngineConfigValueUpdate(config_value="true")
        
        # Toggle config enabled status
        toggle_update = EngineConfigToggleUpdate(enabled=True)
        
        # Full update
        full_update = EngineConfigUpdate(
            config_value="true",
            enabled=True,
            description="CrewAI flow feature enabled"
        )
        
        # Config response (simulating what would come from database)
        now = datetime.now()
        response_data = {
            "id": 1,
            "engine_name": create_config.engine_name,
            "engine_type": create_config.engine_type,
            "config_key": create_config.config_key,
            "config_value": full_update.config_value,  # Updated value
            "enabled": full_update.enabled,  # Updated enabled status
            "description": full_update.description,  # Updated description
            "created_at": now,
            "updated_at": now
        }
        config_response = EngineConfigResponse(**response_data)
        
        # Verify lifecycle
        assert create_config.engine_name == "crewai"
        assert create_config.config_value == "false"  # Original value
        assert value_update.config_value == "true"
        assert toggle_update.enabled is True
        assert config_response.id == 1
        assert config_response.config_value == "true"  # From update
        assert config_response.enabled is True  # From update
        assert config_response.description == "CrewAI flow feature enabled"  # From update
    
    def test_engine_configuration_scenarios(self):
        """Test different engine configuration scenarios."""
        now = datetime.now()
        
        # Multiple engine configurations
        configs = [
            {
                "engine_name": "crewai",
                "engine_type": "workflow",
                "configs": [
                    {"key": "flow_enabled", "value": "true"},
                    {"key": "parallel_execution", "value": "false"},
                    {"key": "max_agents", "value": "10"}
                ]
            },
            {
                "engine_name": "databricks",
                "engine_type": "ai",
                "configs": [
                    {"key": "default_model", "value": "databricks-llama-4-maverick"},
                    {"key": "temperature", "value": "0.7"},
                    {"key": "max_tokens", "value": "1000"}
                ]
            },
            {
                "engine_name": "processor",
                "engine_type": "processing",
                "configs": [
                    {"key": "batch_size", "value": "100"},
                    {"key": "timeout_seconds", "value": "300"},
                    {"key": "retry_attempts", "value": "3"}
                ]
            }
        ]
        
        all_responses = []
        config_id = 1
        
        for engine in configs:
            for config in engine["configs"]:
                response_data = {
                    "id": config_id,
                    "engine_name": engine["engine_name"],
                    "engine_type": engine["engine_type"],
                    "config_key": config["key"],
                    "config_value": config["value"],
                    "enabled": True,
                    "description": f"{config['key']} configuration for {engine['engine_name']}",
                    "created_at": now,
                    "updated_at": now
                }
                response = EngineConfigResponse(**response_data)
                all_responses.append(response)
                config_id += 1
        
        # Create list response
        list_response = EngineConfigListResponse(
            configs=all_responses,
            count=len(all_responses)
        )
        
        # Verify scenarios
        assert len(list_response.configs) == 9
        assert list_response.count == 9
        
        # Group by engine
        by_engine = {}
        for config in list_response.configs:
            if config.engine_name not in by_engine:
                by_engine[config.engine_name] = []
            by_engine[config.engine_name].append(config)
        
        assert len(by_engine["crewai"]) == 3
        assert len(by_engine["databricks"]) == 3
        assert len(by_engine["processor"]) == 3
        
        # Check specific configurations
        crewai_configs = by_engine["crewai"]
        flow_config = next(c for c in crewai_configs if c.config_key == "flow_enabled")
        assert flow_config.config_value == "true"
    
    def test_crewai_flow_configuration_workflow(self):
        """Test CrewAI flow configuration workflow."""
        # Initial flow disabled
        initial_config = EngineConfigResponse(
            id=1,
            engine_name="crewai",
            engine_type="workflow",
            config_key="flow_enabled",
            config_value="false",
            enabled=True,
            description="CrewAI flow feature",
            created_at=datetime.now(),
            updated_at=datetime.now()
        )
        
        # Enable flow using specific CrewAI update
        flow_update = CrewAIFlowConfigUpdate(flow_enabled=True)
        
        # Updated config after flow enable
        updated_config = EngineConfigResponse(
            id=initial_config.id,
            engine_name=initial_config.engine_name,
            engine_type=initial_config.engine_type,
            config_key=initial_config.config_key,
            config_value="true",  # Updated by flow enable
            enabled=initial_config.enabled,
            description=initial_config.description,
            created_at=initial_config.created_at,
            updated_at=datetime.now()  # Updated timestamp
        )
        
        # Verify flow workflow
        assert initial_config.config_value == "false"
        assert flow_update.flow_enabled is True
        assert updated_config.config_value == "true"
        assert updated_config.updated_at > initial_config.updated_at
        assert updated_config.engine_name == "crewai"
        assert updated_config.config_key == "flow_enabled"
    
    def test_engine_config_filtering_scenarios(self):
        """Test engine configuration filtering scenarios."""
        now = datetime.now()
        
        # Mixed configurations - enabled and disabled
        mixed_configs = [
            EngineConfigResponse(
                id=1, engine_name="crewai", engine_type="workflow",
                config_key="flow_enabled", config_value="true", enabled=True,
                created_at=now, updated_at=now
            ),
            EngineConfigResponse(
                id=2, engine_name="crewai", engine_type="workflow",
                config_key="debug_mode", config_value="false", enabled=False,
                created_at=now, updated_at=now
            ),
            EngineConfigResponse(
                id=3, engine_name="databricks", engine_type="ai",
                config_key="model_endpoint", config_value="llama-4", enabled=True,
                created_at=now, updated_at=now
            ),
            EngineConfigResponse(
                id=4, engine_name="databricks", engine_type="ai",
                config_key="fallback_model", config_value="gpt-3.5", enabled=False,
                created_at=now, updated_at=now
            )
        ]
        
        list_response = EngineConfigListResponse(
            configs=mixed_configs,
            count=len(mixed_configs)
        )
        
        # Filter enabled configurations
        enabled_configs = [c for c in list_response.configs if c.enabled]
        disabled_configs = [c for c in list_response.configs if not c.enabled]
        
        assert len(enabled_configs) == 2
        assert len(disabled_configs) == 2
        
        # Filter by engine type
        workflow_configs = [c for c in list_response.configs if c.engine_type == "workflow"]
        ai_configs = [c for c in list_response.configs if c.engine_type == "ai"]
        
        assert len(workflow_configs) == 2
        assert len(ai_configs) == 2
        
        # Filter by engine name
        crewai_configs = [c for c in list_response.configs if c.engine_name == "crewai"]
        databricks_configs = [c for c in list_response.configs if c.engine_name == "databricks"]
        
        assert len(crewai_configs) == 2
        assert len(databricks_configs) == 2
        
        # Complex filtering - enabled CrewAI configs
        enabled_crewai = [c for c in list_response.configs 
                         if c.engine_name == "crewai" and c.enabled]
        assert len(enabled_crewai) == 1
        assert enabled_crewai[0].config_key == "flow_enabled"