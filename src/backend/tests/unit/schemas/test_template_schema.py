"""
Unit tests for template schemas.

Tests the functionality of Pydantic schemas for template operations
including validation, serialization, and field constraints.
"""

from datetime import datetime

import pytest
from pydantic import ValidationError

from src.schemas.template import (
    PromptTemplateBase,
    PromptTemplateCreate,
    PromptTemplateResponse,
    PromptTemplateUpdate,
    ResetResponse,
    TemplateCreate,
    TemplateUpdate,
)


class TestPromptTemplateBase:
    """Test cases for PromptTemplateBase schema."""

    def test_valid_prompt_template_base_minimal(self):
        """Test valid PromptTemplateBase creation with minimal required fields."""
        template_data = {"name": "Test Template", "template": "Hello {name}!"}
        template = PromptTemplateBase(**template_data)
        assert template.name == "Test Template"
        assert template.template == "Hello {name}!"
        assert template.description is None
        assert template.is_active is True

    def test_valid_prompt_template_base_full(self):
        """Test valid PromptTemplateBase creation with all fields."""
        template_data = {
            "name": "Greeting Template",
            "description": "A simple greeting template",
            "template": "Hello {name}, welcome to {platform}!",
            "is_active": False,
        }
        template = PromptTemplateBase(**template_data)
        assert template.name == "Greeting Template"
        assert template.description == "A simple greeting template"
        assert template.template == "Hello {name}, welcome to {platform}!"
        assert template.is_active is False

    def test_prompt_template_base_missing_required_fields(self):
        """Test PromptTemplateBase validation with missing required fields."""
        with pytest.raises(ValidationError) as exc_info:
            PromptTemplateBase(name="Test Template")

        errors = exc_info.value.errors()
        missing_fields = [
            error["loc"][0] for error in errors if error["type"] == "missing"
        ]
        assert "template" in missing_fields

        with pytest.raises(ValidationError) as exc_info:
            PromptTemplateBase(template="Hello {name}!")

        errors = exc_info.value.errors()
        missing_fields = [
            error["loc"][0] for error in errors if error["type"] == "missing"
        ]
        assert "name" in missing_fields

    def test_prompt_template_base_empty_strings(self):
        """Test PromptTemplateBase with empty strings."""
        template_data = {"name": "", "template": ""}
        template = PromptTemplateBase(**template_data)
        assert template.name == ""
        assert template.template == ""

    def test_prompt_template_base_boolean_conversion(self):
        """Test PromptTemplateBase boolean field conversion."""
        # Test with string values that should convert to boolean
        template_data = {
            "name": "Test Template",
            "template": "Hello {name}!",
            "is_active": "true",
        }
        template = PromptTemplateBase(**template_data)
        assert template.is_active is True

        template_data["is_active"] = "false"
        template = PromptTemplateBase(**template_data)
        assert template.is_active is False

        template_data["is_active"] = 0
        template = PromptTemplateBase(**template_data)
        assert template.is_active is False

        template_data["is_active"] = 1
        template = PromptTemplateBase(**template_data)
        assert template.is_active is True


class TestPromptTemplateCreate:
    """Test cases for PromptTemplateCreate schema."""

    def test_valid_prompt_template_create(self):
        """Test valid PromptTemplateCreate creation."""
        template_data = {
            "name": "New Template",
            "description": "A new template for testing",
            "template": "Hello {user}, your score is {score}!",
            "is_active": True,
        }
        template = PromptTemplateCreate(**template_data)
        assert template.name == "New Template"
        assert template.description == "A new template for testing"
        assert template.template == "Hello {user}, your score is {score}!"
        assert template.is_active is True

    def test_prompt_template_create_inheritance(self):
        """Test that PromptTemplateCreate inherits from PromptTemplateBase."""
        template_data = {"name": "Test Template", "template": "Test {placeholder}"}
        template = PromptTemplateCreate(**template_data)

        # Should have all base class attributes
        assert hasattr(template, "name")
        assert hasattr(template, "description")
        assert hasattr(template, "template")
        assert hasattr(template, "is_active")

        # Should behave like base class
        assert template.name == "Test Template"
        assert template.template == "Test {placeholder}"
        assert template.description is None
        assert template.is_active is True

    def test_prompt_template_create_missing_fields(self):
        """Test PromptTemplateCreate validation with missing required fields."""
        with pytest.raises(ValidationError) as exc_info:
            PromptTemplateCreate()

        errors = exc_info.value.errors()
        missing_fields = [
            error["loc"][0] for error in errors if error["type"] == "missing"
        ]
        assert "name" in missing_fields
        assert "template" in missing_fields


class TestPromptTemplateUpdate:
    """Test cases for PromptTemplateUpdate schema."""

    def test_valid_prompt_template_update_all_fields(self):
        """Test valid PromptTemplateUpdate with all fields."""
        update_data = {
            "name": "Updated Template",
            "description": "Updated description",
            "template": "Updated template {value}",
            "is_active": False,
        }
        update = PromptTemplateUpdate(**update_data)
        assert update.name == "Updated Template"
        assert update.description == "Updated description"
        assert update.template == "Updated template {value}"
        assert update.is_active is False

    def test_valid_prompt_template_update_partial(self):
        """Test valid PromptTemplateUpdate with partial fields."""
        update_data = {"name": "Updated Name Only"}
        update = PromptTemplateUpdate(**update_data)
        assert update.name == "Updated Name Only"
        assert update.description is None
        assert update.template is None
        assert update.is_active is None

        update_data = {"template": "New template content {param}", "is_active": True}
        update = PromptTemplateUpdate(**update_data)
        assert update.name is None
        assert update.description is None
        assert update.template == "New template content {param}"
        assert update.is_active is True

    def test_prompt_template_update_empty(self):
        """Test PromptTemplateUpdate with no fields (all optional)."""
        update = PromptTemplateUpdate()
        assert update.name is None
        assert update.description is None
        assert update.template is None
        assert update.is_active is None

    def test_prompt_template_update_none_values(self):
        """Test PromptTemplateUpdate with explicit None values."""
        update_data = {
            "name": None,
            "description": None,
            "template": None,
            "is_active": None,
        }
        update = PromptTemplateUpdate(**update_data)
        assert update.name is None
        assert update.description is None
        assert update.template is None
        assert update.is_active is None

    def test_prompt_template_update_empty_strings(self):
        """Test PromptTemplateUpdate with empty strings."""
        update_data = {"name": "", "description": "", "template": ""}
        update = PromptTemplateUpdate(**update_data)
        assert update.name == ""
        assert update.description == ""
        assert update.template == ""


class TestPromptTemplateResponse:
    """Test cases for PromptTemplateResponse schema."""

    def test_valid_prompt_template_response(self):
        """Test valid PromptTemplateResponse creation."""
        now = datetime.now()
        response_data = {
            "id": 1,
            "name": "Response Template",
            "description": "A response template",
            "template": "Response: {content}",
            "is_active": True,
            "created_at": now,
            "updated_at": now,
        }
        response = PromptTemplateResponse(**response_data)
        assert response.id == 1
        assert response.name == "Response Template"
        assert response.description == "A response template"
        assert response.template == "Response: {content}"
        assert response.is_active is True
        assert response.created_at == now
        assert response.updated_at == now

    def test_prompt_template_response_missing_base_fields(self):
        """Test PromptTemplateResponse validation with missing base fields."""
        now = datetime.now()
        with pytest.raises(ValidationError) as exc_info:
            PromptTemplateResponse(id=1, created_at=now, updated_at=now)

        errors = exc_info.value.errors()
        missing_fields = [
            error["loc"][0] for error in errors if error["type"] == "missing"
        ]
        assert "name" in missing_fields
        assert "template" in missing_fields

    def test_prompt_template_response_missing_response_fields(self):
        """Test PromptTemplateResponse validation with missing response-specific fields."""
        with pytest.raises(ValidationError) as exc_info:
            PromptTemplateResponse(name="Test", template="Test {value}")

        errors = exc_info.value.errors()
        missing_fields = [
            error["loc"][0] for error in errors if error["type"] == "missing"
        ]
        assert "id" in missing_fields
        assert "created_at" in missing_fields
        assert "updated_at" in missing_fields

    def test_prompt_template_response_config(self):
        """Test PromptTemplateResponse model configuration."""
        assert hasattr(PromptTemplateResponse, "model_config")
        assert "from_attributes" in PromptTemplateResponse.model_config
        assert PromptTemplateResponse.model_config["from_attributes"] is True

    def test_prompt_template_response_datetime_types(self):
        """Test PromptTemplateResponse with different datetime formats."""
        response_data = {
            "id": 1,
            "name": "Test Template",
            "template": "Test {value}",
            "created_at": "2023-01-01T12:00:00",
            "updated_at": "2023-01-01T12:00:00",
        }
        response = PromptTemplateResponse(**response_data)
        assert isinstance(response.created_at, datetime)
        assert isinstance(response.updated_at, datetime)


class TestResetResponse:
    """Test cases for ResetResponse schema."""

    def test_valid_reset_response(self):
        """Test valid ResetResponse creation."""
        response_data = {"message": "Templates reset successfully", "reset_count": 5}
        response = ResetResponse(**response_data)
        assert response.message == "Templates reset successfully"
        assert response.reset_count == 5

    def test_reset_response_zero_count(self):
        """Test ResetResponse with zero reset count."""
        response_data = {"message": "No templates to reset", "reset_count": 0}
        response = ResetResponse(**response_data)
        assert response.message == "No templates to reset"
        assert response.reset_count == 0

    def test_reset_response_missing_fields(self):
        """Test ResetResponse validation with missing fields."""
        with pytest.raises(ValidationError) as exc_info:
            ResetResponse(message="Test message")

        errors = exc_info.value.errors()
        missing_fields = [
            error["loc"][0] for error in errors if error["type"] == "missing"
        ]
        assert "reset_count" in missing_fields

        with pytest.raises(ValidationError) as exc_info:
            ResetResponse(reset_count=5)

        errors = exc_info.value.errors()
        missing_fields = [
            error["loc"][0] for error in errors if error["type"] == "missing"
        ]
        assert "message" in missing_fields

    def test_reset_response_empty_message(self):
        """Test ResetResponse with empty message."""
        response_data = {"message": "", "reset_count": 3}
        response = ResetResponse(**response_data)
        assert response.message == ""
        assert response.reset_count == 3

    def test_reset_response_negative_count(self):
        """Test ResetResponse with negative reset count."""
        response_data = {"message": "Error occurred", "reset_count": -1}
        response = ResetResponse(**response_data)
        assert response.message == "Error occurred"
        assert response.reset_count == -1


class TestBackwardCompatibilityAliases:
    """Test cases for backward compatibility aliases."""

    def test_template_create_alias(self):
        """Test TemplateCreate alias works like PromptTemplateCreate."""
        template_data = {"name": "Alias Test", "template": "Testing alias {value}"}

        # Both should create the same type of object
        prompt_template = PromptTemplateCreate(**template_data)
        alias_template = TemplateCreate(**template_data)

        assert type(prompt_template) is type(alias_template)
        assert prompt_template.name == alias_template.name
        assert prompt_template.template == alias_template.template
        assert prompt_template.is_active == alias_template.is_active

        # Verify they are actually the same class
        assert TemplateCreate is PromptTemplateCreate

    def test_template_update_alias(self):
        """Test TemplateUpdate alias works like PromptTemplateUpdate."""
        update_data = {"name": "Updated via alias", "is_active": False}

        # Both should create the same type of object
        prompt_update = PromptTemplateUpdate(**update_data)
        alias_update = TemplateUpdate(**update_data)

        assert type(prompt_update) is type(alias_update)
        assert prompt_update.name == alias_update.name
        assert prompt_update.is_active == alias_update.is_active

        # Verify they are actually the same class
        assert TemplateUpdate is PromptTemplateUpdate


class TestSchemaIntegration:
    """Integration tests for template schema interactions."""

    def test_create_update_response_workflow(self):
        """Test a complete workflow from create to update to response."""
        # Create a template
        create_data = {
            "name": "Workflow Template",
            "description": "A template for testing workflow",
            "template": "Hello {name}, your status is {status}",
            "is_active": True,
        }
        create_schema = PromptTemplateCreate(**create_data)

        # Update the template
        update_data = {"description": "Updated description", "is_active": False}
        update_schema = PromptTemplateUpdate(**update_data)

        # Create response (simulating what would come from database)
        now = datetime.now()
        response_data = {
            "id": 1,
            "name": create_schema.name,
            "description": update_data["description"],  # Updated description
            "template": create_schema.template,
            "is_active": update_data["is_active"],  # Updated status
            "created_at": now,
            "updated_at": now,
        }
        response_schema = PromptTemplateResponse(**response_data)

        # Verify the workflow
        assert response_schema.name == create_schema.name
        assert response_schema.template == create_schema.template
        assert response_schema.description == update_schema.description
        assert response_schema.is_active == update_schema.is_active
        assert response_schema.id == 1
        assert isinstance(response_schema.created_at, datetime)
        assert isinstance(response_schema.updated_at, datetime)

    def test_reset_response_scenarios(self):
        """Test ResetResponse for different reset scenarios."""
        # Successful reset
        success_response = ResetResponse(
            message="Successfully reset 10 templates", reset_count=10
        )
        assert "Successfully" in success_response.message
        assert success_response.reset_count > 0

        # No templates to reset
        empty_response = ResetResponse(
            message="No templates found to reset", reset_count=0
        )
        assert success_response.reset_count != empty_response.reset_count
        assert empty_response.reset_count == 0

        # Partial reset (some templates couldn't be reset)
        partial_response = ResetResponse(
            message="Reset 5 out of 8 templates (3 failed)", reset_count=5
        )
        assert partial_response.reset_count > 0
        assert partial_response.reset_count < 8
