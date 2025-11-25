"""
Integration tests for the complete template generation flow.

Tests the end-to-end template generation process from API endpoint
through service layer to database interactions.
"""
import pytest
import json
from unittest.mock import AsyncMock, patch, MagicMock
from fastapi.testclient import TestClient
from httpx import AsyncClient

from src.main import app
from src.schemas.template_generation import TemplateGenerationRequest, TemplateGenerationResponse


class TestTemplateGenerationIntegration:
    """Integration tests for template generation flow."""

    @pytest.fixture
    def sample_request_data(self):
        """Sample request data for template generation."""
        return {
            "role": "Data Scientist",
            "goal": "Develop machine learning models to solve business problems",
            "backstory": "Expert data scientist with 8 years of experience in ML and analytics",
            "model": "databricks-llama-4-maverick"
        }

    @pytest.fixture
    def sample_generated_templates(self):
        """Sample generated templates that would come from LLM."""
        return {
            "system_template": "You are a {role} expert. {backstory}\n\nYour goal is: {goal}\n\nProvide accurate and insightful analysis based on your expertise.",
            "prompt_template": "Task: {input}\n\nContext: {context}\n\nPlease analyze the task carefully and provide a structured response.",
            "response_template": "ANALYSIS: [Your analytical thoughts]\n\nACTION: [What you will do]\n\nRESULT: [Your final deliverable]"
        }

    @pytest.mark.asyncio
    async def test_template_generation_api_endpoint_success(self, sample_request_data, sample_generated_templates):
        """Test successful template generation through API endpoint."""
        with patch('src.services.template_generation_service.UnitOfWork') as mock_uow, \
             patch('src.services.template_generation_service.ModelConfigService') as mock_model_service, \
             patch('src.services.template_generation_service.TemplateService') as mock_template_service, \
             patch('src.services.template_generation_service.LLMManager') as mock_llm_manager, \
             patch('src.services.template_generation_service.litellm') as mock_litellm, \
             patch('src.services.template_generation_service.robust_json_parser') as mock_parser:
            
            # Setup mocks
            mock_uow_instance = AsyncMock()
            mock_uow.return_value.__aenter__.return_value = mock_uow_instance
            mock_model_service_instance = AsyncMock()
            mock_model_service.from_unit_of_work.return_value = mock_model_service_instance
            mock_model_service_instance.get_model_config.return_value = {"name": "databricks-llama-4-maverick"}
            
            mock_template_service.get_template_content = AsyncMock(return_value="Enhanced system template with {role}, {goal}, {backstory} parameters")
            mock_llm_manager.configure_litellm = AsyncMock(return_value={"model": "databricks-llama-4-maverick"})
            
            # Mock LLM response
            mock_response = {
                "choices": [{
                    "message": {
                        "content": json.dumps(sample_generated_templates)
                    }
                }]
            }
            mock_litellm.acompletion = AsyncMock(return_value=mock_response)
            mock_parser.return_value = sample_generated_templates

            # Make API request
            async with AsyncClient(app=app, base_url="http://test") as client:
                response = await client.post(
                    "/api/v1/template-generation/generate-templates",
                    json=sample_request_data
                )

            # Verify response
            assert response.status_code == 200
            response_data = response.json()
            
            assert "system_template" in response_data
            assert "prompt_template" in response_data
            assert "response_template" in response_data
            
            # Verify templates contain expected parameters
            assert "{role}" in response_data["system_template"]
            assert "{goal}" in response_data["system_template"]
            assert "{backstory}" in response_data["system_template"]
            assert "{input}" in response_data["prompt_template"]
            assert "{context}" in response_data["prompt_template"]
            assert "ANALYSIS" in response_data["response_template"]
            assert "ACTION" in response_data["response_template"]
            assert "RESULT" in response_data["response_template"]

    @pytest.mark.asyncio
    async def test_template_generation_model_not_found(self, sample_request_data):
        """Test template generation when model is not found."""
        with patch('src.services.template_generation_service.UnitOfWork') as mock_uow, \
             patch('src.services.template_generation_service.ModelConfigService') as mock_model_service:
            
            # Setup mocks for model not found
            mock_uow_instance = AsyncMock()
            mock_uow.return_value.__aenter__.return_value = mock_uow_instance
            mock_model_service_instance = AsyncMock()
            mock_model_service.from_unit_of_work.return_value = mock_model_service_instance
            mock_model_service_instance.get_model_config.return_value = None

            async with AsyncClient(app=app, base_url="http://test") as client:
                response = await client.post(
                    "/api/v1/template-generation/generate-templates",
                    json=sample_request_data
                )

            assert response.status_code == 400
            assert "not found in the database" in response.json()["detail"]

    @pytest.mark.asyncio
    async def test_template_generation_invalid_request(self):
        """Test template generation with invalid request data."""
        invalid_request = {
            "role": "",  # Empty role
            "goal": "Test goal",
            "backstory": "Test backstory",
            "model": "test-model"
        }

        async with AsyncClient(app=app, base_url="http://test") as client:
            response = await client.post(
                "/api/v1/template-generation/generate-templates",
                json=invalid_request
            )

        assert response.status_code == 422  # Validation error

    @pytest.mark.asyncio
    async def test_template_generation_missing_prompt_template(self, sample_request_data):
        """Test template generation when prompt template is not found in database."""
        with patch('src.services.template_generation_service.UnitOfWork') as mock_uow, \
             patch('src.services.template_generation_service.ModelConfigService') as mock_model_service, \
             patch('src.services.template_generation_service.TemplateService') as mock_template_service:
            
            # Setup mocks
            mock_uow_instance = AsyncMock()
            mock_uow.return_value.__aenter__.return_value = mock_uow_instance
            mock_model_service_instance = AsyncMock()
            mock_model_service.from_unit_of_work.return_value = mock_model_service_instance
            mock_model_service_instance.get_model_config.return_value = {"name": "databricks-llama-4-maverick"}
            
            # Template service returns None (not found)
            mock_template_service.get_template_content = AsyncMock(return_value=None)

            async with AsyncClient(app=app, base_url="http://test") as client:
                response = await client.post(
                    "/api/v1/template-generation/generate-templates",
                    json=sample_request_data
                )

            assert response.status_code == 400
            assert "not found in database" in response.json()["detail"]

    @pytest.mark.asyncio
    async def test_template_generation_llm_error(self, sample_request_data):
        """Test template generation when LLM fails."""
        with patch('src.services.template_generation_service.UnitOfWork') as mock_uow, \
             patch('src.services.template_generation_service.ModelConfigService') as mock_model_service, \
             patch('src.services.template_generation_service.TemplateService') as mock_template_service, \
             patch('src.services.template_generation_service.LLMManager') as mock_llm_manager, \
             patch('src.services.template_generation_service.litellm') as mock_litellm:
            
            # Setup mocks
            mock_uow_instance = AsyncMock()
            mock_uow.return_value.__aenter__.return_value = mock_uow_instance
            mock_model_service_instance = AsyncMock()
            mock_model_service.from_unit_of_work.return_value = mock_model_service_instance
            mock_model_service_instance.get_model_config.return_value = {"name": "databricks-llama-4-maverick"}
            
            mock_template_service.get_template_content = AsyncMock(return_value="Template content")
            mock_llm_manager.configure_litellm = AsyncMock(return_value={"model": "databricks-llama-4-maverick"})
            
            # LLM fails
            mock_litellm.acompletion = AsyncMock(side_effect=Exception("LLM service unavailable"))

            async with AsyncClient(app=app, base_url="http://test") as client:
                response = await client.post(
                    "/api/v1/template-generation/generate-templates",
                    json=sample_request_data
                )

            assert response.status_code == 400
            assert "Failed to generate templates" in response.json()["detail"]

    @pytest.mark.asyncio
    async def test_template_generation_json_parse_error(self, sample_request_data):
        """Test template generation when LLM returns invalid JSON."""
        with patch('src.services.template_generation_service.UnitOfWork') as mock_uow, \
             patch('src.services.template_generation_service.ModelConfigService') as mock_model_service, \
             patch('src.services.template_generation_service.TemplateService') as mock_template_service, \
             patch('src.services.template_generation_service.LLMManager') as mock_llm_manager, \
             patch('src.services.template_generation_service.litellm') as mock_litellm, \
             patch('src.services.template_generation_service.robust_json_parser') as mock_parser:
            
            # Setup mocks
            mock_uow_instance = AsyncMock()
            mock_uow.return_value.__aenter__.return_value = mock_uow_instance
            mock_model_service_instance = AsyncMock()
            mock_model_service.from_unit_of_work.return_value = mock_model_service_instance
            mock_model_service_instance.get_model_config.return_value = {"name": "databricks-llama-4-maverick"}
            
            mock_template_service.get_template_content = AsyncMock(return_value="Template content")
            mock_llm_manager.configure_litellm = AsyncMock(return_value={"model": "databricks-llama-4-maverick"})
            
            # Mock LLM response with invalid JSON
            mock_response = {
                "choices": [{
                    "message": {
                        "content": "This is not valid JSON"
                    }
                }]
            }
            mock_litellm.acompletion = AsyncMock(return_value=mock_response)
            mock_parser.side_effect = json.JSONDecodeError("Invalid JSON", "doc", 0)

            async with AsyncClient(app=app, base_url="http://test") as client:
                response = await client.post(
                    "/api/v1/template-generation/generate-templates",
                    json=sample_request_data
                )

            assert response.status_code == 400
            assert "parse AI response as JSON" in response.json()["detail"]

    @pytest.mark.asyncio
    async def test_template_generation_missing_required_fields(self, sample_request_data):
        """Test template generation when response is missing required fields."""
        with patch('src.services.template_generation_service.UnitOfWork') as mock_uow, \
             patch('src.services.template_generation_service.ModelConfigService') as mock_model_service, \
             patch('src.services.template_generation_service.TemplateService') as mock_template_service, \
             patch('src.services.template_generation_service.LLMManager') as mock_llm_manager, \
             patch('src.services.template_generation_service.litellm') as mock_litellm, \
             patch('src.services.template_generation_service.robust_json_parser') as mock_parser:
            
            # Setup mocks
            mock_uow_instance = AsyncMock()
            mock_uow.return_value.__aenter__.return_value = mock_uow_instance
            mock_model_service_instance = AsyncMock()
            mock_model_service.from_unit_of_work.return_value = mock_model_service_instance
            mock_model_service_instance.get_model_config.return_value = {"name": "databricks-llama-4-maverick"}
            
            mock_template_service.get_template_content = AsyncMock(return_value="Template content")
            mock_llm_manager.configure_litellm = AsyncMock(return_value={"model": "databricks-llama-4-maverick"})
            
            # Response missing required fields
            incomplete_response = {
                "system_template": "Complete template",
                "prompt_template": "",  # Empty
                "response_template": "Complete template"
            }
            
            mock_response = {
                "choices": [{
                    "message": {
                        "content": json.dumps(incomplete_response)
                    }
                }]
            }
            mock_litellm.acompletion = AsyncMock(return_value=mock_response)
            mock_parser.return_value = incomplete_response

            async with AsyncClient(app=app, base_url="http://test") as client:
                response = await client.post(
                    "/api/v1/template-generation/generate-templates",
                    json=sample_request_data
                )

            assert response.status_code == 400
            assert "Missing or empty required field" in response.json()["detail"]

    @pytest.mark.asyncio
    async def test_template_generation_field_normalization(self, sample_request_data):
        """Test template generation with different field name formats."""
        with patch('src.services.template_generation_service.UnitOfWork') as mock_uow, \
             patch('src.services.template_generation_service.ModelConfigService') as mock_model_service, \
             patch('src.services.template_generation_service.TemplateService') as mock_template_service, \
             patch('src.services.template_generation_service.LLMManager') as mock_llm_manager, \
             patch('src.services.template_generation_service.litellm') as mock_litellm, \
             patch('src.services.template_generation_service.robust_json_parser') as mock_parser:
            
            # Setup mocks
            mock_uow_instance = AsyncMock()
            mock_uow.return_value.__aenter__.return_value = mock_uow_instance
            mock_model_service_instance = AsyncMock()
            mock_model_service.from_unit_of_work.return_value = mock_model_service_instance
            mock_model_service_instance.get_model_config.return_value = {"name": "databricks-llama-4-maverick"}
            
            mock_template_service.get_template_content = AsyncMock(return_value="Template content")
            mock_llm_manager.configure_litellm = AsyncMock(return_value={"model": "databricks-llama-4-maverick"})
            
            # Response with capitalized field names
            capitalized_response = {
                "System Template": "System template content",
                "Prompt Template": "Prompt template content",
                "Response Template": "Response template content"
            }
            
            mock_response = {
                "choices": [{
                    "message": {
                        "content": json.dumps(capitalized_response)
                    }
                }]
            }
            mock_litellm.acompletion = AsyncMock(return_value=mock_response)
            mock_parser.return_value = capitalized_response

            async with AsyncClient(app=app, base_url="http://test") as client:
                response = await client.post(
                    "/api/v1/template-generation/generate-templates",
                    json=sample_request_data
                )

            assert response.status_code == 200
            response_data = response.json()
            
            assert response_data["system_template"] == "System template content"
            assert response_data["prompt_template"] == "Prompt template content"
            assert response_data["response_template"] == "Response template content"

    @pytest.mark.asyncio
    async def test_template_generation_prompt_construction(self, sample_request_data):
        """Test that the prompt is constructed with proper role, goal, and backstory."""
        with patch('src.services.template_generation_service.UnitOfWork') as mock_uow, \
             patch('src.services.template_generation_service.ModelConfigService') as mock_model_service, \
             patch('src.services.template_generation_service.TemplateService') as mock_template_service, \
             patch('src.services.template_generation_service.LLMManager') as mock_llm_manager, \
             patch('src.services.template_generation_service.litellm') as mock_litellm, \
             patch('src.services.template_generation_service.robust_json_parser') as mock_parser:
            
            # Setup mocks
            mock_uow_instance = AsyncMock()
            mock_uow.return_value.__aenter__.return_value = mock_uow_instance
            mock_model_service_instance = AsyncMock()
            mock_model_service.from_unit_of_work.return_value = mock_model_service_instance
            mock_model_service_instance.get_model_config.return_value = {"name": "databricks-llama-4-maverick"}
            
            system_template = "Enhanced template with {role}, {goal}, {backstory} parameters"
            mock_template_service.get_template_content = AsyncMock(return_value=system_template)
            mock_llm_manager.configure_litellm = AsyncMock(return_value={"model": "databricks-llama-4-maverick"})
            
            sample_templates = {
                "system_template": "Generated system template",
                "prompt_template": "Generated prompt template",
                "response_template": "Generated response template"
            }
            
            mock_response = {
                "choices": [{
                    "message": {
                        "content": json.dumps(sample_templates)
                    }
                }]
            }
            mock_litellm.acompletion = AsyncMock(return_value=mock_response)
            mock_parser.return_value = sample_templates

            async with AsyncClient(app=app, base_url="http://test") as client:
                response = await client.post(
                    "/api/v1/template-generation/generate-templates",
                    json=sample_request_data
                )

            # Verify the LLM was called with correct messages
            mock_litellm.acompletion.assert_called_once()
            call_kwargs = mock_litellm.acompletion.call_args.kwargs
            messages = call_kwargs["messages"]
            
            assert len(messages) == 2
            assert messages[0]["role"] == "system"
            assert messages[0]["content"] == system_template
            assert messages[1]["role"] == "user"
            
            user_content = messages[1]["content"]
            assert "Data Scientist" in user_content
            assert "Develop machine learning models" in user_content
            assert "Expert data scientist with 8 years" in user_content
            assert response.status_code == 200