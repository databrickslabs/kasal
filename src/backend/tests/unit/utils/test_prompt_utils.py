"""
Unit tests for prompt_utils module.
"""

from unittest.mock import Mock, patch

import pytest
from sqlalchemy.orm import Session

from src.utils.prompt_utils import get_prompt_template, robust_json_parser


class TestGetPromptTemplate:
    """Test get_prompt_template function."""

    @pytest.mark.asyncio
    async def test_get_prompt_template_success(self):
        """Test successful template retrieval."""
        mock_db = Mock(spec=Session)
        template_name = "test_template"
        expected_template = "This is a test template with {{variable}}"

        with patch(
            "src.services.catalog.templates.TemplateService.get_template_content"
        ) as mock_get_template:
            mock_get_template.return_value = expected_template

            result = await get_prompt_template(mock_db, template_name)

            assert result == expected_template
            mock_get_template.assert_called_once_with(template_name, None)

    @pytest.mark.asyncio
    async def test_get_prompt_template_with_default(self):
        """Test template retrieval with default template."""
        mock_db = Mock(spec=Session)
        template_name = "non_existent_template"
        default_template = "Default template content"

        with patch(
            "src.services.catalog.templates.TemplateService.get_template_content"
        ) as mock_get_template:
            mock_get_template.return_value = default_template

            result = await get_prompt_template(mock_db, template_name, default_template)

            assert result == default_template
            mock_get_template.assert_called_once_with(template_name, default_template)

    @pytest.mark.asyncio
    async def test_get_prompt_template_returns_none(self):
        """Test template retrieval when service returns None."""
        mock_db = Mock(spec=Session)
        template_name = "non_existent_template"

        with patch(
            "src.services.catalog.templates.TemplateService.get_template_content"
        ) as mock_get_template:
            mock_get_template.return_value = None

            result = await get_prompt_template(mock_db, template_name)

            assert result is None
            mock_get_template.assert_called_once_with(template_name, None)


class TestPromptUtilsIntegration:
    """Test integration scenarios for prompt_utils."""

    @pytest.mark.asyncio
    async def test_template_and_json_parsing_workflow(self):
        """Test a workflow using both template retrieval and JSON parsing."""
        mock_db = Mock(spec=Session)

        # Mock template that returns JSON-like content
        template_content = """
        Generate a response in JSON format:
        ```json
        {"status": "success", "data": {"key": "value"}}
        ```
        """

        with patch(
            "src.services.catalog.templates.TemplateService.get_template_content"
        ) as mock_get_template:
            mock_get_template.return_value = template_content

            # Get template
            template = await get_prompt_template(mock_db, "json_template")
            assert template == template_content

            # Extract and parse JSON from template
            result = robust_json_parser(template)
            assert result == {"status": "success", "data": {"key": "value"}}

    def test_json_parser_with_llm_response_simulation(self):
        """Test JSON parser with simulated LLM response containing common issues."""
        # Simulate a typical LLM response with multiple issues
        llm_response = """
        I'll help you create a configuration. Here's the JSON:
        
        ```json
        {
            "name": "My Configuration",
            "settings": {
                "enabled": true,
                "timeout": 30,
                "retries": 3
            },
            "features": [
                "authentication",
                "logging",
                "monitoring"
            ],
            "metadata": {
                "created": "2023-01-01",
                "version": "1.0"
            }
        }
        ```
        
        This configuration should work for your needs.
        """

        result = robust_json_parser(llm_response)

        expected = {
            "name": "My Configuration",
            "settings": {"enabled": True, "timeout": 30, "retries": 3},
            "features": ["authentication", "logging", "monitoring"],
            "metadata": {"created": "2023-01-01", "version": "1.0"},
        }

        assert result == expected
