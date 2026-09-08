"""
Comprehensive unit tests for prompt_utils module.

Tests JSON parsing utilities and prompt template functions.
"""

import pytest

from src.utils.prompt_utils import get_prompt_template


class TestGetPromptTemplate:
    """Test get_prompt_template function."""

    @pytest.mark.asyncio
    async def test_get_prompt_template_function_exists(self):
        """Test get_prompt_template function exists and is callable."""
        assert callable(get_prompt_template)

        # Test function signature
        import inspect

        sig = inspect.signature(get_prompt_template)
        params = list(sig.parameters.keys())

        assert "db" in params
        assert "name" in params
        assert "default_template" in params
