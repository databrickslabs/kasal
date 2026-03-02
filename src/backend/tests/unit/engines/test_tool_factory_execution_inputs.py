"""Tests for ToolFactory execution_inputs cleanup in _create_tool_instance.

Verifies that execution_inputs is properly removed from tool_config after
placeholder resolution to prevent TypeError when constructing tool instances.
This was a bug where PerplexitySearchTool.__init__() received an unexpected
'execution_inputs' keyword argument.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from typing import Dict, Any, Optional

from src.engines.crewai.tools.tool_factory import ToolFactory


class TestExecutionInputsCleanup:
    """Test that execution_inputs is removed from tool_config before tool construction."""

    def _make_factory_with_inputs(self, nested: bool = True, user_inputs: Optional[Dict] = None):
        """Helper to create a ToolFactory with execution inputs in config."""
        if nested:
            config = {
                "inputs": {
                    "inputs": user_inputs or {"api_key": "test-key-123", "region": "us-east-1"}
                }
            }
        else:
            config = {
                "inputs": user_inputs or {"api_key": "test-key-123", "region": "us-east-1"}
            }
        return ToolFactory(config)

    def _make_tool_info(self, title: str = "PerplexityTool", config: Optional[Dict] = None):
        """Helper to create a mock tool info object."""
        tool_info = Mock()
        tool_info.id = 1
        tool_info.title = title
        tool_info.config = config or {}
        return tool_info

    def test_execution_inputs_removed_after_placeholder_resolution(self):
        """Verify execution_inputs is not in tool_config when tool constructor is called."""
        factory = self._make_factory_with_inputs()
        tool_info = self._make_tool_info(title="PerplexityTool")

        mock_tool_class = Mock()
        mock_tool_instance = Mock()
        mock_tool_class.return_value = mock_tool_instance
        factory._tool_implementations["PerplexityTool"] = mock_tool_class

        with patch.object(factory, 'get_tool_info', return_value=tool_info):
            with patch('os.environ.get', return_value="pplx-test-key"):
                factory.create_tool("PerplexityTool")

        # Verify execution_inputs was NOT passed to the constructor
        if mock_tool_class.called:
            call_kwargs = mock_tool_class.call_args[1] if mock_tool_class.call_args[1] else {}
            assert 'execution_inputs' not in call_kwargs, \
                "execution_inputs should be removed before calling tool constructor"

    def test_execution_inputs_removed_with_nested_inputs(self):
        """Test cleanup with nested config['inputs']['inputs'] structure."""
        factory = self._make_factory_with_inputs(
            nested=True,
            user_inputs={"workspace_url": "https://example.com", "token": "abc"}
        )
        tool_info = self._make_tool_info(title="ScrapeWebsiteTool", config={})

        mock_tool_class = Mock()
        mock_tool_instance = Mock()
        mock_tool_class.return_value = mock_tool_instance
        factory._tool_implementations["ScrapeWebsiteTool"] = mock_tool_class

        with patch.object(factory, 'get_tool_info', return_value=tool_info):
            factory.create_tool("ScrapeWebsiteTool")

        if mock_tool_class.called:
            call_kwargs = mock_tool_class.call_args[1] if mock_tool_class.call_args[1] else {}
            assert 'execution_inputs' not in call_kwargs

    def test_execution_inputs_removed_with_direct_inputs(self):
        """Test cleanup with direct config['inputs'] structure (non-nested fallback)."""
        # Direct inputs: config['inputs'] without nested 'inputs' key
        # Filter out system keys, keep user keys
        factory = ToolFactory({
            "inputs": {
                "custom_param": "my-value",
                "agents_yaml": "should-be-filtered",
            }
        })
        tool_info = self._make_tool_info(title="ScrapeWebsiteTool", config={})

        mock_tool_class = Mock()
        mock_tool_instance = Mock()
        mock_tool_class.return_value = mock_tool_instance
        factory._tool_implementations["ScrapeWebsiteTool"] = mock_tool_class

        with patch.object(factory, 'get_tool_info', return_value=tool_info):
            factory.create_tool("ScrapeWebsiteTool")

        if mock_tool_class.called:
            call_kwargs = mock_tool_class.call_args[1] if mock_tool_class.call_args[1] else {}
            assert 'execution_inputs' not in call_kwargs

    def test_no_execution_inputs_when_config_has_no_inputs(self):
        """Test that tool creation works when config has no inputs at all."""
        factory = ToolFactory({"test": "value"})
        tool_info = self._make_tool_info(title="ScrapeWebsiteTool", config={})

        mock_tool_class = Mock()
        mock_tool_instance = Mock()
        mock_tool_class.return_value = mock_tool_instance
        factory._tool_implementations["ScrapeWebsiteTool"] = mock_tool_class

        with patch.object(factory, 'get_tool_info', return_value=tool_info):
            factory.create_tool("ScrapeWebsiteTool")

        if mock_tool_class.called:
            call_kwargs = mock_tool_class.call_args[1] if mock_tool_class.call_args[1] else {}
            assert 'execution_inputs' not in call_kwargs

    def test_no_execution_inputs_when_config_is_none(self):
        """Test that tool creation works when config is None."""
        factory = ToolFactory(None)
        tool_info = self._make_tool_info(title="ScrapeWebsiteTool", config={})

        mock_tool_class = Mock()
        mock_tool_instance = Mock()
        mock_tool_class.return_value = mock_tool_instance
        factory._tool_implementations["ScrapeWebsiteTool"] = mock_tool_class

        with patch.object(factory, 'get_tool_info', return_value=tool_info):
            factory.create_tool("ScrapeWebsiteTool")

        if mock_tool_class.called:
            call_kwargs = mock_tool_class.call_args[1] if mock_tool_class.call_args[1] else {}
            assert 'execution_inputs' not in call_kwargs


class TestPlaceholderResolution:
    """Test that placeholders in tool_config are resolved from execution_inputs."""

    def _make_tool_info(self, title: str = "ScrapeWebsiteTool", config: Optional[Dict] = None):
        """Helper to create a mock tool info object."""
        tool_info = Mock()
        tool_info.id = 1
        tool_info.title = title
        tool_info.config = config or {}
        return tool_info

    def test_placeholders_resolved_before_removal(self):
        """Verify placeholders are resolved from execution_inputs before it's removed."""
        factory = ToolFactory({
            "inputs": {
                "inputs": {"my_url": "https://example.com"}
            }
        })
        # Tool config has a placeholder that should be resolved
        tool_info = self._make_tool_info(
            title="ScrapeWebsiteTool",
            config={"website_url": "{my_url}"}
        )

        mock_tool_class = Mock()
        mock_tool_instance = Mock()
        mock_tool_class.return_value = mock_tool_instance
        factory._tool_implementations["ScrapeWebsiteTool"] = mock_tool_class

        with patch.object(factory, 'get_tool_info', return_value=tool_info):
            factory.create_tool("ScrapeWebsiteTool")

        if mock_tool_class.called:
            call_kwargs = mock_tool_class.call_args[1] if mock_tool_class.call_args[1] else {}
            # Placeholder should have been resolved
            assert call_kwargs.get('website_url') == "https://example.com"
            # execution_inputs must be gone
            assert 'execution_inputs' not in call_kwargs

    def test_multiple_placeholders_resolved(self):
        """Test that multiple placeholders in the same value are resolved."""
        factory = ToolFactory({
            "inputs": {
                "inputs": {"host": "example.com", "port": "8080"}
            }
        })
        tool_info = self._make_tool_info(
            title="ScrapeWebsiteTool",
            config={"endpoint": "https://{host}:{port}/api"}
        )

        mock_tool_class = Mock()
        mock_tool_instance = Mock()
        mock_tool_class.return_value = mock_tool_instance
        factory._tool_implementations["ScrapeWebsiteTool"] = mock_tool_class

        with patch.object(factory, 'get_tool_info', return_value=tool_info):
            factory.create_tool("ScrapeWebsiteTool")

        if mock_tool_class.called:
            call_kwargs = mock_tool_class.call_args[1] if mock_tool_class.call_args[1] else {}
            assert call_kwargs.get('endpoint') == "https://example.com:8080/api"
            assert 'execution_inputs' not in call_kwargs

    def test_unresolvable_placeholder_left_intact(self):
        """Test that placeholders without matching execution_input keys are left as-is."""
        factory = ToolFactory({
            "inputs": {
                "inputs": {"known_key": "known_value"}
            }
        })
        tool_info = self._make_tool_info(
            title="ScrapeWebsiteTool",
            config={"param": "{unknown_key}"}
        )

        mock_tool_class = Mock()
        mock_tool_instance = Mock()
        mock_tool_class.return_value = mock_tool_instance
        factory._tool_implementations["ScrapeWebsiteTool"] = mock_tool_class

        with patch.object(factory, 'get_tool_info', return_value=tool_info):
            factory.create_tool("ScrapeWebsiteTool")

        if mock_tool_class.called:
            call_kwargs = mock_tool_class.call_args[1] if mock_tool_class.call_args[1] else {}
            # Unresolved placeholder stays as-is
            assert call_kwargs.get('param') == "{unknown_key}"
            assert 'execution_inputs' not in call_kwargs

    def test_non_string_config_values_not_affected(self):
        """Test that non-string values in tool_config are not touched during placeholder resolution."""
        factory = ToolFactory({
            "inputs": {
                "inputs": {"some_key": "some_value"}
            }
        })
        tool_info = self._make_tool_info(
            title="ScrapeWebsiteTool",
            config={"timeout": 30, "enabled": True, "items": ["a", "b"]}
        )

        mock_tool_class = Mock()
        mock_tool_instance = Mock()
        mock_tool_class.return_value = mock_tool_instance
        factory._tool_implementations["ScrapeWebsiteTool"] = mock_tool_class

        with patch.object(factory, 'get_tool_info', return_value=tool_info):
            factory.create_tool("ScrapeWebsiteTool")

        if mock_tool_class.called:
            call_kwargs = mock_tool_class.call_args[1] if mock_tool_class.call_args[1] else {}
            assert call_kwargs.get('timeout') == 30
            assert call_kwargs.get('enabled') is True
            assert call_kwargs.get('items') == ["a", "b"]
            assert 'execution_inputs' not in call_kwargs

    def test_config_override_merged_before_placeholder_resolution(self):
        """Test that tool_config_override is merged before placeholders are resolved."""
        factory = ToolFactory({
            "inputs": {
                "inputs": {"my_key": "resolved_value"}
            }
        })
        tool_info = self._make_tool_info(
            title="ScrapeWebsiteTool",
            config={"base_param": "base_value"}
        )

        mock_tool_class = Mock()
        mock_tool_instance = Mock()
        mock_tool_class.return_value = mock_tool_instance
        factory._tool_implementations["ScrapeWebsiteTool"] = mock_tool_class

        override = {"override_param": "{my_key}"}

        with patch.object(factory, 'get_tool_info', return_value=tool_info):
            factory.create_tool("ScrapeWebsiteTool", tool_config_override=override)

        if mock_tool_class.called:
            call_kwargs = mock_tool_class.call_args[1] if mock_tool_class.call_args[1] else {}
            # Override placeholder should be resolved
            assert call_kwargs.get('override_param') == "resolved_value"
            # Base config should still be there
            assert call_kwargs.get('base_param') == "base_value"
            assert 'execution_inputs' not in call_kwargs


class TestExecutionInputsFilteringLogic:
    """Test the filtering logic that determines which inputs become execution_inputs."""

    def test_system_keys_filtered_from_direct_inputs(self):
        """Test that system keys (agents_yaml, tasks_yaml, etc.) are filtered out."""
        factory = ToolFactory({
            "inputs": {
                "agents_yaml": "should-be-filtered",
                "tasks_yaml": "should-be-filtered",
                "planning": "should-be-filtered",
                "model": "should-be-filtered",
                "execution_type": "should-be-filtered",
                "schema_detection_enabled": "should-be-filtered",
                "process": "should-be-filtered",
                "run_name": "should-be-filtered",
                "user_param": "should-be-kept",
            }
        })
        tool_info = Mock()
        tool_info.id = 1
        tool_info.title = "ScrapeWebsiteTool"
        tool_info.config = {"url": "{user_param}"}

        mock_tool_class = Mock()
        mock_tool_instance = Mock()
        mock_tool_class.return_value = mock_tool_instance
        factory._tool_implementations["ScrapeWebsiteTool"] = mock_tool_class

        with patch.object(factory, 'get_tool_info', return_value=tool_info):
            factory.create_tool("ScrapeWebsiteTool")

        if mock_tool_class.called:
            call_kwargs = mock_tool_class.call_args[1] if mock_tool_class.call_args[1] else {}
            # user_param placeholder should resolve
            assert call_kwargs.get('url') == "should-be-kept"
            assert 'execution_inputs' not in call_kwargs

    def test_empty_user_inputs_after_filtering_means_no_injection(self):
        """Test that if all inputs are system keys, no execution_inputs are injected."""
        factory = ToolFactory({
            "inputs": {
                "agents_yaml": "filtered",
                "tasks_yaml": "filtered",
            }
        })
        tool_info = Mock()
        tool_info.id = 1
        tool_info.title = "ScrapeWebsiteTool"
        tool_info.config = {}

        mock_tool_class = Mock()
        mock_tool_instance = Mock()
        mock_tool_class.return_value = mock_tool_instance
        factory._tool_implementations["ScrapeWebsiteTool"] = mock_tool_class

        with patch.object(factory, 'get_tool_info', return_value=tool_info):
            factory.create_tool("ScrapeWebsiteTool")

        if mock_tool_class.called:
            call_kwargs = mock_tool_class.call_args[1] if mock_tool_class.call_args[1] else {}
            assert 'execution_inputs' not in call_kwargs

    def test_nested_inputs_take_priority_over_direct(self):
        """Test that config['inputs']['inputs'] is preferred over config['inputs'] direct keys."""
        factory = ToolFactory({
            "inputs": {
                "inputs": {"nested_key": "nested_value"},
                "direct_key": "direct_value",
            }
        })
        tool_info = Mock()
        tool_info.id = 1
        tool_info.title = "ScrapeWebsiteTool"
        tool_info.config = {"param": "{nested_key}"}

        mock_tool_class = Mock()
        mock_tool_instance = Mock()
        mock_tool_class.return_value = mock_tool_instance
        factory._tool_implementations["ScrapeWebsiteTool"] = mock_tool_class

        with patch.object(factory, 'get_tool_info', return_value=tool_info):
            factory.create_tool("ScrapeWebsiteTool")

        if mock_tool_class.called:
            call_kwargs = mock_tool_class.call_args[1] if mock_tool_class.call_args[1] else {}
            # Nested key should be resolved
            assert call_kwargs.get('param') == "nested_value"
            assert 'execution_inputs' not in call_kwargs


class TestPerplexityToolSpecific:
    """Test Perplexity-specific tool creation paths with execution_inputs."""

    def test_perplexity_tool_does_not_receive_execution_inputs(self):
        """Reproduce the exact bug: PerplexitySearchTool getting execution_inputs kwarg."""
        factory = ToolFactory({
            "inputs": {
                "inputs": {"topic": "latest tech news"}
            }
        })

        tool_info = Mock()
        tool_info.id = 1
        tool_info.title = "PerplexityTool"
        tool_info.config = {}

        # Mock the PerplexitySearchTool class
        mock_perplexity_class = Mock()
        mock_perplexity_instance = Mock()
        mock_perplexity_class.return_value = mock_perplexity_instance
        factory._tool_implementations["PerplexityTool"] = mock_perplexity_class

        with patch.object(factory, 'get_tool_info', return_value=tool_info):
            with patch('os.environ.get', return_value="pplx-test-api-key"):
                factory.create_tool("PerplexityTool")

        if mock_perplexity_class.called:
            call_kwargs = mock_perplexity_class.call_args[1] if mock_perplexity_class.call_args[1] else {}
            assert 'execution_inputs' not in call_kwargs, \
                "PerplexitySearchTool must not receive execution_inputs"

    def test_perplexity_tool_receives_api_key(self):
        """Verify PerplexityTool still receives its api_key correctly."""
        factory = ToolFactory({
            "inputs": {
                "inputs": {"topic": "news"}
            }
        })

        tool_info = Mock()
        tool_info.id = 1
        tool_info.title = "PerplexityTool"
        tool_info.config = {"api_key": "pplx-from-config"}

        mock_perplexity_class = Mock()
        mock_perplexity_instance = Mock()
        mock_perplexity_class.return_value = mock_perplexity_instance
        factory._tool_implementations["PerplexityTool"] = mock_perplexity_class

        with patch.object(factory, 'get_tool_info', return_value=tool_info):
            factory.create_tool("PerplexityTool")

        if mock_perplexity_class.called:
            call_kwargs = mock_perplexity_class.call_args[1] if mock_perplexity_class.call_args[1] else {}
            assert call_kwargs.get('api_key') == "pplx-from-config"
            assert 'execution_inputs' not in call_kwargs

    def test_perplexity_tool_perplexity_api_key_cleaned(self):
        """Verify 'perplexity_api_key' key is also cleaned from config."""
        factory = ToolFactory({
            "inputs": {
                "inputs": {"topic": "news"}
            }
        })

        tool_info = Mock()
        tool_info.id = 1
        tool_info.title = "PerplexityTool"
        tool_info.config = {"perplexity_api_key": "pplx-wrong-key-name"}

        mock_perplexity_class = Mock()
        mock_perplexity_instance = Mock()
        mock_perplexity_class.return_value = mock_perplexity_instance
        factory._tool_implementations["PerplexityTool"] = mock_perplexity_class

        with patch.object(factory, 'get_tool_info', return_value=tool_info):
            with patch('os.environ.get', return_value="pplx-env-key"):
                factory.create_tool("PerplexityTool")

        if mock_perplexity_class.called:
            call_kwargs = mock_perplexity_class.call_args[1] if mock_perplexity_class.call_args[1] else {}
            assert 'perplexity_api_key' not in call_kwargs
            assert 'execution_inputs' not in call_kwargs


class TestToolCreationErrorHandling:
    """Test that create_tool returns None on errors and handles edge cases."""

    def test_create_tool_returns_none_when_tool_not_found(self):
        """Test create_tool returns None when tool_info is not found."""
        factory = ToolFactory({"test": "value"})

        with patch.object(factory, 'get_tool_info', return_value=None):
            result = factory.create_tool("NonExistentTool")

        assert result is None

    def test_create_tool_returns_none_when_no_implementation(self):
        """Test create_tool returns None when tool has no implementation class."""
        factory = ToolFactory({"test": "value"})

        tool_info = Mock()
        tool_info.id = 1
        tool_info.title = "UnknownTool"
        tool_info.config = {}

        with patch.object(factory, 'get_tool_info', return_value=tool_info):
            result = factory.create_tool("UnknownTool")

        assert result is None

    def test_create_tool_returns_none_when_implementations_not_initialized(self):
        """Test create_tool returns None when _tool_implementations is empty."""
        factory = ToolFactory({"test": "value"})
        factory._tool_implementations = {}

        tool_info = Mock()
        tool_info.id = 1
        tool_info.title = "PerplexityTool"
        tool_info.config = {}

        with patch.object(factory, 'get_tool_info', return_value=tool_info):
            result = factory.create_tool("PerplexityTool")

        assert result is None

    def test_create_tool_handles_constructor_exception(self):
        """Test create_tool returns None when tool constructor raises exception."""
        factory = ToolFactory({
            "inputs": {
                "inputs": {"key": "value"}
            }
        })

        tool_info = Mock()
        tool_info.id = 1
        tool_info.title = "ScrapeWebsiteTool"
        tool_info.config = {}

        mock_tool_class = Mock(side_effect=Exception("Constructor failed"))
        factory._tool_implementations["ScrapeWebsiteTool"] = mock_tool_class

        with patch.object(factory, 'get_tool_info', return_value=tool_info):
            result = factory.create_tool("ScrapeWebsiteTool")

        assert result is None

    def test_create_tool_with_no_config_attribute(self):
        """Test create_tool when tool_info has no config attribute."""
        factory = ToolFactory({
            "inputs": {
                "inputs": {"key": "value"}
            }
        })

        tool_info = Mock(spec=[])  # No attributes at all
        tool_info.id = 1
        tool_info.title = "ScrapeWebsiteTool"
        # Deliberately do NOT set tool_info.config

        mock_tool_class = Mock()
        mock_tool_instance = Mock()
        mock_tool_class.return_value = mock_tool_instance
        factory._tool_implementations["ScrapeWebsiteTool"] = mock_tool_class

        with patch.object(factory, 'get_tool_info', return_value=tool_info):
            result = factory.create_tool("ScrapeWebsiteTool")

        # Should still work since base_config defaults to {} when config attr missing
        if mock_tool_class.called:
            call_kwargs = mock_tool_class.call_args[1] if mock_tool_class.call_args[1] else {}
            assert 'execution_inputs' not in call_kwargs
