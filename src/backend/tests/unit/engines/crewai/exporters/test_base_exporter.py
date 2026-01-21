"""
Unit tests for base exporter class.
"""

import pytest
from datetime import datetime, timezone
from unittest.mock import patch

from src.engines.crewai.exporters.base_exporter import BaseExporter


class ConcreteExporter(BaseExporter):
    """Concrete implementation of BaseExporter for testing."""

    async def export(self, crew_data, options):
        """Implement abstract export method."""
        return {'exported': True}


class TestSanitizeName:
    """Tests for _sanitize_name method."""

    @pytest.fixture
    def exporter(self):
        """Create a concrete exporter instance."""
        return ConcreteExporter()

    def test_sanitize_simple_name(self, exporter):
        """Test sanitizing a simple name."""
        result = exporter._sanitize_name("My Crew")
        assert result == "my_crew"

    def test_sanitize_name_with_special_chars(self, exporter):
        """Test sanitizing a name with special characters."""
        result = exporter._sanitize_name("My-Crew@123!")
        assert result == "my_crew_123"

    def test_sanitize_name_with_multiple_spaces(self, exporter):
        """Test sanitizing a name with multiple spaces."""
        result = exporter._sanitize_name("My   Crew   Name")
        assert result == "my_crew_name"

    def test_sanitize_name_starting_with_number(self, exporter):
        """Test sanitizing a name starting with a number."""
        result = exporter._sanitize_name("123 Crew")
        assert result == "crew_123_crew"

    def test_sanitize_name_with_underscores(self, exporter):
        """Test sanitizing a name with existing underscores."""
        result = exporter._sanitize_name("my__crew__name")
        assert result == "my_crew_name"

    def test_sanitize_name_leading_trailing_underscores(self, exporter):
        """Test sanitizing a name with leading/trailing underscores."""
        result = exporter._sanitize_name("_my_crew_")
        assert result == "my_crew"

    def test_sanitize_empty_name(self, exporter):
        """Test sanitizing an empty name."""
        result = exporter._sanitize_name("")
        assert result == "crew"

    def test_sanitize_name_only_special_chars(self, exporter):
        """Test sanitizing a name with only special characters."""
        result = exporter._sanitize_name("@#$%")
        assert result == "crew"

    def test_sanitize_name_unicode(self, exporter):
        """Test sanitizing a name with unicode characters."""
        result = exporter._sanitize_name("Crëw Nàme")
        # Unicode alphanumeric characters are preserved
        assert result == "crëw_nàme"

    def test_sanitize_name_preserves_underscores(self, exporter):
        """Test that valid underscores are preserved."""
        result = exporter._sanitize_name("my_valid_name")
        assert result == "my_valid_name"


class TestGetTimestamp:
    """Tests for _get_timestamp method."""

    @pytest.fixture
    def exporter(self):
        """Create a concrete exporter instance."""
        return ConcreteExporter()

    def test_timestamp_format(self, exporter):
        """Test that timestamp is in correct format."""
        result = exporter._get_timestamp()
        # Should match format: YYYY-MM-DD HH:MM:SS UTC
        assert "UTC" in result
        parts = result.split(" ")
        assert len(parts) == 3  # Date, Time, UTC

    def test_timestamp_has_correct_date_format(self, exporter):
        """Test that date part is correctly formatted."""
        result = exporter._get_timestamp()
        date_part = result.split(" ")[0]
        # YYYY-MM-DD format
        year, month, day = date_part.split("-")
        assert len(year) == 4
        assert len(month) == 2
        assert len(day) == 2


class TestExtractToolsFromConfig:
    """Tests for _extract_tools_from_config method."""

    @pytest.fixture
    def exporter(self):
        """Create a concrete exporter instance."""
        return ConcreteExporter()

    def test_extract_tools_with_tools_list(self, exporter):
        """Test extracting tools from config with tools list."""
        config = {'tools': ['SerperDevTool', 'ScrapeWebsiteTool']}
        result = exporter._extract_tools_from_config(config)
        assert result == ['SerperDevTool', 'ScrapeWebsiteTool']

    def test_extract_tools_without_tools_key(self, exporter):
        """Test extracting tools from config without tools key."""
        config = {'name': 'Agent', 'role': 'Researcher'}
        result = exporter._extract_tools_from_config(config)
        assert result == []

    def test_extract_tools_with_empty_list(self, exporter):
        """Test extracting tools from config with empty tools list."""
        config = {'tools': []}
        result = exporter._extract_tools_from_config(config)
        assert result == []

    def test_extract_tools_with_non_list_tools(self, exporter):
        """Test extracting tools when tools is not a list."""
        config = {'tools': 'SerperDevTool'}  # String instead of list
        result = exporter._extract_tools_from_config(config)
        assert result == []


class TestGetUniqueTools:
    """Tests for _get_unique_tools method."""

    @pytest.fixture
    def exporter(self):
        """Create a concrete exporter instance."""
        return ConcreteExporter()

    def test_unique_tools_from_agents_and_tasks(self, exporter):
        """Test getting unique tools from both agents and tasks."""
        agents = [
            {'name': 'Agent1', 'tools': ['SerperDevTool', 'DallETool']},
            {'name': 'Agent2', 'tools': ['SerperDevTool']},
        ]
        tasks = [
            {'name': 'Task1', 'tools': ['ScrapeWebsiteTool']},
        ]

        result = exporter._get_unique_tools(agents, tasks)
        assert sorted(result) == ['DallETool', 'ScrapeWebsiteTool', 'SerperDevTool']

    def test_unique_tools_removes_duplicates(self, exporter):
        """Test that duplicate tools are removed."""
        agents = [
            {'name': 'Agent1', 'tools': ['SerperDevTool']},
            {'name': 'Agent2', 'tools': ['SerperDevTool']},
        ]
        tasks = [
            {'name': 'Task1', 'tools': ['SerperDevTool']},
        ]

        result = exporter._get_unique_tools(agents, tasks)
        assert result == ['SerperDevTool']

    def test_unique_tools_empty_input(self, exporter):
        """Test with empty agents and tasks."""
        result = exporter._get_unique_tools([], [])
        assert result == []

    def test_unique_tools_no_tools_defined(self, exporter):
        """Test with agents/tasks that have no tools."""
        agents = [{'name': 'Agent1', 'role': 'Researcher'}]
        tasks = [{'name': 'Task1', 'description': 'Do something'}]

        result = exporter._get_unique_tools(agents, tasks)
        assert result == []


class TestFormatDocstring:
    """Tests for _format_docstring method."""

    @pytest.fixture
    def exporter(self):
        """Create a concrete exporter instance."""
        return ConcreteExporter()

    def test_format_single_line_docstring(self, exporter):
        """Test formatting a single line docstring."""
        result = exporter._format_docstring("Simple description")
        assert result == '"""Simple description"""'

    def test_format_empty_docstring(self, exporter):
        """Test formatting an empty docstring."""
        result = exporter._format_docstring("")
        assert result == '"""TODO: Add description"""'

    def test_format_none_docstring(self, exporter):
        """Test formatting a None docstring."""
        result = exporter._format_docstring(None)
        assert result == '"""TODO: Add description"""'

    def test_format_multiline_docstring(self, exporter):
        """Test formatting a multi-line docstring."""
        text = "First line\nSecond line\nThird line"
        result = exporter._format_docstring(text)
        assert '"""' in result
        assert "First line" in result
        assert "Second line" in result

    def test_format_docstring_with_whitespace(self, exporter):
        """Test formatting a docstring with leading/trailing whitespace."""
        result = exporter._format_docstring("  Trimmed text  ")
        assert result == '"""Trimmed text"""'


class TestGetLLMModel:
    """Tests for _get_llm_model method."""

    @pytest.fixture
    def exporter(self):
        """Create a concrete exporter instance."""
        return ConcreteExporter()

    def test_get_llm_model_from_config(self, exporter):
        """Test getting LLM model from config."""
        config = {'llm': 'databricks-meta-llama-3-1-70b'}
        result = exporter._get_llm_model(config)
        assert result == 'databricks-meta-llama-3-1-70b'

    def test_get_llm_model_default(self, exporter):
        """Test getting default LLM model when not in config."""
        config = {}
        result = exporter._get_llm_model(config)
        assert result == 'databricks-llama-4-maverick'

    def test_get_llm_model_custom_default(self, exporter):
        """Test getting custom default LLM model."""
        config = {}
        result = exporter._get_llm_model(config, default='custom-model')
        assert result == 'custom-model'


class TestGetToolImports:
    """Tests for _get_tool_imports method."""

    @pytest.fixture
    def exporter(self):
        """Create a concrete exporter instance."""
        return ConcreteExporter()

    @pytest.mark.asyncio
    async def test_get_tool_imports_standard_tools(self, exporter):
        """Test getting imports for standard tools."""
        tools = ['SerperDevTool', 'ScrapeWebsiteTool']
        result = await exporter._get_tool_imports(tools)

        assert 'from crewai_tools import SerperDevTool' in result
        assert 'from crewai_tools import ScrapeWebsiteTool' in result

    @pytest.mark.asyncio
    async def test_get_tool_imports_custom_tool(self, exporter):
        """Test getting imports for custom tools."""
        tools = ['CustomTool']
        result = await exporter._get_tool_imports(tools)

        # Custom tools should not have standard imports
        assert len(result) == 0

    @pytest.mark.asyncio
    async def test_get_tool_imports_empty_list(self, exporter):
        """Test getting imports with empty tools list."""
        result = await exporter._get_tool_imports([])
        assert result == []

    @pytest.mark.asyncio
    async def test_get_tool_imports_mixed_tools(self, exporter):
        """Test getting imports with mix of standard and custom tools."""
        tools = ['SerperDevTool', 'CustomTool', 'DallETool']
        result = await exporter._get_tool_imports(tools)

        assert 'from crewai_tools import SerperDevTool' in result
        assert 'from crewai_tools import DallETool' in result
        # Custom tool should not generate an import


class TestAbstractExport:
    """Tests for the abstract export method."""

    @pytest.mark.asyncio
    async def test_concrete_export_called(self):
        """Test that concrete export method can be called."""
        exporter = ConcreteExporter()
        result = await exporter.export({'name': 'Test'}, {})
        assert result == {'exported': True}
