"""
Unit tests for prompt templates seed module.

Tests the DEFAULT_TEMPLATES data structure, template constants, and seed functions.
"""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from src.seeds.prompt_templates import (
    DEFAULT_TEMPLATES,
    GENERATE_AGENT_TEMPLATE,
    GENERATE_CONNECTIONS_TEMPLATE,
    GENERATE_JOB_NAME_TEMPLATE,
    GENERATE_TASK_TEMPLATE,
    GENERATE_TEMPLATES_TEMPLATE,
    GENERATE_CREW_TEMPLATE,
    GENERATE_CREW_PLAN_TEMPLATE,
    DETECT_INTENT_TEMPLATE,
    seed_async,
    seed,
)


class TestDefaultTemplatesDataStructure:
    """Test cases for DEFAULT_TEMPLATES data integrity."""

    def test_default_templates_is_list(self):
        """Test that DEFAULT_TEMPLATES is a list."""
        assert isinstance(DEFAULT_TEMPLATES, list)

    def test_default_templates_not_empty(self):
        """Test that DEFAULT_TEMPLATES contains entries."""
        assert len(DEFAULT_TEMPLATES) > 0

    def test_required_fields_present(self):
        """Test that every template has the required fields."""
        required_fields = ["name", "description", "template", "is_active"]
        for tpl in DEFAULT_TEMPLATES:
            for field in required_fields:
                assert field in tpl, (
                    f"Template '{tpl.get('name', 'unknown')}' missing field '{field}'"
                )

    def test_field_types(self):
        """Test that template field types are correct."""
        for tpl in DEFAULT_TEMPLATES:
            assert isinstance(tpl["name"], str)
            assert isinstance(tpl["description"], str)
            assert isinstance(tpl["template"], str)
            assert isinstance(tpl["is_active"], bool)

    def test_expected_template_names(self):
        """Test that all expected template names are present."""
        names = [t["name"] for t in DEFAULT_TEMPLATES]
        expected = [
            "generate_agent",
            "generate_connections",
            "generate_job_name",
            "generate_task",
            "generate_templates",
            "generate_crew",
            "detect_intent",
        ]
        for expected_name in expected:
            assert expected_name in names, f"Missing template: {expected_name}"

    def test_unique_names(self):
        """Test that all template names are unique."""
        names = [t["name"] for t in DEFAULT_TEMPLATES]
        assert len(names) == len(set(names)), "Duplicate template names found"

    def test_all_active_by_default(self):
        """Test that all default templates are active."""
        for tpl in DEFAULT_TEMPLATES:
            assert tpl["is_active"] is True, (
                f"Template '{tpl['name']}' should be active"
            )

    def test_descriptions_not_empty(self):
        """Test that all templates have non-empty descriptions."""
        for tpl in DEFAULT_TEMPLATES:
            assert len(tpl["description"].strip()) > 0, (
                f"Template '{tpl['name']}' has empty description"
            )

    def test_template_content_not_empty(self):
        """Test that all template content strings are non-trivial."""
        for tpl in DEFAULT_TEMPLATES:
            assert len(tpl["template"].strip()) > 50, (
                f"Template '{tpl['name']}' has suspiciously short content"
            )


class TestTemplateConstants:
    """Test cases for individual template constant strings."""

    def test_all_constants_are_non_empty_strings(self):
        """Test that all template constants are non-empty strings."""
        constants = [
            GENERATE_AGENT_TEMPLATE,
            GENERATE_CONNECTIONS_TEMPLATE,
            GENERATE_JOB_NAME_TEMPLATE,
            GENERATE_TASK_TEMPLATE,
            GENERATE_TEMPLATES_TEMPLATE,
            GENERATE_CREW_TEMPLATE,
            DETECT_INTENT_TEMPLATE,
        ]
        for const in constants:
            assert isinstance(const, str)
            assert len(const.strip()) > 0

    def test_agent_template_has_json_instructions(self):
        """Test generate agent template contains JSON instructions."""
        assert "CRITICAL OUTPUT INSTRUCTIONS" in GENERATE_AGENT_TEMPLATE
        assert "JSON" in GENERATE_AGENT_TEMPLATE

    def test_connections_template_structure(self):
        """Test generate connections template has expected structure."""
        assert "assignments" in GENERATE_CONNECTIONS_TEMPLATE
        assert "dependencies" in GENERATE_CONNECTIONS_TEMPLATE

    def test_job_name_template_concise(self):
        """Test generate job name template asks for concise output."""
        assert "concise" in GENERATE_JOB_NAME_TEMPLATE
        assert "2-4 words" in GENERATE_JOB_NAME_TEMPLATE

    def test_task_template_has_json_schema(self):
        """Test generate task template includes JSON schema."""
        assert "expected_output" in GENERATE_TASK_TEMPLATE
        assert "advanced_config" in GENERATE_TASK_TEMPLATE

    def test_templates_template_has_parameters(self):
        """Test generate templates template contains placeholder parameters."""
        assert "{role}" in GENERATE_TEMPLATES_TEMPLATE
        assert "{goal}" in GENERATE_TEMPLATES_TEMPLATE
        assert "{backstory}" in GENERATE_TEMPLATES_TEMPLATE
        assert "{input}" in GENERATE_TEMPLATES_TEMPLATE
        assert "{context}" in GENERATE_TEMPLATES_TEMPLATE

    def test_crew_template_has_agents_and_tasks(self):
        """Test generate crew template contains agents and tasks structures."""
        assert "agents" in GENERATE_CREW_TEMPLATE
        assert "tasks" in GENERATE_CREW_TEMPLATE
        assert "CRITICAL OUTPUT INSTRUCTIONS" in GENERATE_CREW_TEMPLATE

    def test_detect_intent_template_has_categories(self):
        """Test detect intent template contains all intent categories."""
        expected_intents = [
            "generate_task",
            "generate_agent",
            "generate_crew",
            "execute_crew",
            "configure_crew",
            "unknown",
        ]
        for intent in expected_intents:
            assert intent in DETECT_INTENT_TEMPLATE

    def test_json_templates_mention_json(self):
        """Test that JSON-returning templates reference JSON format."""
        json_templates = [
            GENERATE_AGENT_TEMPLATE,
            GENERATE_CONNECTIONS_TEMPLATE,
            GENERATE_TASK_TEMPLATE,
            GENERATE_CREW_TEMPLATE,
            DETECT_INTENT_TEMPLATE,
        ]
        for tpl in json_templates:
            assert "JSON" in tpl or "json" in tpl


class TestSeedAsyncFunction:
    """Test cases for the seed_async function."""

    @pytest.mark.asyncio
    async def test_seed_async_adds_new_templates(self):
        """Test that seed_async adds templates when none exist."""
        mock_session = AsyncMock()

        # First call returns empty existing names, subsequent calls return no match
        initial_result = MagicMock()
        initial_result.scalars.return_value.all.return_value = []

        template_result = MagicMock()
        template_result.scalars.return_value.first.return_value = None

        call_count = [0]

        async def mock_execute(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] == 1:
                return initial_result
            return template_result

        mock_session.execute = AsyncMock(side_effect=mock_execute)
        mock_session.commit = AsyncMock()
        mock_session.rollback = AsyncMock()
        mock_session.add = MagicMock()

        mock_context = AsyncMock()
        mock_context.__aenter__.return_value = mock_session
        mock_context.__aexit__.return_value = None

        with patch("src.seeds.prompt_templates.async_session_factory", return_value=mock_context):
            await seed_async()

        # Should have added templates
        assert mock_session.add.call_count > 0
        mock_session.commit.assert_awaited()


class TestSeedEntryPoint:
    """Test cases for the main seed() entry point."""

    @pytest.mark.asyncio
    async def test_seed_calls_seed_async(self):
        """Test that seed() delegates to seed_async()."""
        with patch("src.seeds.prompt_templates.seed_async", new_callable=AsyncMock) as mock:
            await seed()
            mock.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_seed_does_not_raise_on_error(self):
        """Test that seed() suppresses exceptions and logs them."""
        with patch("src.seeds.prompt_templates.seed_async", new_callable=AsyncMock) as mock:
            mock.side_effect = Exception("Seed failure")
            await seed()
            mock.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_seed_logs_traceback_on_error(self):
        """Test that seed() logs traceback information on error."""
        with patch("src.seeds.prompt_templates.seed_async", new_callable=AsyncMock) as mock_async:
            with patch("src.seeds.prompt_templates.logger") as mock_logger:
                mock_async.side_effect = Exception("Test error")
                await seed()

                mock_logger.info.assert_any_call(
                    "Starting prompt templates seeding process..."
                )
                # Should log the error
                assert mock_logger.error.call_count >= 1


class TestGenerateCrewPlanTemplate:
    """Test cases for the generate_crew_plan template."""

    def test_generate_crew_plan_template_exists(self):
        """Test that generate_crew_plan template key exists in DEFAULT_TEMPLATES."""
        names = [t["name"] for t in DEFAULT_TEMPLATES]
        assert "generate_crew_plan" in names

    def test_generate_crew_plan_template_has_json_structure(self):
        """Test that generate_crew_plan template contains agents/tasks structure."""
        assert "agents" in GENERATE_CREW_PLAN_TEMPLATE
        assert "tasks" in GENERATE_CREW_PLAN_TEMPLATE
        assert '"name"' in GENERATE_CREW_PLAN_TEMPLATE
        assert '"role"' in GENERATE_CREW_PLAN_TEMPLATE
        assert '"assigned_agent"' in GENERATE_CREW_PLAN_TEMPLATE

    def test_generate_crew_plan_template_complexity_tiers(self):
        """Test that generate_crew_plan template defines light/standard/complex tiers."""
        assert "light" in GENERATE_CREW_PLAN_TEMPLATE
        assert "standard" in GENERATE_CREW_PLAN_TEMPLATE
        assert "complex" in GENERATE_CREW_PLAN_TEMPLATE

    def test_generate_crew_plan_template_process_types(self):
        """Test that generate_crew_plan template defines sequential/parallel process types."""
        assert "sequential" in GENERATE_CREW_PLAN_TEMPLATE
        assert "parallel" in GENERATE_CREW_PLAN_TEMPLATE

    def test_generate_crew_plan_template_is_active(self):
        """Test that generate_crew_plan template is active."""
        plan_template = next(
            t for t in DEFAULT_TEMPLATES if t["name"] == "generate_crew_plan"
        )
        assert plan_template["is_active"] is True

    def test_generate_crew_plan_template_has_context_field(self):
        """Test that generate_crew_plan template includes context dependency instructions."""
        assert '"context"' in GENERATE_CREW_PLAN_TEMPLATE


class TestTaskTemplateToolCatalog:
    """Test cases for tool catalog and available tools in the task template."""

    def test_task_template_tool_catalog(self):
        """Test that GENERATE_TASK_TEMPLATE references key tools from the catalog."""
        assert "GenieTool" in GENERATE_TASK_TEMPLATE
        assert "SerperDevTool" in GENERATE_TASK_TEMPLATE
        assert "ScrapeWebsiteTool" in GENERATE_TASK_TEMPLATE
        assert "PerplexityTool" in GENERATE_TASK_TEMPLATE
        assert "DatabricksKnowledgeSearchTool" in GENERATE_TASK_TEMPLATE

    def test_task_template_available_tools_placeholder(self):
        """Test that GENERATE_TASK_TEMPLATE references 'Available tools' for assignment."""
        assert "Available tools" in GENERATE_TASK_TEMPLATE
