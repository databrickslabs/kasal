"""
Unit tests for prompt templates seed module.

Tests the DEFAULT_TEMPLATES data structure, template constants, and seed functions.
"""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch, call

from src.seeds.prompt_templates import (
    DEFAULT_TEMPLATES,
    GENERATE_AGENT_TEMPLATE,
    GENERATE_CONNECTIONS_TEMPLATE,
    GENERATE_JOB_NAME_TEMPLATE,
    GENERATE_TASK_TEMPLATE,
    GENERATE_TEMPLATES_TEMPLATE,
    GENERATE_CREW_TEMPLATE,
    DETECT_INTENT_TEMPLATE,
    seed_async,
    seed,
    seed_sync,
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


class TestSeedAsyncRaceConditionUpdate:
    """Test seed_async when a template is not in existing_names but found in DB (race condition)."""

    @pytest.mark.asyncio
    async def test_seed_async_race_condition_updates_existing(self):
        """Lines 826-831: template not in existing_names but found in DB check."""
        mock_existing_template = MagicMock()

        # Use a single-item DEFAULT_TEMPLATES to keep it simple
        test_templates = [DEFAULT_TEMPLATES[0]]

        # Session for initial query (returns empty existing_names)
        mock_initial_session = AsyncMock()
        initial_result = MagicMock()
        initial_result.scalars.return_value.all.return_value = []
        mock_initial_session.execute = AsyncMock(return_value=initial_result)

        # Session for per-template work: name not in existing_names, but DB returns a template
        mock_template_session = AsyncMock()
        template_result = MagicMock()
        template_result.scalars.return_value.first.return_value = mock_existing_template
        mock_template_session.execute = AsyncMock(return_value=template_result)
        mock_template_session.commit = AsyncMock()
        mock_template_session.add = MagicMock()

        session_call_count = [0]

        def session_factory():
            ctx = AsyncMock()
            session_call_count[0] += 1
            if session_call_count[0] == 1:
                ctx.__aenter__.return_value = mock_initial_session
            else:
                ctx.__aenter__.return_value = mock_template_session
            return ctx

        with patch("src.seeds.prompt_templates.async_session_factory", side_effect=session_factory):
            with patch("src.seeds.prompt_templates.DEFAULT_TEMPLATES", test_templates):
                await seed_async()

        # Should have updated the existing template's fields (race condition path)
        assert mock_existing_template.description == test_templates[0]["description"]
        assert mock_existing_template.template == test_templates[0]["template"]
        assert mock_existing_template.is_active == test_templates[0]["is_active"]
        assert mock_existing_template.updated_at is not None


class TestSeedAsyncUpdateExistingNames:
    """Test seed_async when template name IS in existing_names (lines 847-858)."""

    @pytest.mark.asyncio
    async def test_seed_async_updates_template_in_existing_names(self):
        """Lines 847-858: template is in existing_names, fetched and updated."""
        mock_existing_template = MagicMock()
        test_templates = [DEFAULT_TEMPLATES[0]]
        template_name = test_templates[0]["name"]

        # Session for initial query: returns the template name as existing
        # Note: code does {row[0] for row in result.scalars().all()}
        # so we return tuples so row[0] gives the full name
        mock_initial_session = AsyncMock()
        initial_result = MagicMock()
        initial_result.scalars.return_value.all.return_value = [(template_name,)]
        mock_initial_session.execute = AsyncMock(return_value=initial_result)

        # Session for per-template work
        mock_template_session = AsyncMock()
        update_result = MagicMock()
        update_result.scalars.return_value.first.return_value = mock_existing_template
        mock_template_session.execute = AsyncMock(return_value=update_result)
        mock_template_session.commit = AsyncMock()

        session_call_count = [0]

        def session_factory():
            ctx = AsyncMock()
            session_call_count[0] += 1
            if session_call_count[0] == 1:
                ctx.__aenter__.return_value = mock_initial_session
            else:
                ctx.__aenter__.return_value = mock_template_session
            return ctx

        with patch("src.seeds.prompt_templates.async_session_factory", side_effect=session_factory):
            with patch("src.seeds.prompt_templates.DEFAULT_TEMPLATES", test_templates):
                await seed_async()

        # Should have updated existing template
        assert mock_existing_template.description == test_templates[0]["description"]
        assert mock_existing_template.template == test_templates[0]["template"]
        assert mock_existing_template.is_active == test_templates[0]["is_active"]


class TestSeedAsyncCommitExceptions:
    """Test seed_async commit exception handling (lines 863-874)."""

    @pytest.mark.asyncio
    async def test_seed_async_unique_constraint_error_on_commit(self):
        """Lines 863-867: UNIQUE constraint failed on commit triggers skip."""
        test_templates = [DEFAULT_TEMPLATES[0]]

        mock_initial_session = AsyncMock()
        initial_result = MagicMock()
        initial_result.scalars.return_value.all.return_value = []
        mock_initial_session.execute = AsyncMock(return_value=initial_result)

        mock_template_session = AsyncMock()
        template_result = MagicMock()
        template_result.scalars.return_value.first.return_value = None
        mock_template_session.execute = AsyncMock(return_value=template_result)
        mock_template_session.commit = AsyncMock(
            side_effect=Exception("UNIQUE constraint failed: prompt_templates.name")
        )
        mock_template_session.rollback = AsyncMock()
        mock_template_session.add = MagicMock()

        session_call_count = [0]

        def session_factory():
            ctx = AsyncMock()
            session_call_count[0] += 1
            if session_call_count[0] == 1:
                ctx.__aenter__.return_value = mock_initial_session
            else:
                ctx.__aenter__.return_value = mock_template_session
            return ctx

        with patch("src.seeds.prompt_templates.async_session_factory", side_effect=session_factory):
            with patch("src.seeds.prompt_templates.DEFAULT_TEMPLATES", test_templates):
                with patch("src.seeds.prompt_templates.logger") as mock_logger:
                    await seed_async()

        mock_template_session.rollback.assert_awaited()
        mock_logger.warning.assert_called()

    @pytest.mark.asyncio
    async def test_seed_async_other_commit_error(self):
        """Lines 868-870: non-unique commit error logs error."""
        test_templates = [DEFAULT_TEMPLATES[0]]

        mock_initial_session = AsyncMock()
        initial_result = MagicMock()
        initial_result.scalars.return_value.all.return_value = []
        mock_initial_session.execute = AsyncMock(return_value=initial_result)

        mock_template_session = AsyncMock()
        template_result = MagicMock()
        template_result.scalars.return_value.first.return_value = None
        mock_template_session.execute = AsyncMock(return_value=template_result)
        mock_template_session.commit = AsyncMock(
            side_effect=Exception("Some other database error")
        )
        mock_template_session.rollback = AsyncMock()
        mock_template_session.add = MagicMock()

        session_call_count = [0]

        def session_factory():
            ctx = AsyncMock()
            session_call_count[0] += 1
            if session_call_count[0] == 1:
                ctx.__aenter__.return_value = mock_initial_session
            else:
                ctx.__aenter__.return_value = mock_template_session
            return ctx

        with patch("src.seeds.prompt_templates.async_session_factory", side_effect=session_factory):
            with patch("src.seeds.prompt_templates.DEFAULT_TEMPLATES", test_templates):
                with patch("src.seeds.prompt_templates.logger") as mock_logger:
                    await seed_async()

        mock_template_session.rollback.assert_awaited()
        # Should log error (not warning)
        mock_logger.error.assert_called()

    @pytest.mark.asyncio
    async def test_seed_async_outer_exception(self):
        """Lines 871-874: outer exception handler for template processing."""
        test_templates = [DEFAULT_TEMPLATES[0]]

        mock_initial_session = AsyncMock()
        initial_result = MagicMock()
        initial_result.scalars.return_value.all.return_value = []
        mock_initial_session.execute = AsyncMock(return_value=initial_result)

        # Make the context manager itself raise on __aenter__ for the template session
        session_call_count = [0]

        def session_factory():
            session_call_count[0] += 1
            if session_call_count[0] == 1:
                ctx = AsyncMock()
                ctx.__aenter__.return_value = mock_initial_session
                return ctx
            else:
                ctx = AsyncMock()
                ctx.__aenter__.side_effect = Exception("Connection failed")
                return ctx

        # We need to handle the fact that `session` in the except block
        # references the last assigned session variable. Since __aenter__ fails,
        # session won't be assigned. The code will try session.rollback() but
        # session is from initial_session scope. We need to mock it carefully.
        # Actually, the outer except uses the `session` variable which is from
        # the `async with` statement. Since __aenter__ fails, session won't be
        # bound. But Python's `with` will raise before binding. The except block
        # references `session` which would be the mock_initial_session from the
        # prior `async with` (the initial query). Let's just verify no crash.
        with patch("src.seeds.prompt_templates.async_session_factory", side_effect=session_factory):
            with patch("src.seeds.prompt_templates.DEFAULT_TEMPLATES", test_templates):
                with patch("src.seeds.prompt_templates.logger") as mock_logger:
                    await seed_async()

        mock_logger.error.assert_called()


class TestSeedSyncFunction:
    """Test cases for the seed_sync function (lines 880-957)."""

    def test_seed_sync_adds_new_templates(self):
        """Lines 880-925: seed_sync adds new templates when none exist."""
        test_templates = [DEFAULT_TEMPLATES[0]]

        mock_existing_session = MagicMock()
        initial_result = MagicMock()
        initial_result.scalars.return_value.all.return_value = []
        mock_existing_session.execute.return_value = initial_result

        mock_template_session = MagicMock()
        template_result = MagicMock()
        template_result.scalars.return_value.first.return_value = None
        mock_template_session.execute.return_value = template_result

        session_call_count = [0]

        def session_factory():
            ctx = MagicMock()
            session_call_count[0] += 1
            if session_call_count[0] == 1:
                ctx.__enter__.return_value = mock_existing_session
            else:
                ctx.__enter__.return_value = mock_template_session
            return ctx

        with patch("src.seeds.prompt_templates.SessionLocal", side_effect=session_factory, create=True):
            with patch("src.seeds.prompt_templates.DEFAULT_TEMPLATES", test_templates):
                seed_sync()

        mock_template_session.add.assert_called_once()
        mock_template_session.commit.assert_called_once()

    def test_seed_sync_race_condition_updates(self):
        """Lines 905-912: seed_sync race condition - not in existing_names but found in DB."""
        test_templates = [DEFAULT_TEMPLATES[0]]
        mock_existing_template = MagicMock()

        mock_existing_session = MagicMock()
        initial_result = MagicMock()
        initial_result.scalars.return_value.all.return_value = []
        mock_existing_session.execute.return_value = initial_result

        mock_template_session = MagicMock()
        template_result = MagicMock()
        template_result.scalars.return_value.first.return_value = mock_existing_template
        mock_template_session.execute.return_value = template_result

        session_call_count = [0]

        def session_factory():
            ctx = MagicMock()
            session_call_count[0] += 1
            if session_call_count[0] == 1:
                ctx.__enter__.return_value = mock_existing_session
            else:
                ctx.__enter__.return_value = mock_template_session
            return ctx

        with patch("src.seeds.prompt_templates.SessionLocal", side_effect=session_factory, create=True):
            with patch("src.seeds.prompt_templates.DEFAULT_TEMPLATES", test_templates):
                seed_sync()

        assert mock_existing_template.description == test_templates[0]["description"]
        assert mock_existing_template.is_active == test_templates[0]["is_active"]

    def test_seed_sync_updates_existing_names(self):
        """Lines 926-939: seed_sync updates template when name is in existing_names."""
        test_templates = [DEFAULT_TEMPLATES[0]]
        template_name = test_templates[0]["name"]
        mock_existing_template = MagicMock()

        mock_existing_session = MagicMock()
        initial_result = MagicMock()
        initial_result.scalars.return_value.all.return_value = [(template_name,)]
        mock_existing_session.execute.return_value = initial_result

        mock_template_session = MagicMock()
        update_result = MagicMock()
        update_result.scalars.return_value.first.return_value = mock_existing_template
        mock_template_session.execute.return_value = update_result

        session_call_count = [0]

        def session_factory():
            ctx = MagicMock()
            session_call_count[0] += 1
            if session_call_count[0] == 1:
                ctx.__enter__.return_value = mock_existing_session
            else:
                ctx.__enter__.return_value = mock_template_session
            return ctx

        with patch("src.seeds.prompt_templates.SessionLocal", side_effect=session_factory, create=True):
            with patch("src.seeds.prompt_templates.DEFAULT_TEMPLATES", test_templates):
                seed_sync()

        assert mock_existing_template.description == test_templates[0]["description"]
        assert mock_existing_template.template == test_templates[0]["template"]
        mock_template_session.commit.assert_called_once()

    def test_seed_sync_unique_constraint_on_commit(self):
        """Lines 944-948: seed_sync UNIQUE constraint on commit."""
        test_templates = [DEFAULT_TEMPLATES[0]]

        mock_existing_session = MagicMock()
        initial_result = MagicMock()
        initial_result.scalars.return_value.all.return_value = []
        mock_existing_session.execute.return_value = initial_result

        mock_template_session = MagicMock()
        template_result = MagicMock()
        template_result.scalars.return_value.first.return_value = None
        mock_template_session.execute.return_value = template_result
        mock_template_session.commit.side_effect = Exception(
            "UNIQUE constraint failed: prompt_templates.name"
        )

        session_call_count = [0]

        def session_factory():
            ctx = MagicMock()
            session_call_count[0] += 1
            if session_call_count[0] == 1:
                ctx.__enter__.return_value = mock_existing_session
            else:
                ctx.__enter__.return_value = mock_template_session
            return ctx

        with patch("src.seeds.prompt_templates.SessionLocal", side_effect=session_factory, create=True):
            with patch("src.seeds.prompt_templates.DEFAULT_TEMPLATES", test_templates):
                with patch("src.seeds.prompt_templates.logger") as mock_logger:
                    seed_sync()

        mock_template_session.rollback.assert_called_once()
        mock_logger.warning.assert_called()

    def test_seed_sync_other_commit_error(self):
        """Lines 949-951: seed_sync non-unique commit error."""
        test_templates = [DEFAULT_TEMPLATES[0]]

        mock_existing_session = MagicMock()
        initial_result = MagicMock()
        initial_result.scalars.return_value.all.return_value = []
        mock_existing_session.execute.return_value = initial_result

        mock_template_session = MagicMock()
        template_result = MagicMock()
        template_result.scalars.return_value.first.return_value = None
        mock_template_session.execute.return_value = template_result
        mock_template_session.commit.side_effect = Exception("Disk full")

        session_call_count = [0]

        def session_factory():
            ctx = MagicMock()
            session_call_count[0] += 1
            if session_call_count[0] == 1:
                ctx.__enter__.return_value = mock_existing_session
            else:
                ctx.__enter__.return_value = mock_template_session
            return ctx

        with patch("src.seeds.prompt_templates.SessionLocal", side_effect=session_factory, create=True):
            with patch("src.seeds.prompt_templates.DEFAULT_TEMPLATES", test_templates):
                with patch("src.seeds.prompt_templates.logger") as mock_logger:
                    seed_sync()

        mock_template_session.rollback.assert_called_once()
        mock_logger.error.assert_called()

    def test_seed_sync_outer_exception(self):
        """Lines 952-955: seed_sync outer exception handler."""
        test_templates = [DEFAULT_TEMPLATES[0]]

        mock_existing_session = MagicMock()
        initial_result = MagicMock()
        initial_result.scalars.return_value.all.return_value = []
        mock_existing_session.execute.return_value = initial_result

        session_call_count = [0]

        def session_factory():
            session_call_count[0] += 1
            if session_call_count[0] == 1:
                ctx = MagicMock()
                ctx.__enter__.return_value = mock_existing_session
                return ctx
            else:
                ctx = MagicMock()
                mock_inner_session = MagicMock()
                mock_inner_session.execute.side_effect = Exception("Connection lost")
                mock_inner_session.rollback = MagicMock()
                ctx.__enter__.return_value = mock_inner_session
                return ctx

        with patch("src.seeds.prompt_templates.SessionLocal", side_effect=session_factory, create=True):
            with patch("src.seeds.prompt_templates.DEFAULT_TEMPLATES", test_templates):
                with patch("src.seeds.prompt_templates.logger") as mock_logger:
                    seed_sync()

        mock_logger.error.assert_called()


class TestMainBlock:
    """Test the __main__ block (lines 974-975)."""

    def test_main_block_runs_seed(self):
        """Lines 974-975: __main__ block calls asyncio.run(seed())."""
        import runpy

        with patch("src.seeds.prompt_templates.seed", new_callable=AsyncMock) as mock_seed:
            with patch("asyncio.run") as mock_asyncio_run:
                runpy.run_module(
                    "src.seeds.prompt_templates",
                    run_name="__main__",
                    alter_sys=False,
                )

                mock_asyncio_run.assert_called_once()
