"""
Unit tests for prompt templates seeder.

Tests the functionality of prompt template seeder including
template definitions, database operations, and seeding logic.
"""
import pytest
from unittest.mock import patch, AsyncMock, MagicMock
from datetime import datetime

from src.seeds.prompt_templates import (
    DEFAULT_TEMPLATES, seed_async, seed_sync, seed,
    GENERATE_AGENT_TEMPLATE, GENERATE_CONNECTIONS_TEMPLATE, GENERATE_JOB_NAME_TEMPLATE,
    GENERATE_TASK_TEMPLATE, GENERATE_TEMPLATES_TEMPLATE, GENERATE_CREW_TEMPLATE
)


class TestTemplateConstants:
    """Test cases for template constants."""
    
    def test_generate_agent_template_content(self):
        """Test GENERATE_AGENT_TEMPLATE content."""
        assert isinstance(GENERATE_AGENT_TEMPLATE, str)
        assert "You are an expert at creating AI agents" in GENERATE_AGENT_TEMPLATE
        assert "CRITICAL OUTPUT INSTRUCTIONS" in GENERATE_AGENT_TEMPLATE
        assert "valid, parseable JSON object" in GENERATE_AGENT_TEMPLATE
        assert "databricks-llama-4-maverick" in GENERATE_AGENT_TEMPLATE
        assert '"name":' in GENERATE_AGENT_TEMPLATE
        assert '"role":' in GENERATE_AGENT_TEMPLATE
        assert '"goal":' in GENERATE_AGENT_TEMPLATE
        assert '"backstory":' in GENERATE_AGENT_TEMPLATE
    
    def test_generate_connections_template_content(self):
        """Test GENERATE_CONNECTIONS_TEMPLATE content."""
        assert isinstance(GENERATE_CONNECTIONS_TEMPLATE, str)
        assert "Analyze the provided agents and tasks" in GENERATE_CONNECTIONS_TEMPLATE
        assert "Task-to-agent assignments" in GENERATE_CONNECTIONS_TEMPLATE
        assert "Task dependencies" in GENERATE_CONNECTIONS_TEMPLATE
        assert '"assignments":' in GENERATE_CONNECTIONS_TEMPLATE
        assert '"dependencies":' in GENERATE_CONNECTIONS_TEMPLATE
        assert "reasoning" in GENERATE_CONNECTIONS_TEMPLATE
    
    def test_generate_job_name_template_content(self):
        """Test GENERATE_JOB_NAME_TEMPLATE content."""
        assert isinstance(GENERATE_JOB_NAME_TEMPLATE, str)
        assert "Generate a concise, descriptive name" in GENERATE_JOB_NAME_TEMPLATE
        assert "2-4 words" in GENERATE_JOB_NAME_TEMPLATE
        assert "Lebanese News Monitor" in GENERATE_JOB_NAME_TEMPLATE
        assert "Avoid generic terms" in GENERATE_JOB_NAME_TEMPLATE
    
    def test_generate_task_template_content(self):
        """Test GENERATE_TASK_TEMPLATE content."""
        assert isinstance(GENERATE_TASK_TEMPLATE, str)
        assert "expert in designing structured AI task configurations" in GENERATE_TASK_TEMPLATE
        assert "valid and well-formatted JSON object" in GENERATE_TASK_TEMPLATE
        assert '"name":' in GENERATE_TASK_TEMPLATE
        assert '"description":' in GENERATE_TASK_TEMPLATE
        assert '"expected_output":' in GENERATE_TASK_TEMPLATE
        assert '"advanced_config":' in GENERATE_TASK_TEMPLATE
        assert '"async_execution":' in GENERATE_TASK_TEMPLATE
    
    def test_generate_templates_template_content(self):
        """Test GENERATE_TEMPLATES_TEMPLATE content."""
        assert isinstance(GENERATE_TEMPLATES_TEMPLATE, str)
        assert "expert at creating AI agent templates" in GENERATE_TEMPLATES_TEMPLATE
        assert "System Template" in GENERATE_TEMPLATES_TEMPLATE
        assert "Prompt Template" in GENERATE_TEMPLATES_TEMPLATE
        assert "Response Template" in GENERATE_TEMPLATES_TEMPLATE
        assert '"system_template":' in GENERATE_TEMPLATES_TEMPLATE
        assert '"prompt_template":' in GENERATE_TEMPLATES_TEMPLATE
        assert '"response_template":' in GENERATE_TEMPLATES_TEMPLATE
    
    def test_generate_crew_template_content(self):
        """Test GENERATE_CREW_TEMPLATE content."""
        assert isinstance(GENERATE_CREW_TEMPLATE, str)
        assert "expert at creating AI crews" in GENERATE_CREW_TEMPLATE
        assert "complete crew setup" in GENERATE_CREW_TEMPLATE
        assert '"agents":' in GENERATE_CREW_TEMPLATE
        assert '"tasks":' in GENERATE_CREW_TEMPLATE
        assert "databricks-llama-4-maverick" in GENERATE_CREW_TEMPLATE
        assert "SerperDevTool" in GENERATE_CREW_TEMPLATE
        assert "ScrapeWebsiteTool" in GENERATE_CREW_TEMPLATE


class TestDefaultTemplates:
    """Test cases for DEFAULT_TEMPLATES."""
    
    def test_default_templates_structure(self):
        """Test DEFAULT_TEMPLATES structure."""
        assert isinstance(DEFAULT_TEMPLATES, list)
        assert len(DEFAULT_TEMPLATES) == 6
        
        for template in DEFAULT_TEMPLATES:
            assert isinstance(template, dict)
            assert "name" in template
            assert "description" in template
            assert "template" in template
            assert "is_active" in template
    
    def test_default_templates_names(self):
        """Test DEFAULT_TEMPLATES names."""
        expected_names = {
            "generate_agent",
            "generate_connections", 
            "generate_job_name",
            "generate_task",
            "generate_templates",
            "generate_crew"
        }
        
        actual_names = {template["name"] for template in DEFAULT_TEMPLATES}
        assert actual_names == expected_names
    
    def test_default_templates_active_status(self):
        """Test that all default templates are active."""
        for template in DEFAULT_TEMPLATES:
            assert template["is_active"] is True
    
    def test_default_templates_descriptions(self):
        """Test that all default templates have descriptions."""
        for template in DEFAULT_TEMPLATES:
            assert isinstance(template["description"], str)
            assert len(template["description"]) > 0
    
    def test_default_templates_content(self):
        """Test that all default templates have content."""
        for template in DEFAULT_TEMPLATES:
            assert isinstance(template["template"], str)
            assert len(template["template"]) > 100  # Should be substantial content
    
    def test_specific_template_content(self):
        """Test specific template content mapping."""
        template_mapping = {
            "generate_agent": GENERATE_AGENT_TEMPLATE,
            "generate_connections": GENERATE_CONNECTIONS_TEMPLATE,
            "generate_job_name": GENERATE_JOB_NAME_TEMPLATE,
            "generate_task": GENERATE_TASK_TEMPLATE,
            "generate_templates": GENERATE_TEMPLATES_TEMPLATE,
            "generate_crew": GENERATE_CREW_TEMPLATE
        }
        
        for template in DEFAULT_TEMPLATES:
            expected_content = template_mapping[template["name"]]
            assert template["template"] == expected_content


class TestSeedAsync:
    """Test cases for seed_async function."""
    
    @pytest.mark.asyncio
    @patch('src.seeds.prompt_templates.async_session_factory')
    @patch('src.seeds.prompt_templates.select')
    async def test_seed_async_new_templates(self, mock_select, mock_session_factory):
        """Test seed_async with new templates (no existing ones)."""
        # Mock session factory
        mock_session = AsyncMock()
        mock_session_factory.return_value.__aenter__.return_value = mock_session
        mock_session_factory.return_value.__aexit__.return_value = None
        
        # Mock existing templates query (no existing templates)
        mock_result = MagicMock()
        mock_result.scalars.return_value.all.return_value = []  # No existing templates
        mock_session.execute.return_value = mock_result
        
        # Mock individual template checks (return None for each)
        mock_check_result = MagicMock()
        mock_check_result.scalars.return_value.first.return_value = None
        
        # Set up multiple sessions for each template
        sessions = []
        for _ in range(len(DEFAULT_TEMPLATES)):
            session = AsyncMock()
            session.execute.return_value = mock_check_result
            session.commit.return_value = None
            sessions.append(session)
        
        mock_session_factory.return_value.__aenter__.side_effect = [mock_session] + sessions
        
        await seed_async()
        
        # Verify session factory was called (once for initial check + once per template)
        assert mock_session_factory.call_count == len(DEFAULT_TEMPLATES) + 1
    
    @pytest.mark.asyncio
    @patch('src.seeds.prompt_templates.async_session_factory')
    @patch('src.seeds.prompt_templates.select')
    async def test_seed_async_existing_templates(self, mock_select, mock_session_factory):
        """Test seed_async with existing templates (update scenario)."""
        # Mock session factory
        mock_session = AsyncMock()
        mock_session_factory.return_value.__aenter__.return_value = mock_session
        mock_session_factory.return_value.__aexit__.return_value = None
        
        # Mock existing templates query (all templates exist)
        existing_names = [template["name"] for template in DEFAULT_TEMPLATES]
        mock_result = MagicMock()
        mock_result.scalars.return_value.all.return_value = existing_names
        mock_session.execute.return_value = mock_result
        
        # Mock individual template checks (return existing template for each)
        mock_existing_template = MagicMock()
        mock_check_result = MagicMock()
        mock_check_result.scalars.return_value.first.return_value = mock_existing_template
        
        # Set up multiple sessions for each template
        sessions = []
        for _ in range(len(DEFAULT_TEMPLATES)):
            session = AsyncMock()
            session.execute.return_value = mock_check_result
            session.commit.return_value = None
            sessions.append(session)
        
        mock_session_factory.return_value.__aenter__.side_effect = [mock_session] + sessions
        
        await seed_async()
        
        # Verify session factory was called
        assert mock_session_factory.call_count == len(DEFAULT_TEMPLATES) + 1
    
    @pytest.mark.asyncio
    @patch('src.seeds.prompt_templates.async_session_factory')
    @patch('src.seeds.prompt_templates.select')
    async def test_seed_async_mixed_scenario(self, mock_select, mock_session_factory):
        """Test seed_async with mixed existing/new templates."""
        # Mock session factory
        mock_session = AsyncMock()
        mock_session_factory.return_value.__aenter__.return_value = mock_session
        mock_session_factory.return_value.__aexit__.return_value = None
        
        # Mock existing templates query (some templates exist)
        existing_names = ["generate_agent", "generate_task"]  # Only 2 exist
        mock_result = MagicMock()
        mock_result.scalars.return_value.all.return_value = existing_names
        mock_session.execute.return_value = mock_result
        
        # Mock individual template checks
        def side_effect(*args, **kwargs):
            mock_result = MagicMock()
            # Return existing template for first 2, None for others
            if "generate_agent" in str(args) or "generate_task" in str(args):
                mock_result.scalars.return_value.first.return_value = MagicMock()
            else:
                mock_result.scalars.return_value.first.return_value = None
            return mock_result
        
        # Set up multiple sessions for each template
        sessions = []
        for _ in range(len(DEFAULT_TEMPLATES)):
            session = AsyncMock()
            session.execute.side_effect = side_effect
            session.commit.return_value = None
            sessions.append(session)
        
        mock_session_factory.return_value.__aenter__.side_effect = [mock_session] + sessions
        
        await seed_async()
        
        # Verify session factory was called
        assert mock_session_factory.call_count == len(DEFAULT_TEMPLATES) + 1
    
    @pytest.mark.asyncio
    @patch('src.seeds.prompt_templates.async_session_factory')
    @patch('src.seeds.prompt_templates.select')
    async def test_seed_async_error_handling(self, mock_select, mock_session_factory):
        """Test seed_async error handling."""
        # Mock session factory
        mock_session = AsyncMock()
        mock_session_factory.return_value.__aenter__.return_value = mock_session
        mock_session_factory.return_value.__aexit__.return_value = None
        
        # Mock initial query success
        mock_result = MagicMock()
        mock_result.scalars.return_value.all.return_value = []
        mock_session.execute.return_value = mock_result
        
        # Mock one session to fail
        error_session = AsyncMock()
        error_session.execute.side_effect = Exception("Database error")
        error_session.rollback.return_value = None
        
        # Other sessions succeed
        success_sessions = []
        for _ in range(len(DEFAULT_TEMPLATES) - 1):
            session = AsyncMock()
            check_result = MagicMock()
            check_result.scalars.return_value.first.return_value = None
            session.execute.return_value = check_result
            session.commit.return_value = None
            success_sessions.append(session)
        
        mock_session_factory.return_value.__aenter__.side_effect = [mock_session, error_session] + success_sessions
        
        # Should not raise exception, should handle errors gracefully
        await seed_async()
        
        # Verify error handling occurred
        error_session.rollback.assert_called_once()
    
    @pytest.mark.asyncio
    @patch('src.seeds.prompt_templates.async_session_factory')
    @patch('src.seeds.prompt_templates.select')
    async def test_seed_async_unique_constraint_error(self, mock_select, mock_session_factory):
        """Test seed_async handling unique constraint errors."""
        # Mock session factory
        mock_session = AsyncMock()
        mock_session_factory.return_value.__aenter__.return_value = mock_session
        mock_session_factory.return_value.__aexit__.return_value = None
        
        # Mock initial query
        mock_result = MagicMock()
        mock_result.scalars.return_value.all.return_value = []
        mock_session.execute.return_value = mock_result
        
        # Mock session with unique constraint error
        constraint_session = AsyncMock()
        check_result = MagicMock()
        check_result.scalars.return_value.first.return_value = None
        constraint_session.execute.return_value = check_result
        constraint_session.commit.side_effect = Exception("UNIQUE constraint failed")
        constraint_session.rollback.return_value = None
        
        mock_session_factory.return_value.__aenter__.side_effect = [mock_session, constraint_session]
        
        # Should handle unique constraint gracefully
        await seed_async()
        
        constraint_session.rollback.assert_called_once()


class TestSeedSync:
    """Test cases for seed_sync function."""
    
    @patch('src.seeds.prompt_templates.SessionLocal')
    @patch('src.seeds.prompt_templates.select')
    def test_seed_sync_new_templates(self, mock_select, mock_session_local):
        """Test seed_sync with new templates."""
        # Mock session
        mock_session = MagicMock()
        mock_session_local.return_value.__enter__.return_value = mock_session
        mock_session_local.return_value.__exit__.return_value = None
        
        # Mock existing templates query (no existing templates)
        mock_result = MagicMock()
        mock_result.scalars.return_value.all.return_value = []
        mock_session.execute.return_value = mock_result
        
        # Mock individual template checks (return None for each)
        mock_check_result = MagicMock()
        mock_check_result.scalars.return_value.first.return_value = None
        
        # Set up multiple sessions for each template
        sessions = []
        for _ in range(len(DEFAULT_TEMPLATES)):
            session = MagicMock()
            session.execute.return_value = mock_check_result
            session.commit.return_value = None
            sessions.append(session)
        
        mock_session_local.return_value.__enter__.side_effect = [mock_session] + sessions
        
        seed_sync()
        
        # Verify session factory was called
        assert mock_session_local.call_count == len(DEFAULT_TEMPLATES) + 1
    
    @patch('src.seeds.prompt_templates.SessionLocal')
    @patch('src.seeds.prompt_templates.select')
    def test_seed_sync_error_handling(self, mock_select, mock_session_local):
        """Test seed_sync error handling."""
        # Mock session
        mock_session = MagicMock()
        mock_session_local.return_value.__enter__.return_value = mock_session
        mock_session_local.return_value.__exit__.return_value = None
        
        # Mock initial query success
        mock_result = MagicMock()
        mock_result.scalars.return_value.all.return_value = []
        mock_session.execute.return_value = mock_result
        
        # Mock one session to fail
        error_session = MagicMock()
        error_session.execute.side_effect = Exception("Database error")
        error_session.rollback.return_value = None
        
        mock_session_local.return_value.__enter__.side_effect = [mock_session, error_session]
        
        # Should not raise exception
        seed_sync()
        
        # Verify error handling
        error_session.rollback.assert_called_once()


class TestSeedMain:
    """Test cases for main seed function."""
    
    @pytest.mark.asyncio
    @patch('src.seeds.prompt_templates.seed_async')
    async def test_seed_success(self, mock_seed_async):
        """Test main seed function success."""
        mock_seed_async.return_value = None
        
        await seed()
        
        mock_seed_async.assert_called_once()
    
    @pytest.mark.asyncio
    @patch('src.seeds.prompt_templates.seed_async')
    async def test_seed_error_handling(self, mock_seed_async):
        """Test main seed function error handling."""
        mock_seed_async.side_effect = Exception("Seeding error")
        
        # Should not raise exception, should handle gracefully
        await seed()
        
        mock_seed_async.assert_called_once()


class TestTemplateValidation:
    """Test cases for template validation."""
    
    def test_templates_have_required_fields(self):
        """Test that all templates have required fields."""
        required_fields = ["name", "description", "template", "is_active"]
        
        for template in DEFAULT_TEMPLATES:
            for field in required_fields:
                assert field in template, f"Template {template.get('name', 'unknown')} missing field: {field}"
    
    def test_template_names_unique(self):
        """Test that template names are unique."""
        names = [template["name"] for template in DEFAULT_TEMPLATES]
        assert len(names) == len(set(names)), "Template names are not unique"
    
    def test_template_content_json_format(self):
        """Test that templates contain JSON format instructions."""
        json_templates = ["generate_agent", "generate_connections", "generate_task", "generate_templates", "generate_crew"]
        
        for template in DEFAULT_TEMPLATES:
            if template["name"] in json_templates:
                content = template["template"]
                assert "JSON" in content or "json" in content, f"Template {template['name']} should mention JSON format"
    
    def test_agent_template_structure(self):
        """Test agent template structure."""
        agent_template = next(t for t in DEFAULT_TEMPLATES if t["name"] == "generate_agent")
        content = agent_template["template"]
        
        # Should contain agent fields
        assert '"name":' in content
        assert '"role":' in content
        assert '"goal":' in content
        assert '"backstory":' in content
        assert '"tools":' in content
        assert '"advanced_config":' in content
    
    def test_task_template_structure(self):
        """Test task template structure."""
        task_template = next(t for t in DEFAULT_TEMPLATES if t["name"] == "generate_task")
        content = task_template["template"]
        
        # Should contain task fields
        assert '"name":' in content
        assert '"description":' in content
        assert '"expected_output":' in content
        assert '"tools":' in content
        assert '"advanced_config":' in content
    
    def test_crew_template_structure(self):
        """Test crew template structure."""
        crew_template = next(t for t in DEFAULT_TEMPLATES if t["name"] == "generate_crew")
        content = crew_template["template"]
        
        # Should contain crew structure
        assert '"agents":' in content
        assert '"tasks":' in content
        assert "databricks-llama-4-maverick" in content
    
    def test_connections_template_structure(self):
        """Test connections template structure."""
        connections_template = next(t for t in DEFAULT_TEMPLATES if t["name"] == "generate_connections")
        content = connections_template["template"]
        
        # Should contain connections structure
        assert '"assignments":' in content
        assert '"dependencies":' in content
        assert "reasoning" in content
    
    def test_templates_template_structure(self):
        """Test templates template structure."""
        templates_template = next(t for t in DEFAULT_TEMPLATES if t["name"] == "generate_templates")
        content = templates_template["template"]
        
        # Should contain template types
        assert "system_template" in content
        assert "prompt_template" in content
        assert "response_template" in content


class TestTemplateInstructions:
    """Test cases for template instructions."""
    
    def test_json_output_instructions(self):
        """Test that JSON output instructions are clear."""
        json_templates = ["generate_agent", "generate_crew", "generate_task", "generate_connections"]
        
        for template in DEFAULT_TEMPLATES:
            if template["name"] in json_templates:
                content = template["template"]
                assert "CRITICAL OUTPUT INSTRUCTIONS" in content or "OUTPUT INSTRUCTIONS" in content
                assert "valid" in content.lower()
                assert "json" in content.lower()
    
    def test_no_markdown_instructions(self):
        """Test that templates instruct against markdown."""
        json_templates = ["generate_agent", "generate_crew", "generate_connections"]
        
        for template in DEFAULT_TEMPLATES:
            if template["name"] in json_templates:
                content = template["template"]
                assert "markdown" in content.lower()
                assert "```" in content  # Shows what not to include
    
    def test_format_examples(self):
        """Test that templates provide format examples."""
        format_templates = ["generate_agent", "generate_task", "generate_crew", "generate_connections"]
        
        for template in DEFAULT_TEMPLATES:
            if template["name"] in format_templates:
                content = template["template"]
                # Should have example structure
                assert "{" in content and "}" in content  # JSON structure
    
    def test_specific_field_instructions(self):
        """Test that templates have specific field instructions."""
        agent_template = next(t for t in DEFAULT_TEMPLATES if t["name"] == "generate_agent")
        content = agent_template["template"]
        
        # Should provide specific guidance
        assert "descriptive name" in content
        assert "specific role" in content
        assert "clear objective" in content or "concrete goal" in content
        assert "backstory" in content
    
    def test_tool_restrictions(self):
        """Test that templates mention tool restrictions."""
        crew_template = next(t for t in DEFAULT_TEMPLATES if t["name"] == "generate_crew")
        content = crew_template["template"]
        
        # Should mention tool restrictions
        assert "tools" in content.lower()
        assert "provided tools" in content.lower() or "listed" in content.lower()
        assert "SerperDevTool" in content
        assert "ScrapeWebsiteTool" in content


class TestTemplateIntegration:
    """Test cases for template integration patterns."""
    
    def test_template_consistency(self):
        """Test consistency across templates."""
        # All JSON templates should have similar instruction patterns
        json_templates = [t for t in DEFAULT_TEMPLATES if t["name"] in ["generate_agent", "generate_crew", "generate_task"]]
        
        for template in json_templates:
            content = template["template"]
            # Should all mention avoiding markdown
            assert "markdown" in content.lower()
            # Should all mention JSON validity
            assert "valid" in content.lower() and "json" in content.lower()
    
    def test_model_configuration_consistency(self):
        """Test that model configurations are consistent."""
        model_templates = ["generate_agent", "generate_crew"]
        
        for template in DEFAULT_TEMPLATES:
            if template["name"] in model_templates:
                content = template["template"]
                assert "databricks-llama-4-maverick" in content
    
    def test_template_completeness(self):
        """Test that templates cover all necessary use cases."""
        expected_capabilities = {
            "generate_agent": "agent creation",
            "generate_task": "task creation", 
            "generate_crew": "crew creation",
            "generate_connections": "workflow planning",
            "generate_job_name": "naming",
            "generate_templates": "template creation"
        }
        
        actual_names = {t["name"] for t in DEFAULT_TEMPLATES}
        expected_names = set(expected_capabilities.keys())
        
        assert actual_names == expected_names
    
    def test_template_descriptions_accuracy(self):
        """Test that template descriptions accurately reflect their purpose."""
        description_keywords = {
            "generate_agent": ["agent", "AI agent"],
            "generate_task": ["task", "configuration"],
            "generate_crew": ["crew", "agents and tasks"],
            "generate_connections": ["connections", "agents and tasks"],
            "generate_job_name": ["job name", "name"],
            "generate_templates": ["templates", "system"]
        }
        
        for template in DEFAULT_TEMPLATES:
            name = template["name"]
            description = template["description"].lower()
            
            if name in description_keywords:
                keywords = description_keywords[name]
                assert any(keyword.lower() in description for keyword in keywords), \
                    f"Template {name} description should contain one of: {keywords}"