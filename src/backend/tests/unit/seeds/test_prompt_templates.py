"""
Unit tests for prompt templates seed module.
"""
import pytest
import asyncio
import traceback
from datetime import datetime
from unittest.mock import Mock, AsyncMock, patch, call
from sqlalchemy.exc import IntegrityError

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
    seed_sync,
    seed
)


class TestPromptTemplatesSeed:
    """Test cases for prompt templates seed module."""

    def test_default_templates_structure(self):
        """Test that DEFAULT_TEMPLATES has expected structure."""
        assert isinstance(DEFAULT_TEMPLATES, list)
        assert len(DEFAULT_TEMPLATES) > 0
        
        # Check required fields for each template
        required_fields = ["name", "description", "template", "is_active"]
        for template_data in DEFAULT_TEMPLATES:
            for field in required_fields:
                assert field in template_data, f"Template missing field {field}"
            
            # Check data types
            assert isinstance(template_data["name"], str)
            assert isinstance(template_data["description"], str)
            assert isinstance(template_data["template"], str)
            assert isinstance(template_data["is_active"], bool)

    def test_default_templates_names(self):
        """Test that all expected template names are present."""
        template_names = [template["name"] for template in DEFAULT_TEMPLATES]
        
        expected_names = [
            "generate_agent",
            "generate_connections",
            "generate_job_name",
            "generate_task",
            "generate_templates",
            "generate_crew",
            "detect_intent"
        ]
        
        for expected_name in expected_names:
            assert expected_name in template_names, f"Missing template: {expected_name}"

    def test_template_constants_not_empty(self):
        """Test that all template constants are not empty."""
        templates = [
            GENERATE_AGENT_TEMPLATE,
            GENERATE_CONNECTIONS_TEMPLATE,
            GENERATE_JOB_NAME_TEMPLATE,
            GENERATE_TASK_TEMPLATE,
            GENERATE_TEMPLATES_TEMPLATE,
            GENERATE_CREW_TEMPLATE,
            DETECT_INTENT_TEMPLATE
        ]
        
        for template in templates:
            assert isinstance(template, str)
            assert len(template.strip()) > 0

    def test_template_content_formatting(self):
        """Test that templates contain expected formatting instructions."""
        # Test that JSON-related templates have JSON instructions
        json_templates = [
            GENERATE_AGENT_TEMPLATE,
            GENERATE_CONNECTIONS_TEMPLATE,
            GENERATE_TASK_TEMPLATE,
            GENERATE_CREW_TEMPLATE,
            DETECT_INTENT_TEMPLATE
        ]
        
        for template in json_templates:
            assert "JSON" in template or "json" in template

    def test_generate_agent_template_structure(self):
        """Test generate agent template has required sections."""
        template = GENERATE_AGENT_TEMPLATE
        
        # Should contain key instructions
        assert "CRITICAL OUTPUT INSTRUCTIONS" in template
        assert "parseable JSON object" in template
        assert "name" in template

    def test_generate_templates_template_structure(self):
        """Test generate templates template has required sections and parameters."""
        template = GENERATE_TEMPLATES_TEMPLATE
        
        # Should contain key instructions for template generation
        assert "CRITICAL OUTPUT INSTRUCTIONS" in template
        assert "parseable JSON object" in template
        assert "system_template" in template
        assert "prompt_template" in template
        assert "response_template" in template
        
        # Should contain parameter instructions
        assert "{role}" in template
        assert "{goal}" in template
        assert "{backstory}" in template
        assert "{input}" in template
        assert "{context}" in template
        
        # Should contain template type descriptions
        assert "System Template" in template
        assert "Prompt Template" in template
        assert "Response Template" in template
        
        # Should contain structured output examples
        assert "THOUGHTS" in template
        assert "ACTION" in template
        assert "RESULT" in template
        
        # Should have JSON formatting requirements
        assert "Do NOT include ```json" in template
        assert "double quotes" in template
        assert "trailing commas" in template

    def test_generate_templates_template_parameter_examples(self):
        """Test that the template contains proper parameter usage examples."""
        template = GENERATE_TEMPLATES_TEMPLATE
        
        # Should contain example parameter usage
        assert "You are a {role}" in template
        assert "{backstory}" in template
        assert "Your goal is: {goal}" in template
        assert "Task: {input}" in template
        assert "Context: {context}" in template
        
        # Should show structured response format
        assert "THOUGHTS: [analysis]" in template
        assert "ACTION: [what you will do]" in template
        assert "RESULT: [final output]" in template

    def test_generate_templates_template_requirements(self):
        """Test that the template contains all necessary requirements."""
        template = GENERATE_TEMPLATES_TEMPLATE
        
        # Template requirements section
        assert "TEMPLATE REQUIREMENTS" in template
        assert "System Template MUST incorporate" in template
        assert "Prompt Template should use" in template
        assert "Response Template should enforce" in template
        assert "placeholder syntax with curly braces" in template
        assert "expertise boundaries and ethical guidelines" in template
        assert "model-agnostic and production-ready" in template
        assert "role" in template
        assert "goal" in template
        assert "backstory" in template

    def test_generate_crew_template_structure(self):
        """Test generate crew template has required sections."""
        template = GENERATE_CREW_TEMPLATE
        
        # Should contain agents and tasks structure
        assert "agents" in template
        assert "tasks" in template
        assert "CRITICAL OUTPUT INSTRUCTIONS" in template

    def test_detect_intent_template_structure(self):
        """Test detect intent template has required categories."""
        template = DETECT_INTENT_TEMPLATE
        
        # Should contain all intent categories
        expected_intents = [
            "generate_task",
            "generate_agent",
            "generate_crew",
            "configure_crew",
            "conversation",
            "unknown"
        ]
        
        for intent in expected_intents:
            assert intent in template

    def test_generate_templates_template_structure(self):
        """Test generate templates template has required structure."""
        template = GENERATE_TEMPLATES_TEMPLATE
        
        # Should contain key sections for template generation
        assert "system_template" in template
        assert "prompt_template" in template  
        assert "response_template" in template
        assert "JSON" in template or "json" in template
        assert "{variables}" in template

    def test_generate_job_name_template_structure(self):
        """Test generate job name template has required instructions."""
        template = GENERATE_JOB_NAME_TEMPLATE
        
        # Should contain key instructions for job naming
        assert "concise" in template
        assert "descriptive" in template
        assert "2-4 words" in template
        assert "region" in template or "topic" in template

    def test_all_templates_active_by_default(self):
        """Test that all default templates are active by default."""
        for template_data in DEFAULT_TEMPLATES:
            assert template_data["is_active"] is True, f"Template {template_data['name']} should be active by default"

    def test_template_descriptions_not_empty(self):
        """Test that all templates have non-empty descriptions."""
        for template_data in DEFAULT_TEMPLATES:
            description = template_data["description"]
            assert isinstance(description, str)
            assert len(description.strip()) > 0, f"Template {template_data['name']} has empty description"

    def test_template_uniqueness(self):
        """Test that all template names are unique."""
        template_names = [template["name"] for template in DEFAULT_TEMPLATES]
        unique_names = set(template_names)
        
        assert len(template_names) == len(unique_names), "Duplicate template names found"

    def test_template_content_validation(self):
        """Test that template content appears valid."""
        for template_data in DEFAULT_TEMPLATES:
            template_content = template_data["template"]
            
            # Should be non-empty string
            assert isinstance(template_content, str)
            assert len(template_content.strip()) > 0
            
            # Should contain some kind of instruction or guidance
            content_lower = template_content.lower()
            assert any(word in content_lower for word in ["you", "the", "generate", "create", "provide", "return"])

    def test_json_templates_have_proper_instructions(self):
        """Test that JSON-returning templates have proper JSON instructions."""
        json_template_names = [
            "generate_agent",
            "generate_connections", 
            "generate_task",
            "generate_crew",
            "detect_intent"
        ]
        
        for template_data in DEFAULT_TEMPLATES:
            if template_data["name"] in json_template_names:
                content = template_data["template"]
                content_lower = content.lower()
                
                # Should mention JSON formatting
                assert "json" in content_lower
                
                # Should have instructions about formatting
                assert any(word in content_lower for word in ["format", "structure", "object"])

    def test_logging_configuration(self):
        """Test that logger is properly configured."""
        from src.seeds.prompt_templates import logger
        assert logger.name == "src.seeds.prompt_templates"

    @pytest.mark.asyncio
    async def test_seed_main_entry_point_success(self):
        """Test main seed entry point success."""
        with patch('src.seeds.prompt_templates.seed_async', new_callable=AsyncMock) as mock_seed_async:
            await seed()
            mock_seed_async.assert_called_once()

    @pytest.mark.asyncio
    async def test_seed_main_entry_point_error(self):
        """Test main seed entry point with error."""
        with patch('src.seeds.prompt_templates.seed_async', new_callable=AsyncMock) as mock_seed_async:
            mock_seed_async.side_effect = Exception("Seed error")
            
            # Should not raise exception - errors are logged but not re-raised
            await seed()
            
            mock_seed_async.assert_called_once()

    @pytest.mark.asyncio
    async def test_seed_with_traceback_logging(self):
        """Test seed function logs traceback on exception."""
        with patch('src.seeds.prompt_templates.seed_async', new_callable=AsyncMock) as mock_seed_async:
            with patch('src.seeds.prompt_templates.logger') as mock_logger:
                with patch('traceback.format_exc') as mock_traceback:
                    # Setup exception
                    test_exception = Exception("Test error")
                    mock_seed_async.side_effect = test_exception
                    mock_traceback.return_value = "Test traceback"
                    
                    # Call seed function
                    await seed()
                    
                    # Verify logging behavior
                    mock_logger.info.assert_any_call("Starting prompt templates seeding process...")
                    mock_logger.error.assert_any_call("Error seeding prompt templates: Test error")
                    mock_logger.error.assert_any_call("Prompt templates seeding traceback: Test traceback")
                    mock_traceback.assert_called_once()

    @pytest.mark.asyncio
    async def test_seed_function_success_logging(self):
        """Test seed function success logging."""
        with patch('src.seeds.prompt_templates.seed_async', new_callable=AsyncMock) as mock_seed_async:
            with patch('src.seeds.prompt_templates.logger') as mock_logger:
                await seed()
                
                # Verify success logging
                mock_logger.info.assert_any_call("Starting prompt templates seeding process...")
                mock_logger.info.assert_any_call("Prompt templates seeding completed successfully")
                mock_seed_async.assert_called_once()

    @pytest.mark.asyncio
    async def test_seed_async_full_workflow(self):
        """Test complete async seeding workflow."""
        with patch('src.seeds.prompt_templates.async_session_factory') as mock_session_factory:
            with patch('src.seeds.prompt_templates.logger') as mock_logger:
                with patch('src.seeds.prompt_templates.PromptTemplate') as mock_template_class:
                    with patch('src.seeds.prompt_templates.select') as mock_select:
                        with patch('src.seeds.prompt_templates.datetime') as mock_datetime:
                            # Setup mocks
                            mock_now = Mock()
                            mock_now.replace.return_value = datetime(2023, 1, 1, 12, 0, 0)
                            mock_datetime.now.return_value = mock_now
                            
                            mock_session = Mock()
                            mock_session.add = Mock()
                            mock_session.commit = AsyncMock()
                            mock_session.rollback = AsyncMock()
                            mock_session.execute = AsyncMock()
                            
                            # Mock session context manager
                            async def mock_session_context(self):
                                return mock_session
                            mock_context = Mock()
                            mock_context.__aenter__ = mock_session_context
                            mock_context.__aexit__ = AsyncMock(return_value=None)
                            mock_session_factory.return_value = mock_context
                            
                            # Mock initial query for existing names - empty result
                            initial_result = Mock()
                            initial_result.scalars.return_value.all.return_value = []
                            
                            # Mock template checks - no existing templates
                            template_result = Mock()
                            template_result.scalars.return_value.first.return_value = None
                            
                            # Return different results based on call count
                            call_count = [0]
                            async def mock_execute(*args, **kwargs):
                                call_count[0] += 1
                                if call_count[0] == 1:
                                    return initial_result
                                else:
                                    return template_result
                            
                            mock_session.execute.side_effect = mock_execute
                            
                            await seed_async()
                            
                            # Verify workflow
                            mock_logger.info.assert_called()
                            mock_session.add.assert_called()
                            mock_session.commit.assert_called()
                            mock_datetime.now.assert_called()

    def test_seed_sync_full_workflow(self):
        """Test complete sync seeding workflow."""
        with patch('src.seeds.prompt_templates.SessionLocal') as mock_session_local:
            with patch('src.seeds.prompt_templates.logger') as mock_logger:
                with patch('src.seeds.prompt_templates.PromptTemplate') as mock_template_class:
                    with patch('src.seeds.prompt_templates.select') as mock_select:
                        with patch('src.seeds.prompt_templates.datetime') as mock_datetime:
                            # Setup mocks
                            mock_now = Mock()
                            mock_now.replace.return_value = datetime(2023, 1, 1, 12, 0, 0)
                            mock_datetime.now.return_value = mock_now
                            
                            mock_session = Mock()
                            mock_session.add = Mock()
                            mock_session.commit = Mock()
                            mock_session.rollback = Mock()
                            mock_session.execute = Mock()
                            
                            # Mock session context manager
                            mock_context = Mock()
                            mock_context.__enter__ = Mock(return_value=mock_session)
                            mock_context.__exit__ = Mock(return_value=None)
                            mock_session_local.return_value = mock_context
                            
                            # Mock initial query for existing names - empty result
                            initial_result = Mock()
                            initial_result.scalars.return_value.all.return_value = []
                            
                            # Mock template checks - no existing templates
                            template_result = Mock()
                            template_result.scalars.return_value.first.return_value = None
                            
                            # Return different results based on call count
                            call_count = [0]
                            def mock_execute(*args, **kwargs):
                                call_count[0] += 1
                                if call_count[0] == 1:
                                    return initial_result
                                else:
                                    return template_result
                            
                            mock_session.execute.side_effect = mock_execute
                            
                            seed_sync()
                            
                            # Verify workflow
                            mock_logger.info.assert_called()
                            mock_session.add.assert_called()
                            mock_session.commit.assert_called()
                            mock_datetime.now.assert_called()

    @pytest.mark.asyncio
    async def test_seed_async_update_existing(self):
        """Test async seeding updates existing templates."""
        with patch('src.seeds.prompt_templates.async_session_factory') as mock_session_factory:
            with patch('src.seeds.prompt_templates.PromptTemplate') as mock_template_class:
                with patch('src.seeds.prompt_templates.select') as mock_select:
                    with patch('src.seeds.prompt_templates.datetime') as mock_datetime:
                        # Setup mocks
                        mock_now = Mock()
                        mock_now.replace.return_value = datetime(2023, 1, 1, 12, 0, 0)
                        mock_datetime.now.return_value = mock_now
                        
                        mock_session = Mock()
                        mock_session.add = Mock()
                        mock_session.commit = AsyncMock()
                        mock_session.rollback = AsyncMock()
                        mock_session.execute = AsyncMock()
                        
                        # Mock session context manager
                        async def mock_session_context(self):
                            return mock_session
                        mock_context = Mock()
                        mock_context.__aenter__ = mock_session_context
                        mock_context.__aexit__ = AsyncMock(return_value=None)
                        mock_session_factory.return_value = mock_context
                        
                        # Mock initial query - all templates exist
                        existing_names = [template["name"] for template in DEFAULT_TEMPLATES]
                        initial_result = Mock()
                        initial_result.scalars.return_value.all.return_value = existing_names
                        
                        # Mock existing template
                        existing_template = Mock()
                        existing_template.description = "old description"
                        existing_template.template = "old template"
                        existing_template.is_active = False
                        existing_template.updated_at = datetime.now()
                        
                        template_result = Mock()
                        template_result.scalars.return_value.first.return_value = existing_template
                        
                        call_count = [0]
                        async def mock_execute(*args, **kwargs):
                            call_count[0] += 1
                            if call_count[0] == 1:
                                return initial_result
                            else:
                                return template_result
                        
                        mock_session.execute.side_effect = mock_execute
                        
                        await seed_async()
                        
                        # Should not add new templates, only update
                        mock_session.add.assert_not_called()
                        mock_session.commit.assert_called()

    def test_seed_sync_update_existing(self):
        """Test sync seeding updates existing templates."""
        with patch('src.seeds.prompt_templates.SessionLocal') as mock_session_local:
            with patch('src.seeds.prompt_templates.PromptTemplate') as mock_template_class:
                with patch('src.seeds.prompt_templates.select') as mock_select:
                    with patch('src.seeds.prompt_templates.datetime') as mock_datetime:
                        # Setup mocks
                        mock_now = Mock()
                        mock_now.replace.return_value = datetime(2023, 1, 1, 12, 0, 0)
                        mock_datetime.now.return_value = mock_now
                        
                        mock_session = Mock()
                        mock_session.add = Mock()
                        mock_session.commit = Mock()
                        mock_session.rollback = Mock()
                        mock_session.execute = Mock()
                        
                        # Mock session context manager
                        mock_context = Mock()
                        mock_context.__enter__ = Mock(return_value=mock_session)
                        mock_context.__exit__ = Mock(return_value=None)
                        mock_session_local.return_value = mock_context
                        
                        # Mock initial query - all templates exist
                        existing_names = [template["name"] for template in DEFAULT_TEMPLATES]
                        initial_result = Mock()
                        initial_result.scalars.return_value.all.return_value = existing_names
                        
                        # Mock existing template
                        existing_template = Mock()
                        existing_template.description = "old description"
                        existing_template.template = "old template"
                        existing_template.is_active = False
                        existing_template.updated_at = datetime.now()
                        
                        template_result = Mock()
                        template_result.scalars.return_value.first.return_value = existing_template
                        
                        call_count = [0]
                        def mock_execute(*args, **kwargs):
                            call_count[0] += 1
                            if call_count[0] == 1:
                                return initial_result
                            else:
                                return template_result
                        
                        mock_session.execute.side_effect = mock_execute
                        
                        seed_sync()
                        
                        # Should not add new templates, only update
                        mock_session.add.assert_not_called()
                        mock_session.commit.assert_called()

    @pytest.mark.asyncio
    async def test_seed_async_error_handling(self):
        """Test async seeding handles errors."""
        with patch('src.seeds.prompt_templates.async_session_factory') as mock_session_factory:
            with patch('src.seeds.prompt_templates.PromptTemplate') as mock_template_class:
                with patch('src.seeds.prompt_templates.select') as mock_select:
                    with patch('src.seeds.prompt_templates.logger') as mock_logger:
                        mock_session = Mock()
                        mock_session.add = Mock()
                        mock_session.commit = AsyncMock()
                        mock_session.rollback = AsyncMock()
                        mock_session.execute = AsyncMock()
                        
                        # Mock session context manager
                        async def mock_session_context(self):
                            return mock_session
                        mock_context = Mock()
                        mock_context.__aenter__ = mock_session_context
                        mock_context.__aexit__ = AsyncMock(return_value=None)
                        mock_session_factory.return_value = mock_context
                        
                        # Mock initial query success
                        initial_result = Mock()
                        initial_result.scalars.return_value.all.return_value = []
                        
                        # Mock template check returns None 
                        template_result = Mock()
                        template_result.scalars.return_value.first.return_value = None
                        
                        call_count = [0]
                        async def mock_execute(*args, **kwargs):
                            call_count[0] += 1
                            if call_count[0] == 1:
                                return initial_result
                            else:
                                return template_result
                        
                        mock_session.execute.side_effect = mock_execute
                        
                        # Mock commit error on first template
                        commit_count = [0]
                        async def mock_commit():
                            commit_count[0] += 1
                            if commit_count[0] == 1:
                                raise IntegrityError("statement", "params", "UNIQUE constraint failed")
                        
                        mock_session.commit.side_effect = mock_commit
                        
                        await seed_async()
                        
                        # Should have handled error
                        mock_session.rollback.assert_called()
                        mock_logger.warning.assert_called()

    def test_seed_sync_error_handling(self):
        """Test sync seeding handles errors."""
        with patch('src.seeds.prompt_templates.SessionLocal') as mock_session_local:
            with patch('src.seeds.prompt_templates.PromptTemplate') as mock_template_class:
                with patch('src.seeds.prompt_templates.select') as mock_select:
                    with patch('src.seeds.prompt_templates.logger') as mock_logger:
                        mock_session = Mock()
                        mock_session.add = Mock()
                        mock_session.commit = Mock()
                        mock_session.rollback = Mock()
                        mock_session.execute = Mock()
                        
                        # Mock session context manager
                        mock_context = Mock()
                        mock_context.__enter__ = Mock(return_value=mock_session)
                        mock_context.__exit__ = Mock(return_value=None)
                        mock_session_local.return_value = mock_context
                        
                        # Mock initial query success
                        initial_result = Mock()
                        initial_result.scalars.return_value.all.return_value = []
                        
                        # Mock template check returns None 
                        template_result = Mock()
                        template_result.scalars.return_value.first.return_value = None
                        
                        call_count = [0]
                        def mock_execute(*args, **kwargs):
                            call_count[0] += 1
                            if call_count[0] == 1:
                                return initial_result
                            else:
                                return template_result
                        
                        mock_session.execute.side_effect = mock_execute
                        
                        # Mock commit error on first template
                        commit_count = [0]
                        def mock_commit():
                            commit_count[0] += 1
                            if commit_count[0] == 1:
                                raise Exception("UNIQUE constraint failed")
                        
                        mock_session.commit.side_effect = mock_commit
                        
                        seed_sync()
                        
                        # Should have handled error
                        mock_session.rollback.assert_called()
                        mock_logger.warning.assert_called()

    @pytest.mark.asyncio
    async def test_seed_async_race_condition(self):
        """Test async seeding handles race conditions."""
        with patch('src.seeds.prompt_templates.async_session_factory') as mock_session_factory:
            with patch('src.seeds.prompt_templates.PromptTemplate') as mock_template_class:
                with patch('src.seeds.prompt_templates.select') as mock_select:
                    with patch('src.seeds.prompt_templates.datetime') as mock_datetime:
                        # Setup mocks
                        mock_now = Mock()
                        mock_now.replace.return_value = datetime(2023, 1, 1, 12, 0, 0)
                        mock_datetime.now.return_value = mock_now
                        
                        mock_session = Mock()
                        mock_session.add = Mock()
                        mock_session.commit = AsyncMock()
                        mock_session.rollback = AsyncMock()
                        mock_session.execute = AsyncMock()
                        
                        # Mock session context manager
                        async def mock_session_context(self):
                            return mock_session
                        mock_context = Mock()
                        mock_context.__aenter__ = mock_session_context
                        mock_context.__aexit__ = AsyncMock(return_value=None)
                        mock_session_factory.return_value = mock_context
                        
                        # Mock initial query shows no existing templates
                        initial_result = Mock()
                        initial_result.scalars.return_value.all.return_value = []
                        
                        # Mock race condition - template exists on double-check
                        existing_template = Mock()
                        existing_template.description = "existing description"
                        existing_template.template = "existing template"
                        existing_template.is_active = True
                        existing_template.updated_at = datetime.now()
                        
                        template_result = Mock()
                        template_result.scalars.return_value.first.return_value = existing_template
                        
                        call_count = [0]
                        async def mock_execute(*args, **kwargs):
                            call_count[0] += 1
                            if call_count[0] == 1:
                                return initial_result
                            else:
                                return template_result
                        
                        mock_session.execute.side_effect = mock_execute
                        
                        await seed_async()
                        
                        # Should not add templates (race condition detected)
                        mock_session.add.assert_not_called()
                        mock_session.commit.assert_called()

    @pytest.mark.asyncio
    async def test_seed_async_general_exception(self):
        """Test async seeding handles general exceptions."""
        with patch('src.seeds.prompt_templates.async_session_factory') as mock_session_factory:
            with patch('src.seeds.prompt_templates.PromptTemplate') as mock_template_class:
                with patch('src.seeds.prompt_templates.select') as mock_select:
                    with patch('src.seeds.prompt_templates.logger') as mock_logger:
                        mock_session = Mock()
                        mock_session.add = Mock()
                        mock_session.commit = AsyncMock()
                        mock_session.rollback = AsyncMock()
                        mock_session.execute = AsyncMock()
                        
                        # Mock session context manager
                        async def mock_session_context(self):
                            return mock_session
                        mock_context = Mock()
                        mock_context.__aenter__ = mock_session_context
                        mock_context.__aexit__ = AsyncMock(return_value=None)
                        mock_session_factory.return_value = mock_context
                        
                        # Mock initial query success
                        initial_result = Mock()
                        initial_result.scalars.return_value.all.return_value = []
                        
                        call_count = [0]
                        async def mock_execute(*args, **kwargs):
                            call_count[0] += 1
                            if call_count[0] == 1:
                                return initial_result
                            elif call_count[0] == 2:
                                # Simulate exception on first template
                                raise Exception("Template processing error")
                            else:
                                template_result = Mock()
                                template_result.scalars.return_value.first.return_value = None
                                return template_result
                        
                        mock_session.execute.side_effect = mock_execute
                        
                        await seed_async()
                        
                        # Should have handled error and continued
                        mock_session.rollback.assert_called()
                        mock_logger.error.assert_called()

    def test_seed_sync_general_exception(self):
        """Test sync seeding handles general exceptions."""
        with patch('src.seeds.prompt_templates.SessionLocal') as mock_session_local:
            with patch('src.seeds.prompt_templates.PromptTemplate') as mock_template_class:
                with patch('src.seeds.prompt_templates.select') as mock_select:
                    with patch('src.seeds.prompt_templates.logger') as mock_logger:
                        mock_session = Mock()
                        mock_session.add = Mock()
                        mock_session.commit = Mock()
                        mock_session.rollback = Mock()
                        mock_session.execute = Mock()
                        
                        # Mock session context manager
                        mock_context = Mock()
                        mock_context.__enter__ = Mock(return_value=mock_session)
                        mock_context.__exit__ = Mock(return_value=None)
                        mock_session_local.return_value = mock_context
                        
                        # Mock initial query success
                        initial_result = Mock()
                        initial_result.scalars.return_value.all.return_value = []
                        
                        call_count = [0]
                        def mock_execute(*args, **kwargs):
                            call_count[0] += 1
                            if call_count[0] == 1:
                                return initial_result
                            elif call_count[0] == 2:
                                # Simulate exception on first template
                                raise Exception("Template processing error")
                            else:
                                template_result = Mock()
                                template_result.scalars.return_value.first.return_value = None
                                return template_result
                        
                        mock_session.execute.side_effect = mock_execute
                        
                        seed_sync()
                        
                        # Should have handled error and continued
                        mock_session.rollback.assert_called()
                        mock_logger.error.assert_called()

    def test_main_module_execution(self):
        """Test main module execution via __main__ block."""
        # Test the __main__ block execution by running the module as __main__
        import runpy
        import sys
        from unittest.mock import patch
        
        with patch('src.seeds.prompt_templates.seed', new_callable=AsyncMock) as mock_seed:
            with patch('asyncio.run') as mock_asyncio_run:
                # Temporarily modify sys.argv to simulate command line execution
                original_argv = sys.argv[:]
                try:
                    sys.argv = ['src/seeds/prompt_templates.py']
                    
                    # Run the module as __main__ which will execute the __main__ block
                    runpy.run_module('src.seeds.prompt_templates', run_name='__main__')
                    
                    # Verify that asyncio.run was called with seed()
                    mock_asyncio_run.assert_called_once()
                    
                finally:
                    sys.argv = original_argv

    @pytest.mark.asyncio
    async def test_seed_async_template_update_found(self):
        """Test async seeding when templates exist and get updated."""
        with patch('src.seeds.prompt_templates.async_session_factory') as mock_session_factory:
            with patch('src.seeds.prompt_templates.PromptTemplate') as mock_template_class:
                with patch('src.seeds.prompt_templates.select') as mock_select:
                    with patch('src.seeds.prompt_templates.datetime') as mock_datetime:
                        # Setup mocks
                        mock_now = Mock()
                        mock_now.replace.return_value = datetime(2023, 1, 1, 12, 0, 0)
                        mock_datetime.now.return_value = mock_now
                        
                        mock_session = Mock()
                        mock_session.add = Mock()
                        mock_session.commit = AsyncMock()
                        mock_session.rollback = AsyncMock()
                        mock_session.execute = AsyncMock()
                        
                        # Mock session context manager
                        async def mock_session_context(self):
                            return mock_session
                        mock_context = Mock()
                        mock_context.__aenter__ = mock_session_context
                        mock_context.__aexit__ = AsyncMock(return_value=None)
                        mock_session_factory.return_value = mock_context
                        
                        # Mock initial query - shows template exists in existing_names  
                        initial_result = Mock()
                        # Mock scalars().all() to return rows where row[0] is template name
                        mock_scalars = Mock()
                        mock_scalars.all.return_value = [("test_template",)]  # This becomes existing_names = {"test_template"}
                        initial_result.scalars.return_value = mock_scalars
                        
                        # Mock existing template found during update check
                        existing_template = Mock()
                        existing_template.description = "old description"
                        existing_template.template = "old template"
                        existing_template.is_active = False
                        existing_template.updated_at = datetime.now()
                        
                        template_result = Mock()
                        template_result.scalars.return_value.first.return_value = existing_template
                        
                        call_count = [0]
                        async def mock_execute(*args, **kwargs):
                            call_count[0] += 1
                            if call_count[0] == 1:
                                return initial_result
                            else:
                                return template_result
                        
                        mock_session.execute.side_effect = mock_execute
                        
                        # Patch DEFAULT_TEMPLATES with one template
                        test_template = {
                            "name": "test_template",
                            "description": "new description", 
                            "template": "new template",
                            "is_active": True
                        }
                        
                        with patch('src.seeds.prompt_templates.DEFAULT_TEMPLATES', [test_template]):
                            await seed_async()
                        
                        # Should have updated existing template properties
                        assert existing_template.description == "new description"
                        assert existing_template.template == "new template"
                        assert existing_template.is_active == True
                        mock_session.commit.assert_called()

    @pytest.mark.asyncio
    async def test_seed_async_commit_error_non_unique(self):
        """Test async seeding handles non-unique constraint commit errors."""
        with patch('src.seeds.prompt_templates.async_session_factory') as mock_session_factory:
            with patch('src.seeds.prompt_templates.PromptTemplate') as mock_template_class:
                with patch('src.seeds.prompt_templates.select') as mock_select:
                    with patch('src.seeds.prompt_templates.logger') as mock_logger:
                        mock_session = Mock()
                        mock_session.add = Mock()
                        mock_session.commit = AsyncMock()
                        mock_session.rollback = AsyncMock()
                        mock_session.execute = AsyncMock()
                        
                        # Mock session context manager
                        async def mock_session_context(self):
                            return mock_session
                        mock_context = Mock()
                        mock_context.__aenter__ = mock_session_context
                        mock_context.__aexit__ = AsyncMock(return_value=None)
                        mock_session_factory.return_value = mock_context
                        
                        # Mock initial query success
                        initial_result = Mock()
                        initial_result.scalars.return_value.all.return_value = []
                        
                        # Mock template check returns None 
                        template_result = Mock()
                        template_result.scalars.return_value.first.return_value = None
                        
                        call_count = [0]
                        async def mock_execute(*args, **kwargs):
                            call_count[0] += 1
                            if call_count[0] == 1:
                                return initial_result
                            else:
                                return template_result
                        
                        mock_session.execute.side_effect = mock_execute
                        
                        # Mock commit error - non-unique constraint
                        commit_count = [0]
                        async def mock_commit():
                            commit_count[0] += 1
                            if commit_count[0] == 1:
                                raise Exception("Foreign key constraint failed")
                        
                        mock_session.commit.side_effect = mock_commit
                        
                        # Patch DEFAULT_TEMPLATES with one template
                        test_template = {
                            "name": "test_template",
                            "description": "description", 
                            "template": "template",
                            "is_active": True
                        }
                        
                        with patch('src.seeds.prompt_templates.DEFAULT_TEMPLATES', [test_template]):
                            await seed_async()
                        
                        # Should have handled error
                        mock_session.rollback.assert_called()
                        mock_logger.error.assert_called()

    def test_seed_sync_template_update_found(self):
        """Test sync seeding when templates exist and get updated."""
        with patch('src.seeds.prompt_templates.SessionLocal') as mock_session_local:
            with patch('src.seeds.prompt_templates.PromptTemplate') as mock_template_class:
                with patch('src.seeds.prompt_templates.select') as mock_select:
                    with patch('src.seeds.prompt_templates.datetime') as mock_datetime:
                        # Setup mocks
                        mock_now = Mock()
                        mock_now.replace.return_value = datetime(2023, 1, 1, 12, 0, 0)
                        mock_datetime.now.return_value = mock_now
                        
                        mock_session = Mock()
                        mock_session.add = Mock()
                        mock_session.commit = Mock()
                        mock_session.rollback = Mock()
                        mock_session.execute = Mock()
                        
                        # Mock session context manager
                        mock_context = Mock()
                        mock_context.__enter__ = Mock(return_value=mock_session)
                        mock_context.__exit__ = Mock(return_value=None)
                        mock_session_local.return_value = mock_context
                        
                        # Mock initial query - shows template exists in existing_names
                        initial_result = Mock()
                        # Mock scalars().all() to return rows where row[0] is template name
                        mock_scalars = Mock()
                        mock_scalars.all.return_value = [("test_template",)]  # This becomes existing_names = {"test_template"}
                        initial_result.scalars.return_value = mock_scalars
                        
                        # Mock existing template found during update check
                        existing_template = Mock()
                        existing_template.description = "old description"
                        existing_template.template = "old template"
                        existing_template.is_active = False
                        existing_template.updated_at = datetime.now()
                        
                        template_result = Mock()
                        template_result.scalars.return_value.first.return_value = existing_template
                        
                        call_count = [0]
                        def mock_execute(*args, **kwargs):
                            call_count[0] += 1
                            if call_count[0] == 1:
                                return initial_result
                            else:
                                return template_result
                        
                        mock_session.execute.side_effect = mock_execute
                        
                        # Patch DEFAULT_TEMPLATES with one template
                        test_template = {
                            "name": "test_template",
                            "description": "new description", 
                            "template": "new template",
                            "is_active": True
                        }
                        
                        with patch('src.seeds.prompt_templates.DEFAULT_TEMPLATES', [test_template]):
                            seed_sync()
                        
                        # Should have updated existing template properties
                        assert existing_template.description == "new description"
                        assert existing_template.template == "new template"
                        assert existing_template.is_active == True
                        mock_session.commit.assert_called()

    def test_seed_sync_commit_error_non_unique(self):
        """Test sync seeding handles non-unique constraint commit errors."""
        with patch('src.seeds.prompt_templates.SessionLocal') as mock_session_local:
            with patch('src.seeds.prompt_templates.PromptTemplate') as mock_template_class:
                with patch('src.seeds.prompt_templates.select') as mock_select:
                    with patch('src.seeds.prompt_templates.logger') as mock_logger:
                        mock_session = Mock()
                        mock_session.add = Mock()
                        mock_session.commit = Mock()
                        mock_session.rollback = Mock()
                        mock_session.execute = Mock()
                        
                        # Mock session context manager
                        mock_context = Mock()
                        mock_context.__enter__ = Mock(return_value=mock_session)
                        mock_context.__exit__ = Mock(return_value=None)
                        mock_session_local.return_value = mock_context
                        
                        # Mock initial query success
                        initial_result = Mock()
                        initial_result.scalars.return_value.all.return_value = []
                        
                        # Mock template check returns None 
                        template_result = Mock()
                        template_result.scalars.return_value.first.return_value = None
                        
                        call_count = [0]
                        def mock_execute(*args, **kwargs):
                            call_count[0] += 1
                            if call_count[0] == 1:
                                return initial_result
                            else:
                                return template_result
                        
                        mock_session.execute.side_effect = mock_execute
                        
                        # Mock commit error - non-unique constraint
                        commit_count = [0]
                        def mock_commit():
                            commit_count[0] += 1
                            if commit_count[0] == 1:
                                raise Exception("Foreign key constraint failed")
                        
                        mock_session.commit.side_effect = mock_commit
                        
                        # Patch DEFAULT_TEMPLATES with one template
                        test_template = {
                            "name": "test_template",
                            "description": "description", 
                            "template": "template",
                            "is_active": True
                        }
                        
                        with patch('src.seeds.prompt_templates.DEFAULT_TEMPLATES', [test_template]):
                            seed_sync()
                        
                        # Should have handled error
                        mock_session.rollback.assert_called()
                        mock_logger.error.assert_called()