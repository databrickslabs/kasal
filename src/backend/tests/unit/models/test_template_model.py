"""
Unit tests for template model.

Tests the functionality of the PromptTemplate database model including
field validation, relationships, and data integrity.
"""
import pytest
from datetime import datetime, timezone
from unittest.mock import MagicMock

from src.models.template import PromptTemplate, Template


class TestPromptTemplate:
    """Test cases for PromptTemplate model."""

    def test_prompt_template_creation(self):
        """Test basic PromptTemplate model creation."""
        # Arrange
        name = "generate_agent"
        description = "Template for generating AI agents"
        template = "Create an AI agent named {agent_name} with role {role} and tools {tools}"
        
        # Act
        prompt_template = PromptTemplate(
            name=name,
            description=description,
            template=template
        )
        
        # Assert
        assert prompt_template.name == name
        assert prompt_template.description == description
        assert prompt_template.template == template
        assert prompt_template.is_active is True  # Set by __init__ method
        assert prompt_template.group_id is None
        assert prompt_template.created_by_email is None

    def test_prompt_template_with_all_fields(self):
        """Test PromptTemplate model creation with all fields."""
        # Arrange
        name = "generate_task"
        description = "Template for generating crew tasks"
        template = "Generate a task for {objective} using {context} with expected output: {expected_output}"
        is_active = True
        group_id = "group-123"
        created_by_email = "admin@company.com"
        created_at = datetime.utcnow()
        updated_at = datetime.utcnow()
        
        # Act
        prompt_template = PromptTemplate(
            name=name,
            description=description,
            template=template,
            is_active=is_active,
            group_id=group_id,
            created_by_email=created_by_email,
            created_at=created_at,
            updated_at=updated_at
        )
        
        # Assert
        assert prompt_template.name == name
        assert prompt_template.description == description
        assert prompt_template.template == template
        assert prompt_template.is_active == is_active
        assert prompt_template.group_id == group_id
        assert prompt_template.created_by_email == created_by_email
        assert prompt_template.created_at == created_at
        assert prompt_template.updated_at == updated_at

    def test_prompt_template_inactive(self):
        """Test PromptTemplate model with inactive status."""
        # Arrange
        name = "deprecated_template"
        template = "This is a deprecated template"
        is_active = False
        
        # Act
        prompt_template = PromptTemplate(
            name=name,
            template=template,
            is_active=is_active
        )
        
        # Assert
        assert prompt_template.is_active is False
        assert prompt_template.name == name

    def test_prompt_template_defaults_via_init(self):
        """Test PromptTemplate model defaults set in __init__ method."""
        # Arrange & Act
        prompt_template = PromptTemplate(
            name="test_template",
            template="Test template content"
        )
        
        # Assert
        assert prompt_template.is_active is True  # Set by __init__
        assert prompt_template.created_at is not None  # Set by __init__
        assert prompt_template.updated_at is not None  # Set by __init__

    def test_prompt_template_init_method_logic(self):
        """Test the custom __init__ method logic."""
        # Test 1: When is_active is explicitly None
        template1 = PromptTemplate(
            name="test1",
            template="content",
            is_active=None
        )
        assert template1.is_active is True
        
        # Test 2: When is_active is explicitly False
        template2 = PromptTemplate(
            name="test2",
            template="content",
            is_active=False
        )
        assert template2.is_active is False
        
        # Test 3: When timestamps are None
        template3 = PromptTemplate(
            name="test3",
            template="content",
            created_at=None,
            updated_at=None
        )
        assert template3.created_at is not None
        assert template3.updated_at is not None

    def test_prompt_template_long_content(self):
        """Test PromptTemplate with long template content."""
        # Arrange
        name = "complex_template"
        long_template = """
        You are an AI assistant specialized in {domain}.
        
        Your role is to {role_description}.
        
        Context: {context}
        
        Instructions:
        1. {instruction_1}
        2. {instruction_2}
        3. {instruction_3}
        
        Examples:
        {examples}
        
        Please provide a detailed response following the format:
        {output_format}
        
        Remember to consider:
        - {consideration_1}
        - {consideration_2}
        - {consideration_3}
        
        Expected output: {expected_output}
        """
        
        # Act
        prompt_template = PromptTemplate(
            name=name,
            template=long_template
        )
        
        # Assert
        assert prompt_template.template == long_template
        assert len(prompt_template.template) > 500
        assert "{domain}" in prompt_template.template
        assert "{expected_output}" in prompt_template.template

    def test_prompt_template_with_placeholders(self):
        """Test PromptTemplate with various placeholder formats."""
        # Arrange
        name = "placeholder_template"
        template = """
        Agent Name: {agent_name}
        Role: {role}
        Backstory: {backstory}
        Goal: {goal}
        Tools: {tools}
        Max Iterations: {max_iter}
        Verbose: {verbose}
        Additional Config: {config.advanced_settings}
        """
        
        # Act
        prompt_template = PromptTemplate(
            name=name,
            template=template,
            description="Template with various placeholder types"
        )
        
        # Assert
        assert "{agent_name}" in prompt_template.template
        assert "{config.advanced_settings}" in prompt_template.template
        assert prompt_template.description == "Template with various placeholder types"

    def test_prompt_template_multi_tenant_fields(self):
        """Test multi-tenant fields for group isolation."""
        # Arrange
        group_id = "tenant-abc"
        created_by_email = "user@tenant-abc.com"
        
        # Act
        prompt_template = PromptTemplate(
            name="tenant_template",
            template="Template for tenant {tenant_name}",
            group_id=group_id,
            created_by_email=created_by_email
        )
        
        # Assert
        assert prompt_template.group_id == group_id
        assert prompt_template.created_by_email == created_by_email

    def test_prompt_template_table_name(self):
        """Test that the table name is correctly set."""
        # Act & Assert
        assert PromptTemplate.__tablename__ == "prompttemplate"

    def test_prompt_template_unique_name_constraint(self):
        """Test that the name field has unique constraint."""
        # Act
        name_column = PromptTemplate.__table__.columns['name']
        
        # Assert
        assert name_column.unique is True
        assert name_column.nullable is False

    def test_prompt_template_indexes(self):
        """Test that the model has the expected database indexes."""
        # Act
        columns = PromptTemplate.__table__.columns
        
        # Assert - Check that group_id has index
        group_id_column = columns['group_id']
        assert group_id_column.index is True

    def test_prompt_template_column_types(self):
        """Test that columns have correct data types."""
        # Act
        columns = PromptTemplate.__table__.columns
        
        # Assert
        assert str(columns['id'].type) == "INTEGER"
        assert "VARCHAR" in str(columns['name'].type) or "STRING" in str(columns['name'].type)
        assert "VARCHAR" in str(columns['description'].type) or "STRING" in str(columns['description'].type)
        assert "TEXT" in str(columns['template'].type)
        assert "BOOLEAN" in str(columns['is_active'].type)
        assert "DATETIME" in str(columns['created_at'].type)

    def test_prompt_template_repr(self):
        """Test string representation of PromptTemplate model."""
        # Arrange
        prompt_template = PromptTemplate(
            name="test_template",
            template="Test content"
        )
        
        # Act
        repr_str = repr(prompt_template)
        
        # Assert
        assert "PromptTemplate" in repr_str

    def test_prompt_template_with_special_characters(self):
        """Test PromptTemplate with special characters and formatting."""
        # Arrange
        name = "special_chars_template"
        template = """
        Here's a template with special characters:
        - Quotes: "Hello" and 'World'
        - Symbols: @#$%^&*()
        - Unicode: 🤖 AI Assistant
        - Newlines and tabs
        - JSON-like: {"key": "value", "number": 42}
        - Code: `print("Hello, {name}!")`
        """
        
        # Act
        prompt_template = PromptTemplate(
            name=name,
            template=template
        )
        
        # Assert
        assert '"Hello"' in prompt_template.template
        assert "🤖" in prompt_template.template
        assert '{"key": "value"' in prompt_template.template
        assert "`print(" in prompt_template.template

    def test_prompt_template_timestamp_behavior(self):
        """Test timestamp behavior in PromptTemplate."""
        # Arrange
        before_creation = datetime.utcnow()
        
        # Act
        prompt_template = PromptTemplate(
            name="timestamp_test",
            template="Testing timestamps"
        )
        
        after_creation = datetime.utcnow()
        
        # Assert
        # Note: Timestamps are set by __init__ method, not SQLAlchemy defaults
        assert prompt_template.created_at is not None
        assert prompt_template.updated_at is not None

    def test_prompt_template_empty_description(self):
        """Test PromptTemplate with empty/null description."""
        # Act
        prompt_template = PromptTemplate(
            name="no_description",
            template="Template without description",
            description=None
        )
        
        # Assert
        assert prompt_template.description is None
        assert prompt_template.name == "no_description"

    def test_prompt_template_common_use_cases(self):
        """Test PromptTemplate for common AI workflow use cases."""
        # Test agent generation template
        agent_template = PromptTemplate(
            name="generate_agent",
            description="Generate CrewAI agent configuration",
            template="Create an agent with name: {name}, role: {role}, goal: {goal}, backstory: {backstory}"
        )
        
        # Test task generation template
        task_template = PromptTemplate(
            name="generate_task",
            description="Generate CrewAI task configuration",
            template="Create a task: {description}. Expected output: {expected_output}. Agent: {agent}"
        )
        
        # Test crew generation template
        crew_template = PromptTemplate(
            name="generate_crew",
            description="Generate CrewAI crew configuration",
            template="Create a crew with agents: {agents} and tasks: {tasks}. Process: {process}"
        )
        
        # Assert
        assert agent_template.name == "generate_agent"
        assert task_template.name == "generate_task"
        assert crew_template.name == "generate_crew"
        assert "{name}" in agent_template.template
        assert "{expected_output}" in task_template.template
        assert "{process}" in crew_template.template


class TestTemplateAlias:
    """Test cases for Template alias."""

    def test_template_alias_is_prompt_template(self):
        """Test that Template is an alias for PromptTemplate."""
        # Act & Assert
        assert Template is PromptTemplate

    def test_template_alias_functionality(self):
        """Test that Template alias works the same as PromptTemplate."""
        # Arrange
        name = "alias_test"
        template_content = "Testing alias functionality"
        
        # Act
        template_via_alias = Template(
            name=name,
            template=template_content
        )
        
        template_via_class = PromptTemplate(
            name=name + "_2",
            template=template_content
        )
        
        # Assert
        assert template_via_alias.template == template_via_class.template
        assert type(template_via_alias) == type(template_via_class)
        assert isinstance(template_via_alias, PromptTemplate)


class TestPromptTemplateEdgeCases:
    """Test edge cases and error scenarios for PromptTemplate."""

    def test_prompt_template_minimum_required_fields(self):
        """Test PromptTemplate with only required fields."""
        # Act
        prompt_template = PromptTemplate(
            name="minimal",
            template="Minimal template"
        )
        
        # Assert
        assert prompt_template.name == "minimal"
        assert prompt_template.template == "Minimal template"
        assert prompt_template.description is None
        assert prompt_template.is_active is True

    def test_prompt_template_very_long_name(self):
        """Test PromptTemplate with very long name."""
        # Arrange
        long_name = "very_long_template_name_" * 5  # 120 characters
        
        # Act
        prompt_template = PromptTemplate(
            name=long_name,
            template="Template with long name"
        )
        
        # Assert
        assert prompt_template.name == long_name
        assert len(prompt_template.name) == 120

    def test_prompt_template_empty_template_content(self):
        """Test PromptTemplate with empty template content."""
        # Act
        prompt_template = PromptTemplate(
            name="empty_content",
            template=""
        )
        
        # Assert
        assert prompt_template.template == ""
        assert prompt_template.name == "empty_content"

    def test_prompt_template_group_isolation_scenarios(self):
        """Test various group isolation scenarios."""
        # Scenario 1: Global template (no group)
        global_template = PromptTemplate(
            name="global_template",
            template="Available to all groups",
            group_id=None
        )
        
        # Scenario 2: Group-specific template
        group_template = PromptTemplate(
            name="group_specific",
            template="Only for specific group",
            group_id="group-123"
        )
        
        # Scenario 3: User-created template
        user_template = PromptTemplate(
            name="user_created",
            template="Created by specific user",
            group_id="group-456",
            created_by_email="user@company.com"
        )
        
        # Assert
        assert global_template.group_id is None
        assert group_template.group_id == "group-123"
        assert user_template.group_id == "group-456"
        assert user_template.created_by_email == "user@company.com"