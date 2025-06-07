"""
Unit tests for template models.

Tests the functionality of PromptTemplate model including
template management, versioning, and multi-group support.
"""
import pytest
from datetime import datetime
from unittest.mock import patch

from src.models.template import PromptTemplate


class TestPromptTemplate:
    """Test cases for PromptTemplate model."""
    
    def test_prompt_template_creation(self):
        """Test basic PromptTemplate creation."""
        template = PromptTemplate(
            name="generate_agent",
            description="Template for generating AI agents",
            template="Create an AI agent with the following specifications: {specifications}"
        )
        
        assert template.name == "generate_agent"
        assert template.description == "Template for generating AI agents"
        assert template.template == "Create an AI agent with the following specifications: {specifications}"
    
    def test_prompt_template_required_fields(self):
        """Test PromptTemplate with required fields only."""
        template = PromptTemplate(
            name="basic_template",
            template="This is a basic template with {placeholder}"
        )
        
        assert template.name == "basic_template"
        assert template.template == "This is a basic template with {placeholder}"
        assert template.description is None
        assert template.is_active is True  # Default value
    
    def test_prompt_template_with_all_fields(self):
        """Test PromptTemplate creation with all fields."""
        template = PromptTemplate(
            name="comprehensive_agent_generator",
            description="Comprehensive template for generating AI agents with detailed specifications",
            template="""Create an AI agent with the following details:
Name: {agent_name}
Role: {agent_role}
Goal: {agent_goal}
Backstory: {agent_backstory}
Tools: {agent_tools}
Additional Configuration: {config}""",
            is_active=True,
            group_id="development_team",
            created_by_email="developer@company.com"
        )
        
        assert template.name == "comprehensive_agent_generator"
        assert template.description.startswith("Comprehensive template")
        assert "{agent_name}" in template.template
        assert template.is_active is True
        assert template.group_id == "development_team"
        assert template.created_by_email == "developer@company.com"
    
    def test_prompt_template_defaults(self):
        """Test PromptTemplate default values."""
        template = PromptTemplate(
            name="default_template",
            template="Default template content"
        )
        
        assert template.is_active is True
        assert template.description is None
        assert template.group_id is None
        assert template.tenant_id is None
        assert template.created_by_email is None
    
    def test_prompt_template_inactive(self):
        """Test PromptTemplate with is_active=False."""
        template = PromptTemplate(
            name="inactive_template",
            template="Inactive template content",
            is_active=False
        )
        
        assert template.is_active is False
    
    def test_prompt_template_timestamps(self):
        """Test PromptTemplate timestamp fields."""
        with patch('src.models.template.datetime') as mock_datetime:
            mock_now = datetime(2023, 1, 1, 12, 0, 0)
            mock_datetime.utcnow.return_value = mock_now
            
            template = PromptTemplate(
                name="timestamp_template",
                template="Template for timestamp testing"
            )
            
            assert template.created_at == mock_now
            assert template.updated_at == mock_now
    
    def test_prompt_template_group_fields(self):
        """Test PromptTemplate group-related fields."""
        template = PromptTemplate(
            name="group_template",
            template="Template for group testing",
            group_id="group_123",
            created_by_email="user@group.com"
        )
        
        assert template.group_id == "group_123"
        assert template.created_by_email == "user@group.com"
        assert template.tenant_id is None  # Legacy field should be None
    
    def test_prompt_template_legacy_tenant_fields(self):
        """Test PromptTemplate legacy tenant fields."""
        template = PromptTemplate(
            name="tenant_template",
            template="Template for tenant testing",
            tenant_id="tenant_456"
        )
        
        assert template.tenant_id == "tenant_456"
        assert template.group_id is None  # New field should be None
    
    def test_prompt_template_unique_name(self):
        """Test PromptTemplate name uniqueness constraint."""
        # Note: This tests the constraint definition, actual uniqueness 
        # enforcement would be tested at the database level
        template = PromptTemplate(
            name="unique_template",
            template="Template with unique name"
        )
        
        assert template.name == "unique_template"


class TestPromptTemplateTypes:
    """Test cases for different types of prompt templates."""
    
    def test_agent_generation_template(self):
        """Test template for agent generation."""
        agent_template = PromptTemplate(
            name="generate_agent",
            description="Template for generating AI agents",
            template="""You are an expert AI agent designer. Create an AI agent with these specifications:

Agent Name: {agent_name}
Role: {role}
Goal: {goal}
Backstory: {backstory}
Tools: {tools}

Please provide a comprehensive agent configuration including:
1. Detailed role description
2. Specific goals and objectives
3. Relevant backstory that motivates the agent
4. List of appropriate tools for the role
5. Any special instructions or constraints

Format the output as a structured configuration."""
        )
        
        assert agent_template.name == "generate_agent"
        assert "{agent_name}" in agent_template.template
        assert "{role}" in agent_template.template
        assert "{goal}" in agent_template.template
        assert "structured configuration" in agent_template.template
    
    def test_task_generation_template(self):
        """Test template for task generation."""
        task_template = PromptTemplate(
            name="generate_task",
            description="Template for generating AI tasks",
            template="""Create a detailed task specification:

Task Name: {task_name}
Description: {description}
Expected Output: {expected_output}
Agent Assignment: {agent}
Tools Required: {tools}
Context: {context}

Requirements:
1. Clear and actionable task description
2. Specific expected output format
3. Appropriate tool selection
4. Relevant context information
5. Success criteria

Generate a complete task definition."""
        )
        
        assert task_template.name == "generate_task"
        assert "{task_name}" in task_template.template
        assert "{expected_output}" in task_template.template
        assert "success criteria" in task_template.template
    
    def test_crew_generation_template(self):
        """Test template for crew generation."""
        crew_template = PromptTemplate(
            name="generate_crew",
            description="Template for generating AI crews",
            template="""Design a comprehensive AI crew for the following project:

Project: {project_name}
Objective: {objective}
Domain: {domain}
Timeline: {timeline}
Complexity: {complexity}

Create a crew that includes:
1. Multiple specialized agents with distinct roles
2. Clear task distribution and workflow
3. Inter-agent communication protocols
4. Quality assurance mechanisms
5. Performance monitoring

Provide detailed specifications for each agent and their tasks."""
        )
        
        assert crew_template.name == "generate_crew"
        assert "{project_name}" in crew_template.template
        assert "{objective}" in crew_template.template
        assert "specialized agents" in crew_template.template
    
    def test_workflow_template(self):
        """Test template for workflow generation."""
        workflow_template = PromptTemplate(
            name="generate_workflow",
            description="Template for generating workflows",
            template="""Create a detailed workflow specification:

Workflow Name: {workflow_name}
Purpose: {purpose}
Input Requirements: {inputs}
Output Deliverables: {outputs}
Constraints: {constraints}

Design a workflow that includes:
1. Sequential and parallel task execution
2. Decision points and conditional logic
3. Error handling and retry mechanisms
4. Progress monitoring and reporting
5. Resource allocation and optimization

Provide a complete workflow blueprint."""
        )
        
        assert workflow_template.name == "generate_workflow"
        assert "{workflow_name}" in workflow_template.template
        assert "conditional logic" in workflow_template.template
    
    def test_analysis_template(self):
        """Test template for data analysis tasks."""
        analysis_template = PromptTemplate(
            name="data_analysis",
            description="Template for data analysis tasks",
            template="""Perform comprehensive data analysis:

Dataset: {dataset}
Analysis Type: {analysis_type}
Objectives: {objectives}
Metrics: {metrics}
Visualization Requirements: {visualizations}

Execute the following analysis steps:
1. Data exploration and profiling
2. Statistical analysis and hypothesis testing
3. Pattern identification and trend analysis
4. Insight generation and interpretation
5. Visualization and reporting

Provide actionable insights and recommendations."""
        )
        
        assert analysis_template.name == "data_analysis"
        assert "{dataset}" in analysis_template.template
        assert "actionable insights" in analysis_template.template


class TestPromptTemplateContent:
    """Test cases for prompt template content validation."""
    
    def test_template_with_placeholders(self):
        """Test template with various placeholder formats."""
        template = PromptTemplate(
            name="placeholder_template",
            template="""Template with different placeholders:
Simple: {name}
Nested: {user.profile.name}
With defaults: {description|default_description}
Formatted: {date:%Y-%m-%d}
Multiple: {param1} and {param2} and {param3}"""
        )
        
        assert "{name}" in template.template
        assert "{user.profile.name}" in template.template
        assert "{description|default_description}" in template.template
        assert "{date:%Y-%m-%d}" in template.template
        assert "{param1}" in template.template
    
    def test_template_multiline_content(self):
        """Test template with multiline content."""
        multiline_template = PromptTemplate(
            name="multiline_template",
            template="""This is a multiline template.

It contains multiple paragraphs and sections:

Section 1: {section1}
- Point A: {point_a}
- Point B: {point_b}

Section 2: {section2}
The content can span multiple lines
and include various formatting.

Conclusion: {conclusion}"""
        )
        
        assert "multiline template" in multiline_template.template
        assert "Section 1:" in multiline_template.template
        assert "Conclusion:" in multiline_template.template
        assert multiline_template.template.count('\n') > 5
    
    def test_template_special_characters(self):
        """Test template with special characters."""
        special_template = PromptTemplate(
            name="special_chars_template",
            template="""Template with special characters:
- Unicode: éñ中文🌟
- Symbols: @#$%^&*()
- Quotes: "double" and 'single'
- Brackets: [square] and (round) and {curly}
- Code: `code` and ```block```
- Math: α + β = γ
Parameter: {special_param}"""
        )
        
        assert "éñ中文🌟" in special_template.template
        assert "@#$%^&*()" in special_template.template
        assert '"double"' in special_template.template
        assert "`code`" in special_template.template
        assert "α + β = γ" in special_template.template
    
    def test_template_json_like_content(self):
        """Test template with JSON-like content."""
        json_template = PromptTemplate(
            name="json_template",
            template="""{
  "agent_config": {
    "name": "{agent_name}",
    "role": "{agent_role}",
    "capabilities": [
      "{capability1}",
      "{capability2}",
      "{capability3}"
    ],
    "settings": {
      "max_iterations": {max_iter},
      "temperature": {temperature},
      "verbose": {verbose}
    }
  }
}"""
        )
        
        assert '"name": "{agent_name}"' in json_template.template
        assert '"capabilities":' in json_template.template
        assert '"max_iterations": {max_iter}' in json_template.template
    
    def test_template_code_examples(self):
        """Test template with code examples."""
        code_template = PromptTemplate(
            name="code_example_template",
            template="""Generate code based on requirements:

Language: {language}
Function: {function_name}
Parameters: {parameters}

Example template:
```{language}
def {function_name}({parameters}):
    \"\"\"
    {description}
    \"\"\"
    # Implementation here
    {implementation}
    return {return_value}
```

Requirements:
- Follow {language} best practices
- Include proper documentation
- Handle edge cases
- Return appropriate types"""
        )
        
        assert "```{language}" in code_template.template
        assert "def {function_name}" in code_template.template
        assert "best practices" in code_template.template


class TestPromptTemplateFieldTypes:
    """Test cases for PromptTemplate field types and constraints."""
    
    def test_template_field_existence(self):
        """Test that all expected fields exist."""
        template = PromptTemplate(
            name="field_test_template",
            template="Template for field testing"
        )
        
        # Check field existence
        assert hasattr(template, 'id')
        assert hasattr(template, 'name')
        assert hasattr(template, 'description')
        assert hasattr(template, 'template')
        assert hasattr(template, 'is_active')
        assert hasattr(template, 'group_id')
        assert hasattr(template, 'tenant_id')
        assert hasattr(template, 'created_by_email')
        assert hasattr(template, 'created_at')
        assert hasattr(template, 'updated_at')
    
    def test_template_string_fields(self):
        """Test string field types."""
        template = PromptTemplate(
            name="string_test_template",
            description="Template for string testing",
            template="String template content",
            group_id="group_123",
            created_by_email="user@test.com"
        )
        
        assert isinstance(template.name, str)
        assert isinstance(template.description, str)
        assert isinstance(template.template, str)
        assert isinstance(template.group_id, str)
        assert isinstance(template.created_by_email, str)
    
    def test_template_text_field(self):
        """Test template Text field for large content."""
        large_content = "This is a very large template content. " * 1000
        template = PromptTemplate(
            name="large_template",
            template=large_content
        )
        
        assert template.template == large_content
        assert len(template.template) > 10000
    
    def test_template_boolean_fields(self):
        """Test boolean field types."""
        active_template = PromptTemplate(
            name="active_template",
            template="Active template",
            is_active=True
        )
        
        inactive_template = PromptTemplate(
            name="inactive_template",
            template="Inactive template",
            is_active=False
        )
        
        assert isinstance(active_template.is_active, bool)
        assert isinstance(inactive_template.is_active, bool)
        assert active_template.is_active is True
        assert inactive_template.is_active is False
    
    def test_template_datetime_fields(self):
        """Test datetime field types."""
        template = PromptTemplate(
            name="datetime_template",
            template="DateTime template"
        )
        
        assert isinstance(template.created_at, datetime)
        assert isinstance(template.updated_at, datetime)
    
    def test_template_nullable_fields(self):
        """Test nullable field behavior."""
        template = PromptTemplate(
            name="nullable_template",
            template="Nullable template"
        )
        
        # These fields should be nullable
        assert template.description is None
        assert template.group_id is None
        assert template.tenant_id is None
        assert template.created_by_email is None
    
    def test_template_non_nullable_fields(self):
        """Test non-nullable field requirements."""
        template = PromptTemplate(
            name="non_nullable_template",
            template="Non-nullable template"
        )
        
        # These fields are non-nullable
        assert template.name is not None
        assert template.template is not None


class TestPromptTemplateUsagePatterns:
    """Test cases for common PromptTemplate usage patterns."""
    
    def test_template_versioning_pattern(self):
        """Test template versioning pattern."""
        # Version 1.0
        template_v1 = PromptTemplate(
            name="agent_generator_v1",
            description="Agent generator template version 1.0",
            template="Simple agent: {name} with role {role}",
            is_active=False  # Deprecated
        )
        
        # Version 2.0
        template_v2 = PromptTemplate(
            name="agent_generator_v2",
            description="Agent generator template version 2.0",
            template="""Advanced agent configuration:
Name: {name}
Role: {role}
Goal: {goal}
Backstory: {backstory}
Tools: {tools}""",
            is_active=True  # Current version
        )
        
        assert template_v1.is_active is False
        assert template_v2.is_active is True
        assert len(template_v2.template) > len(template_v1.template)
    
    def test_template_group_isolation(self):
        """Test template group isolation pattern."""
        # Team A templates
        team_a_template = PromptTemplate(
            name="team_a_agent_template",
            description="Agent template for team A",
            template="Team A agent: {name} for {project}",
            group_id="team_a",
            created_by_email="lead@teama.com"
        )
        
        # Team B templates
        team_b_template = PromptTemplate(
            name="team_b_analysis_template",
            description="Analysis template for team B",
            template="Team B analysis: {dataset} with {method}",
            group_id="team_b",
            created_by_email="analyst@teamb.com"
        )
        
        # Verify group isolation
        assert team_a_template.group_id == "team_a"
        assert team_a_template.created_by_email == "lead@teama.com"
        assert team_b_template.group_id == "team_b"
        assert team_b_template.created_by_email == "analyst@teamb.com"
    
    def test_template_lifecycle_management(self):
        """Test template lifecycle management."""
        # Development phase
        dev_template = PromptTemplate(
            name="dev_experimental_template",
            description="Experimental template under development",
            template="Experimental: {feature} with {parameters}",
            is_active=False
        )
        
        # Production phase
        prod_template = PromptTemplate(
            name="prod_stable_template",
            description="Stable production template",
            template="Production ready: {feature} with {parameters}",
            is_active=True
        )
        
        # Deprecated phase
        deprecated_template = PromptTemplate(
            name="deprecated_old_template",
            description="Deprecated template - use prod_stable_template instead",
            template="Old format: {legacy_params}",
            is_active=False
        )
        
        assert dev_template.is_active is False
        assert prod_template.is_active is True
        assert deprecated_template.is_active is False
        assert "deprecated" in deprecated_template.description.lower()
    
    def test_template_specialization_pattern(self):
        """Test template specialization pattern."""
        # Base template
        base_template = PromptTemplate(
            name="base_agent_template",
            description="Base template for all agents",
            template="Agent {name} with role {role}"
        )
        
        # Specialized templates
        analyst_template = PromptTemplate(
            name="data_analyst_template",
            description="Specialized template for data analysts",
            template="""Data Analyst Agent: {name}
Role: {role}
Specialization: Data Analysis
Tools: {data_tools}
Datasets: {datasets}
Analysis Methods: {methods}"""
        )
        
        researcher_template = PromptTemplate(
            name="researcher_template",
            description="Specialized template for researchers",
            template="""Research Agent: {name}
Role: {role}
Specialization: Research
Research Areas: {research_areas}
Sources: {sources}
Methodology: {methodology}"""
        )
        
        # Verify specialization
        assert "Data Analysis" in analyst_template.template
        assert "{data_tools}" in analyst_template.template
        assert "Research" in researcher_template.template
        assert "{research_areas}" in researcher_template.template
    
    def test_template_migration_compatibility(self):
        """Test template migration compatibility between tenant and group fields."""
        # Legacy tenant-based template
        tenant_template = PromptTemplate(
            name="legacy_tenant_template",
            description="Template from tenant system",
            template="Legacy template: {params}",
            tenant_id="tenant_123"
        )
        
        # New group-based template
        group_template = PromptTemplate(
            name="new_group_template",
            description="Template from group system",
            template="Group template: {params}",
            group_id="group_456",
            created_by_email="user@group.com"
        )
        
        # Verify both can coexist
        assert tenant_template.tenant_id == "tenant_123"
        assert tenant_template.group_id is None
        
        assert group_template.group_id == "group_456"
        assert group_template.tenant_id is None
    
    def test_template_parameter_validation(self):
        """Test template parameter validation patterns."""
        # Template with required parameters
        required_params_template = PromptTemplate(
            name="required_params_template",
            description="Template with required parameters",
            template="""Required parameters template:
Name: {name}  # Required
Role: {role}  # Required
Goal: {goal}  # Required

Optional parameters:
Description: {description|Default description}
Tools: {tools|[]}
Config: {config|{}}"""
        )
        
        # Template with validation instructions
        validation_template = PromptTemplate(
            name="validation_template",
            description="Template with validation rules",
            template="""Validation template:
Name: {name}  # Must be 3-50 characters
Email: {email}  # Must be valid email format
Age: {age}  # Must be integer 18-100
Role: {role}  # Must be one of: admin, user, viewer

Validate all parameters before processing."""
        )
        
        assert "{name}" in required_params_template.template
        assert "Required" in required_params_template.template
        assert "Default description" in required_params_template.template
        assert "Must be 3-50 characters" in validation_template.template
        assert "Validate all parameters" in validation_template.template