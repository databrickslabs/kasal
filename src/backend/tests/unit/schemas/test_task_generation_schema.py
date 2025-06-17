"""
Unit tests for task generation schemas.

Tests the functionality of Pydantic schemas for task generation operations
including validation, serialization, and field constraints.
"""
import pytest
from pydantic import ValidationError

from src.schemas.task_generation import (
    Agent, TaskGenerationRequest, AdvancedConfig, TaskGenerationResponse
)


class TestAgent:
    """Test cases for Agent schema."""
    
    def test_valid_agent(self):
        """Test Agent with valid data."""
        agent_data = {
            "name": "data_analyst",
            "role": "Senior Data Analyst",
            "goal": "Analyze complex datasets to extract meaningful insights",
            "backstory": "Expert with 10 years of experience in data science and analytics"
        }
        agent = Agent(**agent_data)
        assert agent.name == "data_analyst"
        assert agent.role == "Senior Data Analyst"
        assert agent.goal == "Analyze complex datasets to extract meaningful insights"
        assert agent.backstory == "Expert with 10 years of experience in data science and analytics"
    
    def test_agent_missing_required_fields(self):
        """Test Agent validation with missing required fields."""
        required_fields = ["name", "role", "goal", "backstory"]
        
        for missing_field in required_fields:
            agent_data = {
                "name": "test_agent",
                "role": "Test Role",
                "goal": "Test Goal",
                "backstory": "Test Backstory"
            }
            del agent_data[missing_field]
            
            with pytest.raises(ValidationError) as exc_info:
                Agent(**agent_data)
            
            errors = exc_info.value.errors()
            missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
            assert missing_field in missing_fields
    
    def test_agent_empty_fields(self):
        """Test Agent with empty string fields."""
        agent_data = {
            "name": "",
            "role": "",
            "goal": "",
            "backstory": ""
        }
        agent = Agent(**agent_data)
        assert agent.name == ""
        assert agent.role == ""
        assert agent.goal == ""
        assert agent.backstory == ""
    
    def test_agent_long_content(self):
        """Test Agent with long field values."""
        long_text = "A" * 1000
        agent_data = {
            "name": "long_content_agent",
            "role": long_text,
            "goal": long_text,
            "backstory": long_text
        }
        agent = Agent(**agent_data)
        assert len(agent.role) == 1000
        assert len(agent.goal) == 1000
        assert len(agent.backstory) == 1000
    
    def test_agent_special_characters(self):
        """Test Agent with special characters."""
        agent_data = {
            "name": "ai_ml_agent_v2.1",
            "role": "AI/ML Engineer & Data Scientist",
            "goal": "Build & deploy ML models (95% accuracy)",
            "backstory": "PhD in CS, specializes in NLP/CV @TechCorp"
        }
        agent = Agent(**agent_data)
        assert "ai_ml_agent_v2.1" == agent.name
        assert "&" in agent.role
        assert "95%" in agent.goal
        assert "@TechCorp" in agent.backstory
    
    def test_agent_realistic_examples(self):
        """Test Agent with realistic examples."""
        agents = [
            {
                "name": "research_analyst",
                "role": "Research Analyst",
                "goal": "Conduct comprehensive market research and analysis",
                "backstory": "Former management consultant with expertise in market analysis and strategic planning"
            },
            {
                "name": "content_writer",
                "role": "Technical Content Writer",
                "goal": "Create clear and engaging technical documentation",
                "backstory": "Technical writer with 8 years of experience in software documentation and developer relations"
            },
            {
                "name": "qa_engineer",
                "role": "Quality Assurance Engineer",
                "goal": "Ensure software quality through comprehensive testing",
                "backstory": "QA professional with expertise in automated testing and quality processes"
            }
        ]
        
        for agent_data in agents:
            agent = Agent(**agent_data)
            assert len(agent.name) > 0
            assert len(agent.role) > 0
            assert len(agent.goal) > 0
            assert len(agent.backstory) > 0


class TestTaskGenerationRequest:
    """Test cases for TaskGenerationRequest schema."""
    
    def test_valid_task_generation_request_minimal(self):
        """Test TaskGenerationRequest with minimal required fields."""
        request_data = {
            "text": "Generate a task for data analysis"
        }
        request = TaskGenerationRequest(**request_data)
        assert request.text == "Generate a task for data analysis"
        assert request.model is None
        assert request.agent is None
        assert request.markdown is False  # Default
    
    def test_valid_task_generation_request_complete(self):
        """Test TaskGenerationRequest with all fields."""
        agent = Agent(
            name="analyst",
            role="Data Analyst",
            goal="Analyze data",
            backstory="Experienced analyst"
        )
        
        request_data = {
            "text": "Create a comprehensive analysis task for sales data",
            "model": "gpt-4",
            "agent": agent,
            "markdown": True
        }
        request = TaskGenerationRequest(**request_data)
        assert request.text == "Create a comprehensive analysis task for sales data"
        assert request.model == "gpt-4"
        assert request.agent == agent
        assert request.agent.name == "analyst"
        assert request.markdown is True
    
    def test_task_generation_request_missing_text(self):
        """Test TaskGenerationRequest validation with missing text."""
        with pytest.raises(ValidationError) as exc_info:
            TaskGenerationRequest(model="gpt-4")
        
        errors = exc_info.value.errors()
        missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
        assert "text" in missing_fields
    
    def test_task_generation_request_empty_text(self):
        """Test TaskGenerationRequest with empty text."""
        request = TaskGenerationRequest(text="")
        assert request.text == ""
    
    def test_task_generation_request_various_models(self):
        """Test TaskGenerationRequest with various model values."""
        models = [
            "gpt-4",
            "gpt-3.5-turbo",
            "claude-3-opus-20240229",
            "databricks-llama-4-maverick",
            "local-model-v1.0"
        ]
        
        for model in models:
            request = TaskGenerationRequest(
                text="Generate task",
                model=model
            )
            assert request.model == model
    
    def test_task_generation_request_with_agent_object(self):
        """Test TaskGenerationRequest with agent object."""
        agent = Agent(
            name="specialized_agent",
            role="Domain Expert",
            goal="Provide specialized knowledge",
            backstory="Expert in specific domain with years of experience"
        )
        
        request = TaskGenerationRequest(
            text="Generate a specialized task",
            agent=agent
        )
        assert request.agent.name == "specialized_agent"
        assert request.agent.role == "Domain Expert"
    
    def test_task_generation_request_markdown_variations(self):
        """Test TaskGenerationRequest with markdown variations."""
        # Markdown enabled
        request_markdown = TaskGenerationRequest(
            text="Generate task with markdown",
            markdown=True
        )
        assert request_markdown.markdown is True
        
        # Markdown disabled
        request_no_markdown = TaskGenerationRequest(
            text="Generate task without markdown",
            markdown=False
        )
        assert request_no_markdown.markdown is False
        
        # Default markdown (should be False)
        request_default = TaskGenerationRequest(
            text="Generate task with default markdown"
        )
        assert request_default.markdown is False


class TestAdvancedConfig:
    """Test cases for AdvancedConfig schema."""
    
    def test_valid_advanced_config_defaults(self):
        """Test AdvancedConfig with default values."""
        config = AdvancedConfig()
        assert config.async_execution is False
        assert config.context == []
        assert config.output_json is None
        assert config.output_pydantic is None
        assert config.output_file is None
        assert config.human_input is False
        assert config.retry_on_fail is True
        assert config.max_retries == 3
        assert config.timeout is None
        assert config.priority == 1
        assert config.dependencies == []
        assert config.callback is None
        assert config.error_handling == "default"
        assert config.output_parser is None
        assert config.cache_response is True
        assert config.cache_ttl == 3600
        assert config.markdown is False
    
    def test_valid_advanced_config_custom(self):
        """Test AdvancedConfig with custom values."""
        config_data = {
            "async_execution": True,
            "context": ["task1", "task2", "task3"],
            "output_json": {"type": "object", "properties": {"result": {"type": "string"}}},
            "output_pydantic": "MyOutputModel",
            "output_file": "/path/to/output.json",
            "human_input": True,
            "retry_on_fail": False,
            "max_retries": 5,
            "timeout": 300,
            "priority": 10,
            "dependencies": ["dep1", "dep2"],
            "callback": "my_callback_function",
            "error_handling": "strict",
            "output_parser": "json_parser",
            "cache_response": False,
            "cache_ttl": 7200,
            "markdown": True
        }
        config = AdvancedConfig(**config_data)
        assert config.async_execution is True
        assert config.context == ["task1", "task2", "task3"]
        assert config.output_json == {"type": "object", "properties": {"result": {"type": "string"}}}
        assert config.output_pydantic == "MyOutputModel"
        assert config.output_file == "/path/to/output.json"
        assert config.human_input is True
        assert config.retry_on_fail is False
        assert config.max_retries == 5
        assert config.timeout == 300
        assert config.priority == 10
        assert config.dependencies == ["dep1", "dep2"]
        assert config.callback == "my_callback_function"
        assert config.error_handling == "strict"
        assert config.output_parser == "json_parser"
        assert config.cache_response is False
        assert config.cache_ttl == 7200
        assert config.markdown is True
    
    def test_advanced_config_context_scenarios(self):
        """Test AdvancedConfig with various context scenarios."""
        # Empty context
        config_empty = AdvancedConfig(context=[])
        assert config_empty.context == []
        
        # Single context task
        config_single = AdvancedConfig(context=["prerequisite_task"])
        assert config_single.context == ["prerequisite_task"]
        
        # Multiple context tasks
        config_multiple = AdvancedConfig(
            context=["setup_task", "data_prep_task", "validation_task"]
        )
        assert len(config_multiple.context) == 3
        assert "setup_task" in config_multiple.context
    
    def test_advanced_config_output_configurations(self):
        """Test AdvancedConfig with various output configurations."""
        # JSON output schema
        json_config = AdvancedConfig(
            output_json={
                "type": "object",
                "properties": {
                    "status": {"type": "string"},
                    "results": {"type": "array"},
                    "confidence": {"type": "number"}
                },
                "required": ["status", "results"]
            }
        )
        assert "properties" in json_config.output_json
        assert "status" in json_config.output_json["properties"]
        
        # Pydantic model output
        pydantic_config = AdvancedConfig(
            output_pydantic="AnalysisResult"
        )
        assert pydantic_config.output_pydantic == "AnalysisResult"
        
        # File output
        file_config = AdvancedConfig(
            output_file="/outputs/analysis_results.json"
        )
        assert file_config.output_file == "/outputs/analysis_results.json"
    
    def test_advanced_config_retry_and_timeout(self):
        """Test AdvancedConfig retry and timeout configurations."""
        # High retry configuration
        retry_config = AdvancedConfig(
            retry_on_fail=True,
            max_retries=10,
            timeout=600
        )
        assert retry_config.retry_on_fail is True
        assert retry_config.max_retries == 10
        assert retry_config.timeout == 600
        
        # No retry configuration
        no_retry_config = AdvancedConfig(
            retry_on_fail=False,
            max_retries=0
        )
        assert no_retry_config.retry_on_fail is False
        assert no_retry_config.max_retries == 0
    
    def test_advanced_config_priority_and_dependencies(self):
        """Test AdvancedConfig priority and dependency configurations."""
        # High priority with dependencies
        priority_config = AdvancedConfig(
            priority=100,
            dependencies=["critical_setup", "data_validation", "security_check"]
        )
        assert priority_config.priority == 100
        assert len(priority_config.dependencies) == 3
        assert "critical_setup" in priority_config.dependencies
        
        # Low priority, no dependencies
        simple_config = AdvancedConfig(
            priority=1,
            dependencies=[]
        )
        assert simple_config.priority == 1
        assert simple_config.dependencies == []
    
    def test_advanced_config_caching_configurations(self):
        """Test AdvancedConfig caching configurations."""
        # Long cache configuration
        long_cache = AdvancedConfig(
            cache_response=True,
            cache_ttl=86400  # 24 hours
        )
        assert long_cache.cache_response is True
        assert long_cache.cache_ttl == 86400
        
        # No cache configuration
        no_cache = AdvancedConfig(
            cache_response=False,
            cache_ttl=0
        )
        assert no_cache.cache_response is False
        assert no_cache.cache_ttl == 0


class TestTaskGenerationResponse:
    """Test cases for TaskGenerationResponse schema."""
    
    def test_valid_task_generation_response_minimal(self):
        """Test TaskGenerationResponse with minimal required fields."""
        response_data = {
            "name": "data_analysis_task",
            "description": "Analyze the provided dataset to identify key trends",
            "expected_output": "A comprehensive analysis report with insights and recommendations"
        }
        response = TaskGenerationResponse(**response_data)
        assert response.name == "data_analysis_task"
        assert response.description == "Analyze the provided dataset to identify key trends"
        assert response.expected_output == "A comprehensive analysis report with insights and recommendations"
        assert response.tools == []  # Default
        assert isinstance(response.advanced_config, AdvancedConfig)
    
    def test_valid_task_generation_response_complete(self):
        """Test TaskGenerationResponse with all fields."""
        tools = [
            {"name": "pandas", "type": "data_processing"},
            {"name": "matplotlib", "type": "visualization"},
            {"name": "scikit-learn", "type": "machine_learning"}
        ]
        
        advanced_config = AdvancedConfig(
            async_execution=True,
            timeout=300,
            priority=5,
            cache_response=True
        )
        
        response_data = {
            "name": "comprehensive_analysis",
            "description": "Perform comprehensive data analysis with multiple tools",
            "expected_output": "Detailed analysis report with visualizations and ML insights",
            "tools": tools,
            "advanced_config": advanced_config
        }
        response = TaskGenerationResponse(**response_data)
        assert response.name == "comprehensive_analysis"
        assert response.description == "Perform comprehensive data analysis with multiple tools"
        assert response.expected_output == "Detailed analysis report with visualizations and ML insights"
        assert len(response.tools) == 3
        assert response.tools[0]["name"] == "pandas"
        assert response.advanced_config.async_execution is True
        assert response.advanced_config.timeout == 300
    
    def test_task_generation_response_missing_required_fields(self):
        """Test TaskGenerationResponse validation with missing required fields."""
        required_fields = ["name", "description", "expected_output"]
        
        for missing_field in required_fields:
            response_data = {
                "name": "test_task",
                "description": "Test description",
                "expected_output": "Test output"
            }
            del response_data[missing_field]
            
            with pytest.raises(ValidationError) as exc_info:
                TaskGenerationResponse(**response_data)
            
            errors = exc_info.value.errors()
            missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
            assert missing_field in missing_fields
    
    def test_task_generation_response_tools_validation(self):
        """Test TaskGenerationResponse tools validation."""
        # Empty tools list
        response_empty_tools = TaskGenerationResponse(
            name="task",
            description="description",
            expected_output="output",
            tools=[]
        )
        assert response_empty_tools.tools == []
        
        # Complex tools configuration
        complex_tools = [
            {
                "name": "database_connector",
                "type": "data_source",
                "config": {
                    "host": "localhost",
                    "port": 5432,
                    "database": "analytics"
                },
                "credentials": {
                    "username": "analyst",
                    "password_env": "DB_PASSWORD"
                }
            },
            {
                "name": "ml_pipeline",
                "type": "processing",
                "version": "2.1.0",
                "parameters": {
                    "algorithm": "random_forest",
                    "n_estimators": 100,
                    "max_depth": 10
                }
            }
        ]
        
        response_complex_tools = TaskGenerationResponse(
            name="ml_analysis_task",
            description="Machine learning analysis with database integration",
            expected_output="ML model predictions and analysis results",
            tools=complex_tools
        )
        assert len(response_complex_tools.tools) == 2
        assert response_complex_tools.tools[0]["name"] == "database_connector"
        assert response_complex_tools.tools[1]["parameters"]["algorithm"] == "random_forest"
    
    def test_task_generation_response_advanced_config_default(self):
        """Test TaskGenerationResponse with default AdvancedConfig."""
        response = TaskGenerationResponse(
            name="default_config_task",
            description="Task with default advanced configuration",
            expected_output="Standard task output"
        )
        
        # Should have default AdvancedConfig values
        assert response.advanced_config.async_execution is False
        assert response.advanced_config.retry_on_fail is True
        assert response.advanced_config.max_retries == 3
        assert response.advanced_config.cache_response is True
        assert response.advanced_config.cache_ttl == 3600
    
    def test_task_generation_response_realistic_examples(self):
        """Test TaskGenerationResponse with realistic examples."""
        # Data analysis task
        data_analysis = TaskGenerationResponse(
            name="sales_data_analysis",
            description="Analyze quarterly sales data to identify trends and generate insights",
            expected_output="Quarterly sales analysis report with trend identification, performance metrics, and strategic recommendations",
            tools=[
                {"name": "pandas", "type": "data_processing"},
                {"name": "plotly", "type": "visualization"},
                {"name": "prophet", "type": "forecasting"}
            ],
            advanced_config=AdvancedConfig(
                timeout=600,
                priority=8,
                cache_response=True,
                cache_ttl=7200
            )
        )
        assert "sales" in data_analysis.name
        assert "quarterly" in data_analysis.description
        assert len(data_analysis.tools) == 3
        
        # Content generation task
        content_generation = TaskGenerationResponse(
            name="technical_documentation",
            description="Generate comprehensive technical documentation for API endpoints",
            expected_output="Complete API documentation with examples, parameters, and response schemas",
            tools=[
                {"name": "swagger_parser", "type": "api_analysis"},
                {"name": "markdown_generator", "type": "documentation"}
            ],
            advanced_config=AdvancedConfig(
                markdown=True,
                output_file="/docs/api_documentation.md",
                human_input=True
            )
        )
        assert content_generation.advanced_config.markdown is True
        assert content_generation.advanced_config.human_input is True


class TestTaskGenerationSchemaIntegration:
    """Integration tests for task generation schema interactions."""
    
    def test_complete_task_generation_workflow(self):
        """Test complete task generation workflow."""
        # Create agent
        agent = Agent(
            name="data_scientist",
            role="Senior Data Scientist",
            goal="Extract actionable insights from complex datasets",
            backstory="PhD in Statistics with 8 years of experience in data science and machine learning"
        )
        
        # Create generation request
        request = TaskGenerationRequest(
            text="Generate a comprehensive data analysis task for customer behavior analysis",
            model="gpt-4",
            agent=agent,
            markdown=True
        )
        
        # Create advanced configuration
        advanced_config = AdvancedConfig(
            async_execution=True,
            context=["data_preprocessing", "feature_engineering"],
            timeout=900,
            priority=7,
            dependencies=["data_validation"],
            retry_on_fail=True,
            max_retries=3,
            cache_response=True,
            markdown=True
        )
        
        # Create generation response
        response = TaskGenerationResponse(
            name="customer_behavior_analysis",
            description="Analyze customer behavior patterns using machine learning techniques to identify key segments and predict churn probability",
            expected_output="Customer behavior analysis report with segmentation results, churn prediction model, and actionable recommendations for customer retention",
            tools=[
                {"name": "pandas", "type": "data_manipulation"},
                {"name": "scikit-learn", "type": "machine_learning"},
                {"name": "seaborn", "type": "visualization"},
                {"name": "plotly", "type": "interactive_viz"}
            ],
            advanced_config=advanced_config
        )
        
        # Verify workflow
        assert request.agent.name == "data_scientist"
        assert request.model == "gpt-4"
        assert request.markdown is True
        assert response.name == "customer_behavior_analysis"
        assert len(response.tools) == 4
        assert response.advanced_config.async_execution is True
        assert response.advanced_config.timeout == 900
        assert "data_preprocessing" in response.advanced_config.context
    
    def test_task_generation_with_dependencies(self):
        """Test task generation with complex dependencies."""
        # Create tasks with dependencies
        setup_config = AdvancedConfig(
            priority=10,
            dependencies=[],
            async_execution=False
        )
        
        processing_config = AdvancedConfig(
            priority=5,
            dependencies=["data_setup", "validation"],
            async_execution=True,
            timeout=1800
        )
        
        analysis_config = AdvancedConfig(
            priority=3,
            dependencies=["data_processing", "feature_extraction"],
            async_execution=True,
            timeout=3600,
            human_input=True
        )
        
        # Create task responses
        setup_task = TaskGenerationResponse(
            name="data_setup",
            description="Set up data infrastructure and connections",
            expected_output="Configured data pipeline and validated connections",
            advanced_config=setup_config
        )
        
        processing_task = TaskGenerationResponse(
            name="data_processing",
            description="Process and clean raw data",
            expected_output="Clean, processed dataset ready for analysis",
            advanced_config=processing_config
        )
        
        analysis_task = TaskGenerationResponse(
            name="advanced_analysis",
            description="Perform advanced statistical analysis and modeling",
            expected_output="Comprehensive analysis results with statistical models",
            advanced_config=analysis_config
        )
        
        # Verify dependency chain
        assert setup_task.advanced_config.priority == 10  # Highest priority
        assert len(setup_task.advanced_config.dependencies) == 0  # No dependencies
        
        assert processing_task.advanced_config.priority == 5
        assert "data_setup" in processing_task.advanced_config.dependencies
        
        assert analysis_task.advanced_config.priority == 3  # Lowest priority
        assert "data_processing" in analysis_task.advanced_config.dependencies
        assert analysis_task.advanced_config.human_input is True
    
    def test_task_generation_error_handling_scenarios(self):
        """Test task generation error handling scenarios."""
        # Task with strict error handling
        strict_config = AdvancedConfig(
            error_handling="strict",
            retry_on_fail=False,
            max_retries=0,
            timeout=60
        )
        
        strict_task = TaskGenerationResponse(
            name="critical_validation",
            description="Perform critical data validation with strict error handling",
            expected_output="Validation results with detailed error reporting",
            advanced_config=strict_config
        )
        
        # Task with lenient error handling
        lenient_config = AdvancedConfig(
            error_handling="lenient",
            retry_on_fail=True,
            max_retries=5,
            timeout=300
        )
        
        lenient_task = TaskGenerationResponse(
            name="best_effort_processing",
            description="Process data with best effort approach and error tolerance",
            expected_output="Processed data with error summary and partial results",
            advanced_config=lenient_config
        )
        
        # Verify error handling configurations
        assert strict_task.advanced_config.error_handling == "strict"
        assert strict_task.advanced_config.retry_on_fail is False
        assert strict_task.advanced_config.max_retries == 0
        
        assert lenient_task.advanced_config.error_handling == "lenient"
        assert lenient_task.advanced_config.retry_on_fail is True
        assert lenient_task.advanced_config.max_retries == 5
    
    def test_task_generation_output_formats(self):
        """Test task generation with various output formats."""
        # JSON output format
        json_config = AdvancedConfig(
            output_json={
                "type": "object",
                "properties": {
                    "summary": {"type": "string"},
                    "metrics": {"type": "object"},
                    "recommendations": {"type": "array"}
                },
                "required": ["summary", "metrics"]
            },
            output_parser="json_validator"
        )
        
        json_task = TaskGenerationResponse(
            name="structured_analysis",
            description="Generate structured analysis with JSON output",
            expected_output="Analysis results in structured JSON format",
            advanced_config=json_config
        )
        
        # File output format
        file_config = AdvancedConfig(
            output_file="/reports/analysis_report.pdf",
            markdown=True,
            output_parser="pdf_generator"
        )
        
        file_task = TaskGenerationResponse(
            name="report_generation",
            description="Generate comprehensive analysis report",
            expected_output="PDF report with analysis results and visualizations",
            advanced_config=file_config
        )
        
        # Verify output configurations
        assert "properties" in json_task.advanced_config.output_json
        assert json_task.advanced_config.output_parser == "json_validator"
        
        assert file_task.advanced_config.output_file.endswith(".pdf")
        assert file_task.advanced_config.markdown is True
        assert file_task.advanced_config.output_parser == "pdf_generator"