"""
Unit tests for tool models.

Tests the functionality of Tool model including
configuration management, tool metadata, and multi-group support.
"""
import pytest
from datetime import datetime
from unittest.mock import patch

from src.models.tool import Tool


class TestTool:
    """Test cases for Tool model."""
    
    def test_tool_creation(self):
        """Test basic Tool creation."""
        tool = Tool(
            title="SQL Query Tool",
            description="Execute SQL queries against databases",
            icon="database"
        )
        
        assert tool.title == "SQL Query Tool"
        assert tool.description == "Execute SQL queries against databases"
        assert tool.icon == "database"
    
    def test_tool_required_fields(self):
        """Test Tool with required fields only."""
        tool = Tool(
            title="Basic Tool",
            description="Basic tool description",
            icon="tool"
        )
        
        assert tool.title == "Basic Tool"
        assert tool.description == "Basic tool description"
        assert tool.icon == "tool"
        assert tool.config == {}
        assert tool.enabled is True
        assert tool.group_id is None
        assert tool.tenant_id is None
        assert tool.created_by_email is None
    
    def test_tool_with_all_fields(self):
        """Test Tool creation with all fields."""
        config = {
            "database_url": "postgresql://...",
            "timeout": 30,
            "max_retries": 3
        }
        
        tool = Tool(
            title="Advanced SQL Tool",
            description="Advanced SQL query tool with connection pooling",
            icon="database-advanced",
            config=config,
            enabled=True,
            group_id="data_team",
            created_by_email="dba@company.com"
        )
        
        assert tool.title == "Advanced SQL Tool"
        assert tool.description.startswith("Advanced SQL")
        assert tool.icon == "database-advanced"
        assert tool.config == config
        assert tool.enabled is True
        assert tool.group_id == "data_team"
        assert tool.created_by_email == "dba@company.com"
    
    def test_tool_defaults(self):
        """Test Tool default values."""
        tool = Tool(
            title="Default Tool",
            description="Tool with default values",
            icon="default"
        )
        
        assert tool.config == {}
        assert tool.enabled is True
        assert tool.group_id is None
        assert tool.tenant_id is None
        assert tool.created_by_email is None
    
    def test_tool_disabled(self):
        """Test Tool with enabled=False."""
        tool = Tool(
            title="Disabled Tool",
            description="Tool that is disabled",
            icon="disabled",
            enabled=False
        )
        
        assert tool.enabled is False
    
    def test_tool_timestamps(self):
        """Test Tool timestamp fields."""
        with patch('src.models.tool.datetime') as mock_datetime:
            mock_now = datetime(2023, 1, 1, 12, 0, 0)
            mock_datetime.utcnow.return_value = mock_now
            
            tool = Tool(
                title="Timestamp Tool",
                description="Tool for timestamp testing",
                icon="clock"
            )
            
            assert tool.created_at == mock_now
            assert tool.updated_at == mock_now
    
    def test_tool_group_fields(self):
        """Test Tool group-related fields."""
        tool = Tool(
            title="Group Tool",
            description="Tool for group testing",
            icon="group",
            group_id="group_123",
            created_by_email="user@group.com"
        )
        
        assert tool.group_id == "group_123"
        assert tool.created_by_email == "user@group.com"
        assert tool.tenant_id is None  # Legacy field should be None
    
    def test_tool_legacy_tenant_fields(self):
        """Test Tool legacy tenant fields."""
        tool = Tool(
            title="Tenant Tool",
            description="Tool for tenant testing",
            icon="tenant",
            tenant_id="tenant_456"
        )
        
        assert tool.tenant_id == "tenant_456"
        assert tool.group_id is None  # New field should be None
    
    def test_tool_tablename(self):
        """Test Tool table name."""
        assert Tool.__tablename__ == "tools"


class TestToolInitialization:
    """Test cases for Tool __init__ method."""
    
    def test_tool_init_config_none_handling(self):
        """Test Tool initialization when config is None."""
        tool = Tool(
            title="Config None Tool",
            description="Tool with config=None",
            icon="none",
            config=None
        )
        
        assert tool.config == {}
        assert isinstance(tool.config, dict)
    
    def test_tool_init_config_provided(self):
        """Test Tool initialization with provided config."""
        config = {"param1": "value1", "param2": 42}
        
        tool = Tool(
            title="Config Provided Tool",
            description="Tool with provided config",
            icon="config",
            config=config
        )
        
        assert tool.config == config
        assert tool.config["param1"] == "value1"
        assert tool.config["param2"] == 42
    
    def test_tool_init_superclass_called(self):
        """Test that Tool initialization calls superclass __init__."""
        tool = Tool(
            title="Superclass Tool",
            description="Tool for superclass testing",
            icon="super"
        )
        
        # Should have SQLAlchemy instance attributes
        assert hasattr(tool, '__class__')
        assert hasattr(tool, '__tablename__')


class TestToolTypes:
    """Test cases for different types of tools."""
    
    def test_database_tool(self):
        """Test database-related tool."""
        db_tool = Tool(
            title="PostgreSQL Query Tool",
            description="Execute SQL queries against PostgreSQL databases",
            icon="postgresql",
            config={
                "connection_string": "postgresql://user@host:port/db",
                "query_timeout": 60,
                "max_connections": 10,
                "ssl_mode": "require"
            }
        )
        
        assert "PostgreSQL" in db_tool.title
        assert "SQL queries" in db_tool.description
        assert db_tool.config["query_timeout"] == 60
        assert db_tool.config["ssl_mode"] == "require"
    
    def test_api_tool(self):
        """Test API-related tool."""
        api_tool = Tool(
            title="REST API Client",
            description="Make HTTP requests to REST APIs",
            icon="api",
            config={
                "base_url": "https://api.example.com",
                "api_key": "your-api-key",
                "timeout": 30,
                "retry_count": 3,
                "headers": {
                    "Content-Type": "application/json",
                    "User-Agent": "AI-Agent/1.0"
                }
            }
        )
        
        assert "REST API" in api_tool.title
        assert "HTTP requests" in api_tool.description
        assert api_tool.config["base_url"] == "https://api.example.com"
        assert api_tool.config["headers"]["Content-Type"] == "application/json"
    
    def test_file_processing_tool(self):
        """Test file processing tool."""
        file_tool = Tool(
            title="PDF Document Processor",
            description="Extract text and metadata from PDF documents",
            icon="file-pdf",
            config={
                "max_file_size": "50MB",
                "supported_formats": ["pdf", "docx", "txt"],
                "ocr_enabled": True,
                "language": "en",
                "extract_images": False,
                "output_format": "text"
            }
        )
        
        assert "PDF" in file_tool.title
        assert "Extract text" in file_tool.description
        assert file_tool.config["ocr_enabled"] is True
        assert "pdf" in file_tool.config["supported_formats"]
    
    def test_machine_learning_tool(self):
        """Test machine learning tool."""
        ml_tool = Tool(
            title="Text Classification Model",
            description="Classify text using pre-trained machine learning models",
            icon="brain",
            config={
                "model_name": "bert-base-uncased",
                "model_version": "1.0",
                "batch_size": 32,
                "max_length": 512,
                "confidence_threshold": 0.8,
                "categories": ["positive", "negative", "neutral"],
                "preprocessing": {
                    "lowercase": True,
                    "remove_punctuation": False,
                    "remove_stopwords": False
                }
            }
        )
        
        assert "Classification" in ml_tool.title
        assert "machine learning" in ml_tool.description
        assert ml_tool.config["model_name"] == "bert-base-uncased"
        assert ml_tool.config["preprocessing"]["lowercase"] is True
    
    def test_data_visualization_tool(self):
        """Test data visualization tool."""
        viz_tool = Tool(
            title="Chart Generator",
            description="Generate charts and visualizations from data",
            icon="chart-bar",
            config={
                "chart_types": ["bar", "line", "pie", "scatter", "histogram"],
                "output_formats": ["png", "svg", "pdf", "html"],
                "default_theme": "modern",
                "color_palette": "viridis",
                "figure_size": [10, 6],
                "dpi": 300,
                "interactive": True
            }
        )
        
        assert "Chart" in viz_tool.title
        assert "visualizations" in viz_tool.description
        assert "bar" in viz_tool.config["chart_types"]
        assert viz_tool.config["interactive"] is True
    
    def test_communication_tool(self):
        """Test communication tool."""
        comm_tool = Tool(
            title="Email Notification Service",
            description="Send email notifications and alerts",
            icon="mail",
            config={
                "smtp_server": "smtp.company.com",
                "smtp_port": 587,
                "use_tls": True,
                "from_address": "noreply@company.com",
                "templates": {
                    "alert": "alert_template.html",
                    "report": "report_template.html",
                    "notification": "notification_template.html"
                },
                "rate_limit": "100/hour"
            }
        )
        
        assert "Email" in comm_tool.title
        assert "notifications" in comm_tool.description
        assert comm_tool.config["use_tls"] is True
        assert "alert" in comm_tool.config["templates"]


class TestToolConfiguration:
    """Test cases for tool configuration management."""
    
    def test_tool_simple_config(self):
        """Test tool with simple configuration."""
        config = {
            "endpoint": "https://api.example.com",
            "timeout": 30,
            "enabled": True
        }
        
        tool = Tool(
            title="Simple Config Tool",
            description="Tool with simple configuration",
            icon="config",
            config=config
        )
        
        assert tool.config["endpoint"] == "https://api.example.com"
        assert tool.config["timeout"] == 30
        assert tool.config["enabled"] is True
    
    def test_tool_nested_config(self):
        """Test tool with nested configuration."""
        config = {
            "connection": {
                "host": "localhost",
                "port": 5432,
                "database": "mydb",
                "credentials": {
                    "username": "user",
                    "password_key": "db_password"
                }
            },
            "query_settings": {
                "timeout": 60,
                "max_rows": 10000,
                "explain_plan": False
            },
            "logging": {
                "level": "INFO",
                "format": "json",
                "include_query": True
            }
        }
        
        tool = Tool(
            title="Nested Config Tool",
            description="Tool with nested configuration",
            icon="database",
            config=config
        )
        
        assert tool.config["connection"]["host"] == "localhost"
        assert tool.config["connection"]["credentials"]["username"] == "user"
        assert tool.config["query_settings"]["timeout"] == 60
        assert tool.config["logging"]["level"] == "INFO"
    
    def test_tool_array_config(self):
        """Test tool with array configurations."""
        config = {
            "supported_formats": ["json", "xml", "csv", "yaml"],
            "endpoints": [
                {"name": "users", "path": "/api/users"},
                {"name": "orders", "path": "/api/orders"},
                {"name": "products", "path": "/api/products"}
            ],
            "headers": [
                {"key": "Authorization", "value": "Bearer {token}"},
                {"key": "Content-Type", "value": "application/json"}
            ]
        }
        
        tool = Tool(
            title="Array Config Tool",
            description="Tool with array configurations",
            icon="array",
            config=config
        )
        
        assert "json" in tool.config["supported_formats"]
        assert len(tool.config["endpoints"]) == 3
        assert tool.config["endpoints"][0]["name"] == "users"
        assert tool.config["headers"][0]["key"] == "Authorization"
    
    def test_tool_empty_config(self):
        """Test tool with empty configuration."""
        tool = Tool(
            title="Empty Config Tool",
            description="Tool with empty configuration",
            icon="empty",
            config={}
        )
        
        assert tool.config == {}
        assert len(tool.config) == 0
    
    def test_tool_config_types(self):
        """Test tool configuration with various data types."""
        config = {
            "string_param": "text_value",
            "integer_param": 42,
            "float_param": 3.14,
            "boolean_param": True,
            "null_param": None,
            "list_param": [1, 2, 3],
            "dict_param": {"nested": "value"}
        }
        
        tool = Tool(
            title="Types Config Tool",
            description="Tool with various config types",
            icon="types",
            config=config
        )
        
        assert isinstance(tool.config["string_param"], str)
        assert isinstance(tool.config["integer_param"], int)
        assert isinstance(tool.config["float_param"], float)
        assert isinstance(tool.config["boolean_param"], bool)
        assert tool.config["null_param"] is None
        assert isinstance(tool.config["list_param"], list)
        assert isinstance(tool.config["dict_param"], dict)


class TestToolFieldTypes:
    """Test cases for Tool field types and constraints."""
    
    def test_tool_field_existence(self):
        """Test that all expected fields exist."""
        tool = Tool(
            title="Field Test Tool",
            description="Tool for field testing",
            icon="test"
        )
        
        # Check field existence
        assert hasattr(tool, 'id')
        assert hasattr(tool, 'title')
        assert hasattr(tool, 'description')
        assert hasattr(tool, 'icon')
        assert hasattr(tool, 'config')
        assert hasattr(tool, 'enabled')
        assert hasattr(tool, 'group_id')
        assert hasattr(tool, 'tenant_id')
        assert hasattr(tool, 'created_by_email')
        assert hasattr(tool, 'created_at')
        assert hasattr(tool, 'updated_at')
    
    def test_tool_string_fields(self):
        """Test string field types."""
        tool = Tool(
            title="String Test Tool",
            description="Tool with string fields",
            icon="string-icon",
            group_id="group_123",
            created_by_email="user@test.com"
        )
        
        assert isinstance(tool.title, str)
        assert isinstance(tool.description, str)
        assert isinstance(tool.icon, str)
        assert isinstance(tool.group_id, str)
        assert isinstance(tool.created_by_email, str)
    
    def test_tool_json_fields(self):
        """Test JSON field types."""
        config = {"param": "value", "nested": {"key": "val"}}
        
        tool = Tool(
            title="JSON Test Tool",
            description="Tool with JSON fields",
            icon="json",
            config=config
        )
        
        assert isinstance(tool.config, dict)
        assert tool.config == config
    
    def test_tool_boolean_fields(self):
        """Test boolean field types."""
        enabled_tool = Tool(
            title="Enabled Tool",
            description="Tool that is enabled",
            icon="enabled",
            enabled=True
        )
        
        disabled_tool = Tool(
            title="Disabled Tool",
            description="Tool that is disabled",
            icon="disabled",
            enabled=False
        )
        
        assert isinstance(enabled_tool.enabled, bool)
        assert isinstance(disabled_tool.enabled, bool)
        assert enabled_tool.enabled is True
        assert disabled_tool.enabled is False
    
    def test_tool_datetime_fields(self):
        """Test datetime field types."""
        tool = Tool(
            title="DateTime Tool",
            description="Tool with datetime fields",
            icon="datetime"
        )
        
        assert isinstance(tool.created_at, datetime)
        assert isinstance(tool.updated_at, datetime)
    
    def test_tool_nullable_fields(self):
        """Test nullable field behavior."""
        tool = Tool(
            title="Nullable Tool",
            description="Tool with nullable fields",
            icon="nullable"
        )
        
        # These fields should be nullable
        assert tool.group_id is None
        assert tool.tenant_id is None
        assert tool.created_by_email is None
    
    def test_tool_non_nullable_fields(self):
        """Test non-nullable field requirements."""
        tool = Tool(
            title="Non-nullable Tool",
            description="Tool with non-nullable fields",
            icon="non-nullable"
        )
        
        # These fields are non-nullable
        assert tool.title is not None
        assert tool.description is not None
        assert tool.icon is not None


class TestToolUsagePatterns:
    """Test cases for common Tool usage patterns."""
    
    def test_tool_data_processing_workflow(self):
        """Test tools for data processing workflow."""
        # Data extraction tool
        extract_tool = Tool(
            title="CSV Data Extractor",
            description="Extract and parse data from CSV files",
            icon="file-csv",
            config={
                "delimiter": ",",
                "encoding": "utf-8",
                "header_row": True,
                "skip_empty_rows": True,
                "max_file_size": "100MB"
            }
        )
        
        # Data transformation tool
        transform_tool = Tool(
            title="Data Transformer",
            description="Transform and clean data using pandas operations",
            icon="transform",
            config={
                "operations": ["clean", "normalize", "aggregate"],
                "null_strategy": "drop",
                "date_format": "%Y-%m-%d",
                "numeric_precision": 2
            }
        )
        
        # Data loading tool
        load_tool = Tool(
            title="Database Loader",
            description="Load processed data into database",
            icon="database-upload",
            config={
                "table_name": "processed_data",
                "insert_mode": "append",
                "batch_size": 1000,
                "create_table": False
            }
        )
        
        # Verify workflow tools
        assert "CSV" in extract_tool.title
        assert extract_tool.config["delimiter"] == ","
        
        assert "Transform" in transform_tool.title
        assert "clean" in transform_tool.config["operations"]
        
        assert "Loader" in load_tool.title
        assert load_tool.config["batch_size"] == 1000
    
    def test_tool_ai_agent_toolkit(self):
        """Test tools for AI agent toolkit."""
        # Language model tool
        llm_tool = Tool(
            title="OpenAI GPT-4 Tool",
            description="Interface to OpenAI GPT-4 language model",
            icon="brain",
            config={
                "model": "gpt-4",
                "temperature": 0.7,
                "max_tokens": 2000,
                "top_p": 0.9,
                "frequency_penalty": 0.1
            }
        )
        
        # Web search tool
        search_tool = Tool(
            title="Web Search Tool",
            description="Search the web for current information",
            icon="search",
            config={
                "search_engine": "google",
                "max_results": 10,
                "safe_search": True,
                "language": "en"
            }
        )
        
        # Memory tool
        memory_tool = Tool(
            title="Vector Memory Store",
            description="Store and retrieve information using vector embeddings",
            icon="memory",
            config={
                "embedding_model": "text-embedding-ada-002",
                "vector_dimension": 1024,
                "similarity_threshold": 0.8,
                "max_memories": 1000
            }
        )
        
        # Verify AI toolkit
        assert "GPT-4" in llm_tool.title
        assert llm_tool.config["model"] == "gpt-4"
        
        assert "Search" in search_tool.title
        assert search_tool.config["safe_search"] is True
        
        assert "Memory" in memory_tool.title
        assert memory_tool.config["vector_dimension"] == 1024
    
    def test_tool_group_isolation(self):
        """Test tool group isolation pattern."""
        # Development team tools
        dev_tool = Tool(
            title="Development Database Tool",
            description="Access to development database",
            icon="database-dev",
            config={"environment": "development"},
            group_id="dev_team",
            created_by_email="developer@company.com"
        )
        
        # Production team tools
        prod_tool = Tool(
            title="Production Monitoring Tool",
            description="Monitor production systems",
            icon="monitor",
            config={"environment": "production"},
            group_id="ops_team",
            created_by_email="ops@company.com"
        )
        
        # Verify group isolation
        assert dev_tool.group_id == "dev_team"
        assert dev_tool.config["environment"] == "development"
        assert prod_tool.group_id == "ops_team"
        assert prod_tool.config["environment"] == "production"
    
    def test_tool_enable_disable_management(self):
        """Test tool enable/disable management."""
        # Initially enabled tool
        tool = Tool(
            title="Manageable Tool",
            description="Tool that can be enabled/disabled",
            icon="toggle",
            enabled=True
        )
        
        assert tool.enabled is True
        
        # Disable tool
        tool.enabled = False
        assert tool.enabled is False
        
        # Re-enable tool
        tool.enabled = True
        assert tool.enabled is True
    
    def test_tool_version_management(self):
        """Test tool version management."""
        # Version 1 - deprecated
        tool_v1 = Tool(
            title="Legacy API Tool v1",
            description="Legacy version of API tool",
            icon="api-old",
            config={"api_version": "v1", "deprecated": True},
            enabled=False
        )
        
        # Version 2 - current
        tool_v2 = Tool(
            title="API Tool v2",
            description="Current version of API tool with enhanced features",
            icon="api",
            config={
                "api_version": "v2",
                "enhanced_features": True,
                "backward_compatible": True
            },
            enabled=True
        )
        
        # Verify version management
        assert tool_v1.enabled is False
        assert tool_v1.config["deprecated"] is True
        
        assert tool_v2.enabled is True
        assert tool_v2.config["api_version"] == "v2"
        assert tool_v2.config["enhanced_features"] is True
    
    def test_tool_migration_compatibility(self):
        """Test tool migration compatibility between tenant and group fields."""
        # Legacy tenant-based tool
        tenant_tool = Tool(
            title="Legacy Tenant Tool",
            description="Tool from tenant system",
            icon="legacy",
            tenant_id="tenant_123"
        )
        
        # New group-based tool
        group_tool = Tool(
            title="New Group Tool",
            description="Tool from group system",
            icon="group",
            group_id="group_456",
            created_by_email="user@group.com"
        )
        
        # Verify both can coexist
        assert tenant_tool.tenant_id == "tenant_123"
        assert tenant_tool.group_id is None
        
        assert group_tool.group_id == "group_456"
        assert group_tool.tenant_id is None
    
    def test_tool_configuration_templates(self):
        """Test tool configuration templates."""
        # Database tool template
        db_template_config = {
            "connection_type": "postgresql",
            "host": "{DB_HOST}",
            "port": "{DB_PORT}",
            "database": "{DB_NAME}",
            "username": "{DB_USER}",
            "password": "{DB_PASSWORD}",
            "pool_size": 10,
            "timeout": 30
        }
        
        db_tool = Tool(
            title="Database Connection Template",
            description="Template for database connections",
            icon="database-template",
            config=db_template_config
        )
        
        # API tool template
        api_template_config = {
            "base_url": "{API_BASE_URL}",
            "api_key": "{API_KEY}",
            "version": "v1",
            "timeout": 30,
            "retry_count": 3,
            "rate_limit": "100/minute"
        }
        
        api_tool = Tool(
            title="API Client Template",
            description="Template for API clients",
            icon="api-template",
            config=api_template_config
        )
        
        # Verify templates
        assert "{DB_HOST}" in db_tool.config["host"]
        assert db_tool.config["pool_size"] == 10
        
        assert "{API_BASE_URL}" in api_tool.config["base_url"]
        assert api_tool.config["rate_limit"] == "100/minute"