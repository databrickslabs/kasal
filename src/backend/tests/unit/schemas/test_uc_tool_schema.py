"""
Unit tests for Unity Catalog tool schemas.

Tests the functionality of Pydantic schemas for Unity Catalog tool operations
including validation, serialization, and field constraints.
"""
import pytest
from pydantic import ValidationError

from src.schemas.uc_tool import (
    UCToolParameterSchema, UCToolSchema, UCToolListResponse
)


class TestUCToolParameterSchema:
    """Test cases for UCToolParameterSchema."""
    
    def test_valid_uc_tool_parameter(self):
        """Test UCToolParameterSchema with valid data."""
        param_data = {
            "name": "input_data",
            "type": "string",
            "required": True
        }
        param = UCToolParameterSchema(**param_data)
        assert param.name == "input_data"
        assert param.type == "string"
        assert param.required is True
    
    def test_uc_tool_parameter_missing_fields(self):
        """Test UCToolParameterSchema validation with missing fields."""
        # Missing name
        with pytest.raises(ValidationError) as exc_info:
            UCToolParameterSchema(type="string", required=True)
        errors = exc_info.value.errors()
        missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
        assert "name" in missing_fields
        
        # Missing type
        with pytest.raises(ValidationError) as exc_info:
            UCToolParameterSchema(name="param", required=True)
        errors = exc_info.value.errors()
        missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
        assert "type" in missing_fields
        
        # Missing required
        with pytest.raises(ValidationError) as exc_info:
            UCToolParameterSchema(name="param", type="string")
        errors = exc_info.value.errors()
        missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
        assert "required" in missing_fields
    
    def test_uc_tool_parameter_various_types(self):
        """Test UCToolParameterSchema with various data types."""
        types = ["string", "int", "float", "boolean", "array", "object", "timestamp", "binary"]
        
        for data_type in types:
            param_data = {
                "name": f"param_{data_type}",
                "type": data_type,
                "required": False
            }
            param = UCToolParameterSchema(**param_data)
            assert param.type == data_type
    
    def test_uc_tool_parameter_required_variations(self):
        """Test UCToolParameterSchema with required field variations."""
        # Required parameter
        required_param = UCToolParameterSchema(
            name="required_param",
            type="string",
            required=True
        )
        assert required_param.required is True
        
        # Optional parameter
        optional_param = UCToolParameterSchema(
            name="optional_param",
            type="int",
            required=False
        )
        assert optional_param.required is False
    
    def test_uc_tool_parameter_empty_name(self):
        """Test UCToolParameterSchema with empty name."""
        param_data = {
            "name": "",
            "type": "string",
            "required": True
        }
        param = UCToolParameterSchema(**param_data)
        assert param.name == ""
    
    def test_uc_tool_parameter_special_characters(self):
        """Test UCToolParameterSchema with special characters in name."""
        param_data = {
            "name": "param_with_underscores_123",
            "type": "custom_type",
            "required": True
        }
        param = UCToolParameterSchema(**param_data)
        assert param.name == "param_with_underscores_123"
        assert param.type == "custom_type"


class TestUCToolSchema:
    """Test cases for UCToolSchema."""
    
    def test_valid_uc_tool_minimal(self):
        """Test UCToolSchema with minimal required fields."""
        tool_data = {
            "name": "data_processor",
            "full_name": "analytics.processing.data_processor",
            "catalog": "analytics",
            "db_schema": "processing"
        }
        tool = UCToolSchema(**tool_data)
        assert tool.name == "data_processor"
        assert tool.full_name == "analytics.processing.data_processor"
        assert tool.catalog == "analytics"
        assert tool.db_schema == "processing"
        assert tool.comment is None
        assert tool.return_type is None
        assert tool.input_params == []
    
    def test_valid_uc_tool_complete(self):
        """Test UCToolSchema with all fields."""
        param1 = UCToolParameterSchema(name="input", type="string", required=True)
        param2 = UCToolParameterSchema(name="threshold", type="float", required=False)
        
        tool_data = {
            "name": "sentiment_analyzer",
            "full_name": "nlp.analysis.sentiment_analyzer",
            "catalog": "nlp",
            "db_schema": "analysis",
            "comment": "Analyzes sentiment of text input",
            "return_type": "object",
            "input_params": [param1, param2]
        }
        tool = UCToolSchema(**tool_data)
        assert tool.name == "sentiment_analyzer"
        assert tool.full_name == "nlp.analysis.sentiment_analyzer"
        assert tool.catalog == "nlp"
        assert tool.db_schema == "analysis"
        assert tool.comment == "Analyzes sentiment of text input"
        assert tool.return_type == "object"
        assert len(tool.input_params) == 2
        assert tool.input_params[0].name == "input"
        assert tool.input_params[1].name == "threshold"
    
    def test_uc_tool_missing_required_fields(self):
        """Test UCToolSchema validation with missing required fields."""
        # Missing name
        with pytest.raises(ValidationError) as exc_info:
            UCToolSchema(
                full_name="test.schema.function",
                catalog="test",
                db_schema="schema"
            )
        errors = exc_info.value.errors()
        missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
        assert "name" in missing_fields
        
        # Missing full_name
        with pytest.raises(ValidationError) as exc_info:
            UCToolSchema(
                name="function",
                catalog="test",
                db_schema="schema"
            )
        errors = exc_info.value.errors()
        missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
        assert "full_name" in missing_fields
        
        # Missing catalog
        with pytest.raises(ValidationError) as exc_info:
            UCToolSchema(
                name="function",
                full_name="test.schema.function",
                db_schema="schema"
            )
        errors = exc_info.value.errors()
        missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
        assert "catalog" in missing_fields
        
        # Missing db_schema
        with pytest.raises(ValidationError) as exc_info:
            UCToolSchema(
                name="function",
                full_name="test.schema.function",
                catalog="test"
            )
        errors = exc_info.value.errors()
        missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
        assert "db_schema" in missing_fields
    
    def test_uc_tool_empty_fields(self):
        """Test UCToolSchema with empty string fields."""
        tool_data = {
            "name": "",
            "full_name": "",
            "catalog": "",
            "db_schema": ""
        }
        tool = UCToolSchema(**tool_data)
        assert tool.name == ""
        assert tool.full_name == ""
        assert tool.catalog == ""
        assert tool.db_schema == ""
    
    def test_uc_tool_multiple_parameters(self):
        """Test UCToolSchema with multiple input parameters."""
        params = [
            UCToolParameterSchema(name="data", type="string", required=True),
            UCToolParameterSchema(name="format", type="string", required=False),
            UCToolParameterSchema(name="encoding", type="string", required=False),
            UCToolParameterSchema(name="max_size", type="int", required=False),
            UCToolParameterSchema(name="validate", type="boolean", required=True)
        ]
        
        tool_data = {
            "name": "data_validator",
            "full_name": "validation.tools.data_validator",
            "catalog": "validation",
            "db_schema": "tools",
            "input_params": params
        }
        tool = UCToolSchema(**tool_data)
        assert len(tool.input_params) == 5
        assert tool.input_params[0].name == "data"
        assert tool.input_params[0].required is True
        assert tool.input_params[3].name == "max_size"
        assert tool.input_params[3].type == "int"
    
    def test_uc_tool_long_comment(self):
        """Test UCToolSchema with long comment."""
        long_comment = """This is a comprehensive data processing tool that handles various data formats.
It supports CSV, JSON, XML, and Parquet formats with automatic schema detection.
The tool can perform data validation, cleaning, and transformation operations.
It includes error handling and logging capabilities for production use."""
        
        tool_data = {
            "name": "comprehensive_processor",
            "full_name": "data.processing.comprehensive_processor",
            "catalog": "data",
            "db_schema": "processing",
            "comment": long_comment
        }
        tool = UCToolSchema(**tool_data)
        assert tool.comment == long_comment
        assert "\n" in tool.comment
    
    def test_uc_tool_various_return_types(self):
        """Test UCToolSchema with various return types."""
        return_types = ["string", "int", "float", "boolean", "array", "object", "void", "custom_type"]
        
        for return_type in return_types:
            tool_data = {
                "name": f"tool_{return_type}",
                "full_name": f"catalog.schema.tool_{return_type}",
                "catalog": "catalog",
                "db_schema": "schema",
                "return_type": return_type
            }
            tool = UCToolSchema(**tool_data)
            assert tool.return_type == return_type
    
    def test_uc_tool_realistic_examples(self):
        """Test UCToolSchema with realistic examples."""
        # ML Model Tool
        ml_params = [
            UCToolParameterSchema(name="features", type="array", required=True),
            UCToolParameterSchema(name="model_version", type="string", required=False)
        ]
        ml_tool = UCToolSchema(
            name="predict_customer_churn",
            full_name="ml_models.customer.predict_customer_churn",
            catalog="ml_models",
            db_schema="customer",
            comment="Predicts customer churn probability using trained ML model",
            return_type="object",
            input_params=ml_params
        )
        assert ml_tool.name == "predict_customer_churn"
        assert len(ml_tool.input_params) == 2
        
        # Data Processing Tool
        data_params = [
            UCToolParameterSchema(name="source_table", type="string", required=True),
            UCToolParameterSchema(name="target_table", type="string", required=True),
            UCToolParameterSchema(name="batch_size", type="int", required=False)
        ]
        data_tool = UCToolSchema(
            name="etl_pipeline",
            full_name="data_engineering.pipelines.etl_pipeline",
            catalog="data_engineering",
            db_schema="pipelines",
            comment="ETL pipeline for data transformation and loading",
            return_type="boolean",
            input_params=data_params
        )
        assert data_tool.name == "etl_pipeline"
        assert len(data_tool.input_params) == 3


class TestUCToolListResponse:
    """Test cases for UCToolListResponse."""
    
    def test_valid_uc_tool_list_response_empty(self):
        """Test UCToolListResponse with empty tools list."""
        response_data = {
            "tools": [],
            "count": 0
        }
        response = UCToolListResponse(**response_data)
        assert response.tools == []
        assert response.count == 0
    
    def test_valid_uc_tool_list_response_single_tool(self):
        """Test UCToolListResponse with single tool."""
        tool = UCToolSchema(
            name="single_tool",
            full_name="catalog.schema.single_tool",
            catalog="catalog",
            db_schema="schema"
        )
        
        response_data = {
            "tools": [tool],
            "count": 1
        }
        response = UCToolListResponse(**response_data)
        assert len(response.tools) == 1
        assert response.count == 1
        assert response.tools[0].name == "single_tool"
    
    def test_valid_uc_tool_list_response_multiple_tools(self):
        """Test UCToolListResponse with multiple tools."""
        tools = [
            UCToolSchema(
                name="tool1",
                full_name="cat1.schema1.tool1",
                catalog="cat1",
                db_schema="schema1"
            ),
            UCToolSchema(
                name="tool2",
                full_name="cat2.schema2.tool2",
                catalog="cat2",
                db_schema="schema2"
            ),
            UCToolSchema(
                name="tool3",
                full_name="cat3.schema3.tool3",
                catalog="cat3",
                db_schema="schema3"
            )
        ]
        
        response_data = {
            "tools": tools,
            "count": 3
        }
        response = UCToolListResponse(**response_data)
        assert len(response.tools) == 3
        assert response.count == 3
        assert response.tools[0].name == "tool1"
        assert response.tools[1].name == "tool2"
        assert response.tools[2].name == "tool3"
    
    def test_uc_tool_list_response_missing_fields(self):
        """Test UCToolListResponse validation with missing fields."""
        # Missing tools
        with pytest.raises(ValidationError) as exc_info:
            UCToolListResponse(count=5)
        errors = exc_info.value.errors()
        missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
        assert "tools" in missing_fields
        
        # Missing count
        with pytest.raises(ValidationError) as exc_info:
            UCToolListResponse(tools=[])
        errors = exc_info.value.errors()
        missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
        assert "count" in missing_fields
    
    def test_uc_tool_list_response_count_mismatch(self):
        """Test UCToolListResponse with count mismatch (validation passes but logically inconsistent)."""
        tools = [
            UCToolSchema(
                name="tool1",
                full_name="cat.schema.tool1",
                catalog="cat",
                db_schema="schema"
            )
        ]
        
        # Count doesn't match actual tools length
        response_data = {
            "tools": tools,
            "count": 5  # Mismatch: actual length is 1
        }
        response = UCToolListResponse(**response_data)
        assert len(response.tools) == 1
        assert response.count == 5  # Schema allows this inconsistency
    
    def test_uc_tool_list_response_large_list(self):
        """Test UCToolListResponse with large list of tools."""
        tools = []
        for i in range(100):
            tools.append(UCToolSchema(
                name=f"tool_{i}",
                full_name=f"catalog_{i % 5}.schema_{i % 10}.tool_{i}",
                catalog=f"catalog_{i % 5}",
                db_schema=f"schema_{i % 10}"
            ))
        
        response_data = {
            "tools": tools,
            "count": 100
        }
        response = UCToolListResponse(**response_data)
        assert len(response.tools) == 100
        assert response.count == 100
        assert response.tools[0].name == "tool_0"
        assert response.tools[99].name == "tool_99"
    
    def test_uc_tool_list_response_complex_tools(self):
        """Test UCToolListResponse with complex tools containing parameters."""
        complex_tools = []
        
        # Tool 1: Simple tool
        complex_tools.append(UCToolSchema(
            name="simple_calculator",
            full_name="math.basic.simple_calculator",
            catalog="math",
            db_schema="basic",
            comment="Performs basic arithmetic operations"
        ))
        
        # Tool 2: Tool with parameters
        params = [
            UCToolParameterSchema(name="x", type="float", required=True),
            UCToolParameterSchema(name="y", type="float", required=True),
            UCToolParameterSchema(name="operation", type="string", required=True)
        ]
        complex_tools.append(UCToolSchema(
            name="advanced_calculator",
            full_name="math.advanced.advanced_calculator",
            catalog="math",
            db_schema="advanced",
            comment="Performs advanced mathematical operations",
            return_type="float",
            input_params=params
        ))
        
        response_data = {
            "tools": complex_tools,
            "count": 2
        }
        response = UCToolListResponse(**response_data)
        assert len(response.tools) == 2
        assert response.tools[0].input_params == []
        assert len(response.tools[1].input_params) == 3


class TestUCToolSchemaIntegration:
    """Integration tests for UC tool schema interactions."""
    
    def test_tool_creation_workflow(self):
        """Test complete tool creation workflow."""
        # Create parameters
        input_params = [
            UCToolParameterSchema(
                name="text_data",
                type="string",
                required=True
            ),
            UCToolParameterSchema(
                name="confidence_threshold",
                type="float",
                required=False
            ),
            UCToolParameterSchema(
                name="model_name",
                type="string",
                required=False
            )
        ]
        
        # Create tool
        tool = UCToolSchema(
            name="text_classifier",
            full_name="nlp.classification.text_classifier",
            catalog="nlp",
            db_schema="classification",
            comment="Classifies text into predefined categories using ML models",
            return_type="object",
            input_params=input_params
        )
        
        # Create response
        tool_list = UCToolListResponse(
            tools=[tool],
            count=1
        )
        
        # Verify workflow
        assert tool.name == "text_classifier"
        assert len(tool.input_params) == 3
        assert tool.input_params[0].required is True
        assert tool.input_params[1].required is False
        assert tool_list.count == 1
        assert tool_list.tools[0].name == "text_classifier"
    
    def test_catalog_organization(self):
        """Test tools organized by catalog and schema."""
        # Analytics catalog tools
        analytics_tools = [
            UCToolSchema(
                name="sales_metrics",
                full_name="analytics.sales.sales_metrics",
                catalog="analytics",
                db_schema="sales"
            ),
            UCToolSchema(
                name="customer_segmentation",
                full_name="analytics.customer.customer_segmentation",
                catalog="analytics",
                db_schema="customer"
            )
        ]
        
        # ML catalog tools
        ml_tools = [
            UCToolSchema(
                name="train_model",
                full_name="ml.training.train_model",
                catalog="ml",
                db_schema="training"
            ),
            UCToolSchema(
                name="predict",
                full_name="ml.inference.predict",
                catalog="ml",
                db_schema="inference"
            )
        ]
        
        all_tools = analytics_tools + ml_tools
        
        response = UCToolListResponse(
            tools=all_tools,
            count=4
        )
        
        # Verify organization
        analytics_count = sum(1 for tool in response.tools if tool.catalog == "analytics")
        ml_count = sum(1 for tool in response.tools if tool.catalog == "ml")
        
        assert analytics_count == 2
        assert ml_count == 2
        assert response.count == 4
    
    def test_parameter_validation_scenarios(self):
        """Test various parameter validation scenarios."""
        # Tool with mix of required and optional parameters
        mixed_params = [
            UCToolParameterSchema(name="required_input", type="string", required=True),
            UCToolParameterSchema(name="optional_config", type="object", required=False),
            UCToolParameterSchema(name="required_output_path", type="string", required=True),
            UCToolParameterSchema(name="optional_batch_size", type="int", required=False)
        ]
        
        tool = UCToolSchema(
            name="data_processor",
            full_name="etl.processing.data_processor",
            catalog="etl",
            db_schema="processing",
            input_params=mixed_params
        )
        
        required_params = [p for p in tool.input_params if p.required]
        optional_params = [p for p in tool.input_params if not p.required]
        
        assert len(required_params) == 2
        assert len(optional_params) == 2
        assert required_params[0].name == "required_input"
        assert required_params[1].name == "required_output_path"
    
    def test_error_handling_scenarios(self):
        """Test error handling in UC tool schemas."""
        # Invalid parameter in tool creation
        with pytest.raises(ValidationError):
            UCToolParameterSchema(
                name="param",
                type="string"
                # Missing required field
            )
        
        # Invalid tool with missing required fields
        with pytest.raises(ValidationError):
            UCToolSchema(
                name="incomplete_tool"
                # Missing other required fields
            )
        
        # Invalid tools list with wrong parameter structure
        invalid_param_data = {
            "name": "param",
            "type": "string"
            # Missing required field
        }
        
        with pytest.raises(ValidationError):
            UCToolSchema(
                name="tool_with_invalid_param",
                full_name="cat.schema.tool",
                catalog="cat",
                db_schema="schema",
                input_params=[invalid_param_data]  # Should be UCToolParameterSchema objects
            )
    
    def test_realistic_uc_environment(self):
        """Test realistic Unity Catalog environment simulation."""
        # Simulate a realistic UC environment with multiple catalogs and schemas
        catalogs = {
            "finance": ["accounting", "reporting", "analytics"],
            "marketing": ["campaigns", "analytics", "attribution"],
            "operations": ["inventory", "logistics", "monitoring"]
        }
        
        all_tools = []
        
        for catalog, schemas in catalogs.items():
            for schema in schemas:
                # Create 2-3 tools per schema
                for i in range(2):
                    tool_name = f"{schema}_tool_{i + 1}"
                    params = [
                        UCToolParameterSchema(
                            name="input_data",
                            type="object",
                            required=True
                        ),
                        UCToolParameterSchema(
                            name="options",
                            type="object",
                            required=False
                        )
                    ]
                    
                    tool = UCToolSchema(
                        name=tool_name,
                        full_name=f"{catalog}.{schema}.{tool_name}",
                        catalog=catalog,
                        db_schema=schema,
                        comment=f"Tool for {schema} operations in {catalog} catalog",
                        return_type="object",
                        input_params=params
                    )
                    all_tools.append(tool)
        
        response = UCToolListResponse(
            tools=all_tools,
            count=len(all_tools)
        )
        
        # Verify realistic environment
        assert len(response.tools) == 18  # 3 catalogs * 3 schemas * 2 tools
        assert response.count == 18
        
        # Check catalog distribution
        finance_tools = [t for t in response.tools if t.catalog == "finance"]
        marketing_tools = [t for t in response.tools if t.catalog == "marketing"]
        operations_tools = [t for t in response.tools if t.catalog == "operations"]
        
        assert len(finance_tools) == 6
        assert len(marketing_tools) == 6
        assert len(operations_tools) == 6
        
        # Verify all tools have parameters
        for tool in response.tools:
            assert len(tool.input_params) == 2
            assert tool.input_params[0].required is True
            assert tool.input_params[1].required is False