"""
Unit tests for Unity Catalog function schemas.

Tests the functionality of Pydantic schemas for Unity Catalog function operations
including validation, serialization, and field constraints.
"""
import pytest
from pydantic import ValidationError
from typing import List

from src.schemas.uc_function import (
    FunctionParameter, UCFunction, UCFunctionListResponse,
    UCFunctionResponse, CatalogSchemaRequest
)


class TestFunctionParameter:
    """Test cases for FunctionParameter schema."""
    
    def test_valid_function_parameter_minimal(self):
        """Test FunctionParameter with minimal required fields."""
        data = {
            "name": "input_data",
            "param_type": "STRING"
        }
        param = FunctionParameter(**data)
        assert param.name == "input_data"
        assert param.param_type == "STRING"
        assert param.description is None  # Default

    def test_valid_function_parameter_full(self):
        """Test FunctionParameter with all fields specified."""
        data = {
            "name": "user_id",
            "param_type": "BIGINT",
            "description": "Unique identifier for the user"
        }
        param = FunctionParameter(**data)
        assert param.name == "user_id"
        assert param.param_type == "BIGINT"
        assert param.description == "Unique identifier for the user"

    def test_function_parameter_missing_required_fields(self):
        """Test FunctionParameter validation with missing required fields."""
        with pytest.raises(ValidationError) as exc_info:
            FunctionParameter(name="incomplete_param")
        
        errors = exc_info.value.errors()
        missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
        assert "param_type" in missing_fields

    def test_function_parameter_various_types(self):
        """Test FunctionParameter with various data types."""
        data_types = [
            "STRING", "INT", "BIGINT", "DOUBLE", "BOOLEAN", 
            "TIMESTAMP", "DATE", "DECIMAL", "ARRAY<STRING>", 
            "MAP<STRING, STRING>", "STRUCT<field1:STRING, field2:INT>"
        ]
        
        for data_type in data_types:
            data = {
                "name": f"param_{data_type.lower().replace('<', '_').replace('>', '_').replace(',', '_').replace(':', '_').replace(' ', '')}",
                "param_type": data_type,
                "description": f"Parameter of type {data_type}"
            }
            param = FunctionParameter(**data)
            assert param.param_type == data_type
            assert param.description == f"Parameter of type {data_type}"

    def test_function_parameter_empty_strings(self):
        """Test FunctionParameter with empty strings."""
        data = {
            "name": "",
            "param_type": "",
            "description": ""
        }
        param = FunctionParameter(**data)
        assert param.name == ""
        assert param.param_type == ""
        assert param.description == ""


class TestUCFunction:
    """Test cases for UCFunction schema."""
    
    def test_valid_uc_function_minimal(self):
        """Test UCFunction with minimal required fields."""
        data = {
            "name": "calculate_total",
            "return_type": "DOUBLE"
        }
        func = UCFunction(**data)
        assert func.name == "calculate_total"
        assert func.return_type == "DOUBLE"
        assert func.comment is None  # Default
        assert func.input_params == []  # Default
        assert func.catalog_name is None  # Default
        assert func.schema_name is None  # Default

    def test_valid_uc_function_full(self):
        """Test UCFunction with all fields specified."""
        input_params = [
            FunctionParameter(name="amount", param_type="DOUBLE", description="Base amount"),
            FunctionParameter(name="tax_rate", param_type="DOUBLE", description="Tax rate percentage"),
            FunctionParameter(name="discount", param_type="DOUBLE", description="Discount amount")
        ]
        
        data = {
            "name": "calculate_final_price",
            "comment": "Calculate final price with tax and discount",
            "return_type": "DOUBLE",
            "input_params": input_params,
            "catalog_name": "finance",
            "schema_name": "calculations"
        }
        func = UCFunction(**data)
        assert func.name == "calculate_final_price"
        assert func.comment == "Calculate final price with tax and discount"
        assert func.return_type == "DOUBLE"
        assert len(func.input_params) == 3
        assert func.input_params[0].name == "amount"
        assert func.input_params[1].param_type == "DOUBLE"
        assert func.input_params[2].description == "Discount amount"
        assert func.catalog_name == "finance"
        assert func.schema_name == "calculations"

    def test_uc_function_missing_required_fields(self):
        """Test UCFunction validation with missing required fields."""
        with pytest.raises(ValidationError) as exc_info:
            UCFunction(name="incomplete_function")
        
        errors = exc_info.value.errors()
        missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
        assert "return_type" in missing_fields

    def test_uc_function_empty_input_params(self):
        """Test UCFunction with empty input_params list."""
        data = {
            "name": "get_current_timestamp",
            "return_type": "TIMESTAMP",
            "input_params": []
        }
        func = UCFunction(**data)
        assert func.name == "get_current_timestamp"
        assert func.return_type == "TIMESTAMP"
        assert func.input_params == []

    def test_uc_function_complex_return_types(self):
        """Test UCFunction with complex return types."""
        complex_types = [
            "ARRAY<STRING>",
            "MAP<STRING, INT>",
            "STRUCT<name:STRING, age:INT, active:BOOLEAN>",
            "ARRAY<STRUCT<id:BIGINT, value:DOUBLE>>",
            "MAP<STRING, ARRAY<STRING>>"
        ]
        
        for return_type in complex_types:
            data = {
                "name": f"function_returning_{return_type.lower().replace('<', '_').replace('>', '_').replace(',', '_').replace(':', '_').replace(' ', '')}",
                "return_type": return_type,
                "comment": f"Function that returns {return_type}"
            }
            func = UCFunction(**data)
            assert func.return_type == return_type
            assert func.comment == f"Function that returns {return_type}"

    def test_uc_function_with_many_parameters(self):
        """Test UCFunction with many input parameters."""
        input_params = []
        for i in range(10):
            param = FunctionParameter(
                name=f"param_{i}",
                param_type="STRING" if i % 2 == 0 else "INT",
                description=f"Parameter number {i}"
            )
            input_params.append(param)
        
        data = {
            "name": "function_with_many_params",
            "return_type": "STRING",
            "input_params": input_params,
            "comment": "Function with many parameters for testing"
        }
        func = UCFunction(**data)
        assert func.name == "function_with_many_params"
        assert len(func.input_params) == 10
        assert func.input_params[0].param_type == "STRING"
        assert func.input_params[1].param_type == "INT"
        assert func.input_params[9].name == "param_9"


class TestUCFunctionListResponse:
    """Test cases for UCFunctionListResponse schema."""
    
    def test_valid_uc_function_list_response(self):
        """Test UCFunctionListResponse with all fields."""
        functions = [
            UCFunction(
                name="add_numbers",
                return_type="DOUBLE",
                input_params=[
                    FunctionParameter(name="a", param_type="DOUBLE"),
                    FunctionParameter(name="b", param_type="DOUBLE")
                ]
            ),
            UCFunction(
                name="format_string",
                return_type="STRING",
                input_params=[
                    FunctionParameter(name="template", param_type="STRING"),
                    FunctionParameter(name="values", param_type="ARRAY<STRING>")
                ]
            )
        ]
        
        data = {
            "functions": functions,
            "count": 2,
            "catalog_name": "analytics",
            "schema_name": "utilities"
        }
        list_response = UCFunctionListResponse(**data)
        
        assert len(list_response.functions) == 2
        assert list_response.count == 2
        assert list_response.catalog_name == "analytics"
        assert list_response.schema_name == "utilities"
        assert list_response.functions[0].name == "add_numbers"
        assert list_response.functions[1].name == "format_string"
        assert len(list_response.functions[0].input_params) == 2
        assert len(list_response.functions[1].input_params) == 2

    def test_empty_uc_function_list_response(self):
        """Test UCFunctionListResponse with empty function list."""
        data = {
            "functions": [],
            "count": 0,
            "catalog_name": "empty_catalog",
            "schema_name": "empty_schema"
        }
        list_response = UCFunctionListResponse(**data)
        assert len(list_response.functions) == 0
        assert list_response.count == 0
        assert list_response.catalog_name == "empty_catalog"
        assert list_response.schema_name == "empty_schema"

    def test_uc_function_list_response_missing_fields(self):
        """Test UCFunctionListResponse validation with missing fields."""
        functions = [
            UCFunction(name="test_function", return_type="STRING")
        ]
        
        with pytest.raises(ValidationError) as exc_info:
            UCFunctionListResponse(functions=functions, count=1)
        
        errors = exc_info.value.errors()
        missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
        assert "catalog_name" in missing_fields
        assert "schema_name" in missing_fields

    def test_uc_function_list_response_large_list(self):
        """Test UCFunctionListResponse with a large list of functions."""
        functions = []
        for i in range(50):
            func = UCFunction(
                name=f"function_{i}",
                return_type="STRING" if i % 2 == 0 else "INT",
                comment=f"Generated function number {i}",
                input_params=[
                    FunctionParameter(name="input", param_type="STRING")
                ]
            )
            functions.append(func)
        
        data = {
            "functions": functions,
            "count": 50,
            "catalog_name": "large_catalog",
            "schema_name": "generated_functions"
        }
        list_response = UCFunctionListResponse(**data)
        assert len(list_response.functions) == 50
        assert list_response.count == 50
        assert list_response.functions[0].name == "function_0"
        assert list_response.functions[49].name == "function_49"
        assert list_response.functions[0].return_type == "STRING"
        assert list_response.functions[1].return_type == "INT"


class TestUCFunctionResponse:
    """Test cases for UCFunctionResponse schema."""
    
    def test_valid_uc_function_response(self):
        """Test UCFunctionResponse with all fields."""
        function = UCFunction(
            name="calculate_discount",
            comment="Calculate discount based on customer tier",
            return_type="DOUBLE",
            input_params=[
                FunctionParameter(name="original_price", param_type="DOUBLE", description="Original price"),
                FunctionParameter(name="customer_tier", param_type="STRING", description="Customer tier level"),
                FunctionParameter(name="promo_code", param_type="STRING", description="Optional promotional code")
            ],
            catalog_name="sales",
            schema_name="pricing"
        )
        
        data = {
            "function": function,
            "catalog_name": "sales",
            "schema_name": "pricing"
        }
        response = UCFunctionResponse(**data)
        
        assert response.function.name == "calculate_discount"
        assert response.function.comment == "Calculate discount based on customer tier"
        assert response.function.return_type == "DOUBLE"
        assert len(response.function.input_params) == 3
        assert response.catalog_name == "sales"
        assert response.schema_name == "pricing"
        assert response.function.input_params[0].name == "original_price"
        assert response.function.input_params[1].description == "Customer tier level"

    def test_uc_function_response_missing_fields(self):
        """Test UCFunctionResponse validation with missing fields."""
        function = UCFunction(name="test_function", return_type="STRING")
        
        with pytest.raises(ValidationError) as exc_info:
            UCFunctionResponse(function=function, catalog_name="test_catalog")
        
        errors = exc_info.value.errors()
        missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
        assert "schema_name" in missing_fields

    def test_uc_function_response_function_without_params(self):
        """Test UCFunctionResponse with function that has no parameters."""
        function = UCFunction(
            name="get_random_number",
            comment="Generate a random number",
            return_type="DOUBLE"
        )
        
        data = {
            "function": function,
            "catalog_name": "utilities",
            "schema_name": "random"
        }
        response = UCFunctionResponse(**data)
        
        assert response.function.name == "get_random_number"
        assert response.function.comment == "Generate a random number"
        assert response.function.return_type == "DOUBLE"
        assert len(response.function.input_params) == 0
        assert response.catalog_name == "utilities"
        assert response.schema_name == "random"


class TestCatalogSchemaRequest:
    """Test cases for CatalogSchemaRequest schema."""
    
    def test_valid_catalog_schema_request(self):
        """Test CatalogSchemaRequest with all fields."""
        data = {
            "catalog_name": "production",
            "schema_name": "analytics"
        }
        request = CatalogSchemaRequest(**data)
        assert request.catalog_name == "production"
        assert request.schema_name == "analytics"

    def test_catalog_schema_request_missing_fields(self):
        """Test CatalogSchemaRequest validation with missing fields."""
        with pytest.raises(ValidationError) as exc_info:
            CatalogSchemaRequest(catalog_name="test_catalog")
        
        errors = exc_info.value.errors()
        missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
        assert "schema_name" in missing_fields

    def test_catalog_schema_request_empty_strings(self):
        """Test CatalogSchemaRequest with empty strings."""
        data = {
            "catalog_name": "",
            "schema_name": ""
        }
        request = CatalogSchemaRequest(**data)
        assert request.catalog_name == ""
        assert request.schema_name == ""

    def test_catalog_schema_request_various_names(self):
        """Test CatalogSchemaRequest with various naming patterns."""
        test_cases = [
            ("simple", "basic"),
            ("catalog_with_underscores", "schema_with_underscores"),
            ("CamelCaseCatalog", "CamelCaseSchema"),
            ("mixed_Case_catalog", "mixed_Case_schema"),
            ("catalog123", "schema456"),
            ("c", "s"),  # Single character names
        ]
        
        for catalog, schema in test_cases:
            data = {
                "catalog_name": catalog,
                "schema_name": schema
            }
            request = CatalogSchemaRequest(**data)
            assert request.catalog_name == catalog
            assert request.schema_name == schema


class TestSchemaIntegration:
    """Integration tests for UC function schema interactions."""
    
    def test_uc_function_workflow(self):
        """Test complete UC function workflow."""
        # Create catalog/schema request
        catalog_request = CatalogSchemaRequest(
            catalog_name="finance",
            schema_name="calculations"
        )
        
        # Create function parameters
        params = [
            FunctionParameter(
                name="principal",
                param_type="DOUBLE",
                description="Principal amount for loan calculation"
            ),
            FunctionParameter(
                name="interest_rate",
                param_type="DOUBLE",
                description="Annual interest rate as decimal"
            ),
            FunctionParameter(
                name="years",
                param_type="INT",
                description="Number of years for the loan"
            )
        ]
        
        # Create UC function
        function = UCFunction(
            name="calculate_monthly_payment",
            comment="Calculate monthly payment for a loan",
            return_type="DOUBLE",
            input_params=params,
            catalog_name=catalog_request.catalog_name,
            schema_name=catalog_request.schema_name
        )
        
        # Create function response
        function_response = UCFunctionResponse(
            function=function,
            catalog_name=catalog_request.catalog_name,
            schema_name=catalog_request.schema_name
        )
        
        # Create list response with multiple functions
        functions = [
            function,
            UCFunction(
                name="calculate_compound_interest",
                comment="Calculate compound interest",
                return_type="DOUBLE",
                input_params=[
                    FunctionParameter(name="principal", param_type="DOUBLE"),
                    FunctionParameter(name="rate", param_type="DOUBLE"),
                    FunctionParameter(name="time", param_type="DOUBLE"),
                    FunctionParameter(name="n", param_type="INT", description="Compounding frequency")
                ]
            )
        ]
        
        list_response = UCFunctionListResponse(
            functions=functions,
            count=len(functions),
            catalog_name=catalog_request.catalog_name,
            schema_name=catalog_request.schema_name
        )
        
        # Verify the complete workflow
        assert catalog_request.catalog_name == "finance"
        assert catalog_request.schema_name == "calculations"
        
        assert function.name == "calculate_monthly_payment"
        assert function.return_type == "DOUBLE"
        assert len(function.input_params) == 3
        assert function.input_params[0].name == "principal"
        assert function.input_params[2].description == "Number of years for the loan"
        
        assert function_response.function.name == "calculate_monthly_payment"
        assert function_response.catalog_name == "finance"
        assert function_response.schema_name == "calculations"
        
        assert list_response.count == 2
        assert len(list_response.functions) == 2
        assert list_response.functions[0].name == "calculate_monthly_payment"
        assert list_response.functions[1].name == "calculate_compound_interest"
        assert len(list_response.functions[1].input_params) == 4

    def test_uc_function_different_scenarios(self):
        """Test UC functions in different scenarios."""
        # Simple utility function
        utility_function = UCFunction(
            name="upper_case",
            comment="Convert string to uppercase",
            return_type="STRING",
            input_params=[
                FunctionParameter(name="input_string", param_type="STRING")
            ]
        )
        assert utility_function.name == "upper_case"
        assert len(utility_function.input_params) == 1
        
        # Complex aggregation function
        aggregation_function = UCFunction(
            name="calculate_weighted_average",
            comment="Calculate weighted average from arrays of values and weights",
            return_type="DOUBLE",
            input_params=[
                FunctionParameter(
                    name="values",
                    param_type="ARRAY<DOUBLE>",
                    description="Array of values"
                ),
                FunctionParameter(
                    name="weights",
                    param_type="ARRAY<DOUBLE>",
                    description="Array of corresponding weights"
                )
            ]
        )
        assert aggregation_function.name == "calculate_weighted_average"
        assert aggregation_function.input_params[0].param_type == "ARRAY<DOUBLE>"
        assert len(aggregation_function.input_params) == 2
        
        # Function with no parameters (generator function)
        generator_function = UCFunction(
            name="generate_uuid",
            comment="Generate a random UUID",
            return_type="STRING"
        )
        assert generator_function.name == "generate_uuid"
        assert len(generator_function.input_params) == 0
        
        # Function with complex return type
        complex_return_function = UCFunction(
            name="analyze_data",
            comment="Analyze data and return structured results",
            return_type="STRUCT<mean:DOUBLE, median:DOUBLE, mode:ARRAY<DOUBLE>, std_dev:DOUBLE>",
            input_params=[
                FunctionParameter(
                    name="data_array",
                    param_type="ARRAY<DOUBLE>",
                    description="Array of numeric data to analyze"
                )
            ]
        )
        assert complex_return_function.return_type == "STRUCT<mean:DOUBLE, median:DOUBLE, mode:ARRAY<DOUBLE>, std_dev:DOUBLE>"
        assert complex_return_function.input_params[0].param_type == "ARRAY<DOUBLE>"

    def test_uc_function_catalog_organization(self):
        """Test UC function organization across catalogs and schemas."""
        # Functions from different catalogs and schemas
        functions_data = [
            ("analytics", "statistics", "calculate_mean", "DOUBLE"),
            ("analytics", "statistics", "calculate_std_dev", "DOUBLE"),
            ("analytics", "ml", "linear_regression", "ARRAY<DOUBLE>"),
            ("finance", "accounting", "calculate_depreciation", "DOUBLE"),
            ("finance", "risk", "calculate_var", "DOUBLE"),
            ("utilities", "string", "concat_with_separator", "STRING"),
            ("utilities", "date", "format_date", "STRING")
        ]
        
        # Create functions for each catalog/schema combination
        catalog_functions = {}
        for catalog, schema, func_name, return_type in functions_data:
            key = f"{catalog}.{schema}"
            if key not in catalog_functions:
                catalog_functions[key] = []
            
            function = UCFunction(
                name=func_name,
                return_type=return_type,
                catalog_name=catalog,
                schema_name=schema
            )
            catalog_functions[key].append(function)
        
        # Create list responses for each catalog/schema
        list_responses = {}
        for key, functions in catalog_functions.items():
            catalog, schema = key.split('.')
            list_response = UCFunctionListResponse(
                functions=functions,
                count=len(functions),
                catalog_name=catalog,
                schema_name=schema
            )
            list_responses[key] = list_response
        
        # Verify organization
        assert len(list_responses) == 6  # 6 different catalog.schema combinations
        
        # Analytics statistics should have 2 functions
        analytics_stats = list_responses["analytics.statistics"]
        assert analytics_stats.count == 2
        assert len(analytics_stats.functions) == 2
        assert analytics_stats.catalog_name == "analytics"
        assert analytics_stats.schema_name == "statistics"
        
        # Finance schemas should have 1 function each
        finance_accounting = list_responses["finance.accounting"]
        finance_risk = list_responses["finance.risk"]
        assert finance_accounting.count == 1
        assert finance_risk.count == 1
        assert finance_accounting.functions[0].name == "calculate_depreciation"
        assert finance_risk.functions[0].name == "calculate_var"
        
        # Utilities schemas should have 1 function each
        utilities_string = list_responses["utilities.string"]
        utilities_date = list_responses["utilities.date"]
        assert utilities_string.functions[0].name == "concat_with_separator"
        assert utilities_date.functions[0].name == "format_date"

    def test_uc_function_parameter_variations(self):
        """Test UC function parameters with various configurations."""
        # Function with optional parameters (using description to indicate)
        optional_params_function = UCFunction(
            name="format_currency",
            comment="Format number as currency with optional parameters",
            return_type="STRING",
            input_params=[
                FunctionParameter(
                    name="amount",
                    param_type="DOUBLE",
                    description="Amount to format (required)"
                ),
                FunctionParameter(
                    name="currency_code",
                    param_type="STRING",
                    description="Currency code (optional, defaults to USD)"
                ),
                FunctionParameter(
                    name="decimal_places",
                    param_type="INT",
                    description="Number of decimal places (optional, defaults to 2)"
                )
            ]
        )
        assert len(optional_params_function.input_params) == 3
        assert "optional" in optional_params_function.input_params[1].description
        assert "required" in optional_params_function.input_params[0].description
        
        # Function with complex parameter types
        complex_params_function = UCFunction(
            name="process_user_data",
            comment="Process complex user data structure",
            return_type="STRUCT<processed:BOOLEAN, errors:ARRAY<STRING>>",
            input_params=[
                FunctionParameter(
                    name="user_profile",
                    param_type="STRUCT<id:BIGINT, name:STRING, email:STRING, preferences:MAP<STRING, STRING>>",
                    description="User profile data structure"
                ),
                FunctionParameter(
                    name="validation_rules",
                    param_type="ARRAY<STRUCT<field:STRING, rule:STRING, message:STRING>>",
                    description="Array of validation rules"
                ),
                FunctionParameter(
                    name="config",
                    param_type="MAP<STRING, STRING>",
                    description="Processing configuration options"
                )
            ]
        )
        assert len(complex_params_function.input_params) == 3
        assert "STRUCT<" in complex_params_function.input_params[0].param_type
        assert "ARRAY<STRUCT<" in complex_params_function.input_params[1].param_type
        assert "MAP<" in complex_params_function.input_params[2].param_type