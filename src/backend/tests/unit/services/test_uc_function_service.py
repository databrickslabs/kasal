"""
Unit tests for UCFunctionService.

Tests the functionality of Unity Catalog function operations including
function listing, function details retrieval, parameter handling, and error cases.
"""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from sqlalchemy.ext.asyncio import AsyncSession

from src.services.uc_function_service import UCFunctionService
from src.schemas.uc_function import UCFunction, UCFunctionListResponse, UCFunctionResponse, FunctionParameter


class MockFunction:
    """Mock UC function for testing."""
    
    def __init__(self, name="test_function", comment="Test function", return_type="STRING", input_params=None):
        self.name = name
        self.comment = comment
        self.return_type = return_type
        self.input_params = input_params or []


class MockFunctionParameter:
    """Mock function parameter for testing."""
    
    def __init__(self, name="param1", param_type="STRING", description="Test parameter"):
        self.name = name
        self.param_type = param_type
        self.description = description


@pytest.fixture
def mock_db():
    """Create a mock database session."""
    return AsyncMock(spec=AsyncSession)


@pytest.fixture
def uc_function_service(mock_db):
    """Create a UCFunctionService with mocked dependencies."""
    with patch('src.services.uc_function_service.UCClient') as mock_uc_client_class:
        mock_uc_client_instance = MagicMock()
        mock_uc_client_class.return_value = mock_uc_client_instance
        
        service = UCFunctionService(db=mock_db)
        service.uc_client = mock_uc_client_instance
        return service


class TestUCFunctionService:
    """Test cases for UCFunctionService."""
    
    def test_uc_function_service_initialization(self, mock_db):
        """Test UCFunctionService initialization."""
        with patch('src.services.uc_function_service.UCClient') as mock_uc_client_class:
            mock_uc_client_instance = MagicMock()
            mock_uc_client_class.return_value = mock_uc_client_instance
            
            service = UCFunctionService(db=mock_db)
            
            assert service.db == mock_db
            assert service.uc_client == mock_uc_client_instance
            mock_uc_client_class.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_list_functions_success_no_parameters(self, uc_function_service):
        """Test successful function listing without parameters."""
        catalog_name = "test_catalog"
        schema_name = "test_schema"
        
        # Mock UC client response
        mock_functions = [
            MockFunction(name="func1", comment="Function 1", return_type="STRING"),
            MockFunction(name="func2", comment="Function 2", return_type="INT")
        ]
        uc_function_service.uc_client.list_functions.return_value = mock_functions
        
        result = await uc_function_service.list_functions(catalog_name, schema_name)
        
        assert isinstance(result, UCFunctionListResponse)
        assert result.count == 2
        assert result.catalog_name == catalog_name
        assert result.schema_name == schema_name
        assert len(result.functions) == 2
        
        # Verify first function
        func1 = result.functions[0]
        assert func1.name == "func1"
        assert func1.comment == "Function 1"
        assert func1.return_type == "STRING"
        assert func1.catalog_name == catalog_name
        assert func1.schema_name == schema_name
        assert len(func1.input_params) == 0
        
        # Verify second function
        func2 = result.functions[1]
        assert func2.name == "func2"
        assert func2.comment == "Function 2"
        assert func2.return_type == "INT"
        
        uc_function_service.uc_client.list_functions.assert_called_once_with(catalog_name, schema_name)
    
    @pytest.mark.asyncio
    async def test_list_functions_success_with_parameters(self, uc_function_service):
        """Test successful function listing with parameters."""
        catalog_name = "test_catalog"
        schema_name = "test_schema"
        
        # Mock function with parameters
        mock_params = [
            MockFunctionParameter(name="input1", param_type="STRING", description="First input"),
            MockFunctionParameter(name="input2", param_type="INT", description="Second input")
        ]
        mock_function = MockFunction(
            name="complex_func", 
            comment="Complex function", 
            return_type="DOUBLE",
            input_params=mock_params
        )
        
        uc_function_service.uc_client.list_functions.return_value = [mock_function]
        
        result = await uc_function_service.list_functions(catalog_name, schema_name)
        
        assert isinstance(result, UCFunctionListResponse)
        assert result.count == 1
        assert len(result.functions) == 1
        
        func = result.functions[0]
        assert func.name == "complex_func"
        assert func.comment == "Complex function"
        assert func.return_type == "DOUBLE"
        assert len(func.input_params) == 2
        
        # Verify parameters
        param1 = func.input_params[0]
        assert param1.name == "input1"
        assert param1.param_type == "STRING"
        assert param1.description == "First input"
        
        param2 = func.input_params[1]
        assert param2.name == "input2"
        assert param2.param_type == "INT"
        assert param2.description == "Second input"
    
    @pytest.mark.asyncio
    async def test_list_functions_empty_result(self, uc_function_service):
        """Test function listing with empty result."""
        catalog_name = "empty_catalog"
        schema_name = "empty_schema"
        
        uc_function_service.uc_client.list_functions.return_value = []
        
        result = await uc_function_service.list_functions(catalog_name, schema_name)
        
        assert isinstance(result, UCFunctionListResponse)
        assert result.count == 0
        assert result.catalog_name == catalog_name
        assert result.schema_name == schema_name
        assert len(result.functions) == 0
    
    
    @pytest.mark.asyncio
    async def test_list_functions_exception_handling(self, uc_function_service):
        """Test function listing with UC client exception."""
        catalog_name = "error_catalog"
        schema_name = "error_schema"
        
        uc_function_service.uc_client.list_functions.side_effect = Exception("UC client error")
        
        with pytest.raises(Exception, match="UC client error"):
            await uc_function_service.list_functions(catalog_name, schema_name)
        
        uc_function_service.uc_client.list_functions.assert_called_once_with(catalog_name, schema_name)
    
    @pytest.mark.asyncio
    async def test_get_function_success(self, uc_function_service):
        """Test successful function details retrieval."""
        catalog_name = "test_catalog"
        schema_name = "test_schema"
        function_name = "test_function"
        
        # Mock function with parameters
        mock_params = [
            MockFunctionParameter(name="param1", param_type="STRING", description="First parameter")
        ]
        mock_function = MockFunction(
            name=function_name,
            comment="Detailed function",
            return_type="BOOLEAN",
            input_params=mock_params
        )
        
        uc_function_service.uc_client.get_function_details.return_value = mock_function
        
        result = await uc_function_service.get_function(catalog_name, schema_name, function_name)
        
        assert isinstance(result, UCFunctionResponse)
        assert result.catalog_name == catalog_name
        assert result.schema_name == schema_name
        
        func = result.function
        assert func.name == function_name
        assert func.comment == "Detailed function"
        assert func.return_type == "BOOLEAN"
        assert func.catalog_name == catalog_name
        assert func.schema_name == schema_name
        assert len(func.input_params) == 1
        
        param = func.input_params[0]
        assert param.name == "param1"
        assert param.param_type == "STRING"
        assert param.description == "First parameter"
        
        uc_function_service.uc_client.get_function_details.assert_called_once_with(
            catalog_name, schema_name, function_name
        )
    
    @pytest.mark.asyncio
    async def test_get_function_without_parameters(self, uc_function_service):
        """Test function details retrieval for function without parameters."""
        catalog_name = "test_catalog"
        schema_name = "test_schema"
        function_name = "simple_function"
        
        mock_function = MockFunction(
            name=function_name,
            comment="Simple function",
            return_type="VOID"
        )
        
        uc_function_service.uc_client.get_function_details.return_value = mock_function
        
        result = await uc_function_service.get_function(catalog_name, schema_name, function_name)
        
        assert isinstance(result, UCFunctionResponse)
        func = result.function
        assert func.name == function_name
        assert func.comment == "Simple function"
        assert func.return_type == "VOID"
        assert len(func.input_params) == 0
    
    
    @pytest.mark.asyncio
    async def test_get_function_not_found(self, uc_function_service):
        """Test function details retrieval when function not found."""
        catalog_name = "test_catalog"
        schema_name = "test_schema"
        function_name = "nonexistent_function"
        
        uc_function_service.uc_client.get_function_details.side_effect = ValueError("Function not found")
        
        with pytest.raises(ValueError, match="Function not found"):
            await uc_function_service.get_function(catalog_name, schema_name, function_name)
        
        uc_function_service.uc_client.get_function_details.assert_called_once_with(
            catalog_name, schema_name, function_name
        )
    
    @pytest.mark.asyncio
    async def test_get_function_general_exception(self, uc_function_service):
        """Test function details retrieval with general exception."""
        catalog_name = "test_catalog"
        schema_name = "test_schema"
        function_name = "error_function"
        
        uc_function_service.uc_client.get_function_details.side_effect = Exception("General error")
        
        with pytest.raises(Exception, match="General error"):
            await uc_function_service.get_function(catalog_name, schema_name, function_name)
        
        uc_function_service.uc_client.get_function_details.assert_called_once_with(
            catalog_name, schema_name, function_name
        )
    
    @pytest.mark.asyncio
    @patch('src.services.uc_function_service.logger')
    async def test_list_functions_logging(self, mock_logger, uc_function_service):
        """Test that function listing is properly logged."""
        catalog_name = "log_catalog"
        schema_name = "log_schema"
        
        mock_functions = [MockFunction(name="logged_func")]
        uc_function_service.uc_client.list_functions.return_value = mock_functions
        
        await uc_function_service.list_functions(catalog_name, schema_name)
        
        # Verify info logs
        mock_logger.info.assert_any_call(f"Listing functions in {catalog_name}.{schema_name}")
        mock_logger.info.assert_any_call(f"Found 1 functions in {catalog_name}.{schema_name}")
    
    @pytest.mark.asyncio
    @patch('src.services.uc_function_service.logger')
    async def test_list_functions_error_logging(self, mock_logger, uc_function_service):
        """Test that function listing errors are properly logged."""
        catalog_name = "error_catalog"
        schema_name = "error_schema"
        error_message = "Test error"
        
        uc_function_service.uc_client.list_functions.side_effect = Exception(error_message)
        
        with pytest.raises(Exception):
            await uc_function_service.list_functions(catalog_name, schema_name)
        
        mock_logger.error.assert_called_once_with(
            f"Error listing functions in {catalog_name}.{schema_name}: {error_message}"
        )
    
    @pytest.mark.asyncio
    @patch('src.services.uc_function_service.logger')
    async def test_get_function_logging(self, mock_logger, uc_function_service):
        """Test that function details retrieval is properly logged."""
        catalog_name = "log_catalog"
        schema_name = "log_schema"
        function_name = "logged_function"
        
        mock_function = MockFunction(name=function_name)
        uc_function_service.uc_client.get_function_details.return_value = mock_function
        
        await uc_function_service.get_function(catalog_name, schema_name, function_name)
        
        # Verify info logs
        mock_logger.info.assert_any_call(f"Getting function {catalog_name}.{schema_name}.{function_name}")
        mock_logger.info.assert_any_call(f"Function {function_name} details retrieved")
    
    @pytest.mark.asyncio
    @patch('src.services.uc_function_service.logger')
    async def test_get_function_not_found_logging(self, mock_logger, uc_function_service):
        """Test that function not found is properly logged."""
        catalog_name = "log_catalog"
        schema_name = "log_schema"
        function_name = "missing_function"
        
        uc_function_service.uc_client.get_function_details.side_effect = ValueError("Not found")
        
        with pytest.raises(ValueError):
            await uc_function_service.get_function(catalog_name, schema_name, function_name)
        
        mock_logger.warning.assert_called_once_with(
            f"Function {function_name} not found in {catalog_name}.{schema_name}"
        )
    
    @pytest.mark.asyncio
    @patch('src.services.uc_function_service.logger')
    async def test_get_function_error_logging(self, mock_logger, uc_function_service):
        """Test that function details errors are properly logged."""
        catalog_name = "error_catalog"
        schema_name = "error_schema"
        function_name = "error_function"
        error_message = "Test error"
        
        uc_function_service.uc_client.get_function_details.side_effect = Exception(error_message)
        
        with pytest.raises(Exception):
            await uc_function_service.get_function(catalog_name, schema_name, function_name)
        
        mock_logger.error.assert_called_once_with(
            f"Error getting function {catalog_name}.{schema_name}.{function_name}: {error_message}"
        )
    
    def test_service_attributes(self, uc_function_service, mock_db):
        """Test that service has correct attributes."""
        assert hasattr(uc_function_service, 'db')
        assert hasattr(uc_function_service, 'uc_client')
        assert uc_function_service.db == mock_db
    
    @pytest.mark.asyncio
    async def test_list_functions_parameter_edge_cases(self, uc_function_service):
        """Test function listing with edge cases for parameters."""
        catalog_name = "edge_catalog"
        schema_name = "edge_schema"
        
        # Mock function with None input_params
        mock_function1 = MockFunction(name="func_none_params", input_params=None)
        
        # Mock function with empty input_params
        mock_function2 = MockFunction(name="func_empty_params", input_params=[])
        
        uc_function_service.uc_client.list_functions.return_value = [
            mock_function1, mock_function2
        ]
        
        result = await uc_function_service.list_functions(catalog_name, schema_name)
        
        assert result.count == 2
        # All should have empty parameter lists
        for func in result.functions:
            assert len(func.input_params) == 0
    
    @pytest.mark.asyncio
    async def test_get_function_parameter_edge_cases(self, uc_function_service):
        """Test function details retrieval with parameter edge cases."""
        catalog_name = "edge_catalog"
        schema_name = "edge_schema"
        function_name = "edge_function"
        
        # Mock function with None input_params
        mock_function = MockFunction(name=function_name, input_params=None)
        
        uc_function_service.uc_client.get_function_details.return_value = mock_function
        
        result = await uc_function_service.get_function(catalog_name, schema_name, function_name)
        
        func = result.function
        assert func.name == function_name
        assert len(func.input_params) == 0
    
    
    @pytest.mark.asyncio
    async def test_different_catalog_schema_combinations(self, uc_function_service):
        """Test service with different catalog and schema name combinations."""
        test_cases = [
            ("catalog_1", "schema_1"),
            ("my-catalog", "my-schema"),
            ("catalog_with_underscores", "schema_with_underscores"),
            ("123_numeric_catalog", "456_numeric_schema")
        ]
        
        for catalog_name, schema_name in test_cases:
            uc_function_service.uc_client.list_functions.return_value = []
            
            result = await uc_function_service.list_functions(catalog_name, schema_name)
            
            assert result.catalog_name == catalog_name
            assert result.schema_name == schema_name
            assert result.count == 0