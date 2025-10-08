import pytest
from unittest.mock import Mock, patch, AsyncMock
import os

# Test DatabricksURLUtils - based on actual code inspection

from src.utils.databricks_url_utils import DatabricksURLUtils


class TestDatabricksURLUtilsNormalizeWorkspaceUrl:
    """Test normalize_workspace_url static method"""

    def test_normalize_workspace_url_none(self):
        """Test normalize_workspace_url with None input"""
        result = DatabricksURLUtils.normalize_workspace_url(None)
        assert result is None

    def test_normalize_workspace_url_empty_string(self):
        """Test normalize_workspace_url with empty string"""
        result = DatabricksURLUtils.normalize_workspace_url("")
        assert result is None

    def test_normalize_workspace_url_whitespace(self):
        """Test normalize_workspace_url with whitespace"""
        result = DatabricksURLUtils.normalize_workspace_url("   ")
        assert result is None

    def test_normalize_workspace_url_basic_domain(self):
        """Test normalize_workspace_url with basic domain"""
        result = DatabricksURLUtils.normalize_workspace_url("workspace.databricks.com")
        assert result == "https://workspace.databricks.com"

    def test_normalize_workspace_url_with_https(self):
        """Test normalize_workspace_url with https already present"""
        result = DatabricksURLUtils.normalize_workspace_url("https://workspace.databricks.com")
        assert result == "https://workspace.databricks.com"

    def test_normalize_workspace_url_with_http(self):
        """Test normalize_workspace_url with http protocol"""
        result = DatabricksURLUtils.normalize_workspace_url("http://workspace.databricks.com")
        assert result == "http://workspace.databricks.com"

    def test_normalize_workspace_url_with_path(self):
        """Test normalize_workspace_url removes path components"""
        result = DatabricksURLUtils.normalize_workspace_url("https://workspace.databricks.com/serving-endpoints")
        assert result == "https://workspace.databricks.com"

    def test_normalize_workspace_url_with_complex_path(self):
        """Test normalize_workspace_url removes complex path components"""
        result = DatabricksURLUtils.normalize_workspace_url("https://workspace.databricks.com/api/2.0/serving-endpoints/model")
        assert result == "https://workspace.databricks.com"

    def test_normalize_workspace_url_with_trailing_slash(self):
        """Test normalize_workspace_url handles trailing slash"""
        result = DatabricksURLUtils.normalize_workspace_url("workspace.databricks.com/")
        assert result == "https://workspace.databricks.com"

    def test_normalize_workspace_url_with_port(self):
        """Test normalize_workspace_url handles port numbers"""
        result = DatabricksURLUtils.normalize_workspace_url("workspace.databricks.com:8080")
        assert result == "https://workspace.databricks.com:8080"


class TestDatabricksURLUtilsConstructServingEndpointsUrl:
    """Test construct_serving_endpoints_url static method"""

    def test_construct_serving_endpoints_url_none(self):
        """Test construct_serving_endpoints_url with None input"""
        result = DatabricksURLUtils.construct_serving_endpoints_url(None)
        assert result is None

    def test_construct_serving_endpoints_url_basic(self):
        """Test construct_serving_endpoints_url with basic workspace URL"""
        result = DatabricksURLUtils.construct_serving_endpoints_url("https://workspace.databricks.com")
        assert result == "https://workspace.databricks.com/serving-endpoints"

    def test_construct_serving_endpoints_url_without_https(self):
        """Test construct_serving_endpoints_url normalizes URL first"""
        result = DatabricksURLUtils.construct_serving_endpoints_url("workspace.databricks.com")
        assert result == "https://workspace.databricks.com/serving-endpoints"

    def test_construct_serving_endpoints_url_with_existing_path(self):
        """Test construct_serving_endpoints_url removes existing paths"""
        result = DatabricksURLUtils.construct_serving_endpoints_url("https://workspace.databricks.com/api/2.0")
        assert result == "https://workspace.databricks.com/serving-endpoints"


class TestDatabricksURLUtilsConstructModelInvocationUrl:
    """Test construct_model_invocation_url static method"""

    def test_construct_model_invocation_url_none_workspace(self):
        """Test construct_model_invocation_url with None workspace"""
        result = DatabricksURLUtils.construct_model_invocation_url(None, "test-model")
        assert result is None

    def test_construct_model_invocation_url_basic(self):
        """Test construct_model_invocation_url with basic parameters"""
        result = DatabricksURLUtils.construct_model_invocation_url(
            "https://workspace.databricks.com", 
            "test-model"
        )
        assert result == "https://workspace.databricks.com/serving-endpoints/test-model/invocations"

    def test_construct_model_invocation_url_with_served_model(self):
        """Test construct_model_invocation_url with served_model_name"""
        result = DatabricksURLUtils.construct_model_invocation_url(
            "https://workspace.databricks.com",
            "test-model",
            "served-model-v1"
        )
        assert result == "https://workspace.databricks.com/serving-endpoints/test-model/served-models/served-model-v1/invocations"

    def test_construct_model_invocation_url_normalizes_workspace(self):
        """Test construct_model_invocation_url normalizes workspace URL"""
        result = DatabricksURLUtils.construct_model_invocation_url(
            "workspace.databricks.com", 
            "test-model"
        )
        assert result == "https://workspace.databricks.com/serving-endpoints/test-model/invocations"


class TestDatabricksURLUtilsExtractWorkspaceFromEndpoint:
    """Test extract_workspace_from_endpoint static method"""

    def test_extract_workspace_from_endpoint_none(self):
        """Test extract_workspace_from_endpoint with None input"""
        result = DatabricksURLUtils.extract_workspace_from_endpoint(None)
        assert result is None

    def test_extract_workspace_from_endpoint_empty(self):
        """Test extract_workspace_from_endpoint with empty string"""
        result = DatabricksURLUtils.extract_workspace_from_endpoint("")
        assert result is None

    def test_extract_workspace_from_endpoint_basic(self):
        """Test extract_workspace_from_endpoint with serving endpoint URL"""
        result = DatabricksURLUtils.extract_workspace_from_endpoint(
            "https://workspace.databricks.com/serving-endpoints/model/invocations"
        )
        assert result == "https://workspace.databricks.com"

    def test_extract_workspace_from_endpoint_simple_serving(self):
        """Test extract_workspace_from_endpoint with simple serving endpoint URL"""
        result = DatabricksURLUtils.extract_workspace_from_endpoint(
            "https://workspace.databricks.com/serving-endpoints"
        )
        assert result == "https://workspace.databricks.com"

    def test_extract_workspace_from_endpoint_api_path(self):
        """Test extract_workspace_from_endpoint with API path"""
        result = DatabricksURLUtils.extract_workspace_from_endpoint(
            "https://workspace.databricks.com/api/2.0/serving-endpoints"
        )
        assert result == "https://workspace.databricks.com"

    def test_extract_workspace_from_endpoint_just_workspace(self):
        """Test extract_workspace_from_endpoint with just workspace URL"""
        result = DatabricksURLUtils.extract_workspace_from_endpoint(
            "https://workspace.databricks.com"
        )
        assert result == "https://workspace.databricks.com"


class TestDatabricksURLUtilsValidateAndFixEnvironment:
    """Test validate_and_fix_environment async static method"""

    @pytest.mark.asyncio
    @patch('src.utils.databricks_auth.get_auth_context')
    @patch.dict(os.environ, {}, clear=True)
    async def test_validate_and_fix_environment_no_auth(self, mock_get_auth_context):
        """Test validate_and_fix_environment when auth context is None"""
        mock_get_auth_context.return_value = None
        
        result = await DatabricksURLUtils.validate_and_fix_environment()
        
        assert result is False

    @pytest.mark.asyncio
    @patch('src.utils.databricks_auth.get_auth_context')
    @patch.dict(os.environ, {}, clear=True)
    async def test_validate_and_fix_environment_sets_databricks_host(self, mock_get_auth_context):
        """Test validate_and_fix_environment sets DATABRICKS_HOST when missing"""
        mock_auth = Mock()
        mock_auth.workspace_url = "https://workspace.databricks.com"
        mock_get_auth_context.return_value = mock_auth
        
        result = await DatabricksURLUtils.validate_and_fix_environment()
        
        assert result is True
        assert os.environ["DATABRICKS_HOST"] == "https://workspace.databricks.com"

    @pytest.mark.asyncio
    @patch('src.utils.databricks_auth.get_auth_context')
    @patch.dict(os.environ, {"DATABRICKS_HOST": "https://workspace.databricks.com"}, clear=True)
    async def test_validate_and_fix_environment_host_matches(self, mock_get_auth_context):
        """Test validate_and_fix_environment when DATABRICKS_HOST matches auth context"""
        mock_auth = Mock()
        mock_auth.workspace_url = "https://workspace.databricks.com"
        mock_get_auth_context.return_value = mock_auth
        
        result = await DatabricksURLUtils.validate_and_fix_environment()
        
        assert result is True
        assert os.environ["DATABRICKS_HOST"] == "https://workspace.databricks.com"

    @pytest.mark.asyncio
    @patch('src.utils.databricks_auth.get_auth_context')
    @patch.dict(os.environ, {"DATABRICKS_HOST": "https://workspace.databricks.com/serving-endpoints"}, clear=True)
    async def test_validate_and_fix_environment_corrects_host_with_path(self, mock_get_auth_context):
        """Test validate_and_fix_environment corrects DATABRICKS_HOST with path components"""
        mock_auth = Mock()
        mock_auth.workspace_url = "https://workspace.databricks.com"
        mock_get_auth_context.return_value = mock_auth
        
        result = await DatabricksURLUtils.validate_and_fix_environment()
        
        assert result is True
        assert os.environ["DATABRICKS_HOST"] == "https://workspace.databricks.com"

    @pytest.mark.asyncio
    @patch('src.utils.databricks_auth.get_auth_context')
    @patch.dict(os.environ, {"DATABRICKS_HOST": "https://different.databricks.com"}, clear=True)
    async def test_validate_and_fix_environment_syncs_different_host(self, mock_get_auth_context):
        """Test validate_and_fix_environment syncs different DATABRICKS_HOST"""
        mock_auth = Mock()
        mock_auth.workspace_url = "https://workspace.databricks.com"
        mock_get_auth_context.return_value = mock_auth
        
        result = await DatabricksURLUtils.validate_and_fix_environment()
        
        assert result is True
        assert os.environ["DATABRICKS_HOST"] == "https://workspace.databricks.com"

    @pytest.mark.asyncio
    @patch('src.utils.databricks_auth.get_auth_context')
    @patch.dict(os.environ, {"DATABRICKS_ENDPOINT": "https://workspace.databricks.com/serving-endpoints/serving-endpoints"}, clear=True)
    async def test_validate_and_fix_environment_fixes_duplicate_serving_endpoints(self, mock_get_auth_context):
        """Test validate_and_fix_environment fixes duplicate /serving-endpoints in DATABRICKS_ENDPOINT"""
        mock_auth = Mock()
        mock_auth.workspace_url = "https://workspace.databricks.com"
        mock_get_auth_context.return_value = mock_auth
        
        result = await DatabricksURLUtils.validate_and_fix_environment()
        
        assert result is True
        assert os.environ["DATABRICKS_ENDPOINT"] == "https://workspace.databricks.com/serving-endpoints"

    @pytest.mark.asyncio
    @patch('src.utils.databricks_auth.get_auth_context')
    async def test_validate_and_fix_environment_exception_handling(self, mock_get_auth_context):
        """Test validate_and_fix_environment handles exceptions"""
        mock_get_auth_context.side_effect = Exception("Test error")

        result = await DatabricksURLUtils.validate_and_fix_environment()

        assert result is False
