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

    def test_construct_model_invocation_url_empty_model(self):
        """Test construct_model_invocation_url returns None when model name is empty"""
        result = DatabricksURLUtils.construct_model_invocation_url(
            "https://workspace.databricks.com",
            ""
        )
        assert result is None

    def test_construct_model_invocation_url_only_databricks_prefix(self):
        """Test construct_model_invocation_url returns None when model is only the databricks/ prefix"""
        result = DatabricksURLUtils.construct_model_invocation_url(
            "https://workspace.databricks.com",
            "databricks/"
        )
        assert result is None


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

    @pytest.mark.asyncio
    @patch('src.utils.databricks_auth.get_auth_context')
    @patch.dict(os.environ, {"DATABRICKS_HOST": "not a url with spaces /serving-endpoints"}, clear=True)
    async def test_validate_and_fix_environment_uncorrectable_host(self, mock_get_auth_context):
        """Test validate_and_fix_environment returns False when DATABRICKS_HOST can't be normalized"""
        mock_auth = Mock()
        mock_auth.workspace_url = "https://workspace.databricks.com"
        mock_get_auth_context.return_value = mock_auth

        # Patch normalize_workspace_url to return None so the auto-correct branch fails (lines 368-369)
        with patch.object(DatabricksURLUtils, 'normalize_workspace_url', return_value=None):
            result = await DatabricksURLUtils.validate_and_fix_environment()

        assert result is False


# Fixture to safely save/restore the AI Gateway env var so global default (off)
# is never leaked into other tests.
@pytest.fixture
def ai_gateway_env():
    """Yield a setter for DATABRICKS_ENABLE_AI_GATEWAY and restore prior state after."""
    env_var = DatabricksURLUtils.AI_GATEWAY_ENV_VAR
    sentinel = object()
    original = os.environ.get(env_var, sentinel)

    def _set(value):
        if value is None:
            os.environ.pop(env_var, None)
        else:
            os.environ[env_var] = value

    try:
        yield _set
    finally:
        if original is sentinel:
            os.environ.pop(env_var, None)
        else:
            os.environ[env_var] = original


class TestDatabricksURLUtilsIsAiGatewayEnabled:
    """Test is_ai_gateway_enabled static method"""

    def test_is_ai_gateway_enabled_default_unset(self, ai_gateway_env):
        """Test is_ai_gateway_enabled defaults to False when env var is unset"""
        ai_gateway_env(None)
        assert DatabricksURLUtils.is_ai_gateway_enabled() is False

    @pytest.mark.parametrize("value", ["true", "True", "TRUE", "1", "yes", "YES", "on", "ON", "  true  "])
    def test_is_ai_gateway_enabled_truthy_values(self, ai_gateway_env, value):
        """Test is_ai_gateway_enabled returns True for truthy values (case-insensitive, trimmed)"""
        ai_gateway_env(value)
        assert DatabricksURLUtils.is_ai_gateway_enabled() is True

    @pytest.mark.parametrize("value", ["false", "False", "0", "no", "off", "", "   ", "anything"])
    def test_is_ai_gateway_enabled_falsy_values(self, ai_gateway_env, value):
        """Test is_ai_gateway_enabled returns False for falsy/unrecognized values"""
        ai_gateway_env(value)
        assert DatabricksURLUtils.is_ai_gateway_enabled() is False


class TestDatabricksURLUtilsConstructLlmBaseUrl:
    """Test construct_llm_base_url static method"""

    def test_construct_llm_base_url_none(self, ai_gateway_env):
        """Test construct_llm_base_url returns None for None input"""
        ai_gateway_env(None)
        assert DatabricksURLUtils.construct_llm_base_url(None) is None

    def test_construct_llm_base_url_empty(self, ai_gateway_env):
        """Test construct_llm_base_url returns None for empty input"""
        ai_gateway_env(None)
        assert DatabricksURLUtils.construct_llm_base_url("") is None

    def test_construct_llm_base_url_gateway_off(self, ai_gateway_env):
        """Test construct_llm_base_url uses serving-endpoints when gateway off"""
        ai_gateway_env("false")
        result = DatabricksURLUtils.construct_llm_base_url("workspace.databricks.com")
        assert result == "https://workspace.databricks.com/serving-endpoints"

    def test_construct_llm_base_url_gateway_on(self, ai_gateway_env):
        """Test construct_llm_base_url uses AI gateway path when gateway on"""
        ai_gateway_env("true")
        result = DatabricksURLUtils.construct_llm_base_url("workspace.databricks.com")
        assert result == "https://workspace.databricks.com/ai-gateway/mlflow/v1"

    def test_construct_llm_base_url_normalizes_existing_path(self, ai_gateway_env):
        """Test construct_llm_base_url normalizes a URL that already has a path"""
        ai_gateway_env("false")
        result = DatabricksURLUtils.construct_llm_base_url("https://h.databricks.com/serving-endpoints")
        assert result == "https://h.databricks.com/serving-endpoints"


class TestDatabricksURLUtilsConstructResponsesBaseUrl:
    """Test construct_responses_base_url static method"""

    def test_construct_responses_base_url_none(self, ai_gateway_env):
        """Test construct_responses_base_url returns None for None input"""
        ai_gateway_env(None)
        assert DatabricksURLUtils.construct_responses_base_url(None) is None

    def test_construct_responses_base_url_empty(self, ai_gateway_env):
        """Test construct_responses_base_url returns None for empty input"""
        ai_gateway_env(None)
        assert DatabricksURLUtils.construct_responses_base_url("") is None

    def test_construct_responses_base_url_gateway_off(self, ai_gateway_env):
        """Test construct_responses_base_url uses serving-endpoints when gateway off"""
        ai_gateway_env("false")
        result = DatabricksURLUtils.construct_responses_base_url("workspace.databricks.com")
        assert result == "https://workspace.databricks.com/serving-endpoints"

    def test_construct_responses_base_url_gateway_on(self, ai_gateway_env):
        """Test construct_responses_base_url uses OpenAI gateway path when gateway on"""
        ai_gateway_env("on")
        result = DatabricksURLUtils.construct_responses_base_url("workspace.databricks.com")
        assert result == "https://workspace.databricks.com/ai-gateway/openai/v1"

    def test_construct_responses_base_url_normalizes_existing_path(self, ai_gateway_env):
        """Test construct_responses_base_url normalizes a URL that already has a path"""
        ai_gateway_env("true")
        result = DatabricksURLUtils.construct_responses_base_url("https://h.databricks.com/serving-endpoints")
        assert result == "https://h.databricks.com/ai-gateway/openai/v1"


class TestDatabricksURLUtilsConstructChatCompletionsUrl:
    """Test construct_chat_completions_url static method"""

    def test_construct_chat_completions_url_missing_workspace(self, ai_gateway_env):
        """Test construct_chat_completions_url returns (None, None) when workspace missing"""
        ai_gateway_env(None)
        assert DatabricksURLUtils.construct_chat_completions_url(None, "my-model") == (None, None)

    def test_construct_chat_completions_url_missing_model(self, ai_gateway_env):
        """Test construct_chat_completions_url returns (None, None) when model missing"""
        ai_gateway_env(None)
        assert DatabricksURLUtils.construct_chat_completions_url("workspace.databricks.com", "") == (None, None)

    def test_construct_chat_completions_url_none_model(self, ai_gateway_env):
        """Test construct_chat_completions_url returns (None, None) when model is None"""
        ai_gateway_env(None)
        assert DatabricksURLUtils.construct_chat_completions_url("workspace.databricks.com", None) == (None, None)

    def test_construct_chat_completions_url_gateway_off(self, ai_gateway_env):
        """Test construct_chat_completions_url builds /invocations URL with model in path when gateway off"""
        ai_gateway_env("false")
        url, model_for_body = DatabricksURLUtils.construct_chat_completions_url(
            "workspace.databricks.com", "my-model"
        )
        assert url == "https://workspace.databricks.com/serving-endpoints/my-model/invocations"
        assert model_for_body is None

    def test_construct_chat_completions_url_gateway_on(self, ai_gateway_env):
        """Test construct_chat_completions_url builds gateway URL with model in body when gateway on"""
        ai_gateway_env("true")
        url, model_for_body = DatabricksURLUtils.construct_chat_completions_url(
            "workspace.databricks.com", "my-model"
        )
        assert url == "https://workspace.databricks.com/ai-gateway/mlflow/v1/chat/completions"
        assert model_for_body == "my-model"

    def test_construct_chat_completions_url_strips_databricks_prefix_gateway_off(self, ai_gateway_env):
        """Test construct_chat_completions_url strips databricks/ prefix (gateway off)"""
        ai_gateway_env("false")
        url, model_for_body = DatabricksURLUtils.construct_chat_completions_url(
            "workspace.databricks.com", "databricks/my-model"
        )
        assert url == "https://workspace.databricks.com/serving-endpoints/my-model/invocations"
        assert model_for_body is None

    def test_construct_chat_completions_url_strips_databricks_prefix_gateway_on(self, ai_gateway_env):
        """Test construct_chat_completions_url strips databricks/ prefix (gateway on)"""
        ai_gateway_env("true")
        url, model_for_body = DatabricksURLUtils.construct_chat_completions_url(
            "workspace.databricks.com", "databricks/my-model"
        )
        assert url == "https://workspace.databricks.com/ai-gateway/mlflow/v1/chat/completions"
        assert model_for_body == "my-model"

    def test_construct_chat_completions_url_normalizes_existing_path(self, ai_gateway_env):
        """Test construct_chat_completions_url normalizes a URL that already has a path"""
        ai_gateway_env("false")
        url, model_for_body = DatabricksURLUtils.construct_chat_completions_url(
            "https://h.databricks.com/serving-endpoints", "my-model"
        )
        assert url == "https://h.databricks.com/serving-endpoints/my-model/invocations"
        assert model_for_body is None


class TestDatabricksURLUtilsConstructEmbeddingsUrl:
    """Test construct_embeddings_url static method"""

    def test_construct_embeddings_url_missing_workspace(self, ai_gateway_env):
        """Test construct_embeddings_url returns (None, None) when workspace missing"""
        ai_gateway_env(None)
        assert DatabricksURLUtils.construct_embeddings_url(None, "my-model") == (None, None)

    def test_construct_embeddings_url_missing_model(self, ai_gateway_env):
        """Test construct_embeddings_url returns (None, None) when model missing"""
        ai_gateway_env(None)
        assert DatabricksURLUtils.construct_embeddings_url("workspace.databricks.com", "") == (None, None)

    def test_construct_embeddings_url_none_model(self, ai_gateway_env):
        """Test construct_embeddings_url returns (None, None) when model is None"""
        ai_gateway_env(None)
        assert DatabricksURLUtils.construct_embeddings_url("workspace.databricks.com", None) == (None, None)

    def test_construct_embeddings_url_gateway_off(self, ai_gateway_env):
        """Test construct_embeddings_url builds /invocations URL with model in path when gateway off"""
        ai_gateway_env("false")
        url, model_for_body = DatabricksURLUtils.construct_embeddings_url(
            "workspace.databricks.com", "embed-model"
        )
        assert url == "https://workspace.databricks.com/serving-endpoints/embed-model/invocations"
        assert model_for_body is None

    def test_construct_embeddings_url_gateway_on(self, ai_gateway_env):
        """Test construct_embeddings_url builds gateway /embeddings URL with model in body when gateway on"""
        ai_gateway_env("1")
        url, model_for_body = DatabricksURLUtils.construct_embeddings_url(
            "workspace.databricks.com", "embed-model"
        )
        assert url == "https://workspace.databricks.com/ai-gateway/mlflow/v1/embeddings"
        assert model_for_body == "embed-model"

    def test_construct_embeddings_url_strips_databricks_prefix_gateway_off(self, ai_gateway_env):
        """Test construct_embeddings_url strips databricks/ prefix (gateway off)"""
        ai_gateway_env("false")
        url, model_for_body = DatabricksURLUtils.construct_embeddings_url(
            "workspace.databricks.com", "databricks/embed-model"
        )
        assert url == "https://workspace.databricks.com/serving-endpoints/embed-model/invocations"
        assert model_for_body is None

    def test_construct_embeddings_url_strips_databricks_prefix_gateway_on(self, ai_gateway_env):
        """Test construct_embeddings_url strips databricks/ prefix (gateway on)"""
        ai_gateway_env("yes")
        url, model_for_body = DatabricksURLUtils.construct_embeddings_url(
            "workspace.databricks.com", "databricks/embed-model"
        )
        assert url == "https://workspace.databricks.com/ai-gateway/mlflow/v1/embeddings"
        assert model_for_body == "embed-model"

    def test_construct_embeddings_url_normalizes_existing_path(self, ai_gateway_env):
        """Test construct_embeddings_url normalizes a URL that already has a path"""
        ai_gateway_env("false")
        url, model_for_body = DatabricksURLUtils.construct_embeddings_url(
            "https://h.databricks.com/serving-endpoints", "embed-model"
        )
        assert url == "https://h.databricks.com/serving-endpoints/embed-model/invocations"
        assert model_for_body is None


class TestDatabricksURLUtilsClassConstants:
    """Test the AI Gateway class constants are present and correct"""

    def test_class_constants(self):
        """Test class constants match expected path/env-var values"""
        assert DatabricksURLUtils.SERVING_ENDPOINTS_PATH == "serving-endpoints"
        assert DatabricksURLUtils.AI_GATEWAY_PATH == "ai-gateway/mlflow/v1"
        assert DatabricksURLUtils.AI_GATEWAY_RESPONSES_PATH == "ai-gateway/openai/v1"
        assert DatabricksURLUtils.AI_GATEWAY_ENV_VAR == "DATABRICKS_ENABLE_AI_GATEWAY"
