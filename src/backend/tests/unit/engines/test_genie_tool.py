import sys
import types as _types_mod
from unittest.mock import Mock, MagicMock
from importlib.abc import MetaPathFinder
from importlib.machinery import ModuleSpec
from pydantic import BaseModel, ConfigDict


# Provide a real BaseTool stand-in so GenieTool can inherit from a proper class.
class _FakeBaseTool(BaseModel):
    """Minimal stand-in for crewai.tools.BaseTool."""
    model_config = ConfigDict(arbitrary_types_allowed=True)

    def __init__(self, **kwargs):
        # Accept and discard any unknown kwargs (e.g. result_as_answer)
        known = {k: v for k, v in kwargs.items() if k in type(self).model_fields}
        super().__init__(**known)


# Pre-create the crewai.tools module with a real BaseTool
_crewai_tools_mod = _types_mod.ModuleType("crewai.tools")
_crewai_tools_mod.BaseTool = _FakeBaseTool
sys.modules["crewai.tools"] = _crewai_tools_mod


# Install a meta-path finder that intercepts ALL other crewai / crewai_tools imports.
class _CrewAIMockFinder(MetaPathFinder):
    """Intercept any import of crewai.* or crewai_tools.* and return a MagicMock."""

    _PREFIXES = ("crewai", "crewai_tools")

    def find_module(self, fullname, path=None):
        # Skip crewai.tools — already handled above
        if fullname == "crewai.tools":
            return None
        if any(fullname == p or fullname.startswith(p + ".") for p in self._PREFIXES):
            return self
        return None

    def load_module(self, fullname):
        if fullname in sys.modules:
            return sys.modules[fullname]
        mod = MagicMock()
        mod.__path__ = []
        mod.__name__ = fullname
        mod.__spec__ = ModuleSpec(fullname, None)
        sys.modules[fullname] = mod
        return mod


sys.meta_path.insert(0, _CrewAIMockFinder())

import pytest
from unittest.mock import patch, AsyncMock
from src.engines.crewai.tools.custom.genie_tool import GenieTool, GenieInput
import logging

logger = logging.getLogger(__name__)


class TestGenieInput:
    """Test cases for GenieInput schema validation."""

    def test_parse_question_string_input(self):
        """Test that string input is handled correctly."""
        result = GenieInput.model_validate({"question": "What are top customers?"})
        assert result.question == "What are top customers?"

    def test_parse_question_dict_with_description(self):
        """Test that dict with description field is parsed correctly."""
        input_data = {"question": {"description": "Find top customers"}}
        result = GenieInput.model_validate(input_data)
        assert result.question == "Find top customers"

    def test_parse_question_dict_with_text(self):
        """Test that dict with text field is parsed correctly."""
        input_data = {"question": {"text": "Show revenue data"}}
        result = GenieInput.model_validate(input_data)
        assert result.question == "Show revenue data"

    def test_parse_question_dict_with_query(self):
        """Test that dict with query field is parsed correctly."""
        input_data = {"question": {"query": "List all products"}}
        result = GenieInput.model_validate(input_data)
        assert result.question == "List all products"

    def test_parse_question_dict_with_question(self):
        """Test that dict with question field is parsed correctly."""
        input_data = {"question": {"question": "What is the sales trend?"}}
        result = GenieInput.model_validate(input_data)
        assert result.question == "What is the sales trend?"

    def test_parse_question_unknown_dict(self):
        """Test that unknown dict format is converted to string."""
        input_data = {"question": {"unknown_field": "some value"}}
        result = GenieInput.model_validate(input_data)
        assert "unknown_field" in result.question
        assert "some value" in result.question

    def test_parse_question_other_types(self):
        """Test that non-string, non-dict types are converted to string."""
        # Test with number
        result = GenieInput.model_validate({"question": 12345})
        assert result.question == "12345"

        # Test with list
        result = GenieInput.model_validate({"question": ["item1", "item2"]})
        assert "item1" in result.question
        assert "item2" in result.question


class TestGenieTool:
    """Test cases for GenieTool."""

    def test_init_with_space_id(self):
        """Test initialization with space_id in tool configuration."""
        tool_config = {"spaceId": "test-space-id"}
        tool = GenieTool(tool_config=tool_config)
        assert tool._space_id == "test-space-id"

    def test_init_with_space_id_list(self):
        """Test initialization with space_id as list."""
        tool_config = {"spaceId": ["list-space-id"]}
        tool = GenieTool(tool_config=tool_config)
        assert tool._space_id == "list-space-id"

    def test_init_with_space_alternative_names(self):
        """Test initialization with alternative space field names."""
        # Test 'space'
        tool = GenieTool(tool_config={"space": "space-value"})
        assert tool._space_id == "space-value"

        # Test 'space_id'
        tool = GenieTool(tool_config={"space_id": "space-id-value"})
        assert tool._space_id == "space-id-value"

    def test_init_with_polling_config(self):
        """Test initialization with polling configuration."""
        tool_config = {
            "spaceId": "test-space",
            "polling_delay": 10,
            "max_polling_delay": 60,
            "timeout_minutes": 20,
            "exponential_backoff": False,
            "backoff_after_seconds": 180
        }
        tool = GenieTool(tool_config=tool_config)
        assert tool._base_polling_delay == 10
        assert tool._max_polling_delay == 60
        assert tool._polling_timeout_minutes == 20
        assert tool._enable_exponential_backoff is False
        assert tool._backoff_after_seconds == 180

    def test_init_with_tool_id(self):
        """Test initialization with custom tool ID."""
        tool = GenieTool(tool_id=42)
        assert tool._tool_id == 42

    def test_init_with_user_token(self):
        """Test initialization with user token."""
        tool = GenieTool(user_token="test-user-token")
        assert tool._user_token == "test-user-token"

    def test_set_user_token(self):
        """Test setting user token after initialization."""
        tool = GenieTool()
        tool.set_user_token("new-user-token")
        assert tool._user_token == "new-user-token"

    def test_init_default_max_result_rows(self):
        """Test that default max_result_rows is 200."""
        tool = GenieTool(tool_config={"spaceId": "test-space"})
        assert tool._max_result_rows == 200

    def test_init_max_result_rows_from_config(self):
        """Test max_result_rows is configurable via tool_config."""
        tool = GenieTool(tool_config={"spaceId": "test-space", "max_result_rows": 500})
        assert tool._max_result_rows == 500

    def test_init_max_result_rows_default_without_config_key(self):
        """Test max_result_rows defaults to 200 when not specified in config."""
        tool = GenieTool(tool_config={"spaceId": "test-space"})
        assert tool._max_result_rows == 200

    def test_description_contains_aggregation_guidance(self):
        """Test that tool description includes aggregation query guidelines."""
        tool = GenieTool(tool_config={"spaceId": "test-space"})
        assert "aggregated queries" in tool.description
        assert "SUM, COUNT, AVG, GROUP BY, TOP N" in tool.description
        assert "Bad:" in tool.description
        assert "Good:" in tool.description

    @pytest.mark.asyncio
    async def test_get_workspace_url_success(self):
        """Test getting workspace URL from databricks_auth."""
        tool = GenieTool(tool_config={"spaceId": "test-space"})

        with patch('src.utils.databricks_auth._databricks_auth') as mock_auth:
            mock_auth.get_workspace_url = AsyncMock(return_value="https://test.databricks.com")

            url = await tool._get_workspace_url()
            assert url == "https://test.databricks.com"

    @pytest.mark.asyncio
    async def test_get_workspace_url_failure(self):
        """Test workspace URL retrieval failure."""
        tool = GenieTool(tool_config={"spaceId": "test-space"})

        with patch('src.utils.databricks_auth._databricks_auth') as mock_auth:
            mock_auth.get_workspace_url = AsyncMock(return_value=None)

            with pytest.raises(ValueError, match="Could not obtain workspace URL"):
                await tool._get_workspace_url()

    def test_make_url(self):
        """Test URL construction."""
        tool = GenieTool(tool_config={"spaceId": "test-space"})

        url = tool._make_url("https://test.databricks.com", "/api/test")
        assert url == "https://test.databricks.com/api/test"

    def test_make_url_strips_trailing_slash(self):
        """Test URL construction strips trailing slash from workspace URL."""
        tool = GenieTool(tool_config={"spaceId": "test-space"})

        url = tool._make_url("https://test.databricks.com/", "/api/test")
        assert url == "https://test.databricks.com/api/test"

    def test_make_url_adds_leading_slash(self):
        """Test URL construction adds leading slash to path."""
        tool = GenieTool(tool_config={"spaceId": "test-space"})

        url = tool._make_url("https://test.databricks.com", "api/test")
        assert url == "https://test.databricks.com/api/test"

    def test_make_url_with_space_id_placeholder(self):
        """Test URL construction with space_id placeholder."""
        tool = GenieTool(tool_config={"spaceId": "my-space"})

        url = tool._make_url("https://test.databricks.com", "/api/spaces/{self._space_id}/test")
        assert url == "https://test.databricks.com/api/spaces/my-space/test"

    def test_make_url_without_space_id_raises_error(self):
        """Test URL construction without space_id raises error when placeholder is used."""
        tool = GenieTool()  # No space_id configured

        with pytest.raises(ValueError, match="Genie space ID is not configured"):
            tool._make_url("https://test.databricks.com", "/api/spaces/{self._space_id}/test")

    @pytest.mark.asyncio
    async def test_get_auth_headers_success(self):
        """Test getting auth headers successfully."""
        tool = GenieTool(tool_config={"spaceId": "test-space"})

        with patch('src.utils.databricks_auth.get_auth_context') as mock_get_auth_ctx:
            mock_headers = {"Authorization": "Bearer test-token"}
            mock_auth_ctx = Mock()
            mock_auth_ctx.get_headers.return_value = mock_headers
            mock_get_auth_ctx.return_value = mock_auth_ctx

            headers = await tool._get_auth_headers()
            assert headers == mock_headers
            mock_get_auth_ctx.assert_called_once_with(user_token=None)

    @pytest.mark.asyncio
    async def test_get_auth_headers_with_user_token(self):
        """Test getting auth headers with user token."""
        tool = GenieTool(tool_config={"spaceId": "test-space"}, user_token="user-token")

        with patch('src.utils.databricks_auth.get_auth_context') as mock_get_auth_ctx:
            mock_headers = {"Authorization": "Bearer obo-token"}
            mock_auth_ctx = Mock()
            mock_auth_ctx.get_headers.return_value = mock_headers
            mock_get_auth_ctx.return_value = mock_auth_ctx

            headers = await tool._get_auth_headers()
            assert headers == mock_headers
            mock_get_auth_ctx.assert_called_once_with(user_token="user-token")

    @pytest.mark.asyncio
    async def test_get_auth_headers_failure(self):
        """Test getting auth headers failure."""
        tool = GenieTool(tool_config={"spaceId": "test-space"})

        with patch('src.utils.databricks_auth.get_auth_context') as mock_get_auth_ctx:
            mock_get_auth_ctx.return_value = None  # No auth available

            headers = await tool._get_auth_headers()
            assert headers is None

    @pytest.mark.asyncio
    async def test_test_token_permissions_success(self):
        """Test token permission validation success."""
        tool = GenieTool(tool_config={"spaceId": "test-space"})
        headers = {"Authorization": "Bearer test-token"}

        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.__aenter__ = AsyncMock(return_value=mock_response)
        mock_response.__aexit__ = AsyncMock(return_value=None)

        mock_session = AsyncMock()
        mock_session.get = Mock(return_value=mock_response)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=None)

        with patch('aiohttp.ClientSession', return_value=mock_session):
            result = await tool._test_token_permissions(headers, "https://test.databricks.com")
            assert result is True

    @pytest.mark.asyncio
    async def test_test_token_permissions_forbidden(self):
        """Test token permission validation with 403 forbidden."""
        tool = GenieTool(tool_config={"spaceId": "test-space"})
        headers = {"Authorization": "Bearer test-token"}

        mock_response = AsyncMock()
        mock_response.status = 403
        mock_response.text = AsyncMock(return_value="Forbidden")
        mock_response.__aenter__ = AsyncMock(return_value=mock_response)
        mock_response.__aexit__ = AsyncMock(return_value=None)

        mock_session = AsyncMock()
        mock_session.get = Mock(return_value=mock_response)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=None)

        with patch('aiohttp.ClientSession', return_value=mock_session):
            result = await tool._test_token_permissions(headers, "https://test.databricks.com")
            assert result is False

    def test_run_without_space_id(self):
        """Test run method without space_id configured."""
        tool = GenieTool()  # No space_id

        result = tool._run("Test question")
        assert "ERROR: Genie space ID is not configured" in result

    def test_run_with_empty_question(self):
        """Test run method with empty question returns aggregation guidance."""
        tool = GenieTool(tool_config={"spaceId": "test-space"})

        result = tool._run("")
        assert "provide a specific, focused business question" in result
        assert "aggregated queries" in result

    def test_run_with_none_question(self):
        """Test run method with 'none' as question returns aggregation guidance."""
        tool = GenieTool(tool_config={"spaceId": "test-space"})

        result = tool._run("none")
        assert "provide a specific, focused business question" in result
        assert "aggregations" in result

    def test_extract_response_with_text_attachment(self):
        """Test extracting response from message status with text attachment."""
        tool = GenieTool(tool_config={"spaceId": "test-space"})

        message_status = {
            "attachments": [
                {"text": {"content": "This is the response"}}
            ]
        }

        response = tool._extract_response(message_status)
        assert response == "This is the response"

    def test_extract_response_with_query_results(self):
        """Test extracting response with query results."""
        tool = GenieTool(tool_config={"spaceId": "test-space"})

        message_status = {"attachments": []}
        result_data = {
            "statement_response": {
                "result": {
                    "data_typed_array": [
                        {"values": [{"str": "Customer1"}, {"str": "1000"}]},
                        {"values": [{"str": "Customer2"}, {"str": "2000"}]}
                    ]
                }
            }
        }

        response = tool._extract_response(message_status, result_data)
        assert "Query returned 2 rows" in response
        assert "Customer1" in response
        assert "Customer2" in response

    def test_extract_response_no_content(self):
        """Test extracting response when no content is found."""
        tool = GenieTool(tool_config={"spaceId": "test-space"})

        message_status = {"attachments": []}

        response = tool._extract_response(message_status)
        assert response == "No response content found"

    def test_extract_response_truncated_rows_shows_aggregation_hint(self):
        """Test that truncated results include aggregation suggestion."""
        tool = GenieTool(tool_config={"spaceId": "test-space", "max_result_rows": 3})

        message_status = {"attachments": []}
        # Create 5 rows — exceeds max_result_rows of 3
        result_data = {
            "statement_response": {
                "result": {
                    "data_typed_array": [
                        {"values": [{"str": f"Row{i}"}]} for i in range(5)
                    ]
                }
            }
        }

        response = tool._extract_response(message_status, result_data)
        assert "Showing first 3 of 5 rows" in response
        assert "Consider rephrasing your question to use aggregations" in response
        assert "GROUP BY, SUM, COUNT, AVG, TOP N" in response
        # Verify only 3 rows are included, not all 5
        assert "Row0" in response
        assert "Row2" in response
        assert "Row4" not in response

    def test_extract_response_not_truncated_no_hint(self):
        """Test that non-truncated results do NOT include aggregation suggestion."""
        tool = GenieTool(tool_config={"spaceId": "test-space", "max_result_rows": 200})

        message_status = {"attachments": []}
        result_data = {
            "statement_response": {
                "result": {
                    "data_typed_array": [
                        {"values": [{"str": "Row1"}]},
                        {"values": [{"str": "Row2"}]}
                    ]
                }
            }
        }

        response = tool._extract_response(message_status, result_data)
        assert "Consider rephrasing" not in response
        assert "Row1" in response
        assert "Row2" in response
