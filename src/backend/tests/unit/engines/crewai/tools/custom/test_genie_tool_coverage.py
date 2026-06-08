"""
Unit tests for engines/crewai/tools/custom/genie_tool.py

Covers GenieTool initialisation, config parsing, helper methods, _run,
_run_async (mocked HTTP), and _extract_response.
"""

import asyncio
import json
import pytest
from unittest.mock import AsyncMock, MagicMock, Mock, patch

from src.engines.crewai.tools.custom.genie_tool import GenieInput, GenieTool


# ---------------------------------------------------------------------------
# GenieInput validator tests
# ---------------------------------------------------------------------------

class TestGenieInput:
    def test_string_passthrough(self):
        inp = GenieInput(question="What is the total revenue?")
        assert inp.question == "What is the total revenue?"

    def test_dict_with_description_key(self):
        inp = GenieInput(question={"description": "top 10 campaigns"})
        assert inp.question == "top 10 campaigns"

    def test_dict_with_text_key(self):
        inp = GenieInput(question={"text": "average spend"})
        assert inp.question == "average spend"

    def test_dict_with_query_key(self):
        inp = GenieInput(question={"query": "total orders"})
        assert inp.question == "total orders"

    def test_dict_with_question_key(self):
        inp = GenieInput(question={"question": "revenue by month"})
        assert inp.question == "revenue by month"

    def test_dict_fallback_to_str(self):
        inp = GenieInput(question={"unknown_key": "data"})
        assert "unknown_key" in inp.question

    def test_non_dict_non_string(self):
        inp = GenieInput(question=42)
        assert inp.question == "42"


# ---------------------------------------------------------------------------
# GenieTool.__init__ tests
# ---------------------------------------------------------------------------

class TestGenieToolInit:
    def test_defaults_when_no_config(self):
        tool = GenieTool()
        assert tool._space_id is None
        assert tool._base_polling_delay == 5
        assert tool._max_calls == 5

    def test_space_id_from_spaceId(self):
        tool = GenieTool(tool_config={"spaceId": "abc123"})
        assert tool._space_id == "abc123"

    def test_space_id_from_spaceId_list(self):
        tool = GenieTool(tool_config={"spaceId": ["sp1", "sp2"]})
        assert tool._space_id == "sp1"

    def test_space_id_from_space_key(self):
        tool = GenieTool(tool_config={"space": "space-abc"})
        assert tool._space_id == "space-abc"

    def test_space_id_from_space_id_key(self):
        tool = GenieTool(tool_config={"space_id": "sid-123"})
        assert tool._space_id == "sid-123"

    def test_polling_config_from_tool_config(self):
        tool = GenieTool(tool_config={
            "polling_delay": 2,
            "max_polling_delay": 15,
            "timeout_minutes": 5,
            "exponential_backoff": False,
            "backoff_after_seconds": 60,
            "max_calls": 3,
            "max_result_rows": 50,
        })
        assert tool._base_polling_delay == 2
        assert tool._max_polling_delay == 15
        assert tool._polling_timeout_minutes == 5
        assert tool._enable_exponential_backoff is False
        assert tool._max_calls == 3
        assert tool._max_result_rows == 50

    def test_tool_id_override(self):
        tool = GenieTool(tool_id=99)
        assert tool._tool_id == 99

    def test_user_token_stored(self):
        tool = GenieTool(user_token="tok123")
        assert tool._user_token == "tok123"

    def test_group_id_stored(self):
        tool = GenieTool(group_id="g-abc")
        assert tool._group_id == "g-abc"

    def test_set_user_token(self):
        tool = GenieTool()
        tool.set_user_token("newtok")
        assert tool._user_token == "newtok"


# ---------------------------------------------------------------------------
# _make_url
# ---------------------------------------------------------------------------

class TestMakeUrl:
    def test_basic_url_construction(self):
        tool = GenieTool()
        url = tool._make_url("https://example.com/", "/api/2.0/genie")
        assert url == "https://example.com/api/2.0/genie"

    def test_path_without_leading_slash(self):
        tool = GenieTool()
        url = tool._make_url("https://example.com", "api/2.0/genie")
        assert url == "https://example.com/api/2.0/genie"

    def test_space_id_placeholder_replaced(self):
        tool = GenieTool(tool_config={"spaceId": "space-99"})
        url = tool._make_url("https://x.com", "/api/{self._space_id}/conversations")
        assert "space-99" in url

    def test_space_id_placeholder_missing_raises(self):
        tool = GenieTool()  # no space_id
        with pytest.raises(ValueError, match="not configured"):
            tool._make_url("https://x.com", "/api/{self._space_id}/conv")


# ---------------------------------------------------------------------------
# _get_workspace_url
# ---------------------------------------------------------------------------

class TestGetWorkspaceUrl:
    @pytest.mark.asyncio
    async def test_returns_workspace_url(self):
        tool = GenieTool()
        mock_auth = AsyncMock()
        mock_auth.get_workspace_url = AsyncMock(return_value="https://ws.example.com")

        with patch("src.utils.databricks_auth._databricks_auth", mock_auth):
            url = await tool._get_workspace_url()
        assert url == "https://ws.example.com"

    @pytest.mark.asyncio
    async def test_raises_when_no_url(self):
        tool = GenieTool()
        mock_auth = AsyncMock()
        mock_auth.get_workspace_url = AsyncMock(return_value=None)

        with patch("src.utils.databricks_auth._databricks_auth", mock_auth):
            with pytest.raises(ValueError, match="workspace URL"):
                await tool._get_workspace_url()

    @pytest.mark.asyncio
    async def test_raises_on_exception(self):
        tool = GenieTool()
        mock_auth = AsyncMock()
        mock_auth.get_workspace_url = AsyncMock(side_effect=RuntimeError("network down"))

        with patch("src.utils.databricks_auth._databricks_auth", mock_auth):
            with pytest.raises(ValueError):
                await tool._get_workspace_url()


# ---------------------------------------------------------------------------
# _get_auth_headers
# ---------------------------------------------------------------------------

class TestGetAuthHeaders:
    @pytest.mark.asyncio
    async def test_returns_headers_when_auth_ok(self):
        tool = GenieTool(tool_config={"spaceId": "s1"}, user_token="tok", group_id="g1")
        mock_auth_ctx = MagicMock()
        mock_auth_ctx.get_headers.return_value = {"Authorization": "Bearer tok"}

        with patch("src.utils.databricks_auth.get_auth_context",
                   AsyncMock(return_value=mock_auth_ctx)):
            with patch("src.utils.user_context.UserContext"):
                headers = await tool._get_auth_headers()
        assert headers["Authorization"] == "Bearer tok"
        assert "User-Agent" in headers

    @pytest.mark.asyncio
    async def test_returns_none_when_no_auth(self):
        tool = GenieTool()
        with patch("src.utils.databricks_auth.get_auth_context",
                   AsyncMock(return_value=None)):
            with patch("src.utils.user_context.UserContext"):
                headers = await tool._get_auth_headers()
        assert headers is None

    @pytest.mark.asyncio
    async def test_returns_none_on_exception(self):
        tool = GenieTool()
        with patch("src.utils.databricks_auth.get_auth_context",
                   AsyncMock(side_effect=Exception("boom"))):
            headers = await tool._get_auth_headers()
        assert headers is None


# ---------------------------------------------------------------------------
# _run_async — no space_id configured
# ---------------------------------------------------------------------------

class TestRunAsyncNoSpaceId:
    @pytest.mark.asyncio
    async def test_returns_error_when_no_space_id(self):
        tool = GenieTool()
        result = await tool._run_async("What is revenue?")
        assert "not configured" in result.lower() or "space id" in result.lower()

    @pytest.mark.asyncio
    async def test_returns_guidance_for_empty_question(self):
        tool = GenieTool(tool_config={"spaceId": "s1"})
        result = await tool._run_async("")
        assert "GenieTool" in result or "question" in result.lower()

    @pytest.mark.asyncio
    async def test_returns_guidance_for_none_question(self):
        tool = GenieTool(tool_config={"spaceId": "s1"})
        result = await tool._run_async("none")
        assert "GenieTool" in result or "question" in result.lower()


# ---------------------------------------------------------------------------
# _run_async — call limit enforcement
# ---------------------------------------------------------------------------

class TestRunAsyncCallLimit:
    @pytest.mark.asyncio
    async def test_call_limit_rejection(self):
        tool = GenieTool(tool_config={"spaceId": "s1", "max_calls": 2})
        # Exhaust the limit
        tool._call_count = 2
        result = await tool._run_async("How many orders?")
        assert "maximum" in result.lower() or "limit" in result.lower()


# ---------------------------------------------------------------------------
# _run_async — happy path: COMPLETED with text attachment
# ---------------------------------------------------------------------------

class TestRunAsyncHappyPath:
    @pytest.mark.asyncio
    async def test_completed_with_text_attachment(self):
        tool = GenieTool(tool_config={"spaceId": "s1"}, user_token="t", group_id="g")
        tool._base_polling_delay = 1

        conv_response = {"conversation_id": "conv-1", "message_id": "msg-1"}
        status_completed = {
            "status": "COMPLETED",
            "attachments": [
                {"text": {"content": "Total revenue is $1M"}}
            ]
        }

        with patch.object(tool, "_start_or_continue_conversation",
                          AsyncMock(return_value=conv_response)):
            with patch.object(tool, "_get_message_status",
                              AsyncMock(return_value=status_completed)):
                with patch.object(tool, "_get_query_result",
                                  AsyncMock(return_value={})):
                    with patch("src.engines.crewai.tools.custom.genie_tool.asyncio.sleep",
                               AsyncMock()):
                        result = await tool._run_async("What is revenue?")

        assert "Total revenue is $1M" in result

    @pytest.mark.asyncio
    async def test_completed_with_query_results(self):
        tool = GenieTool(tool_config={"spaceId": "s1", "max_result_rows": 100})
        tool._base_polling_delay = 1

        conv_response = {"conversation_id": "conv-2", "message_id": "msg-2"}
        status_completed = {"status": "COMPLETED", "attachments": []}
        query_result = {
            "statement_response": {
                "result": {
                    "data_typed_array": [
                        {"values": [{"str": "row1col1"}, {"str": "row1col2"}]},
                        {"values": [{"str": "row2col1"}, {"str": "row2col2"}]},
                    ]
                }
            }
        }

        with patch.object(tool, "_start_or_continue_conversation",
                          AsyncMock(return_value=conv_response)):
            with patch.object(tool, "_get_message_status",
                              AsyncMock(return_value=status_completed)):
                with patch.object(tool, "_get_query_result",
                                  AsyncMock(return_value=query_result)):
                    with patch("src.engines.crewai.tools.custom.genie_tool.asyncio.sleep",
                               AsyncMock()):
                        result = await tool._run_async("Show me data")

        assert "row1col1" in result

    @pytest.mark.asyncio
    async def test_status_failed_returns_error_message(self):
        tool = GenieTool(tool_config={"spaceId": "s1"})
        tool._base_polling_delay = 1

        conv_response = {"conversation_id": "c1", "message_id": "m1"}
        status_failed = {"status": "FAILED"}

        with patch.object(tool, "_start_or_continue_conversation",
                          AsyncMock(return_value=conv_response)):
            with patch.object(tool, "_get_message_status",
                              AsyncMock(return_value=status_failed)):
                with patch("src.engines.crewai.tools.custom.genie_tool.asyncio.sleep",
                           AsyncMock()):
                    result = await tool._run_async("Any question")

        assert "failed" in result.lower() or "Genie" in result

    @pytest.mark.asyncio
    async def test_status_cancelled(self):
        tool = GenieTool(tool_config={"spaceId": "s1"})
        tool._base_polling_delay = 1

        conv_response = {"conversation_id": "c1", "message_id": "m1"}
        with patch.object(tool, "_start_or_continue_conversation",
                          AsyncMock(return_value=conv_response)):
            with patch.object(tool, "_get_message_status",
                              AsyncMock(return_value={"status": "CANCELLED"})):
                with patch("src.engines.crewai.tools.custom.genie_tool.asyncio.sleep",
                           AsyncMock()):
                    result = await tool._run_async("question")
        assert "cancel" in result.lower()

    @pytest.mark.asyncio
    async def test_timeout_when_never_completes(self):
        tool = GenieTool(tool_config={"spaceId": "s1"})
        tool._base_polling_delay = 1
        tool._max_retries = 2

        conv_response = {"conversation_id": "c1", "message_id": "m1"}
        in_progress = {"status": "IN_PROGRESS"}

        with patch.object(tool, "_start_or_continue_conversation",
                          AsyncMock(return_value=conv_response)):
            with patch.object(tool, "_get_message_status",
                              AsyncMock(return_value=in_progress)):
                with patch("src.engines.crewai.tools.custom.genie_tool.asyncio.sleep",
                           AsyncMock()):
                    result = await tool._run_async("question")

        assert "timed out" in result.lower() or "timeout" in result.lower()

    @pytest.mark.asyncio
    async def test_connection_error_handled(self):
        tool = GenieTool(tool_config={"spaceId": "s1"})

        import aiohttp
        with patch.object(tool, "_start_or_continue_conversation",
                          AsyncMock(side_effect=aiohttp.ClientConnectionError("conn refused"))):
            result = await tool._run_async("question")

        assert "connecting" in result.lower() or "error" in result.lower()

    @pytest.mark.asyncio
    async def test_http_response_error_handled(self):
        tool = GenieTool(tool_config={"spaceId": "s1"})

        import aiohttp
        err = aiohttp.ClientResponseError(None, None, status=403)
        with patch.object(tool, "_start_or_continue_conversation",
                          AsyncMock(side_effect=err)):
            result = await tool._run_async("question")

        assert "403" in result or "HTTP" in result or "error" in result.lower()

    @pytest.mark.asyncio
    async def test_generic_exception_caught(self):
        tool = GenieTool(tool_config={"spaceId": "s1"})

        with patch.object(tool, "_start_or_continue_conversation",
                          AsyncMock(side_effect=RuntimeError("unexpected"))):
            result = await tool._run_async("question")

        assert "error" in result.lower()

    @pytest.mark.asyncio
    async def test_conv_returns_no_ids(self):
        tool = GenieTool(tool_config={"spaceId": "s1"})
        conv_response = {"conversation_id": None, "message_id": None}
        with patch.object(tool, "_start_or_continue_conversation",
                          AsyncMock(return_value=conv_response)):
            result = await tool._run_async("question")
        assert "error" in result.lower() or "Failed" in result

    @pytest.mark.asyncio
    async def test_generated_sql_appended(self):
        tool = GenieTool(tool_config={"spaceId": "s1"})
        tool._base_polling_delay = 1

        conv_response = {"conversation_id": "c1", "message_id": "m1"}
        status_completed = {
            "status": "COMPLETED",
            "attachments": [
                {"text": {"content": "Result is X"}},
                {"query": {"statement": "SELECT * FROM t"}},
            ]
        }
        with patch.object(tool, "_start_or_continue_conversation",
                          AsyncMock(return_value=conv_response)):
            with patch.object(tool, "_get_message_status",
                              AsyncMock(return_value=status_completed)):
                with patch.object(tool, "_get_query_result",
                                  AsyncMock(return_value={})):
                    with patch("src.engines.crewai.tools.custom.genie_tool.asyncio.sleep",
                               AsyncMock()):
                        result = await tool._run_async("question")

        assert "SELECT * FROM t" in result

    @pytest.mark.asyncio
    async def test_last_call_notice_appended(self):
        tool = GenieTool(tool_config={"spaceId": "s1", "max_calls": 3})
        tool._call_count = 2  # This call becomes #3 == max_calls
        tool._base_polling_delay = 1

        conv_response = {"conversation_id": "c1", "message_id": "m1"}
        status_completed = {
            "status": "COMPLETED",
            "attachments": [{"text": {"content": "Answer"}}],
        }
        with patch.object(tool, "_start_or_continue_conversation",
                          AsyncMock(return_value=conv_response)):
            with patch.object(tool, "_get_message_status",
                              AsyncMock(return_value=status_completed)):
                with patch.object(tool, "_get_query_result",
                                  AsyncMock(return_value={})):
                    with patch("src.engines.crewai.tools.custom.genie_tool.asyncio.sleep",
                               AsyncMock()):
                        result = await tool._run_async("question")

        assert "NOTICE" in result or "last allowed" in result

    @pytest.mark.asyncio
    async def test_exponential_backoff_applied(self):
        """Verify exponential backoff path is hit (backoff_threshold reached)."""
        tool = GenieTool(tool_config={"spaceId": "s1"})
        tool._base_polling_delay = 1
        tool._enable_exponential_backoff = True
        tool._backoff_after_seconds = 1  # Start backoff almost immediately
        tool._max_retries = 3

        conv_response = {"conversation_id": "c1", "message_id": "m1"}
        in_progress = {"status": "IN_PROGRESS"}

        sleep_calls = []

        async def mock_sleep(delay):
            sleep_calls.append(delay)

        with patch.object(tool, "_start_or_continue_conversation",
                          AsyncMock(return_value=conv_response)):
            with patch.object(tool, "_get_message_status",
                              AsyncMock(return_value=in_progress)):
                with patch("src.engines.crewai.tools.custom.genie_tool.asyncio.sleep", mock_sleep):
                    await tool._run_async("question")

        # Some sleep calls should have been made (backoff in effect)
        assert len(sleep_calls) >= 1


# ---------------------------------------------------------------------------
# _run — synchronous wrapper
# ---------------------------------------------------------------------------

class TestRunSync:
    def test_run_calls_run_async(self):
        tool = GenieTool(tool_config={"spaceId": "s1"})

        async def fake_async(question):
            return "sync result"

        with patch.object(tool, "_run_async", fake_async):
            result = tool._run("test question")

        assert result == "sync result"


# ---------------------------------------------------------------------------
# _extract_response
# ---------------------------------------------------------------------------

class TestExtractResponse:
    def _make_tool(self):
        return GenieTool(tool_config={"spaceId": "s1", "max_result_rows": 3})

    def test_extracts_text_from_attachment(self):
        tool = self._make_tool()
        status = {"attachments": [{"text": {"content": "Revenue is $1M"}}]}
        result = tool._extract_response(status)
        assert "Revenue is $1M" in result

    def test_extracts_from_answer_field(self):
        """The 'answer' field is used since 'content' is excluded when it matches message context."""
        tool = self._make_tool()
        # Use 'answer' field — this is NOT the context 'content' field so it passes through
        status = {"answer": "Answer from the answer field"}
        result = tool._extract_response(status)
        assert "Answer from the answer field" in result

    def test_extracts_from_response_field(self):
        tool = self._make_tool()
        status = {"response": "Response field content"}
        result = tool._extract_response(status)
        assert "Response field content" in result

    def test_query_results_formatted(self):
        tool = self._make_tool()
        result_data = {
            "statement_response": {
                "result": {
                    "data_typed_array": [
                        {"values": [{"str": "A"}, {"str": "1"}]},
                        {"values": [{"str": "B"}, {"str": "2"}]},
                    ]
                }
            }
        }
        status = {}
        result = tool._extract_response(status, result_data)
        assert "Query Results" in result
        assert "A" in result

    def test_truncation_notice_when_rows_exceed_max(self):
        tool = self._make_tool()
        tool._max_result_rows = 1
        data_rows = [{"values": [{"str": str(i)}]} for i in range(5)]
        result_data = {
            "statement_response": {
                "result": {"data_typed_array": data_rows}
            }
        }
        result = tool._extract_response({}, result_data)
        assert "5" in result  # total rows mentioned
        assert "1" in result  # showing first N

    def test_empty_returns_no_content(self):
        tool = self._make_tool()
        result = tool._extract_response({})
        assert "No response content found" in result

    def test_text_not_echoed_as_response(self):
        """Text matching the 'content' field should not become the response."""
        tool = self._make_tool()
        question = "my question"
        status = {
            "content": question,
            "attachments": [{"text": {"content": question}}]
        }
        result = tool._extract_response(status)
        # The text attachment content equals the question, so it should be excluded
        assert result == "No response content found"

    def test_row_summary_added_when_no_text(self):
        tool = self._make_tool()
        data_rows = [{"values": [{"str": "val"}]}]
        result_data = {
            "statement_response": {
                "result": {"data_typed_array": data_rows}
            }
        }
        result = tool._extract_response({}, result_data)
        assert "1 row" in result or "Query returned" in result


# ---------------------------------------------------------------------------
# _get_message_status
# ---------------------------------------------------------------------------

class TestGetMessageStatus:
    @pytest.mark.asyncio
    async def test_returns_parsed_json(self):
        tool = GenieTool(tool_config={"spaceId": "space-1"})

        mock_session = _make_aiohttp_mock(200, json_data={"status": "COMPLETED"})

        with patch.object(tool, "_get_workspace_url",
                          AsyncMock(return_value="https://ws.example.com")):
            with patch.object(tool, "_get_auth_headers",
                              AsyncMock(return_value={"Authorization": "Bearer tok"})):
                with patch("src.engines.crewai.tools.custom.genie_tool.aiohttp.ClientSession",
                           return_value=mock_session):
                    result = await tool._get_message_status("conv-1", "msg-1")

        assert result == {"status": "COMPLETED"}

    @pytest.mark.asyncio
    async def test_raises_when_no_space_id(self):
        tool = GenieTool()
        with patch.object(tool, "_get_workspace_url",
                          AsyncMock(return_value="https://ws.example.com")):
            with pytest.raises(ValueError, match="space ID"):
                await tool._get_message_status("conv-1", "msg-1")

    @pytest.mark.asyncio
    async def test_raises_when_no_auth(self):
        tool = GenieTool(tool_config={"spaceId": "s1"})
        with patch.object(tool, "_get_workspace_url",
                          AsyncMock(return_value="https://ws.example.com")):
            with patch.object(tool, "_get_auth_headers", AsyncMock(return_value=None)):
                with pytest.raises(Exception, match="No authentication"):
                    await tool._get_message_status("conv-1", "msg-1")


# ---------------------------------------------------------------------------
# _get_query_result
# ---------------------------------------------------------------------------

class TestGetQueryResult:
    @pytest.mark.asyncio
    async def test_raises_when_no_space_id(self):
        tool = GenieTool()
        with patch.object(tool, "_get_workspace_url",
                          AsyncMock(return_value="https://ws.example.com")):
            with pytest.raises(ValueError, match="space ID"):
                await tool._get_query_result("conv-1", "msg-1")

    @pytest.mark.asyncio
    async def test_raises_when_no_auth(self):
        tool = GenieTool(tool_config={"spaceId": "s1"})
        with patch.object(tool, "_get_workspace_url",
                          AsyncMock(return_value="https://ws.example.com")):
            with patch.object(tool, "_get_auth_headers", AsyncMock(return_value=None)):
                with pytest.raises(Exception, match="No authentication"):
                    await tool._get_query_result("conv-1", "msg-1")


# ---------------------------------------------------------------------------
# _start_or_continue_conversation
# ---------------------------------------------------------------------------

class TestStartOrContinueConversation:
    @pytest.mark.asyncio
    async def test_raises_when_no_space_id(self):
        tool = GenieTool()
        with patch.object(tool, "_get_workspace_url",
                          AsyncMock(return_value="https://ws.example.com")):
            with pytest.raises(ValueError, match="space ID"):
                await tool._start_or_continue_conversation("question")

    @pytest.mark.asyncio
    async def test_raises_when_no_auth(self):
        tool = GenieTool(tool_config={"spaceId": "s1"})
        with patch.object(tool, "_get_workspace_url",
                          AsyncMock(return_value="https://ws.example.com")):
            with patch.object(tool, "_get_auth_headers", AsyncMock(return_value=None)):
                with pytest.raises(Exception, match="No authentication"):
                    await tool._start_or_continue_conversation("question")

    @pytest.mark.asyncio
    async def test_continues_existing_conversation(self):
        tool = GenieTool(tool_config={"spaceId": "s1"})
        tool._current_conversation_id = "existing-conv"

        mock_session = _make_aiohttp_mock(200, json_data={"id": "msg-new"})

        # Suppress permission test
        with patch.object(tool, "_get_workspace_url",
                          AsyncMock(return_value="https://ws.example.com")):
            with patch.object(tool, "_get_auth_headers",
                              AsyncMock(return_value={"Authorization": "Bearer tok"})):
                with patch.object(tool, "_test_token_permissions",
                                  AsyncMock(return_value=True)):
                    with patch("src.engines.crewai.tools.custom.genie_tool.aiohttp.ClientSession",
                               return_value=mock_session):
                        result = await tool._start_or_continue_conversation("follow-up question")

        assert result["conversation_id"] == "existing-conv"

    @pytest.mark.asyncio
    async def test_starts_new_conversation(self):
        tool = GenieTool(tool_config={"spaceId": "s1"})

        mock_session = _make_aiohttp_mock(
            200,
            json_data={"conversation_id": "new-conv", "message_id": "msg-1"},
        )

        with patch.object(tool, "_get_workspace_url",
                          AsyncMock(return_value="https://ws.example.com")):
            with patch.object(tool, "_get_auth_headers",
                              AsyncMock(return_value={"Authorization": "Bearer tok"})):
                with patch.object(tool, "_test_token_permissions",
                                  AsyncMock(return_value=True)):
                    with patch("src.engines.crewai.tools.custom.genie_tool.aiohttp.ClientSession",
                               return_value=mock_session):
                        result = await tool._start_or_continue_conversation("new question")

        assert result["conversation_id"] == "new-conv"
        assert tool._current_conversation_id == "new-conv"

    @pytest.mark.asyncio
    async def test_alternative_response_format_message_object(self):
        """Handle responses where conversation_id is nested under 'conversation'."""
        tool = GenieTool(tool_config={"spaceId": "s1"})

        mock_session = _make_aiohttp_mock(
            200,
            json_data={"conversation": {"id": "nested-conv"}, "message": {"id": "nested-msg"}},
        )

        with patch.object(tool, "_get_workspace_url",
                          AsyncMock(return_value="https://ws.example.com")):
            with patch.object(tool, "_get_auth_headers",
                              AsyncMock(return_value={"Authorization": "Bearer tok"})):
                with patch.object(tool, "_test_token_permissions",
                                  AsyncMock(return_value=True)):
                    with patch("src.engines.crewai.tools.custom.genie_tool.aiohttp.ClientSession",
                               return_value=mock_session):
                        result = await tool._start_or_continue_conversation("question")

        assert result["conversation_id"] == "nested-conv"
        assert result["message_id"] == "nested-msg"


# ---------------------------------------------------------------------------
# _test_token_permissions
# ---------------------------------------------------------------------------

def _make_aiohttp_mock(status, text="", json_data=None):
    """Helper to build a properly-configured aiohttp mock session."""
    mock_response = MagicMock()
    mock_response.status = status
    mock_response.text = AsyncMock(return_value=text)
    if json_data is not None:
        mock_response.json = AsyncMock(return_value=json_data)
    mock_response.raise_for_status = Mock()
    mock_response.__aenter__ = AsyncMock(return_value=mock_response)
    mock_response.__aexit__ = AsyncMock(return_value=False)

    mock_session = MagicMock()
    mock_session.get = MagicMock(return_value=mock_response)
    mock_session.post = MagicMock(return_value=mock_response)
    mock_session.__aenter__ = AsyncMock(return_value=mock_session)
    mock_session.__aexit__ = AsyncMock(return_value=False)
    return mock_session


class TestTestTokenPermissions:
    @pytest.mark.asyncio
    async def test_returns_true_on_200(self):
        tool = GenieTool()
        headers = {"Authorization": "Bearer tok"}

        mock_session = _make_aiohttp_mock(200)

        with patch("src.engines.crewai.tools.custom.genie_tool.aiohttp.ClientSession",
                   return_value=mock_session):
            result = await tool._test_token_permissions(headers, "https://ws.example.com")

        assert result is True

    @pytest.mark.asyncio
    async def test_returns_false_on_403(self):
        tool = GenieTool()
        headers = {"Authorization": "Bearer tok"}

        mock_session = _make_aiohttp_mock(403, text="Forbidden")

        with patch("src.engines.crewai.tools.custom.genie_tool.aiohttp.ClientSession",
                   return_value=mock_session):
            result = await tool._test_token_permissions(headers, "https://ws.example.com")

        assert result is False

    @pytest.mark.asyncio
    async def test_returns_false_on_unexpected_status(self):
        tool = GenieTool()
        headers = {"Authorization": "Bearer tok"}

        mock_session = _make_aiohttp_mock(500, text="Internal Error")

        with patch("src.engines.crewai.tools.custom.genie_tool.aiohttp.ClientSession",
                   return_value=mock_session):
            result = await tool._test_token_permissions(headers, "https://ws.example.com")

        assert result is False

    @pytest.mark.asyncio
    async def test_returns_false_on_exception(self):
        tool = GenieTool()
        headers = {"Authorization": "Bearer tok"}

        with patch("src.engines.crewai.tools.custom.genie_tool.aiohttp.ClientSession",
                   side_effect=Exception("network error")):
            result = await tool._test_token_permissions(headers, "https://ws.example.com")

        assert result is False

    @pytest.mark.asyncio
    async def test_jwt_token_decoded_for_scopes(self):
        """JWT token with missing scopes logs warning but 200 response returns True."""
        tool = GenieTool()
        import base64
        payload = base64.urlsafe_b64encode(
            json.dumps({"sub": "test", "scope": "other-scope"}).encode()
        ).decode().rstrip("=")
        fake_jwt = f"eyJhbGciOiJSUzI1NiJ9.{payload}.sig"
        headers = {"Authorization": f"Bearer {fake_jwt}"}

        mock_session = _make_aiohttp_mock(200)

        with patch("src.engines.crewai.tools.custom.genie_tool.aiohttp.ClientSession",
                   return_value=mock_session):
            result = await tool._test_token_permissions(headers, "https://ws.example.com")

        assert result is True  # 200 response means success regardless of scope warning
