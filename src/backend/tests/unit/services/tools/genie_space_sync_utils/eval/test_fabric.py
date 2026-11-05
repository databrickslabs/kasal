"""Fabric data agent over MCP: URL validation, the JSON-RPC sequence, and error handling."""

import json

import pytest

from src.services.tools.genie_space_sync_utils.eval.fabric import (
    McpReply,
    ask_fabric,
    parse_rpc_body,
    validate_mcp_url,
)

URL = (
    "https://api.fabric.microsoft.com/v1/mcp/workspaces/"
    "11111111-1111-1111-1111-111111111111/dataagents/22222222-2222-2222-2222-222222222222/agent"
)


def _json_reply(result=None, error=None, headers=None):
    body = {"jsonrpc": "2.0", "id": 1}
    body.update({"error": error} if error else {"result": result or {}})
    return McpReply(200, "application/json", json.dumps(body), session_id="sess-1")


def _agent(
    answer_text="The total is 42", tool_name="DataAgent_Sales", argument="question"
):
    """A scripted MCP: returns the right reply for each method; records what it was sent."""
    sent = []

    def send(body, headers):
        sent.append((body, dict(headers)))
        method = body["method"]
        if method == "initialize":
            return _json_reply({"capabilities": {}})
        if method == "notifications/initialized":
            return McpReply(202, "application/json", "")
        if method == "tools/list":
            tool = {"name": tool_name, "inputSchema": {"required": [argument]}}
            return _json_reply({"tools": [tool]})
        return _json_reply({"content": [{"type": "text", "text": answer_text}]})

    return send, sent


def test_the_url_must_be_the_fabric_mcp_route():
    assert validate_mcp_url(URL) == URL
    for bad in (
        "http://api.fabric.microsoft.com/v1/mcp/workspaces/x/dataagents/y/agent",
        "https://example.com/v1/mcp/workspaces/x/dataagents/y/agent",
        URL + "?x=1",
    ):
        with pytest.raises(ValueError):
            validate_mcp_url(bad)


def test_a_question_runs_initialize_list_then_call_with_the_session_header():
    send, sent = _agent()
    answer = ask_fabric(send, "How many orders?")
    assert answer.status == "COMPLETED"
    assert answer.text == "The total is 42"
    assert answer.tables == ()
    methods = [body["method"] for body, _ in sent]
    assert methods == [
        "initialize",
        "notifications/initialized",
        "tools/list",
        "tools/call",
    ]
    call_body, call_headers = sent[-1]
    assert call_body["params"] == {
        "name": "DataAgent_Sales",
        "arguments": {"question": "How many orders?"},
    }
    assert call_headers.get("Mcp-Session-Id") == "sess-1"


def test_a_tool_error_is_a_failed_answer_and_a_transport_error_is_an_error_answer():
    send, _ = _agent()

    def failing(body, headers):
        if body["method"] == "tools/call":
            return McpReply(
                200,
                "application/json",
                json.dumps(
                    {
                        "result": {
                            "isError": True,
                            "content": [{"type": "text", "text": "bad"}],
                        }
                    }
                ),
            )
        return send(body, headers)

    assert ask_fabric(failing, "q").status == "FAILED"

    def rejected(body, headers):
        return McpReply(401, "application/json", "")

    answer = ask_fabric(rejected, "q")
    assert answer.status == "ERROR"
    assert "401" in answer.error


def test_more_than_one_tool_is_refused():
    def two_tools(body, headers):
        if body["method"] == "tools/list":
            return _json_reply({"tools": [{"name": "a"}, {"name": "b"}]})
        return _json_reply({})

    assert ask_fabric(two_tools, "q").status == "ERROR"


def test_an_event_stream_reply_uses_its_last_data_line():
    text = 'data: {"result": {"x": 1}}\n\ndata: {"result": {"y": 2}}\n'
    assert parse_rpc_body("text/event-stream", text) == {"result": {"y": 2}}
