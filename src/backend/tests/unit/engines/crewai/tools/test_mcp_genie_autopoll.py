"""Tests for managed-Databricks-Genie MCP auto-polling (mcp_handler._genie_autopoll).

The Databricks-managed Genie MCP server splits a question into two tools:
`query_space_<space>` returns an in-progress status envelope immediately, and
`poll_response_<space>` fetches the latest status. Left to the LLM agent, the
poll loop gets abandoned early (the agent fabricates a "placeholder" answer
while the query is still PENDING_WAREHOUSE/EXECUTING_QUERY) and the ids get
mixed up (conversation_id passed as message_id, crashing the poll).

`_genie_autopoll` makes a single `query_space` call block until the query
reaches a terminal status, polling internally with the correct ids.
"""

import asyncio
import json
from types import SimpleNamespace

import src.engines.crewai.tools.mcp_handler as mcp_handler


def _envelope(status, conv="conv-1", msg="msg-1"):
    """A managed-Genie status envelope as structuredContent (its native shape)."""
    return SimpleNamespace(
        structuredContent={
            "status": status,
            "conversationId": conv,
            "messageId": msg,
            "content": {"textAttachments": [f"status {status}"]},
        }
    )


def _text_result(text):
    return SimpleNamespace(content=[SimpleNamespace(type="text", text=text)])


class FakeAdapter:
    def __init__(self, server_url, poll_results, tool_names):
        self.server_url = server_url
        self._poll_results = list(poll_results)
        self.tools = [{"name": n} for n in tool_names]
        self.poll_calls = []

    async def execute_tool(self, name, params):
        self.poll_calls.append((name, params))
        return self._poll_results.pop(0)


class RaisingAdapter(FakeAdapter):
    async def execute_tool(self, name, params):
        self.poll_calls.append((name, params))
        raise RuntimeError("network boom")


class FakeWrapper:
    def __init__(self, name, adapter, initial_result):
        self.name = name
        self.adapter = adapter
        self._initial = initial_result
        self.execute_calls = []

    async def execute(self, params):
        self.execute_calls.append(params)
        return self._initial


# --- helpers ----------------------------------------------------------------


def test_is_managed_genie_adapter():
    assert mcp_handler._is_managed_genie_adapter(
        SimpleNamespace(server_url="https://ws/api/2.0/mcp/genie/abc123")
    )
    assert not mcp_handler._is_managed_genie_adapter(
        SimpleNamespace(server_url="https://ws/api/2.0/mcp/sql")
    )
    assert not mcp_handler._is_managed_genie_adapter(SimpleNamespace())


def test_genie_poll_tool_name_derivation():
    assert mcp_handler._genie_poll_tool_name("query_space_abc") == "poll_response_abc"
    assert mcp_handler._genie_poll_tool_name("some_other_tool") is None


def test_status_envelope_from_structured_content():
    env = mcp_handler._genie_status_envelope(_envelope("COMPLETED"))
    assert env["status"] == "COMPLETED"


def test_status_envelope_from_text_block():
    result = _text_result(
        json.dumps({"status": "COMPLETED", "conversationId": "c", "messageId": "m"})
    )
    env = mcp_handler._genie_status_envelope(result)
    assert env and env["status"] == "COMPLETED"


def test_status_envelope_none_for_plain_answer():
    assert (
        mcp_handler._genie_status_envelope(_text_result("just a plain answer")) is None
    )


# --- auto-poll behaviour ----------------------------------------------------


def test_autopoll_blocks_until_completed(monkeypatch):
    monkeypatch.setattr(mcp_handler, "_GENIE_POLL_INTERVAL_SECONDS", 0)
    space = "abc123"
    adapter = FakeAdapter(
        server_url=f"https://ws/api/2.0/mcp/genie/{space}",
        poll_results=[_envelope("PENDING_WAREHOUSE"), _envelope("COMPLETED")],
        tool_names=[f"query_space_{space}", f"poll_response_{space}"],
    )
    wrapper = FakeWrapper(f"query_space_{space}", adapter, _envelope("ASKING_AI"))

    result = asyncio.run(mcp_handler._genie_autopoll(wrapper, {"query": "q"}))

    assert mcp_handler._genie_status_envelope(result)["status"] == "COMPLETED"
    assert len(adapter.poll_calls) == 2  # polled past ASKING_AI and PENDING_WAREHOUSE


def test_autopoll_uses_message_id_not_conversation_id(monkeypatch):
    """Regression for the conversation_id-as-message_id mix-up: the poll must
    carry the envelope's messageId as message_id, not the conversationId."""
    monkeypatch.setattr(mcp_handler, "_GENIE_POLL_INTERVAL_SECONDS", 0)
    space = "s1"
    adapter = FakeAdapter(
        server_url=f"https://ws/api/2.0/mcp/genie/{space}",
        poll_results=[_envelope("COMPLETED", conv="CONV", msg="MSG")],
        tool_names=[f"query_space_{space}", f"poll_response_{space}"],
    )
    wrapper = FakeWrapper(
        f"query_space_{space}", adapter, _envelope("ASKING_AI", conv="CONV", msg="MSG")
    )

    asyncio.run(mcp_handler._genie_autopoll(wrapper, {"query": "q"}))

    name, params = adapter.poll_calls[0]
    assert name == f"poll_response_{space}"
    assert params == {"conversation_id": "CONV", "message_id": "MSG"}


def test_autopoll_no_poll_when_already_completed():
    space = "abc"
    adapter = FakeAdapter(
        server_url=f"https://ws/api/2.0/mcp/genie/{space}",
        poll_results=[],
        tool_names=[f"query_space_{space}", f"poll_response_{space}"],
    )
    wrapper = FakeWrapper(f"query_space_{space}", adapter, _envelope("COMPLETED"))

    result = asyncio.run(mcp_handler._genie_autopoll(wrapper, {"query": "q"}))

    assert adapter.poll_calls == []
    assert mcp_handler._genie_status_envelope(result)["status"] == "COMPLETED"


def test_autopoll_skipped_for_non_genie_tool():
    adapter = FakeAdapter(
        server_url="https://ws/api/2.0/mcp/sql",
        poll_results=[],
        tool_names=["run_sql"],
    )
    wrapper = FakeWrapper("run_sql", adapter, _text_result("rows"))

    result = asyncio.run(mcp_handler._genie_autopoll(wrapper, {"q": "x"}))

    assert adapter.poll_calls == []
    assert len(wrapper.execute_calls) == 1
    assert result is wrapper._initial


def test_autopoll_skipped_when_no_sibling_poll_tool():
    space = "abc"
    adapter = FakeAdapter(
        server_url=f"https://ws/api/2.0/mcp/genie/{space}",
        poll_results=[],
        tool_names=[f"query_space_{space}"],  # poll_response not advertised
    )
    wrapper = FakeWrapper(f"query_space_{space}", adapter, _envelope("ASKING_AI"))

    result = asyncio.run(mcp_handler._genie_autopoll(wrapper, {"query": "q"}))

    assert adapter.poll_calls == []
    assert result is wrapper._initial


def test_autopoll_timeout_returns_directive_not_fabrication(monkeypatch):
    monkeypatch.setattr(mcp_handler, "_GENIE_POLL_INTERVAL_SECONDS", 0)
    monkeypatch.setattr(mcp_handler, "_GENIE_POLL_TIMEOUT_SECONDS", 0)
    space = "abc"
    adapter = FakeAdapter(
        server_url=f"https://ws/api/2.0/mcp/genie/{space}",
        poll_results=[_envelope("EXECUTING_QUERY")],
        tool_names=[f"query_space_{space}", f"poll_response_{space}"],
    )
    wrapper = FakeWrapper(f"query_space_{space}", adapter, _envelope("EXECUTING_QUERY"))

    result = asyncio.run(mcp_handler._genie_autopoll(wrapper, {"query": "q"}))

    assert isinstance(result, str)
    assert "timed out" in result.lower()
    assert "do not fabricate" in result.lower()


def test_autopoll_returns_last_snapshot_on_poll_error(monkeypatch):
    monkeypatch.setattr(mcp_handler, "_GENIE_POLL_INTERVAL_SECONDS", 0)
    space = "abc"
    adapter = RaisingAdapter(
        server_url=f"https://ws/api/2.0/mcp/genie/{space}",
        poll_results=[],
        tool_names=[f"query_space_{space}", f"poll_response_{space}"],
    )
    initial = _envelope("ASKING_AI")
    wrapper = FakeWrapper(f"query_space_{space}", adapter, initial)

    result = asyncio.run(mcp_handler._genie_autopoll(wrapper, {"query": "q"}))

    assert len(adapter.poll_calls) == 1  # tried once, then gave back last snapshot
    assert result is initial
