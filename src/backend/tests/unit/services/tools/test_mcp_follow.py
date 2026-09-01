"""Tests for the agnostic MCP follow-up loop (services/tools/mcp_follow).

The MCP layer names no server: a server's OWN ``follow`` configuration (shipped
as preset data for the managed Databricks Genie endpoints, writable for any
other server) declares its start-tool + poll-tool pair, and the runner follows
it to completion so the agent sees one finished result.
"""

import asyncio
import json
from types import SimpleNamespace

from src.services.tools.mcp_follow import config as follow_config
from src.services.tools.mcp_follow import follow_spec_from_config, follow_tool_call
from src.services.tools.mcp_follow import runner as follow_runner

# The managed-Genie preset shapes, exactly as the catalog ships them — here
# they are TEST DATA, not engine knowledge.
PER_SPACE_FOLLOW = [
    {
        "name": "Genie",
        "start_tool": "query_space",
        "poll_tool": "poll_response",
        "id_params": ["conversation_id", "message_id"],
    }
]
GENIE_ONE_FOLLOW = [
    {
        "name": "Genie",
        "start_tool": "genie_ask",
        "poll_tool": "genie_poll_response",
        "id_params": ["conversation_id", "response_id"],
    }
]


def _envelope(status, conv="conv-1", msg="msg-1", **extra):
    """A per-space status envelope as structuredContent (its native shape)."""
    return SimpleNamespace(
        structuredContent={
            "status": status,
            "conversationId": conv,
            "messageId": msg,
            **extra,
        }
    )


def _one_envelope(status, final_answer=None, conv="conv-1", resp="resp-1"):
    """A Genie One envelope: lowercase status, snake_case ids, final_answer."""
    return SimpleNamespace(
        structuredContent={
            "response_id": resp,
            "conversation_id": conv,
            "status": status,
            "final_answer": final_answer,
        }
    )


def _text_result(text):
    return SimpleNamespace(content=[SimpleNamespace(type="text", text=text)])


def _not_ready():
    """An empty / not-ready poll acknowledgement (HTTP 202)."""
    return SimpleNamespace(structuredContent=None, content=[])


def _error_result(text="429 too many requests"):
    return SimpleNamespace(
        isError=True, content=[SimpleNamespace(type="text", text=text)]
    )


class FakeAdapter:
    def __init__(self, follow, poll_results, tool_names):
        self.server_params = {"follow": follow}
        self._poll_results = list(poll_results)
        self.tools = [{"name": n} for n in tool_names]
        self.poll_calls = []

    async def execute_tool(self, name, params):
        self.poll_calls.append((name, params))
        item = self._poll_results.pop(0)
        if isinstance(item, Exception):
            raise item
        return item


class FakeWrapper:
    def __init__(self, name, adapter, initial_result):
        self.name = name
        self.adapter = adapter
        self._initial = initial_result
        self.execute_calls = []

    async def execute(self, params):
        self.execute_calls.append(params)
        return self._initial


def _follow(wrapper, params=None):
    return asyncio.run(
        follow_tool_call(wrapper, params or {"query": "q"}, follow_spec_from_config)
    )


def _per_space(poll_results, initial, tool="query_space_s1", follow=PER_SPACE_FOLLOW):
    adapter = FakeAdapter(
        follow=follow,
        poll_results=poll_results,
        tool_names=["query_space_s1", "poll_response_s1"],
    )
    return adapter, FakeWrapper(tool, adapter, initial)


# --- spec resolution --------------------------------------------------------


def test_no_follow_config_means_plain_passthrough():
    adapter = FakeAdapter(follow=None, poll_results=[], tool_names=["query_space_s1"])
    wrapper = FakeWrapper("query_space_s1", adapter, _envelope("ASKING_AI"))
    assert follow_spec_from_config(wrapper, None) is None
    assert _follow(wrapper) is wrapper._initial
    assert adapter.poll_calls == []


def test_unrelated_tool_passes_through():
    adapter = FakeAdapter(
        follow=PER_SPACE_FOLLOW, poll_results=[], tool_names=["run_sql"]
    )
    wrapper = FakeWrapper("run_sql", adapter, _text_result("rows"))
    assert _follow(wrapper) is wrapper._initial
    assert adapter.poll_calls == []


def test_skipped_when_poll_tool_not_advertised():
    adapter = FakeAdapter(
        follow=PER_SPACE_FOLLOW,
        poll_results=[],
        tool_names=["query_space_s1"],  # poll_response_s1 missing
    )
    wrapper = FakeWrapper("query_space_s1", adapter, _envelope("ASKING_AI"))
    assert _follow(wrapper) is wrapper._initial
    assert adapter.poll_calls == []


def test_prefixed_tool_names_derive_the_prefixed_poll_tool():
    prefix = "databricks genie: genie one_"
    adapter = FakeAdapter(
        follow=GENIE_ONE_FOLLOW,
        poll_results=[_one_envelope("completed", final_answer="5 emails")],
        tool_names=[f"{prefix}genie_ask", f"{prefix}genie_poll_response"],
    )
    wrapper = FakeWrapper(f"{prefix}genie_ask", adapter, _one_envelope("in_progress"))
    _follow(wrapper)
    assert adapter.poll_calls[0][0] == f"{prefix}genie_poll_response"


# --- envelope helpers -------------------------------------------------------


def test_status_envelope_from_structured_and_text():
    assert (
        follow_config.status_envelope(_envelope("COMPLETED"))["status"] == "COMPLETED"
    )
    as_text = _text_result(json.dumps({"status": "COMPLETED", "conversationId": "c"}))
    assert follow_config.status_envelope(as_text)["status"] == "COMPLETED"
    assert follow_config.status_envelope(_text_result("plain answer")) is None


# --- the loop ---------------------------------------------------------------


def test_blocks_until_terminal_status(monkeypatch):
    monkeypatch.setattr(follow_runner, "FOLLOW_INTERVAL_SECONDS", 0)
    adapter, wrapper = _per_space(
        [_envelope("PENDING_WAREHOUSE"), _envelope("COMPLETED")],
        _envelope("ASKING_AI"),
    )
    result = _follow(wrapper)
    assert result.structuredContent["status"] == "COMPLETED"
    assert len(adapter.poll_calls) == 2


def test_not_ready_polls_keep_going(monkeypatch):
    """An HTTP-202 style poll (no envelope, no content) means 'still running' —
    the loop must keep polling, not hand back an unfinished result."""
    monkeypatch.setattr(follow_runner, "FOLLOW_INTERVAL_SECONDS", 0)
    adapter, wrapper = _per_space(
        [_not_ready(), _not_ready(), _envelope("COMPLETED")], _envelope("ASKING_AI")
    )
    result = _follow(wrapper)
    assert result.structuredContent["status"] == "COMPLETED"
    assert len(adapter.poll_calls) == 3


def test_answer_payload_without_envelope_is_completion(monkeypatch):
    monkeypatch.setattr(follow_runner, "FOLLOW_INTERVAL_SECONDS", 0)
    answer = _text_result("42 credit cases across 7 industries")
    adapter, wrapper = _per_space([answer], _envelope("EXECUTING_QUERY"))
    assert _follow(wrapper) is answer
    assert len(adapter.poll_calls) == 1


def test_poll_sends_only_the_declared_ids(monkeypatch):
    """Only the pair's declared id_params travel — an envelope carrying BOTH
    shapes' ids must not leak an undeclared parameter to a strict server (the
    adapter does not trim unknown keys)."""
    monkeypatch.setattr(follow_runner, "FOLLOW_INTERVAL_SECONDS", 0)
    both_ids = _envelope("ASKING_AI", conv="CONV", msg="MSG", response_id="RESP")
    adapter, wrapper = _per_space([_envelope("COMPLETED")], both_ids)
    _follow(wrapper)
    assert adapter.poll_calls[0] == (
        "poll_response_s1",
        {"conversation_id": "CONV", "message_id": "MSG"},
    )


def test_genie_one_shape_polls_with_response_id_until_final_answer(monkeypatch):
    monkeypatch.setattr(follow_runner, "FOLLOW_INTERVAL_SECONDS", 0)
    adapter = FakeAdapter(
        follow=GENIE_ONE_FOLLOW,
        poll_results=[
            _one_envelope("in_progress"),
            _one_envelope("completed", final_answer="You have 5 emails today."),
        ],
        tool_names=["genie_ask", "genie_poll_response"],
    )
    wrapper = FakeWrapper("genie_ask", adapter, _one_envelope("in_progress"))
    result = _follow(wrapper)
    assert result.structuredContent["final_answer"] == "You have 5 emails today."
    assert adapter.poll_calls[0][1] == {
        "conversation_id": "conv-1",
        "response_id": "resp-1",
    }


def test_done_field_counts_as_final_even_without_terminal_status(monkeypatch):
    monkeypatch.setattr(follow_runner, "FOLLOW_INTERVAL_SECONDS", 0)
    adapter = FakeAdapter(
        follow=GENIE_ONE_FOLLOW,
        poll_results=[_one_envelope("in_progress", final_answer="done early")],
        tool_names=["genie_ask", "genie_poll_response"],
    )
    wrapper = FakeWrapper("genie_ask", adapter, _one_envelope("in_progress"))
    result = _follow(wrapper)
    assert result.structuredContent["final_answer"] == "done early"


def test_error_status_is_terminal(monkeypatch):
    monkeypatch.setattr(follow_runner, "FOLLOW_INTERVAL_SECONDS", 0)
    adapter, wrapper = _per_space([_envelope("ERROR")], _envelope("ASKING_AI"))
    result = _follow(wrapper)
    assert result.structuredContent["status"] == "ERROR"
    assert len(adapter.poll_calls) == 1


def test_no_poll_when_opening_result_is_already_final():
    adapter, wrapper = _per_space([], _envelope("COMPLETED"))
    assert _follow(wrapper).structuredContent["status"] == "COMPLETED"
    assert adapter.poll_calls == []


def test_timeout_returns_directive_not_fabrication(monkeypatch):
    monkeypatch.setattr(follow_runner, "FOLLOW_INTERVAL_SECONDS", 0)
    monkeypatch.setattr(follow_runner, "FOLLOW_TIMEOUT_SECONDS", 0)
    adapter, wrapper = _per_space(
        [_envelope("EXECUTING_QUERY")], _envelope("EXECUTING_QUERY")
    )
    result = _follow(wrapper)
    assert isinstance(result, str)
    assert "not available" in result.lower()
    assert "do not fabricate" in result.lower()


def test_transient_poll_failure_is_retried(monkeypatch):
    monkeypatch.setattr(follow_runner, "FOLLOW_INTERVAL_SECONDS", 0)
    adapter, wrapper = _per_space(
        [RuntimeError("network blip"), _envelope("COMPLETED")], _envelope("ASKING_AI")
    )
    result = _follow(wrapper)
    assert result.structuredContent["status"] == "COMPLETED"
    assert len(adapter.poll_calls) == 2


def test_repeated_failures_return_a_directive_not_an_in_progress_envelope(monkeypatch):
    """Regression: the old loop returned the last snapshot on a poll error —
    which after the first advance was an IN-PROGRESS envelope, the exact shape
    agents misread as an answer."""
    monkeypatch.setattr(follow_runner, "FOLLOW_INTERVAL_SECONDS", 0)
    errors = [RuntimeError(f"boom {i}") for i in range(3)]
    adapter, wrapper = _per_space(errors, _envelope("ASKING_AI"))
    result = _follow(wrapper)
    assert isinstance(result, str)
    assert "not available" in result.lower()
    assert len(adapter.poll_calls) == 3


def test_error_results_count_as_failures_not_answers(monkeypatch):
    """An MCP isError result (a 429/5xx surfaced as a result, not an
    exception) must never be returned as the finished answer."""
    monkeypatch.setattr(follow_runner, "FOLLOW_INTERVAL_SECONDS", 0)
    adapter, wrapper = _per_space(
        [_error_result(), _error_result(), _error_result()], _envelope("ASKING_AI")
    )
    result = _follow(wrapper)
    assert isinstance(result, str)
    assert "not available" in result.lower()
