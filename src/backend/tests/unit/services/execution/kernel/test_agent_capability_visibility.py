"""What an agent can actually do, said once and plainly at build time.

Three consecutive runs were given the goal "Source Swiss apartment listings
from available data sources" together with four database-introspection tools
and nothing capable of fetching a listing. The facts were in the log the whole
time, spread across twenty lines of per-server detail. Nobody spotted it and
~135 tool calls were spent discovering it at run time.

Two checks, and the split between them is the point:

- An MCP server that was REQUESTED and returned NO tools is a hard mismatch —
  the crew asked for a capability and got nothing — so it warns.
- Whether a toolset can satisfy a goal requires reading intent from prose. A
  warning that guesses wrong gets switched off, which costs more than it saves.
  So that half is an INVENTORY, and the judgement is left to the reader.
"""

import logging

import pytest

from src.services.execution.kernel.agent_tools import (
    _log_capability_inventory,
    _warn_on_empty_mcp_servers,
)


@pytest.fixture(autouse=True)
def _capturable_crew_logger():
    """These log through LoggerManager's "crew" logger, which sets
    propagate=False so crew output lands in crew.log rather than the root
    handlers. caplog attaches to root, so without this it captures nothing and
    the assertions below would pass or fail for the wrong reason."""
    from src.core.logger import LoggerManager

    crew = LoggerManager.get_instance().crew
    previous = crew.propagate
    crew.propagate = True
    yield
    crew.propagate = previous


class _Tool:
    def __init__(self, name):
        self.name = name


def _db_tools():
    return [
        _Tool("database_execute_sql"),
        _Tool("database_list_schemas"),
        _Tool("database_list_tables"),
        _Tool("database_describe_table"),
    ]


class TestEmptyServerWarning:
    def test_a_requested_server_that_returned_nothing_warns(self, caplog):
        with caplog.at_level(logging.WARNING):
            _warn_on_empty_mcp_servers("agent-1", ["Parse", "database"], _db_tools())

        assert "[CAPABILITY]" in caplog.text
        assert "Parse" in caplog.text
        assert "database" not in caplog.text.split("server(s)")[1].split("which")[0]

    def test_all_servers_delivering_is_silent(self, caplog):
        tools = _db_tools() + [_Tool("Parse_call_endpoint")]
        with caplog.at_level(logging.WARNING):
            _warn_on_empty_mcp_servers("agent-1", ["Parse", "database"], tools)
        assert caplog.text == ""

    def test_every_server_empty_names_them_all(self, caplog):
        with caplog.at_level(logging.WARNING):
            _warn_on_empty_mcp_servers("agent-1", ["Parse", "Genie"], [])
        assert "Genie, Parse" in caplog.text

    def test_it_says_what_to_check(self, caplog):
        """A warning an operator cannot act on is noise."""
        with caplog.at_level(logging.WARNING):
            _warn_on_empty_mcp_servers("agent-1", ["Parse"], [])
        assert "reachable" in caplog.text and "credentials" in caplog.text

    def test_it_never_raises(self):
        """An agent that lost one of three servers may still do useful work;
        killing the run at build time tells an operator less than a degraded
        answer with the cause recorded."""
        _warn_on_empty_mcp_servers("agent-1", None, [])
        _warn_on_empty_mcp_servers("agent-1", ["X"], [object()])  # nameless tool


class TestCapabilityInventory:
    def test_it_groups_tools_by_server(self, caplog):
        with caplog.at_level(logging.INFO):
            _log_capability_inventory("agent-1", _db_tools())
        assert "database(4)" in caplog.text

    def test_the_failing_run_would_have_been_visible_in_one_line(self, caplog):
        """The actual regression: only database tools, for a sourcing goal."""
        with caplog.at_level(logging.INFO):
            _log_capability_inventory("Swiss listings agent", _db_tools())
        line = [r for r in caplog.messages if "[CAPABILITY]" in r][0]
        assert line == "[CAPABILITY] Agent Swiss listings agent can use: database(4)"

    def test_mixed_sources_are_all_named(self, caplog):
        tools = _db_tools() + [
            _Tool("Parse_call_endpoint"),
            _Tool("Parse_marketplace_search"),
        ]
        with caplog.at_level(logging.INFO):
            _log_capability_inventory("agent-1", tools)
        assert "Parse(2)" in caplog.text and "database(4)" in caplog.text

    def test_a_tool_with_no_underscore_stands_for_itself(self, caplog):
        with caplog.at_level(logging.INFO):
            _log_capability_inventory("agent-1", [_Tool("PerplexityTool")])
        assert "PerplexityTool(1)" in caplog.text

    def test_no_tools_logs_nothing(self, caplog):
        """The caller already logs "will not have any tools"; a second empty
        line adds noise."""
        with caplog.at_level(logging.INFO):
            _log_capability_inventory("agent-1", [])
        assert caplog.text == ""

    def test_it_states_rather_than_judges(self):
        """Guard against this becoming a guess later: the inventory must not
        contain verdict language, because a wrong verdict gets it switched off
        and then nobody sees the facts either."""
        import inspect

        source = inspect.getsource(_log_capability_inventory)
        for verdict in ("cannot", "insufficient", "missing", "unable"):
            assert f'"{verdict}' not in source.lower()
