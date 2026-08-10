"""
Comprehensive unit tests for
services/tools/custom/databricks_knowledge_search_tool.py

Covers what the TOOL is responsible for: the argument schema, initialisation,
resolving the paths an agent named against the ones configured on it, the
per-agent search budget, and delegating the search itself.

The search — running it, filtering by relevance, saying "not in the knowledge
base" — moved to ``services.knowledge.KnowledgeSearch`` and is tested there,
because it is reachable without an agent now (crew generation researching
before it plans, a chat turn answering from an attached file).
"""

import asyncio
from concurrent.futures import TimeoutError as FuturesTimeoutError
from unittest.mock import AsyncMock, MagicMock, Mock, patch

import pytest

from src.services.tools.databricks_knowledge_search_tool import (
    DatabricksKnowledgeSearchInput,
    DatabricksKnowledgeSearchTool,
)

# ---------------------------------------------------------------------------
# DatabricksKnowledgeSearchInput
# ---------------------------------------------------------------------------


class TestDatabricksKnowledgeSearchInput:
    def test_minimal_required_query(self):
        inp = DatabricksKnowledgeSearchInput(query="show me something")
        assert inp.query == "show me something"
        assert inp.limit == 10  # default
        assert inp.file_paths is None

    def test_custom_limit(self):
        inp = DatabricksKnowledgeSearchInput(query="q", limit=5)
        assert inp.limit == 5

    def test_file_paths_provided(self):
        inp = DatabricksKnowledgeSearchInput(
            query="q", file_paths=["/Volumes/a/b/c.pdf"]
        )
        assert inp.file_paths == ["/Volumes/a/b/c.pdf"]

    def test_limit_min_boundary(self):
        inp = DatabricksKnowledgeSearchInput(query="q", limit=1)
        assert inp.limit == 1

    def test_limit_max_boundary(self):
        inp = DatabricksKnowledgeSearchInput(query="q", limit=20)
        assert inp.limit == 20

    def test_limit_too_small_raises(self):
        with pytest.raises(Exception):
            DatabricksKnowledgeSearchInput(query="q", limit=0)

    def test_limit_too_large_raises(self):
        with pytest.raises(Exception):
            DatabricksKnowledgeSearchInput(query="q", limit=21)


# ---------------------------------------------------------------------------
# DatabricksKnowledgeSearchTool.__init__
# ---------------------------------------------------------------------------


class TestDatabricksKnowledgeSearchToolInit:
    def test_defaults(self):
        tool = DatabricksKnowledgeSearchTool()
        assert tool._group_id == "default"
        assert tool._execution_id is None
        assert tool._user_token is None

    def test_custom_params(self):
        tool = DatabricksKnowledgeSearchTool(
            group_id="g1",
            execution_id="exec-123",
            user_token="tok",
            file_paths=["/Volumes/a/b/c.pdf"],
            agent_id="agent-1",
        )
        assert tool._group_id == "g1"
        assert tool._execution_id == "exec-123"
        assert tool._user_token == "tok"
        assert tool._configured_file_paths == ["/Volumes/a/b/c.pdf"]
        assert tool._agent_id == "agent-1"

    def test_tool_name(self):
        tool = DatabricksKnowledgeSearchTool()
        assert tool.name == "DatabricksKnowledgeSearchTool"

    def test_explicit_token_wins_over_context(self):
        """An explicitly passed token is never overridden by context recovery."""
        from src.utils.user_context import UserContext

        UserContext.set_user_token("ctx-tok")
        try:
            tool = DatabricksKnowledgeSearchTool(group_id="g1", user_token="explicit")
            assert tool._user_token == "explicit"
        finally:
            UserContext.clear_context()

    def test_token_recovered_from_user_context(self):
        """The crux of the deployed-App fix: when the caller does not thread the
        OBO token in, the tool recovers it from UserContext on THIS (parent)
        thread — because the search later runs in a worker thread where the
        ContextVar no longer carries it."""
        from src.utils.user_context import UserContext

        UserContext.set_user_token("recovered-tok")
        try:
            tool = DatabricksKnowledgeSearchTool(group_id="g1")
            assert tool._user_token == "recovered-tok"
        finally:
            UserContext.clear_context()

    def test_token_recovered_from_group_context_access_token(self):
        """Falls back to the group context's access_token when no user token."""
        from src.utils.user_context import GroupContext, UserContext

        UserContext.set_group_context(
            GroupContext(group_ids=["g1"], access_token="grp-tok")
        )
        try:
            tool = DatabricksKnowledgeSearchTool(group_id="g1")
            assert tool._user_token == "grp-tok"
        finally:
            UserContext.clear_context()


# ---------------------------------------------------------------------------
# _resolve_file_paths
# ---------------------------------------------------------------------------


class TestResolveFilePaths:
    def _make_tool(self, configured_paths=None):
        return DatabricksKnowledgeSearchTool(
            group_id="g1",
            file_paths=configured_paths,
        )

    def test_none_input_returns_none(self):
        tool = self._make_tool(configured_paths=["/Volumes/a/b/c.pdf"])
        result = tool._resolve_file_paths(None)
        assert result is None

    def test_empty_list_returns_none(self):
        tool = self._make_tool(configured_paths=["/Volumes/a/b/c.pdf"])
        result = tool._resolve_file_paths([])
        assert result is None

    def test_full_volume_path_returned_as_is(self):
        tool = self._make_tool()
        result = tool._resolve_file_paths(["/Volumes/cat/sch/vol/file.pdf"])
        assert result == ["/Volumes/cat/sch/vol/file.pdf"]

    def test_filename_matched_to_configured_path(self):
        tool = self._make_tool(
            configured_paths=["/Volumes/catalog/schema/volume/report.pdf"]
        )
        result = tool._resolve_file_paths(["report.pdf"])
        assert result == ["/Volumes/catalog/schema/volume/report.pdf"]

    def test_unmatched_path_returned_as_is(self):
        tool = self._make_tool(
            configured_paths=["/Volumes/catalog/schema/volume/other.pdf"]
        )
        result = tool._resolve_file_paths(["unknown.pdf"])
        assert result == ["unknown.pdf"]

    def test_no_configured_paths_returns_agent_paths(self):
        tool = self._make_tool(configured_paths=None)
        result = tool._resolve_file_paths(["some/file.pdf"])
        assert result == ["some/file.pdf"]

    def test_multiple_paths_resolved(self):
        tool = self._make_tool(
            configured_paths=[
                "/Volumes/cat/sch/vol/a.pdf",
                "/Volumes/cat/sch/vol/b.pdf",
            ]
        )
        result = tool._resolve_file_paths(["a.pdf", "b.pdf"])
        assert "/Volumes/cat/sch/vol/a.pdf" in result
        assert "/Volumes/cat/sch/vol/b.pdf" in result

    def test_relative_path_with_dir_matched_by_filename(self):
        tool = self._make_tool(configured_paths=["/Volumes/cat/sch/vol/sub/file.txt"])
        result = tool._resolve_file_paths(["folder/file.txt"])
        assert result == ["/Volumes/cat/sch/vol/sub/file.txt"]


# ---------------------------------------------------------------------------
# _run — uses ThreadPoolExecutor internally
# ---------------------------------------------------------------------------


class TestRun:
    """The tool's own contract: budget, path resolution, delegation."""

    def _make_tool(self):
        return DatabricksKnowledgeSearchTool(
            group_id="g1",
            execution_id="exec-1",
            file_paths=["/Volumes/a/b/c.pdf"],
        )

    def test_returns_what_the_search_answered(self):
        tool = self._make_tool()
        with patch.object(
            tool, "_search_in_thread", return_value="Found 1 relevant results:"
        ):
            assert tool._run("revenue query", limit=5).startswith(
                "Found 1 relevant results:"
            )

    def test_agent_file_paths_override_configured(self):
        """The agent knows what it wants; its paths are resolved and used."""
        tool = self._make_tool()
        seen = []

        with patch.object(
            tool,
            "_search_in_thread",
            side_effect=lambda q, l, paths: seen.append(paths) or "",
        ):
            tool._run("query", limit=5, file_paths=["c.pdf"])

        assert seen[0] is not None

    def test_no_agent_file_paths_uses_configured(self):
        tool = self._make_tool()
        seen = []

        with patch.object(
            tool,
            "_search_in_thread",
            side_effect=lambda q, l, paths: seen.append(paths) or "",
        ):
            tool._run("query")

        assert seen[0] == ["/Volumes/a/b/c.pdf"]

    def test_a_repeated_search_is_answered_from_the_first_one(self):
        """An index cannot say "I don't have this", so an agent hunting for
        something absent rephrases until the round limit kills the run. The
        budget is the agent's problem, so it stays on the tool."""
        tool = self._make_tool()
        calls = []

        with patch.object(
            tool,
            "_search_in_thread",
            side_effect=lambda q, l, paths: calls.append(q) or "twenty chunks",
        ):
            first = tool._run("expense policy")
            second = tool._run("expense policy")

        assert calls == ["expense policy"], "the second search never ran"
        assert "already searched" in second
        assert "twenty chunks" in second

    def test_the_budget_runs_out(self):
        tool = self._make_tool()
        with patch.object(tool, "_search_in_thread", return_value="results"):
            for i in range(tool._budget.max_searches):
                tool._run(f"query {i}")
            refused = tool._run("one more rephrasing")

        assert "Search budget reached" in refused
