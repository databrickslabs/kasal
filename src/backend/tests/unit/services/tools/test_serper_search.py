"""SerperDevTool result ranking: ranked in Kasal, unranked in exported apps.

Ranking comes from ``services/decisions``, which needs the database layer and
is deliberately NOT vendored into exported apps. An exported Serper search used
to raise ``ModuleNotFoundError`` after the API call had already succeeded.
"""

import importlib
import sys
from unittest.mock import patch

import pytest

from src.services.export.runtime_vendor import VENDOR_PKG, kasal_runtime_files
from src.services.tools import serper_search

API_RESPONSE = {
    "searchParameters": {"q": "kasal"},
    "organic": [
        {"title": "First", "link": "https://example.com/1", "snippet": "a"},
        {"title": "Second", "link": "https://example.com/2", "snippet": "b"},
    ],
    "credits": 1,
}


def _run(tool_module):
    tool = tool_module.SerperDevTool()
    with patch.object(
        tool_module.SerperDevTool, "_make_api_request", return_value=API_RESPONSE
    ):
        return tool._run(search_query="kasal")


class TestRankingInKasal:
    def test_results_are_ranked_by_the_decisions_policy(self):
        calls = []

        def fake_rank(policy, request, items, descriptions, **_):
            calls.append((policy, request))
            return list(reversed(items))

        with patch("src.services.decisions.policies.rank_sync", fake_rank):
            result = _run(serper_search)

        assert calls == [("research_triage", "kasal")]
        assert [r["title"] for r in result["organic"]] == ["Second", "First"]

    def test_the_ranker_resolves_to_kasals_decisions_package(self):
        from src.services.decisions.policies import rank_sync

        assert serper_search._load_ranker() is rank_sync

    def test_a_broken_import_inside_decisions_is_not_swallowed(self):
        """Only the decisions package being ABSENT means 'unranked'; a missing
        dependency inside it is a real bug and must surface in Kasal."""
        err = ModuleNotFoundError("No module named 'nope'", name="nope")
        with patch.object(serper_search.importlib, "import_module", side_effect=err):
            with pytest.raises(ModuleNotFoundError):
                serper_search._load_ranker()

    def test_absent_decisions_package_falls_back_to_unranked(self, monkeypatch):
        monkeypatch.setitem(sys.modules, "src.services.decisions.policies", None)
        assert serper_search._load_ranker() is None
        result = _run(serper_search)
        assert [r["title"] for r in result["organic"]] == ["First", "Second"]


class TestUnrankedInExportedApp:
    @pytest.mark.asyncio
    async def test_vendored_serper_returns_unranked_results(
        self, tmp_path, monkeypatch
    ):
        """Import the vendored tool from a written bundle, with ``src`` blocked,
        and run a search end to end."""
        for f in await kasal_runtime_files():
            dest = tmp_path / f["path"]
            dest.parent.mkdir(parents=True, exist_ok=True)
            dest.write_text(f["content"], encoding="utf-8")
        (tmp_path / "agent_server" / "__init__.py").write_text("", encoding="utf-8")

        class _BlockSrc:
            def find_spec(self, fullname, path=None, target=None):
                if fullname == "src" or fullname.startswith("src."):
                    raise ImportError(f"vendored runtime reached Kasal: {fullname}")
                return None

        blocker = _BlockSrc()
        monkeypatch.syspath_prepend(str(tmp_path))
        for m in [m for m in sys.modules if m.startswith(VENDOR_PKG)]:
            sys.modules.pop(m, None)
        sys.meta_path.insert(0, blocker)
        try:
            vendored = importlib.import_module(
                f"{VENDOR_PKG}.services.tools.serper_search"
            )
            assert vendored._load_ranker() is None
            result = _run(vendored)
        finally:
            sys.meta_path.remove(blocker)
            for m in [m for m in sys.modules if m.startswith(VENDOR_PKG)]:
                sys.modules.pop(m, None)

        assert [r["title"] for r in result["organic"]] == ["First", "Second"]
        assert result["credits"] == 1
