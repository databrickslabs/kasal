"""Deep Research's JSON envelope must not reach the user as JSON.

``output_schema`` routes through ``output_json``, which rewrites
``TaskOutput.raw`` to the JSON dump — so a finished deep run's text is a JSON
document. Traced end to end: the composer ran 5.4s on that JSON, produced a
``document`` with no data component, and the prose gate dropped it ("would have
repeated the answer's own words"). A dropped surface leaves the result a plain
string, so the chat showed raw JSON.

Rendering the envelope to markdown first fixes both halves: the composer sees a
table and emits a ``Table`` (a data component, so the surface survives the gate),
and the fallback text is readable when no surface is produced.
"""

import json

import pytest

from src.services.a2ui.structured_text import render_research_envelope


def _envelope(**overrides):
    base = {
        "summary": "Memory work moved beyond passive vector retrieval.",
        "findings": [
            {
                "claim": "A-MEM links memory notes",
                "evidence": "Agentic note construction",
                "source": "https://arxiv.org/abs/2502.12110",
                "confidence": 0.85,
            },
            {
                "claim": "Zep uses a temporal knowledge graph",
                "evidence": "Beats MemGPT on DMR",
                "source": "https://arxiv.org/abs/2501.13956",
                "confidence": 0.9,
            },
        ],
        "open_questions": ["No head-to-head benchmark exists."],
        "limitations": "All are preprints.",
    }
    base.update(overrides)
    return json.dumps(base)


class TestItProducesATable:
    """A markdown table is what makes the composer emit a Table component."""

    def test_a_table_is_rendered(self):
        out = render_research_envelope(_envelope())
        assert "| Finding | Evidence | Source | Confidence |" in out
        assert out.count("\n|") >= 3  # header, rule, one row per finding

    def test_every_finding_becomes_a_row(self):
        out = render_research_envelope(_envelope())
        assert "A-MEM links memory notes" in out
        assert "Zep uses a temporal knowledge graph" in out

    def test_sources_become_links(self):
        out = render_research_envelope(_envelope())
        assert (
            "[https://arxiv.org/abs/2502.12110](https://arxiv.org/abs/2502.12110)"
            in out
        )

    def test_confidence_reads_as_a_percentage(self):
        out = render_research_envelope(_envelope())
        assert "85%" in out and "90%" in out

    def test_summary_open_questions_and_limitations_survive(self):
        out = render_research_envelope(_envelope())
        assert "moved beyond passive vector retrieval" in out
        assert "No head-to-head benchmark exists." in out
        assert "All are preprints." in out

    def test_an_unfilled_optional_column_is_dropped(self):
        """An Evidence column of em-dashes tells the composer there is a column
        worth rendering when there is not; evidence is optional in the schema."""
        out = render_research_envelope(
            _envelope(
                findings=[
                    {"claim": "c", "source": "https://example.com", "confidence": 0.7}
                ]
            )
        )
        assert "Evidence" not in out

    def test_the_rendered_form_reaches_the_composer_when_asked(self):
        """Composition is intent-driven: a rendered envelope composes when the
        request asks for a rich surface, and a plain table request stays
        markdown (tables render fine in chat; the surface only duplicated)."""
        from src.services.a2ui.compose import wants_rich_surface

        rendered = render_research_envelope(_envelope())
        assert (
            wants_rich_surface(
                rendered, "visualize the innovations and their citations"
            )
            is True
        )
        assert (
            wants_rich_surface(
                rendered, "give me a table with the innovation and the citation link"
            )
            is False
        )


class TestItLeavesEverythingElseAlone:
    @pytest.mark.parametrize(
        "text",
        [
            "just a prose answer",
            "",
            "   ",
            "[1, 2, 3]",
            '{"not": "an envelope"}',
            '{"summary": "no findings key"}',
            '{"findings": []}',  # no summary
            "{broken json",
        ],
    )
    def test_a_non_envelope_returns_none(self, text):
        assert render_research_envelope(text) is None

    def test_an_empty_envelope_returns_none(self):
        """Rather than replacing the answer with an empty string."""
        assert render_research_envelope('{"summary": "", "findings": []}') is None


class TestDegradedAnswers:
    """The degrade paths append a note AFTER the JSON; those answers are the
    ones a reader most needs rendered, and the note must stay visible."""

    def test_trailing_json_prose_still_parses(self):
        raw = _envelope() + "\n\n> ⚠️ Truncated: exceeded the execution budget."
        out = render_research_envelope(raw)
        assert out is not None
        assert "A-MEM links memory notes" in out

    def test_the_degrade_note_is_preserved(self):
        raw = _envelope() + "\n\n> ⚠️ Unverified: the source was unavailable."
        out = render_research_envelope(raw)
        assert "⚠️ Unverified" in out


class TestCellSafety:
    def test_a_pipe_in_content_cannot_break_the_table(self):
        out = render_research_envelope(
            _envelope(
                findings=[
                    {"claim": "a | b | c", "source": "https://x.com", "confidence": 1}
                ]
            )
        )
        assert "\\|" in out

    def test_a_newline_in_content_cannot_break_the_row(self):
        out = render_research_envelope(
            _envelope(
                findings=[
                    {
                        "claim": "line one\nline two",
                        "source": "https://x.com",
                        "confidence": 1,
                    }
                ]
            )
        )
        assert "line one line two" in out

    def test_a_non_dict_finding_is_skipped(self):
        out = render_research_envelope(
            _envelope(
                findings=[
                    "oops",
                    {"claim": "ok", "source": "https://x.com", "confidence": 0.5},
                ]
            )
        )
        assert "ok" in out


class TestWrapResultWiring:
    """``wrap_result_with_surface`` must never hand raw envelope JSON onward.

    Both exits matter: the composed envelope carries the rendered text, and the
    no-surface exit returns the rendered markdown instead of ``result`` — that
    second path is the one that put JSON on screen.
    """

    def _run(self, monkeypatch, surface):
        import asyncio

        from src.services.a2ui import runner as R

        captured = {}

        async def _fake_compose(text, **kw):
            captured["text"] = text
            return surface

        monkeypatch.setattr(R, "compose_surface", _fake_compose)
        result = asyncio.run(R.wrap_result_with_surface(_envelope(), config={}))
        return result, captured["text"]

    def test_the_composer_receives_markdown_not_json(self, monkeypatch):
        _result, seen = self._run(monkeypatch, None)
        assert not seen.lstrip().startswith("{")
        assert "| Finding |" in seen

    def test_without_a_surface_the_fallback_is_markdown(self, monkeypatch):
        result, _seen = self._run(monkeypatch, None)
        assert isinstance(result, str)
        assert "| Finding |" in result
        assert not result.lstrip().startswith("{")

    def test_with_a_surface_the_envelope_text_is_markdown(self, monkeypatch):
        surface = {"surfaceKind": "document", "components": [{"component": "Table"}]}
        result, _seen = self._run(monkeypatch, surface)
        assert result["a2ui"] is surface
        assert "| Finding |" in result["text"]

    def test_a_plain_answer_is_returned_untouched(self, monkeypatch):
        import asyncio

        from src.services.a2ui import runner as R

        async def _no_surface(text, **kw):
            return None

        monkeypatch.setattr(R, "compose_surface", _no_surface)
        original = "a plain prose answer"
        out = asyncio.run(R.wrap_result_with_surface(original, config={}))
        assert out is original
