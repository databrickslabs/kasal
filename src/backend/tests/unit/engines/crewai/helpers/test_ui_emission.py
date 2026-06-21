"""Unit tests for the modular Predefined UI emission helper."""

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.engines.crewai.helpers.ui_emission import (
    _build_directives_block,
    _build_theme_block,
    _infer_deliverable,
    _palette_str,
    apply_ui_emission,
    build_ui_instruction,
)


class _FakeSessionCM:
    """Stand-in async context manager for request_scoped_session()."""

    async def __aenter__(self):
        return AsyncMock()

    async def __aexit__(self, *exc):
        return False


def _patch_config(config):
    """Patch request_scoped_session + UIConfigService so get_config returns `config`."""
    rss = patch("src.db.session.request_scoped_session", return_value=_FakeSessionCM())
    svc_patch = patch("src.services.ui_config_service.UIConfigService")
    return rss, svc_patch


# --- build_ui_instruction -------------------------------------------------


def test_build_ui_instruction_without_accent():
    s = build_ui_instruction()
    assert "OUTPUT FORMAT" in s
    assert "JSON" in s
    assert "accent color" not in s


def test_build_ui_instruction_with_accent():
    s = build_ui_instruction("#5aa2ff")
    assert "#5aa2ff" in s
    assert "accent color" in s


def test_build_ui_instruction_with_themes_emits_palette_block_and_suppresses_accent():
    themes = {
        "default": {"accent": "#2272B4", "background": "#FFFFFF", "font": "sans"},
        "dashboard": {"accent": "#38BDF8", "background": "#0F172A"},
    }
    s = build_ui_instruction("#5aa2ff", themes)
    assert "THEME / BRANDING" in s
    assert "Default (any deliverable" in s
    assert "Dashboard / metrics" in s
    assert "#38BDF8" in s
    # When per-type themes drive the palette, the bare legacy accent hint is dropped.
    assert "accent color where relevant" not in s


def test_build_ui_instruction_with_directives_emits_settings_block():
    directives = {
        "presentation": "Aim for about 8 slides; at most 4 bullet points per slide.",
        "default": "",  # default is never emitted as a deliverable line
    }
    s = build_ui_instruction(None, None, directives)
    assert "DELIVERABLE SETTINGS" in s
    assert "Presentation / slides" in s
    assert "about 8 slides" in s
    assert "Default (any deliverable" not in s  # default key skipped


def test_palette_str_only_includes_present_keys():
    assert (
        _palette_str({"accent": "#111", "font": "serif"}) == "accent #111, font serif"
    )
    assert _palette_str({}) == ""


def test_theme_and_directive_blocks_ignore_empty_or_non_dict():
    assert _build_theme_block({}) == []
    assert _build_theme_block("nope") == []
    assert _build_theme_block({"dashboard": {}}) == []  # no usable palette tokens
    assert _build_directives_block({}) == []
    assert _build_directives_block("nope") == []
    assert (
        _build_directives_block({"default": "x", "quiz": "  "}) == []
    )  # only default/blank


# --- apply_ui_emission: no-ops --------------------------------------------


@pytest.mark.asyncio
async def test_apply_ui_emission_noop_without_tasks():
    await apply_ui_emission([], "g1")  # must not raise


@pytest.mark.asyncio
async def test_apply_ui_emission_noop_without_group():
    tasks = [{"description": "orig"}]
    await apply_ui_emission(tasks, None)
    assert tasks[0]["description"] == "orig"


@pytest.mark.asyncio
async def test_apply_ui_emission_noop_when_disabled():
    tasks = [{"description": "orig"}]
    rss, svc_patch = _patch_config(None)
    with rss, svc_patch as Svc:
        inst = MagicMock()
        inst.get_config = AsyncMock(return_value=MagicMock(enabled=False))
        Svc.return_value = inst
        await apply_ui_emission(tasks, "g1")
    assert tasks[0]["description"] == "orig"


# --- apply_ui_emission: enabled -------------------------------------------


@pytest.mark.asyncio
async def test_apply_ui_emission_appends_to_last_task_with_accent():
    tasks = [{"description": "first"}, {"description": "last"}]
    rss, svc_patch = _patch_config(None)
    with rss, svc_patch as Svc:
        inst = MagicMock()
        inst.get_config = AsyncMock(
            return_value=MagicMock(enabled=True, style_json='{"accent":"#123456"}')
        )
        Svc.return_value = inst
        await apply_ui_emission(tasks, "g1")
    assert tasks[0]["description"] == "first"  # earlier tasks untouched
    assert tasks[1]["description"].startswith("last")
    assert "OUTPUT FORMAT" in tasks[1]["description"]
    assert "#123456" in tasks[1]["description"]


@pytest.mark.asyncio
async def test_apply_ui_emission_appends_themes_and_directives():
    style = {
        "accent": "#2272B4",
        "density": "comfortable",
        "themes": {
            "default": {"accent": "#2272B4", "background": "#FFFFFF"},
            "genie": {"accent": "#38BDF8"},
        },
        "directives": {
            "genie": "Add a chart when it aids understanding; show at most 20 rows.",
            "default": "",
        },
    }
    tasks = [{"description": "last"}]
    rss, svc_patch = _patch_config(None)
    with rss, svc_patch as Svc:
        inst = MagicMock()
        inst.get_config = AsyncMock(
            return_value=MagicMock(enabled=True, style_json=json.dumps(style))
        )
        Svc.return_value = inst
        await apply_ui_emission(tasks, "g1")
    desc = tasks[0]["description"]
    assert "THEME / BRANDING" in desc
    assert "DELIVERABLE SETTINGS" in desc
    assert "#38BDF8" in desc
    assert "at most 20 rows" in desc
    # expected_output overwritten to the single-UI-document target.
    assert tasks[0]["expected_output"].startswith("A single JSON")


@pytest.mark.asyncio
async def test_apply_ui_emission_narrows_theme_to_inferred_deliverable():
    """When the final task names a deliverable, only that deliverable's theme/
    directive guidance is emitted (the other types are dropped to save tokens)."""
    style = {
        "themes": {
            "default": {"accent": "#222222"},
            "dashboard": {"accent": "#DA5500"},
            "presentation": {"accent": "#9911FF"},
        },
        "directives": {
            "dashboard": "Lay out KPI tiles three per row.",
            "presentation": "Aim for about eight slides.",
        },
    }
    tasks = [{"description": "Create a metrics dashboard with KPI tiles."}]
    rss, svc_patch = _patch_config(None)
    with rss, svc_patch as Svc:
        inst = MagicMock()
        inst.get_config = AsyncMock(
            return_value=MagicMock(enabled=True, style_json=json.dumps(style))
        )
        Svc.return_value = inst
        await apply_ui_emission(tasks, "g1")
    desc = tasks[0]["description"]
    # dashboard (inferred) + default fallback palette kept; presentation dropped.
    assert "#DA5500" in desc
    assert "#222222" in desc
    assert "#9911FF" not in desc
    # only the dashboard directive survives.
    assert "KPI tiles three per row" in desc
    assert "about eight slides" not in desc


@pytest.mark.asyncio
async def test_apply_ui_emission_enabled_with_no_style_json():
    tasks = [{"description": "only"}]
    rss, svc_patch = _patch_config(None)
    with rss, svc_patch as Svc:
        inst = MagicMock()
        inst.get_config = AsyncMock(
            return_value=MagicMock(enabled=True, style_json=None)
        )
        Svc.return_value = inst
        await apply_ui_emission(tasks, "g1")
    assert "OUTPUT FORMAT" in tasks[0]["description"]


@pytest.mark.asyncio
async def test_apply_ui_emission_tolerates_invalid_style_json():
    tasks = [{"description": "only"}]
    rss, svc_patch = _patch_config(None)
    with rss, svc_patch as Svc:
        inst = MagicMock()
        inst.get_config = AsyncMock(
            return_value=MagicMock(enabled=True, style_json="not json")
        )
        Svc.return_value = inst
        await apply_ui_emission(tasks, "g1")
    # Bad style JSON -> accent ignored, instruction still appended.
    assert "OUTPUT FORMAT" in tasks[0]["description"]


@pytest.mark.asyncio
async def test_apply_ui_emission_swallows_errors():
    """UI formatting must never break a run — any error is logged and ignored."""
    tasks = [{"description": "orig"}]
    with patch(
        "src.db.session.request_scoped_session", side_effect=RuntimeError("boom")
    ):
        await apply_ui_emission(tasks, "g1")  # must not raise
    assert tasks[0]["description"] == "orig"


# --- deliverable-specific narrowing (context-size optimization) -----------

_ALL_THEMES = {
    k: {"accent": "#111", "background": "#fff", "font": "sans"}
    for k in [
        "default",
        "dashboard",
        "presentation",
        "genie",
        "mindmap",
        "album",
        "quiz",
        "report",
    ]
}
_ALL_DIRECTIVES = {
    k: f"settings for {k}"
    for k in [
        "dashboard",
        "presentation",
        "genie",
        "mindmap",
        "album",
        "quiz",
        "report",
    ]
}


def test_theme_block_emits_all_when_no_deliverable():
    lines = _build_theme_block(_ALL_THEMES)
    # one palette line per configured deliverable (all 8)
    assert sum(1 for ln in lines if ln.startswith("- ")) == 8


def test_theme_block_narrows_to_deliverable_plus_default():
    lines = _build_theme_block(_ALL_THEMES, deliverable="dashboard")
    palette_lines = [ln for ln in lines if ln.startswith("- ")]
    # only the chosen deliverable + the default fallback palette
    assert len(palette_lines) == 2
    assert any("Dashboard" in ln for ln in palette_lines)
    assert any("Default" in ln for ln in palette_lines)
    assert not any("Presentation" in ln for ln in palette_lines)


def test_theme_block_lists_deliverable_palette_before_default():
    """Regression: agents copy whichever palette is listed first, so the
    deliverable's own palette must come BEFORE the Default fallback (a deck was
    rendering white because the Default palette led the list)."""
    lines = _build_theme_block(_ALL_THEMES, deliverable="presentation")
    palette_lines = [ln for ln in lines if ln.startswith("- ")]
    assert "Presentation" in palette_lines[0]
    assert "Default" in palette_lines[1]
    # deliverable == "default" emits just the one palette, not a duplicate
    only_default = [
        ln
        for ln in _build_theme_block(_ALL_THEMES, deliverable="default")
        if ln.startswith("- ")
    ]
    assert len(only_default) == 1
    assert "Default" in only_default[0]


def test_theme_block_example_does_not_anchor_a_concrete_palette():
    """Regression: the example JSON used literal colors (background #ffffff),
    which agents copied verbatim instead of the configured palette. The example
    must use placeholders and demand the MATCHING palette."""
    block = "\n".join(_build_theme_block(_ALL_THEMES, deliverable="presentation"))
    assert '"background": "#ffffff"' not in block
    assert '"background": "<background>"' in block
    assert "MATCHES the deliverable" in block


def test_directives_block_narrows_to_single_deliverable():
    all_lines = [
        ln for ln in _build_directives_block(_ALL_DIRECTIVES) if ln.startswith("- ")
    ]
    assert len(all_lines) == 7
    one = [
        ln
        for ln in _build_directives_block(_ALL_DIRECTIVES, deliverable="quiz")
        if ln.startswith("- ")
    ]
    assert len(one) == 1
    assert "Quiz" in one[0]


def test_build_ui_instruction_deliverable_is_smaller():
    full = build_ui_instruction(themes=_ALL_THEMES, directives=_ALL_DIRECTIVES)
    narrowed = build_ui_instruction(
        themes=_ALL_THEMES, directives=_ALL_DIRECTIVES, deliverable="dashboard"
    )
    assert len(narrowed) < len(full)


@pytest.mark.parametrize(
    "text,expected",
    [
        ("Create a metrics dashboard with KPI tiles", "dashboard"),
        ("Build a slide presentation deck", "presentation"),
        ("an interactive quiz that tracks score", "quiz"),
        ("a concept map / mindmap of the topic", "mindmap"),
        ("a data answer from Genie", "genie"),
        # Multi-keyword resolves by specificity order (dashboard precedes
        # report in _DELIVERABLE_KEYWORDS) — the old None fallback re-sent
        # all eight theme/directive blocks every iteration (LLM-022).
        ("a dashboard AND a report", "dashboard"),
        ("just analyze the numbers", None),  # no match → safe fallback
        ("", None),
    ],
)
def test_infer_deliverable(text, expected):
    assert _infer_deliverable(text) == expected


class TestInferDeliverableMultiKeyword:
    """Regression (LLM-022): multi-keyword final tasks used to return None and
    re-send ALL eight theme/directive blocks (~1.5-1.9k tokens) every agent
    iteration. The ordered keyword list now decides by specificity."""

    def test_multi_keyword_resolves_by_specificity_order(self):
        from src.engines.crewai.helpers.ui_emission import _infer_deliverable

        # presentation + kpi(dashboard) both match — presentation is earlier
        # in the specificity order.
        text = "Create a presentation from the data, include KPI charts per slide"
        assert _infer_deliverable(text) == "presentation"

    def test_single_keyword_unchanged(self):
        from src.engines.crewai.helpers.ui_emission import _infer_deliverable

        assert _infer_deliverable("build an interactive quiz about Rome") == "quiz"

    def test_no_keyword_falls_back_to_none(self):
        from src.engines.crewai.helpers.ui_emission import _infer_deliverable

        assert _infer_deliverable("summarize the findings for the team") is None
        assert _infer_deliverable("") is None

    def test_narrowed_instruction_is_materially_smaller(self):
        from src.engines.crewai.helpers.ui_emission import build_ui_instruction

        themes = {
            k: {"accent": "#2563eb", "background": "#fff"}
            for k in (
                "default",
                "presentation",
                "dashboard",
                "quiz",
                "report",
                "album",
                "mindmap",
                "genie",
            )
        }
        directives = {
            k: f"Detailed {k} behavior settings. " * 8
            for k in (
                "presentation",
                "dashboard",
                "quiz",
                "report",
                "album",
                "mindmap",
                "genie",
            )
        }

        full = build_ui_instruction(
            themes=themes, directives=directives, deliverable=None
        )
        narrowed = build_ui_instruction(
            themes=themes, directives=directives, deliverable="presentation"
        )

        assert len(narrowed) < len(full) - 1000  # at least ~250 tokens saved


# --- catalog slicing per deliverable (the sustainability fix) -------------


class TestCatalogSlicing:
    """The component catalog (shapes + rules) is sliced per deliverable just like
    the theme/directive blocks, so the prompt stays ~constant as new artifact
    types are added — a new type costs tokens only on its OWN runs."""

    def test_core_is_always_present(self):
        # The output contract + universal primitives appear regardless of type.
        for d in (None, "presentation", "dashboard", "quiz", "flashcards"):
            s = build_ui_instruction(deliverable=d)
            assert "OUTPUT FORMAT" in s
            assert 'MUST use the key "component"' in s
            assert "Text (text, variant" in s  # a universal primitive line

    def test_known_deliverable_emits_only_its_own_slice(self):
        # A quiz instruction carries the Quiz shape but NOT presentation/mindmap/
        # album/flashcards guidance — those slices are dropped to save tokens.
        s = build_ui_instruction(deliverable="quiz")
        assert "BUILD A QUIZ" in s
        assert "ONE Quiz component" in s
        assert "SLIDE DESIGN RULES" not in s  # presentation-only
        assert "BUILD A MINDMAP" not in s
        assert "Flashcards (title?, cards" not in s

    def test_presentation_slice_carries_slide_rules(self):
        s = build_ui_instruction(deliverable="presentation")
        assert "BUILD A PRESENTATION" in s
        assert "SLIDE DESIGN RULES" in s
        assert "BUILD A DASHBOARD" not in s

    def test_flashcards_slice_describes_flip_card(self):
        s = build_ui_instruction(deliverable="flashcards")
        assert "BUILD FLASHCARDS" in s
        # The Flashcards component signature is described; assert the stable
        # parts (the option list may grow, e.g. layout?: grid|carousel).
        assert "Flashcards (title?" in s
        assert "cards:[{ front, back }]" in s
        assert "FLIPS to reveal" in s
        # not conflated with the quiz slice
        assert "ONE Quiz component" not in s

    def test_unknown_deliverable_offers_the_full_menu(self):
        # When nothing is inferred, every deliverable's slice is included behind
        # the chooser header so the agent can still pick the right one.
        s = build_ui_instruction(deliverable=None)
        assert "MATCH THE REQUESTED DELIVERABLE" in s
        assert "BUILD A PRESENTATION" in s
        assert "BUILD A DASHBOARD" in s
        assert "BUILD FLASHCARDS" in s
        assert "BUILD A MINDMAP" in s

    def test_known_slice_is_smaller_than_the_full_menu(self):
        full = build_ui_instruction(deliverable=None)
        one = build_ui_instruction(deliverable="flashcards")
        assert len(one) < len(full)


@pytest.mark.parametrize(
    "text,expected",
    [
        ("make an anki deck about spark", "flashcards"),
        ("create flashcards for the exam", "flashcards"),
        ("a flash card set on history", "flashcards"),
        ("study with spaced repetition cards", "flashcards"),
        # flashcards is more specific than a co-mentioned quiz
        ("anki flashcards, not a quiz", "flashcards"),
    ],
)
def test_infer_deliverable_flashcards(text, expected):
    assert _infer_deliverable(text) == expected


# --- quiz question count: defer to the user's request, don't hard-cap ------


def test_quiz_default_directive_defers_to_requested_count():
    """Regression: the quiz default directive must NOT hard-cap the count (it
    used to say 'write exactly 5 questions'), so a chat prompt asking for 50 or
    100 questions is honored instead of being limited to 5."""
    from src.engines.crewai.helpers.ui_emission import _DEFAULT_DIRECTIVES

    quiz = _DEFAULT_DIRECTIVES["quiz"]
    assert "exactly 5 questions" not in quiz
    # It instructs the agent to use the count from the request.
    assert "request" in quiz.lower()
    # A sensible fallback only when no count is given.
    assert "default" in quiz.lower()


@pytest.mark.asyncio
async def test_apply_ui_emission_quiz_directive_not_capped_at_five():
    """A quiz task on an un-customized workspace (no configured directives) gets
    the default quiz directive, which defers to the requested count rather than
    forcing exactly 5."""
    tasks = [{"description": "Build an interactive quiz with 50 questions about Rome",
              "expected_output": "an interactive quiz"}]
    rss, svc_patch = _patch_config(None)
    with rss, svc_patch as Svc:
        inst = MagicMock()
        # Enabled, but no style/directives configured → default directive applies.
        inst.get_config = AsyncMock(
            return_value=MagicMock(enabled=True, style_json=json.dumps({}))
        )
        Svc.return_value = inst
        await apply_ui_emission(tasks, "g1")

    desc = tasks[0]["description"]
    assert "exactly 5 questions" not in desc
    assert "as many questions as the request asks for" in desc
