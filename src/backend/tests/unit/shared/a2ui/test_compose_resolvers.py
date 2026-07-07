"""Unit tests for the shared, dict-based UIConfig resolvers in the portable A2UI
composer (``src.shared.a2ui.compose``).

These resolvers are the SINGLE source of truth used by BOTH the live runner
(via thin adapters in a2ui_runner) AND the exporter/exported app — so the live
chat and a deployed export resolve a workspace's catalog + directives identically.
The dict-based API (not pydantic) is what keeps the module stdlib-only and
bundleable into a self-contained export.
"""

import json

from src.shared.a2ui.compose import (
    MINIMAL_COMPONENTS,
    compose_a2ui,
    guidance_for,
    infer_deliverable,
    load_catalog,
    plan_presentation_outline,
    presentation_design_lint,
    presentation_needs_body,
    quiz_needs_work,
    resolve_catalog,
    resolve_directives,
    resolve_themes,
)

CATALOG = load_catalog()
FULL = set(CATALOG["components"])


# --- presentation_needs_body (hollow-deck detection) -----------------------
def _deck(*slides):
    comps = [
        {"id": "deck", "component": "SlideDeck", "children": [s["id"] for s in slides]}
    ]
    for s in slides:
        comps.append(s)
        comps.extend(s.pop("_children", []))
    return {"surfaceKind": "presentation", "root": "deck", "components": comps}


def test_presentation_needs_body_flags_title_only_content_slides():
    hollow = _deck(
        {"id": "s1", "component": "Slide", "variant": "content", "title": "A"},
        {"id": "s2", "component": "Slide", "variant": "content", "title": "B"},
    )
    assert presentation_needs_body(hollow) is True


def test_presentation_needs_body_passes_slides_with_real_bodies():
    filled = _deck(
        {
            "id": "s1",
            "component": "Slide",
            "variant": "content",
            "title": "A",
            "children": ["t1"],
            "_children": [{"id": "t1", "component": "Text", "text": "A real point."}],
        },
        {
            "id": "s2",
            "component": "Slide",
            "variant": "content",
            "title": "B",
            "children": ["c1"],
            "_children": [
                {
                    "id": "c1",
                    "component": "Chart",
                    "chartType": "bar",
                    "xKey": "x",
                    "yKeys": ["y"],
                    "data": {"path": "/d"},
                }
            ],
        },
    )
    assert presentation_needs_body(filled) is False


def test_presentation_needs_body_ignores_titleonly_and_nonpresentations():
    # title/section slides are body-less by design and must not trip the check.
    only_title_slides = _deck(
        {"id": "s1", "component": "Slide", "variant": "title", "title": "Welcome"},
    )
    assert presentation_needs_body(only_title_slides) is False
    assert (
        presentation_needs_body({"surfaceKind": "document", "components": []}) is False
    )


# --- compose_a2ui retries on hollow decks, then falls back to markdown ------
def _seq_llm(*replies):
    it = iter(replies)
    return lambda messages: next(it)


def test_compose_retries_past_a_hollow_deck_to_a_filled_one(monkeypatch):
    monkeypatch.setenv("A2UI_PRESENTATION_OUTLINE", "0")
    hollow = json.dumps(
        _deck(
            {"id": "s1", "component": "Slide", "variant": "content", "title": "Empty"}
        )
    )
    filled = json.dumps(
        _deck(
            {
                "id": "s1",
                "component": "Slide",
                "variant": "content",
                "title": "Full",
                "children": ["t1"],
                "_children": [
                    {"id": "t1", "component": "Text", "text": "A genuine bullet point."}
                ],
            }
        )
    )
    out = compose_a2ui(
        "some answer text",
        query="make a slide deck about x",
        llm_call=_seq_llm(hollow, filled),
        catalog=CATALOG,
    )
    assert out["surfaceKind"] == "presentation"
    assert any(c.get("component") == "Text" for c in out["components"])


def test_compose_falls_back_to_markdown_when_deck_stays_hollow(monkeypatch):
    monkeypatch.setenv("A2UI_PRESENTATION_OUTLINE", "0")
    hollow = json.dumps(
        _deck(
            {"id": "s1", "component": "Slide", "variant": "content", "title": "Empty"}
        )
    )
    out = compose_a2ui(
        "the full answer body",
        query="make a slide deck about x",
        llm_call=_seq_llm(hollow, hollow),
        catalog=CATALOG,
        retries=2,
    )
    # Hollow after every retry → readable markdown document instead of empty slides.
    assert out["surfaceKind"] == "conversation"
    assert out["dataModel"]["md"] == "the full answer body"


def test_presentation_needs_body_covers_new_body_variants():
    # two-column / visual / agenda slides are body-bearing too: a deck whose
    # two-column slides have no children is just as hollow as empty content ones.
    hollow = _deck(
        {"id": "s1", "component": "Slide", "variant": "two-column", "title": "A"},
        {"id": "s2", "component": "Slide", "variant": "agenda", "title": "B"},
    )
    assert presentation_needs_body(hollow) is True


# --- presentation_design_lint (deck visual-density critique) ----------------
def _text_slide(i):
    return {
        "id": f"s{i}",
        "component": "Slide",
        "variant": "content",
        "title": f"Slide {i}",
        "children": [f"t{i}"],
        "_children": [{"id": f"t{i}", "component": "Text", "text": "A real point."}],
    }


def _diagram_slide(i):
    return {
        "id": f"s{i}",
        "component": "Slide",
        "variant": "visual",
        "title": f"Slide {i}",
        "children": [f"d{i}"],
        "_children": [
            {
                "id": f"d{i}",
                "component": "Diagram",
                "archetype": "process",
                "items": [{"label": "One"}, {"label": "Two"}],
            }
        ],
    }


def test_design_lint_flags_a_text_only_deck():
    flat = _deck(*[_text_slide(i) for i in range(1, 8)])
    findings = presentation_design_lint(flat)
    assert findings
    assert any("no slide carries any visual" in f for f in findings)
    assert any("consecutive text-only" in f for f in findings)


def test_design_lint_passes_a_deck_with_enough_visuals():
    slides = [
        _text_slide(1),
        _text_slide(2),
        _diagram_slide(3),
        _text_slide(4),
        _text_slide(5),
        _diagram_slide(6),
    ]
    assert presentation_design_lint(_deck(*slides)) == []


def test_design_lint_ignores_short_decks_and_non_presentations():
    short = _deck(*[_text_slide(i) for i in range(1, 4)])
    assert presentation_design_lint(short) == []
    assert presentation_design_lint({"surfaceKind": "document", "components": []}) == []
    assert presentation_design_lint(None) == []


def test_compose_design_retry_upgrades_a_flat_deck(monkeypatch):
    monkeypatch.setenv("A2UI_PRESENTATION_OUTLINE", "0")
    flat = json.dumps(_deck(*[_text_slide(i) for i in range(1, 8)]))
    visual = json.dumps(
        _deck(
            *[
                (_diagram_slide(i) if i % 3 == 0 else _text_slide(i))
                for i in range(1, 8)
            ]
        )
    )
    out = compose_a2ui(
        "answer",
        query="make a slide deck about x",
        llm_call=_seq_llm(flat, visual),
        catalog=CATALOG,
    )
    assert any(c.get("component") == "Diagram" for c in out["components"])


def test_compose_keeps_flat_deck_when_design_retry_fails(monkeypatch):
    # A valid-but-flat deck is the floor: if the design retry returns garbage,
    # ship the flat deck rather than falling back to markdown.
    monkeypatch.setenv("A2UI_PRESENTATION_OUTLINE", "0")
    flat = json.dumps(_deck(*[_text_slide(i) for i in range(1, 8)]))
    out = compose_a2ui(
        "answer",
        query="make a slide deck about x",
        llm_call=_seq_llm(flat, "not json at all"),
        catalog=CATALOG,
        retries=2,
    )
    assert out["surfaceKind"] == "presentation"


# --- plan_presentation_outline (two-stage presentation compose) --------------
def test_plan_outline_parses_clamps_and_rejects_weak_plans():
    plan = {
        "slides": [
            {"title": f"S{i}", "variant": "content", "visual": "none", "focus": "x"}
            for i in range(30)
        ]
    }
    out = plan_presentation_outline(
        "text", "make a deck", "", _seq_llm(json.dumps(plan))
    )
    assert out is not None and len(out) == 24  # clamped
    assert out[0]["title"] == "S0"
    # Under 3 slides → weaker than no plan.
    assert (
        plan_presentation_outline(
            "text", "q", "", _seq_llm(json.dumps({"slides": [{"title": "only"}]}))
        )
        is None
    )
    # Garbage / raising llm_call → None, never raises.
    assert plan_presentation_outline("text", "q", "", _seq_llm("not json")) is None
    assert plan_presentation_outline("text", "q", "", _seq_llm()) is None


def test_compose_feeds_slide_plan_to_the_composer():
    plan = json.dumps(
        {
            "slides": [
                {"title": "Intro", "variant": "title", "visual": "none"},
                {"title": "Steps", "variant": "visual", "visual": "diagram:process"},
                {"title": "Close", "variant": "quote", "visual": "none"},
            ]
        }
    )
    deck = json.dumps(
        _deck(
            {
                "id": "s1",
                "component": "Slide",
                "variant": "content",
                "title": "Full",
                "children": ["t1"],
                "_children": [
                    {"id": "t1", "component": "Text", "text": "A genuine point."}
                ],
            }
        )
    )
    seen = []

    def llm(messages):
        seen.append(messages)
        return plan if len(seen) == 1 else deck

    out = compose_a2ui(
        "answer body",
        query="make a slide deck about x",
        llm_call=llm,
        catalog=CATALOG,
    )
    assert out["surfaceKind"] == "presentation"
    # First call plans the outline; the second (composer) call gets the plan
    # appended to the user content.
    assert len(seen) == 2
    assert "OUTLINE" in seen[0][0]["content"]
    assert "[SLIDE PLAN" in seen[1][1]["content"]
    assert "diagram:process" in seen[1][1]["content"]


def test_outline_skipped_for_non_presentations():
    dash = json.dumps(
        {
            "surfaceKind": "dashboard",
            "root": "g",
            "components": [
                {"id": "g", "component": "Grid", "columns": 2, "children": ["k"]},
                {"id": "k", "component": "KeyValue", "label": "A", "value": "1"},
            ],
        }
    )
    calls = []

    def llm(messages):
        calls.append(messages)
        return dash

    out = compose_a2ui(
        "answer", query="build a KPI dashboard", llm_call=llm, catalog=CATALOG
    )
    assert out["surfaceKind"] == "dashboard"
    assert len(calls) == 1  # no outline pre-pass


# --- catalog: new visual vocabulary ------------------------------------------
def test_catalog_declares_diagram_and_new_chart_and_slide_variants():
    comps = CATALOG["components"]
    assert "Diagram" in comps
    archetypes = set(comps["Diagram"]["props"]["archetype"]["values"])
    assert {
        "process",
        "timeline",
        "cycle",
        "funnel",
        "pyramid",
        "comparison",
        "matrix2x2",
        "hierarchy",
    } <= archetypes
    chart_types = set(comps["Chart"]["props"]["chartType"]["values"])
    assert {"bar", "line", "pie", "area", "scatter", "radar"} <= chart_types
    variants = set(comps["Slide"]["props"]["variant"]["enum"])
    assert {"two-column", "visual", "agenda"} <= variants
    assert "icon" in comps["KeyValue"]["props"]


def test_infer_deliverable_diagram_keywords():
    assert infer_deliverable("draw a flowchart of onboarding") == "diagram"
    assert infer_deliverable("show the conversion funnel") == "diagram"
    assert infer_deliverable("an org chart for the team") == "diagram"
    # Specific diagram kinds keep their own deliverables.
    assert infer_deliverable("a sequence diagram of login") == "sequence"
    assert infer_deliverable("a network diagram of services") == "graph"
    # Presentations with diagram words stay presentations (keyword order).
    assert infer_deliverable("a presentation with a timeline") == "presentation"


# --- quiz_needs_work (quiz quality guard) ----------------------------------
def _quiz(questions, bound=True):
    quiz = {"id": "q", "component": "Quiz", "title": "T"}
    payload = {"surfaceKind": "quiz", "root": "q", "components": [quiz]}
    if bound:
        quiz["questions"] = {"path": "/questions"}
        payload["dataModel"] = {"questions": questions}
    else:
        quiz["questions"] = questions
    return payload


def _q(stem, opts, ans, expl="because."):
    return {"question": stem, "options": opts, "answer": ans, "explanation": expl}


_GOOD_QS = [
    _q("What is A?", ["a1", "a2", "a3", "a4"], 0),
    _q("What is B?", ["b1", "b2", "b3", "b4"], 2),
    _q("What is C?", ["c1", "c2", "c3", "c4"], 1),
]


def test_quiz_needs_work_passes_a_real_quiz_via_binding():
    assert quiz_needs_work(_quiz(_GOOD_QS)) is False
    # inline (unbound) questions are accepted too
    assert quiz_needs_work(_quiz(_GOOD_QS, bound=False)) is False


def test_quiz_needs_work_flags_too_few_or_empty():
    assert quiz_needs_work(_quiz([])) is True  # no questions
    assert quiz_needs_work(_quiz(_GOOD_QS[:2])) is True  # only 2 real questions


def test_quiz_needs_work_flags_malformed_items():
    malformed = [
        _q("ok", ["a", "b", "c", "d"], 0),
        _q("dup options", ["x", "x", "y", "z"], 1),  # not distinct
        _q("bad index", ["a", "b", "c", "d"], 9),  # out of range
        _q("", ["a", "b", "c", "d"], 0),  # empty stem
    ]
    # 1 valid of 4 → under the floor of 3 → needs work
    assert quiz_needs_work(_quiz(malformed)) is True


def test_quiz_needs_work_accepts_string_answer_index():
    qs = [_q("A", ["a", "b", "c", "d"], "0"), *(_GOOD_QS[1:])]
    assert quiz_needs_work(_quiz(qs)) is False


def test_quiz_needs_work_ignores_non_quiz():
    assert quiz_needs_work({"surfaceKind": "document", "components": []}) is False


def test_compose_retries_past_a_weak_quiz_to_a_real_one():
    weak = json.dumps(_quiz([_q("only one", ["a", "b", "c", "d"], 0)]))
    real = json.dumps(_quiz(_GOOD_QS))
    out = compose_a2ui(
        "make me a quiz",
        query="quiz me on this topic",
        llm_call=_seq_llm(weak, real),
        catalog=CATALOG,
    )
    assert out["surfaceKind"] == "quiz"
    assert len(out["dataModel"]["questions"]) == 3


# --- infer_deliverable -----------------------------------------------------
def test_infer_deliverable_first_keyword_wins():
    assert infer_deliverable("make a slide deck about Q3") == "presentation"
    assert infer_deliverable("build a KPI dashboard") == "dashboard"
    assert infer_deliverable("quiz me on history") == "quiz"
    assert infer_deliverable("a mind map of the org") == "mindmap"
    assert infer_deliverable("just answer in prose") is None
    assert infer_deliverable("") is None


# --- resolve_catalog (dict-based) ------------------------------------------
def test_unconfigured_dict_keeps_full_catalog():
    # No id (never-saved workspace) → schema's 'minimal' default must NOT restrict.
    assert (
        set(
            resolve_catalog({"id": None, "catalog_type": "minimal"}, CATALOG)[
                "components"
            ]
        )
        == FULL
    )
    assert set(resolve_catalog({}, CATALOG)["components"]) == FULL


def test_saved_minimal_restricts_to_subset():
    out = resolve_catalog({"id": 7, "catalog_type": "minimal"}, CATALOG)
    assert set(out["components"]) == set(MINIMAL_COMPONENTS) & FULL
    assert "SlideDeck" not in out["components"]


def test_saved_basic_uses_full_catalog():
    assert (
        set(resolve_catalog({"id": 7, "catalog_type": "basic"}, CATALOG)["components"])
        == FULL
    )


def test_custom_catalog_parsed_and_surfacekinds_backfilled():
    out = resolve_catalog(
        {
            "id": 7,
            "catalog_type": "custom",
            "catalog_json": '{"components": {"Text": {"props": {}}}}',
        },
        CATALOG,
    )
    assert set(out["components"]) == {"Text"}
    assert out["surfaceKinds"] == CATALOG["surfaceKinds"]  # backfilled from default


def test_invalid_custom_catalog_falls_back_to_default():
    out = resolve_catalog(
        {"id": 7, "catalog_type": "custom", "catalog_json": "{not valid"}, CATALOG
    )
    assert set(out["components"]) == FULL


# --- resolve_directives + guidance_for -------------------------------------
def test_resolve_directives_from_string_or_dict():
    style = json.dumps(
        {"directives": {"presentation": "8 slides", "default": "be concise"}}
    )
    assert resolve_directives({"style_json": style}) == {
        "presentation": "8 slides",
        "default": "be concise",
    }
    # Already-parsed dict is accepted too.
    assert resolve_directives({"style_json": {"directives": {"quiz": "10 q"}}}) == {
        "quiz": "10 q"
    }
    # Missing / invalid → {}
    assert resolve_directives({"style_json": None}) == {}
    assert resolve_directives({"style_json": "{not valid"}) == {}
    assert resolve_directives({}) == {}


def test_guidance_for_picks_inferred_then_default_then_empty():
    directives = {"presentation": "aim for about 8 slides", "default": "be concise"}
    assert guidance_for(directives, "make a slide deck") == "aim for about 8 slides"
    assert (
        guidance_for(directives, "hello there") == "be concise"
    )  # falls back to default
    assert guidance_for({"quiz": "x"}, "hello there") == ""  # no match, no default
    assert guidance_for({}, "anything") == ""


# --- resolve_themes (drives baking workspace themes into exports) -----------
def test_resolve_themes_from_string_or_dict():
    palette = {"accent": "#2272B4", "background": "#FFFFFF", "text": "#0F172A"}
    style = json.dumps({"themes": {"presentation": palette}})
    assert resolve_themes({"style_json": style}) == {"presentation": palette}
    # Already-parsed dict is accepted too.
    assert resolve_themes({"style_json": {"themes": {"quiz": palette}}}) == {
        "quiz": palette
    }
    # Missing / invalid / no-themes → {}
    assert resolve_themes({"style_json": None}) == {}
    assert resolve_themes({"style_json": "{not valid"}) == {}
    assert resolve_themes({"style_json": json.dumps({"directives": {"x": "y"}})}) == {}
    assert resolve_themes({}) == {}
