"""Unit tests for the shared, dict-based UIConfig resolvers in the portable A2UI
composer (``src.services.a2ui.compose``).

These resolvers are the SINGLE source of truth used by BOTH the live runner
(via thin adapters in a2ui_runner) AND the exporter/exported app — so the live
chat and a deployed export resolve a workspace's catalog + directives identically.
The dict-based API (not pydantic) is what keeps the module stdlib-only and
bundleable into a self-contained export.
"""

import json

from src.services.a2ui.compose import (
    MINIMAL_COMPONENTS,
    a2ui_system_prompt,
    compose_a2ui,
    guidance_for,
    infer_deliverable,
    load_catalog,
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


# --- compose_a2ui retries on hollow decks, then falls back to markdown ------
def _seq_llm(*replies):
    it = iter(replies)
    return lambda messages: next(it)


# --- presentation_design_lint (deck visual-density critique) ----------------
def _text_slide(i, bullets=3):
    """A content slide with `bullets` Text children.

    Defaults to 3 because that is the floor the composer prompt asks for ("3-5
    Text nodes"); a 1-bullet slide is a thin slide the lint deliberately flags,
    so tests about VISUAL density must not accidentally build one.
    """
    ids = [f"t{i}_{b}" for b in range(bullets)]
    return {
        "id": f"s{i}",
        "component": "Slide",
        "variant": "content",
        "title": f"Slide {i}",
        "children": ids,
        "_children": [
            {"id": tid, "component": "Text", "text": f"A real point {b}."}
            for b, tid in enumerate(ids)
        ],
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


# --- plan_presentation_outline (two-stage presentation compose) --------------
# --- catalog: new visual vocabulary ------------------------------------------
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
    # "presentation" is no longer a deliverable — decks left A2UI for the chat
    # HTML path, so a deck request resolves to no deliverable here.
    assert infer_deliverable("make a slide deck about Q3") is None
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


def test_custom_catalog_that_is_a_surface_not_a_catalog_falls_back():
    """A saved SURFACE pasted into ``catalog_json`` has ``components`` as a LIST of
    component INSTANCES — not the dict of name->spec a catalog is.

    It parses as a dict with a truthy ``components``, so the old guard accepted it
    and returned it as the catalog; ``a2ui_system_prompt`` then did ``.items()`` on
    the list and raised ``'list' object has no attribute 'items'`` INSIDE the
    composer's broad except, dropping every rich surface for that workspace to a
    silent plain-text fallback. A non-dict (or empty) ``components`` is not a
    usable catalog — resolve to the full bundled one instead."""
    surface_as_catalog = json.dumps(
        {
            "surfaceKind": "presentation",
            "title": "Vietnam Country Energy Profile",
            "root": "c1",
            "components": [
                {"id": "c1", "component": "Slide", "variant": "title", "title": "X"},
                {"id": "c2", "component": "Slide", "variant": "content", "title": "Y"},
            ],
        }
    )
    out = resolve_catalog(
        {"id": 7, "catalog_type": "custom", "catalog_json": surface_as_catalog}, CATALOG
    )
    assert set(out["components"]) == FULL
    # The resolved catalog must be USABLE by the prompt builder — the exact call
    # that crashed on the surface-shaped components. Must not raise.
    a2ui_system_prompt(out, "purpose", "", "make a slide deck", "")


def test_custom_catalog_with_empty_components_falls_back():
    """``{"components": {}}`` is a dict but names nothing the composer may emit;
    like the disabled-everything ``select`` case, treat it as a mistake and use
    the full catalog rather than a catalog that can produce no surface."""
    out = resolve_catalog(
        {"id": 7, "catalog_type": "custom", "catalog_json": '{"components": {}}'},
        CATALOG,
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
    directives = {"dashboard": "keep KPI tiles to one row", "default": "be concise"}
    assert guidance_for(directives, "build a KPI dashboard") == "keep KPI tiles to one row"
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
