"""Unit tests for the live-app A2UI adapter resolution helpers.

These lock in that the UIConfigurator (workspace UI config) is the source of truth
for the new shared composer: which catalog the composer may use, and the
per-deliverable directive injected for a turn — while UNCONFIGURED workspaces keep
the full catalog so rich surfaces don't silently regress.
"""

import json
from types import SimpleNamespace

from src.engines.crewai.kernel import a2ui_runner as R
from src.shared.a2ui.compose import (
    MINIMAL_COMPONENTS,
    a2ui_system_prompt,
    compose_a2ui,
    load_catalog,
    subset_catalog,
)

CATALOG = load_catalog()
FULL = set(CATALOG["components"])


def _cfg(**kw):
    """A stand-in for UIConfigResponse (attribute access only)."""
    kw.setdefault("id", None)
    kw.setdefault("catalog_type", "minimal")
    kw.setdefault("catalog_json", None)
    kw.setdefault("style_json", None)
    return SimpleNamespace(**kw)


# --- subset_catalog --------------------------------------------------------
def test_subset_catalog_keeps_only_named_components():
    mini = subset_catalog(CATALOG, MINIMAL_COMPONENTS)["components"]
    assert set(mini) == set(MINIMAL_COMPONENTS) & FULL
    assert "SlideDeck" not in mini and "Quiz" not in mini  # rich dropped
    assert "Markdown" in mini and "Table" in mini  # essentials kept


# --- _infer_deliverable ----------------------------------------------------
def test_infer_deliverable_first_keyword_wins():
    assert R._infer_deliverable("make a slide deck about Q3") == "presentation"
    assert R._infer_deliverable("build a KPI dashboard") == "dashboard"
    assert R._infer_deliverable("quiz me on history") == "quiz"
    assert R._infer_deliverable("a mind map of the org") == "mindmap"
    assert R._infer_deliverable("just answer in prose") is None


def test_infer_deliverable_routes_new_component_deliverables():
    # The new data-viz / diagram / gallery deliverables must route so their
    # per-type directives + branding apply (and rule 5 emits the right component).
    assert R._infer_deliverable("forecast the expected loss by region") == "forecast"
    assert R._infer_deliverable("show a dependency graph of the services") == "graph"
    assert R._infer_deliverable("a sequence diagram of the login flow") == "sequence"
    assert R._infer_deliverable("build a ferrari photo album") == "album"
    # A bare "bar graph" must NOT hijack the node-link Graph — it's a plain chart.
    assert R._infer_deliverable("draw a bar graph of sales") != "graph"


# --- _resolve_catalog ------------------------------------------------------
def test_unconfigured_workspace_keeps_full_catalog():
    # No saved row (id is None) → schema's 'minimal' default must NOT restrict.
    cfg = _cfg(id=None, catalog_type="minimal")
    assert set(R._resolve_catalog(cfg, CATALOG)["components"]) == FULL


def test_saved_minimal_restricts_to_subset():
    cfg = _cfg(id=7, catalog_type="minimal")
    assert (
        set(R._resolve_catalog(cfg, CATALOG)["components"])
        == set(MINIMAL_COMPONENTS) & FULL
    )


def test_saved_basic_uses_full_catalog():
    cfg = _cfg(id=7, catalog_type="basic")
    assert set(R._resolve_catalog(cfg, CATALOG)["components"]) == FULL


def test_custom_catalog_parsed_and_surfacekinds_backfilled():
    cfg = _cfg(
        id=7,
        catalog_type="custom",
        catalog_json='{"components": {"Text": {"props": {}}}}',
    )
    out = R._resolve_catalog(cfg, CATALOG)
    assert set(out["components"]) == {"Text"}
    assert out["surfaceKinds"] == CATALOG["surfaceKinds"]  # backfilled from default


def test_invalid_custom_catalog_falls_back_to_default():
    cfg = _cfg(id=7, catalog_type="custom", catalog_json="{not valid json")
    assert set(R._resolve_catalog(cfg, CATALOG)["components"]) == FULL


# --- _resolve_guidance -----------------------------------------------------
def test_guidance_picks_inferred_deliverables_directive():
    cfg = _cfg(
        id=7,
        style_json=json.dumps(
            {
                "directives": {
                    "presentation": "aim for about 8 slides",
                    "quiz": "10 questions",
                }
            }
        ),
    )
    assert R._resolve_guidance(cfg, "make a slide deck") == "aim for about 8 slides"
    assert R._resolve_guidance(cfg, "give me a quiz") == "10 questions"


def test_guidance_falls_back_to_default_then_empty():
    cfg = _cfg(id=7, style_json=json.dumps({"directives": {"default": "be concise"}}))
    assert R._resolve_guidance(cfg, "hello there") == "be concise"
    cfg_none = _cfg(id=7, style_json=json.dumps({"directives": {"quiz": "x"}}))
    assert R._resolve_guidance(cfg_none, "hello there") == ""
    assert R._resolve_guidance(_cfg(id=7, style_json=None), "anything") == ""


# --- guidance threads into the composer prompt -----------------------------
def test_guidance_appears_in_system_prompt():
    sp = a2ui_system_prompt(
        CATALOG, "purpose", "", "make a deck", "aim for about 8 slides"
    )
    assert "DELIVERABLE SETTINGS" in sp
    assert "aim for about 8 slides" in sp


def test_compose_a2ui_threads_guidance_to_llm():
    seen = {}

    def stub(messages):
        seen["sys"] = messages[0]["content"]
        # A content slide must carry a real body, else the hollow-deck guard
        # rejects it and falls back to markdown.
        return (
            '{"surfaceKind":"presentation","root":"d","components":'
            '[{"id":"d","component":"SlideDeck","children":["s"]},'
            '{"id":"s","component":"Slide","title":"Hi","children":["t"]},'
            '{"id":"t","component":"Text","text":"A real point."}],"dataModel":{}}'
        )

    surf = compose_a2ui(
        "body text",
        "purpose",
        "",
        "make a 8-slide deck",
        llm_call=stub,
        catalog=CATALOG,
        guidance="aim for about 8 slides",
    )
    assert surf["surfaceKind"] == "presentation"
    assert "aim for about 8 slides" in seen["sys"]


# --- _has_data_component ---------------------------------------------------
def test_has_data_component_detects_data_bearing_nodes():
    # Data-viz + diagram + gallery components all make a surface a real deliverable
    # (else their prose-free surfaces would be dropped back to plain text).
    for comp in (
        "Chart",
        "Table",
        "Stat",
        "KeyValue",
        "Grid",
        "Forecast",
        "Graph",
        "Sequence",
        "Album",
        "Map",
    ):
        surface = {"components": [{"id": "a", "component": comp}]}
        assert R._has_data_component(surface), comp
    # `type` is accepted as an alias for `component` (renderer parity).
    assert R._has_data_component({"components": [{"id": "a", "type": "Chart"}]})


def test_has_data_component_false_for_text_only_surface():
    surface = {
        "components": [
            {"id": "root", "component": "Column", "children": ["t"]},
            {"id": "t", "component": "Text", "text": "just prose"},
        ]
    }
    assert not R._has_data_component(surface)
    assert not R._has_data_component({"components": []})
    assert not R._has_data_component({})


# --- compose_surface: prose-only dashboard/document is dropped --------------
def _run(coro):
    import asyncio

    return asyncio.run(coro)


def _stub_compose_surface(monkeypatch, surface):
    """Wire compose_surface's dependencies so it reaches the surface-worthiness
    gate: A2UI enabled, a rich-intent turn, and a composer that returns `surface`."""

    async def _resolve(group_id, query):
        return True, CATALOG, ""

    monkeypatch.setattr(R, "_resolve_config", _resolve)
    monkeypatch.setattr(R, "wants_rich_surface", lambda text, intent: True)
    monkeypatch.setattr(R, "compose_a2ui", lambda *a, **k: surface)

    class _LLM:
        def call(self, messages):
            return ""

    async def _get_llm(*a, **k):
        return _LLM()

    import src.core.llm_manager as llm_mod

    monkeypatch.setattr(llm_mod.LLMManager, "get_llm", staticmethod(_get_llm))


def test_compose_surface_drops_text_only_document(monkeypatch):
    """A Genie/analytics turn fires the composer, but a prose-only document surface
    would render the SAME text twice (chat bubble + surface). It must be dropped so
    the result stays a plain string."""
    text_only = {
        "surfaceKind": "document",
        "root": "root",
        "components": [{"id": "root", "component": "Text", "text": "overview prose"}],
    }
    _stub_compose_surface(monkeypatch, text_only)
    out = _run(R.compose_surface("overview prose", query="usage analytics"))
    assert out is None


def test_compose_surface_keeps_dashboard_with_chart(monkeypatch):
    """A surface that actually materializes a graph is a real deliverable — keep it."""
    with_chart = {
        "surfaceKind": "dashboard",
        "root": "root",
        "components": [
            {"id": "root", "component": "Grid", "children": ["c"]},
            {"id": "c", "component": "Chart", "chartType": "bar"},
        ],
    }
    _stub_compose_surface(monkeypatch, with_chart)
    out = _run(R.compose_surface("here is a chart", query="usage analytics"))
    assert out is with_chart


def test_compose_surface_keeps_document_rooted_on_album(monkeypatch):
    """A photo album is a real deliverable even though it has no Chart/Table — an
    Album/Graph/Sequence/Forecast surface must NOT be dropped by the prose gate."""
    album = {
        "surfaceKind": "document",
        "root": "al",
        "components": [{"id": "al", "component": "Album", "items": {"path": "/pics"}}],
    }
    _stub_compose_surface(monkeypatch, album)
    out = _run(R.compose_surface("here are 10 photos", query="ferrari photo album"))
    assert out is album


def test_compose_surface_keeps_text_only_presentation(monkeypatch):
    """Explicitly-requested rich kinds (presentation/quiz/…) are legitimately
    non-tabular, so they are NOT gated on data content."""
    deck = {
        "surfaceKind": "presentation",
        "root": "d",
        "components": [{"id": "d", "component": "SlideDeck", "children": []}],
    }
    _stub_compose_surface(monkeypatch, deck)
    out = _run(R.compose_surface("slides", query="make a deck"))
    assert out is deck
