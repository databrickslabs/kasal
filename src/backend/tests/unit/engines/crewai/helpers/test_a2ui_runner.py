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


# --- _resolve_catalog ------------------------------------------------------
def test_unconfigured_workspace_keeps_full_catalog():
    # No saved row (id is None) → schema's 'minimal' default must NOT restrict.
    cfg = _cfg(id=None, catalog_type="minimal")
    assert set(R._resolve_catalog(cfg, CATALOG)["components"]) == FULL


def test_saved_minimal_restricts_to_subset():
    cfg = _cfg(id=7, catalog_type="minimal")
    assert set(R._resolve_catalog(cfg, CATALOG)["components"]) == set(MINIMAL_COMPONENTS) & FULL


def test_saved_basic_uses_full_catalog():
    cfg = _cfg(id=7, catalog_type="basic")
    assert set(R._resolve_catalog(cfg, CATALOG)["components"]) == FULL


def test_custom_catalog_parsed_and_surfacekinds_backfilled():
    cfg = _cfg(id=7, catalog_type="custom", catalog_json='{"components": {"Text": {"props": {}}}}')
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
            {"directives": {"presentation": "aim for about 8 slides", "quiz": "10 questions"}}
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
    sp = a2ui_system_prompt(CATALOG, "purpose", "", "make a deck", "aim for about 8 slides")
    assert "DELIVERABLE SETTINGS" in sp
    assert "aim for about 8 slides" in sp


def test_compose_a2ui_threads_guidance_to_llm():
    seen = {}

    def stub(messages):
        seen["sys"] = messages[0]["content"]
        return (
            '{"surfaceKind":"presentation","root":"d","components":'
            '[{"id":"d","component":"SlideDeck","children":["s"]},'
            '{"id":"s","component":"Slide","title":"Hi"}],"dataModel":{}}'
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
