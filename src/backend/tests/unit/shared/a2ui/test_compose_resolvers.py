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
    guidance_for,
    infer_deliverable,
    load_catalog,
    resolve_catalog,
    resolve_directives,
    resolve_themes,
)

CATALOG = load_catalog()
FULL = set(CATALOG["components"])


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
