"""A surface's ``theme`` reaches CSS verbatim, so the composer keeps only hex
colours from what the model sends and drops everything else."""

import json

import pytest

from src.services.a2ui.compose import compose_a2ui, drop_unsafe_theme, load_catalog

SURFACE = {
    "surfaceKind": "dashboard",
    "root": "t",
    "components": [
        {"id": "t", "component": "Table", "columns": ["a"], "rows": [["1"]]}
    ],
}


@pytest.mark.parametrize("colour", ["#abc", "#A1B2C3", "#a1b2c3d4", "  #fff "])
def test_hex_colours_are_kept(colour):
    out = drop_unsafe_theme({**SURFACE, "theme": {"accent": colour}})
    assert out["theme"] == {"accent": colour.strip()}


@pytest.mark.parametrize(
    "value",
    [
        "url(https://images.example.org/x.png?d=1)",
        "#fff url(https://images.example.org/x.png)",
        "red",
        "rgb(1, 2, 3)",
        "#abcd",
        "#ggg",
        "image-set('https://images.example.org/x.png' 1x)",
        12,
        None,
        {"nested": "#fff"},
    ],
)
def test_anything_but_a_hex_colour_is_dropped(value):
    out = drop_unsafe_theme(
        {**SURFACE, "theme": {"accent": "#2272b4", "surface": value}}
    )
    assert out["theme"] == {"accent": "#2272b4"}


def test_a_theme_with_nothing_safe_left_is_removed():
    assert "theme" not in drop_unsafe_theme({**SURFACE, "theme": {"font": "url(x)"}})
    assert "theme" not in drop_unsafe_theme({**SURFACE, "theme": "url(x)"})
    assert drop_unsafe_theme(dict(SURFACE)) == SURFACE


def test_the_composer_returns_the_filtered_theme():
    sent = {
        **SURFACE,
        "theme": {
            "accent": "#2272b4",
            "background": "url(https://images.example.org/x.png?d=1)",
        },
    }
    out = compose_a2ui(
        "A table of values",
        query="build a dashboard of values",
        llm_call=lambda _m: json.dumps(sent),
        catalog=load_catalog(),
    )
    assert out["theme"] == {"accent": "#2272b4"}
