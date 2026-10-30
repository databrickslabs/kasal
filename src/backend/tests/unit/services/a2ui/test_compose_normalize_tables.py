"""A Table bound Chart-style (``data: {path}`` over row objects) must come out of
the composer with catalog-shaped ``rows`` — the renderer reads only ``rows``, so
run 542's dashboard showed "No matching rows" in every table."""

import copy
import json

from src.services.a2ui.compose import compose_a2ui, load_catalog, normalize_tables

CATALOG = load_catalog()

# Trimmed from a stored run's surface: the exact shape the model emitted.
RUN_542 = {
    "surfaceKind": "dashboard",
    "root": "root",
    "components": [
        {"id": "root", "component": "Grid", "columns": 3, "children": ["llm-card"]},
        {
            "id": "llm-card",
            "component": "Card",
            "title": "Large Language Models",
            "children": ["llm-table"],
        },
        {
            "id": "llm-table",
            "component": "Table",
            "columns": ["Model", "Params", "4-bit VRAM", "Tokens/sec", "Notes"],
            "data": {"path": "/llm_models"},
        },
    ],
    "dataModel": {
        "llm_models": [
            {
                "Model": "LLaMA-2-7B",
                "Params": "7B",
                "4-bit VRAM": "~5 GB",
                "Tokens/sec": "45-60",
                "Notes": "Excellent fit, headroom for context",
            },
            {
                "Model": "Phi-2",
                "Params": "2.7B",
                "4-bit VRAM": "~2 GB",
                "Tokens/sec": "80-100",
                "Notes": "Compact, efficient",
            },
        ]
    },
}


def _table(surface):
    return next(c for c in surface["components"] if c["component"] == "Table")


def test_data_path_over_row_objects_becomes_row_arrays():
    table = _table(normalize_tables(copy.deepcopy(RUN_542)))
    assert "data" not in table
    assert table["rows"] == [
        ["LLaMA-2-7B", "7B", "~5 GB", "45-60", "Excellent fit, headroom for context"],
        ["Phi-2", "2.7B", "~2 GB", "80-100", "Compact, efficient"],
    ]


def test_compose_returns_the_normalised_table():
    out = compose_a2ui(
        "RTX 3090 Ti model compatibility",
        query="build a dashboard of models",
        llm_call=lambda _m: json.dumps(RUN_542),
        catalog=CATALOG,
    )
    assert out["surfaceKind"] == "dashboard"
    assert _table(out)["rows"][1][0] == "Phi-2"


def test_bound_row_objects_without_columns_derive_the_header():
    surface = copy.deepcopy(RUN_542)
    table = _table(surface)
    del table["columns"], table["data"]
    table["rows"] = {"path": "/llm_models"}
    table = _table(normalize_tables(surface))
    assert table["columns"][0] == "Model"
    assert table["rows"][0][0] == "LLaMA-2-7B"


def test_catalog_shaped_tables_are_untouched():
    literal = {
        "root": "t",
        "components": [
            {"id": "t", "component": "Table", "columns": ["a"], "rows": [["1"]]},
            {"id": "u", "component": "Table", "rows": {"path": "/r"}},
        ],
        "dataModel": {"r": [["2"]]},
    }
    before = copy.deepcopy(literal)
    assert normalize_tables(literal) == before
