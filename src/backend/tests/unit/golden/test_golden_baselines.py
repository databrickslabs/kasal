"""Structural validation of the golden baseline files (WP3 parity scaffold).

These tests assert the golden files keep the structured shape the pipeline
contracts rely on. During the WP3 migrations, parity checks compare fresh flow
outputs against these baselines using the same field rules (see
tests/golden/README.md for what is and is not compared).
"""
import json
import re
from pathlib import Path

import pytest

GOLDEN_DIR = Path(__file__).resolve().parents[2] / "golden"

# Same scrubbing as applied at capture — use on fresh outputs before diffing.
SCRUB = [
    (re.compile(r"https://[a-zA-Z0-9.-]+\.cloud\.databricks\.com"), "https://example.databricks.com"),
    (re.compile(r"https://[a-zA-Z0-9.-]+\.azuredatabricks\.net"), "https://example.azuredatabricks.net"),
    (re.compile(r"\bdapi[a-f0-9]{30,}\b"), "TOKEN_REDACTED"),
]


def _load(name: str) -> dict:
    path = GOLDEN_DIR / name
    assert path.exists(), f"golden file missing: {path}"
    return json.loads(path.read_text())


class TestUCMVOutputBaseline:
    def test_structure(self):
        data = _load("ucmv_output.json")
        assert isinstance(data["yaml"], dict) and data["yaml"], "yaml table map must be non-empty"
        assert isinstance(data["sql"], dict)
        assert "stats" in data
        assert isinstance(data["measures_with_dax"], list)

    def test_yaml_specs_are_metric_views(self):
        data = _load("ucmv_output.json")
        # Some dimension tables legitimately carry empty specs (generator skips
        # them; they enter via joins). Every NON-empty spec must be a metric view.
        non_empty = {t: s for t, s in data["yaml"].items() if s}
        assert len(non_empty) >= 20, "baseline contains 24 non-empty specs; large drop = regression"
        for table, spec in non_empty.items():
            assert "source:" in spec, f"{table}: yaml spec missing source"


class TestGenieSpaceConfigBaseline:
    def test_llm_generated_fields_present(self):
        data = _load("genie_space_config.json")
        # These are LLM-generated — non-empty proves the LLM path ran (the C1
        # regression produced empty/structural-only configs).
        assert data["text_instructions"].strip()
        assert data["sample_questions"].strip()
        json.loads(data["example_sqls_json"])
        json.loads(data["join_specs_json"])

    def test_connection_params(self):
        data = _load("genie_space_config.json")
        assert data["catalog"] and data["schema_name"]


class TestVisualMappingsBaseline:
    def test_mappings_structure(self):
        data = _load("visual_mappings.json")
        mappings = data["visual_mappings"]
        assert mappings, "visual_mappings must be non-empty"
        for m in mappings:
            assert m["visual_id"] and m["visual_type"] and m["ucmv_view"]
            assert isinstance(m["dimensions"], list)
            assert isinstance(m["measures"], list)

    def test_sql_uses_measure_syntax(self):
        data = _load("visual_mappings.json")
        for m in data["visual_mappings"]:
            if m.get("sql"):
                assert "MEASURE(" in m["sql"], f"{m['visual_id']}: SQL must use MEASURE() syntax"


class TestReducedModelContextBaseline:
    def test_reduction_ran_and_cached(self):
        data = _load("reduced_model_context.json")
        assert data["status"] == "success"
        assert data["cache_saved"] is True, (
            "cache_saved=False would mean the Reducer→DAX cache handoff is broken "
            "(group-id resolution regression)"
        )
        assert data["reduction_summary"]


class TestSanitization:
    @pytest.mark.parametrize(
        "name",
        ["ucmv_output.json", "genie_space_config.json", "visual_mappings.json", "reduced_model_context.json"],
    )
    def test_no_real_endpoints_or_tokens(self, name):
        text = (GOLDEN_DIR / name).read_text()
        for rx, _ in SCRUB:
            leftover = [m for m in rx.findall(text) if "example." not in m]
            assert not leftover, f"{name}: unsanitized value matches {rx.pattern}"
