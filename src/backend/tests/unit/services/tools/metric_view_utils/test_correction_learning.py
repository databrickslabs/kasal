"""Core of the 'learn from customer-corrected UCMVs' flywheel: diff + distill."""

from src.services.tools.metric_view_utils.correction_learning import (
    build_distillation_prompt,
    diff_all,
    diff_view,
    distill_corrections_to_readme,
)

_ORIG = """
source: cat.sch.fact_sales
filter: ""
measures:
  - name: revenue
    expr: SUM(source.amount)
  - name: cost
    expr: SUM(source.cost_components)
  - name: legacy_only
    expr: SUM(source.x)
"""

_CORR = """
source: cat.sch.fact_sales_v2
filter: "source.version = '0000'"
measures:
  - name: revenue
    expr: SUM(source.amount)
  - name: cost
    expr: SUM(source.booked_cost)
  - name: margin
    expr: MEASURE(revenue) - MEASURE(cost)
"""


def test_diff_view_detects_changed_added_removed_and_source_filter():
    d = diff_view(_ORIG, _CORR)
    changed = {c["name"] for c in d["changed_measures"]}
    assert changed == {"cost"}  # revenue identical, cost expr changed
    assert d["changed_measures"][0]["to_expr"] == "SUM(source.booked_cost)"
    assert [m["name"] for m in d["added_measures"]] == ["margin"]
    assert d["removed_measures"] == ["legacy_only"]
    assert d["source_from"] != d["source_to"]
    assert d["filter_to"] == "source.version = '0000'"


def test_whitespace_only_change_is_not_flagged():
    a = "measures:\n  - name: r\n    expr: SUM(source.a)\n"
    b = "measures:\n  - name: r\n    expr: SUM(source.a)\n"  # same
    assert diff_view(a, b)["changed_measures"] == []


def test_diff_all_drops_views_with_no_change():
    pairs = [
        {"view": "fact_sales", "original_yaml": _ORIG, "corrected_yaml": _CORR},
        {"view": "fact_noop", "original_yaml": _ORIG, "corrected_yaml": _ORIG},
    ]
    out = diff_all(pairs)
    assert set(out) == {"fact_sales"}


def test_malformed_yaml_is_safe():
    d = diff_view("::: not yaml :::", _CORR)
    # Original parsed empty → every corrected measure reads as "added", no crash.
    assert {m["name"] for m in d["added_measures"]} == {"revenue", "cost", "margin"}


def test_prompt_includes_the_concrete_corrections():
    diffs = diff_all(
        [{"view": "fact_sales", "original_yaml": _ORIG, "corrected_yaml": _CORR}]
    )
    prompt = build_distillation_prompt(diffs, model_hint="Total SC")
    assert "Total SC" in prompt
    assert "booked_cost" in prompt
    assert "fact_sales" in prompt


def test_distill_returns_none_when_nothing_changed():
    assert distill_corrections_to_readme({}, lambda s, u: "x") is None


def test_distill_invokes_llm_and_returns_readme():
    diffs = diff_all(
        [{"view": "fact_sales", "original_yaml": _ORIG, "corrected_yaml": _CORR}]
    )
    captured = {}

    def fake_llm(system, user):
        captured["system"] = system
        captured["user"] = user
        return "# Learned context\n- Prefer booked_cost."

    out = distill_corrections_to_readme(diffs, fake_llm, model_hint="Total SC")
    assert out and out.startswith("# Learned context")
    assert "booked_cost" in captured["user"]
    assert "GENERALISABLE" in captured["system"]


class TestResolveOriginalMatching:
    """Pairing an uploaded YAML to our original: exact → name-sim → measure-overlap."""

    _ORIG_YAML = "measures:\n  - name: sales\n    expr: SUM(source.a)\n  - name: cost\n    expr: SUM(source.b)\n"
    _CORR_SAME_MEASURES = "measures:\n  - name: sales\n    expr: SUM(source.a)\n  - name: cost\n    expr: SUM(source.c)\n"

    def test_exact_name(self):
        from src.services.tools.metric_view_utils.correction_learning import (
            resolve_original,
        )

        assert resolve_original("fact_x", "", {"fact_x": "m"}) == ("fact_x", "exact")

    def test_fuzzy_name_strips_suffix_and_separators(self):
        from src.services.tools.metric_view_utils.correction_learning import (
            resolve_original,
        )

        # upload named "<view>_uc_metric_view" vs stored "FT_PE005"
        k, how = resolve_original("FT_PE005_uc_metric_view", "", {"FT-PE005": "m"})
        assert k == "FT-PE005" and how == "fuzzy_name"

    def test_fuzzy_measure_overlap_when_names_differ(self):
        from src.services.tools.metric_view_utils.correction_learning import (
            resolve_original,
        )

        k, how = resolve_original(
            "totally_renamed", self._CORR_SAME_MEASURES, {"orig_view": self._ORIG_YAML}
        )
        assert k == "orig_view" and how == "fuzzy_measures"

    def test_no_match_when_name_and_measures_both_differ(self):
        from src.services.tools.metric_view_utils.correction_learning import (
            resolve_original,
        )

        k, how = resolve_original(
            "zzz", "measures:\n  - name: q\n    expr: x\n", {"orig": self._ORIG_YAML}
        )
        assert k is None and how == "none"

    def test_measure_names_helper(self):
        from src.services.tools.metric_view_utils.correction_learning import (
            measure_names,
        )

        assert measure_names(self._ORIG_YAML) == {"sales", "cost"}
        assert measure_names("not yaml :::") == set()


def test_prompt_has_dax_corrections_section():
    from src.services.tools.metric_view_utils.correction_learning import _SYSTEM_PROMPT

    assert "DAX translation corrections" in _SYSTEM_PROMPT
    assert "self-healing" in _SYSTEM_PROMPT.lower()
