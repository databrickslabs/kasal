"""Unit tests for the DAX function-reference retriever."""
from src.services.tools.metric_view_utils import function_ref_retriever as R


class TestDetection:
    def test_detects_known_function_calls(self):
        found = R.detect_functions(["GEOMEAN(Sales[margin])", "DATEDIFF(a, b, DAY)"])
        assert "GEOMEAN" in found
        assert "DATEDIFF" in found

    def test_ignores_unknown_and_columns(self):
        # bracketed column refs and unknown tokens must not match
        found = R.detect_functions(["[Some Measure] + NOTAFUNC(x) + 'C_Dim'"])
        assert "NOTAFUNC" not in found

    def test_dotted_function_names(self):
        found = R.detect_functions(["PERCENTILE.INC(Sales[amt], 0.9)"])
        assert "PERCENTILE.INC" in found

    def test_empty_and_none_safe(self):
        assert R.detect_functions([]) == []
        assert R.detect_functions([None, ""]) == []


class TestRender:
    def test_skips_prefix_covered_head_functions(self):
        # CALCULATE/SUM/DIVIDE are taught deeply in the static prefix → not injected
        block = R.render_function_refs(["CALCULATE(SUM(t[x]), t[y]=1)", "DIVIDE(a, b)"])
        assert block == ""

    def test_injects_tail_trap_with_curated_form(self):
        block = R.render_function_refs(["FIND(\"@\", Cust[email])"])
        assert "FIND" in block
        assert "INSTR(source.hay, 'needle')" in block
        assert "ARG ORDER SWAPS" in block

    def test_uncurated_tail_renders_generic_with_caveat(self):
        block = R.render_function_refs(["ACOSH(Metrics[x])"])
        assert "ACOSH" in block
        assert "adapt per §0" in block
        assert "(default)" in block

    def test_declines_are_marked_not_a_measure_expr(self):
        # HASONEVALUE is a tail decline (curated unsupported, sql=None) not taught
        # in the prefix, so retrieval surfaces it with the "not a measure expr" note.
        block = R.render_function_refs(["IF(HASONEVALUE(Dim[Year]), [M], BLANK())"])
        assert "HASONEVALUE" in block
        assert "unsupported" in block
        assert "not a measure expr" in block

    def test_empty_when_no_tail_functions(self):
        assert R.render_function_refs(["SUM(t[x])"]) == ""

    def test_respects_max_render_cap(self):
        block = R.render_function_refs(["GEOMEAN(a) MEDIAN(b) STDEV.S(c)"], max_render=1)
        assert block.count("### ") == 1


class TestDataIntegrity:
    def test_base_data_loaded(self):
        assert len(R._BASE) > 300  # ~386 functions

    def test_every_curated_key_exists_in_base(self):
        missing = [k for k in R._CURATED if k not in R._BASE]
        assert missing == [], f"curated keys not in base: {missing}"

    def test_curated_entries_have_class_and_note(self):
        for name, entry in R._CURATED.items():
            assert entry.get("cls"), f"{name} missing dax_class"
            assert "note" in entry, f"{name} missing note key"

    def test_prefix_deep_excludes_head_from_curated_noise(self):
        # sanity: high-frequency head functions are recognised as prefix-covered
        assert "CALCULATE" in R._PREFIX_DEEP
        assert "DIVIDE" in R._PREFIX_DEEP
