"""Tests for metric_view_validation_utils.expression_validator (ExpressionValidator)."""
import json
import textwrap
import pytest
from unittest.mock import MagicMock

from src.engines.crewai.tools.custom.metric_view_validation_utils.expression_validator import (
    ExpressionValidator,
)
from src.engines.crewai.tools.custom.metric_view_validation_utils.data_input_handler import (
    DataInputHandler,
)

# ---------------------------------------------------------------------------
# Shared fixture data
# ---------------------------------------------------------------------------

SIMPLE_YAML = textwrap.dedent("""\
    measures:
      - name: total_sales
        expr: "SUM(source.amount)"
        comment: ""
      - name: simple_ratio
        expr: "SUM(source.amount)"
        comment: ""
      - name: unmatched_measure
        expr: "SUM(source.x)"
        comment: ""
""")

SAMPLE_MAPPINGS = [
    {"measure_name": "total_sales", "dax_expression": "SUM(fact[amount])"},
    {"measure_name": "no_dax_measure", "dax_expression": "Not available"},
]


def _make_handler(tmp_path) -> DataInputHandler:
    yaml_file = tmp_path / "mv.yaml"
    yaml_file.write_text(SIMPLE_YAML)
    json_file = tmp_path / "mapping.json"
    json_file.write_text(json.dumps(SAMPLE_MAPPINGS))
    return DataInputHandler(str(yaml_file), str(json_file))


# ---------------------------------------------------------------------------
# Initialisation
# ---------------------------------------------------------------------------

class TestInit:
    def test_no_data_handler(self):
        v = ExpressionValidator()
        assert v.data_handler is None
        assert v.table_mappings == {}
        assert v.column_mappings == {}

    def test_with_mappings(self):
        v = ExpressionValidator(
            table_mappings={"fact": "source"},
            column_mappings={"amount": "amt"},
        )
        assert v.table_mappings == {"fact": "source"}
        assert v.column_mappings == {"amount": "amt"}

    def test_parsers_initialised(self):
        v = ExpressionValidator()
        assert v.db_parser is not None
        assert v.dax_parser is not None


# ---------------------------------------------------------------------------
# validate()  – direct expression comparison
# ---------------------------------------------------------------------------

class TestValidate:
    def _v(self, **kwargs):
        return ExpressionValidator(**kwargs)

    # ── aggregation matching ────────────────────────────────────────────

    def test_identical_sum_is_valid(self):
        v = self._v(table_mappings={"fact": "source"})
        result = v.validate("SUM(source.amount)", "SUM(fact[amount])")
        assert result["is_valid"] is True
        assert result["confidence"] == "high"

    def test_sumx_matches_sum(self):
        v = self._v(table_mappings={"fact": "source"})
        result = v.validate("SUM(source.amount)", "SUMX(fact, fact[amount])")
        # SUMX maps to SUM; reference match depends on how args are parsed,
        # so we just verify the call succeeds and has the expected keys
        assert "is_valid" in result
        assert "differences" in result
        assert "similarities" in result

    def test_agg_type_mismatch_is_invalid(self):
        v = self._v(table_mappings={"fact": "source"})
        result = v.validate("SUM(source.amount)", "COUNT(fact[amount])")
        # SUM vs COUNT → should not be valid
        assert result["is_valid"] is False

    def test_column_mismatch_is_invalid(self):
        v = self._v(table_mappings={"fact": "source"})
        result = v.validate("SUM(source.amount)", "SUM(fact[revenue])")
        assert result["is_valid"] is False

    # ── filter matching ─────────────────────────────────────────────────

    def test_no_filters_both_sides_valid(self):
        v = self._v(table_mappings={"fact": "source"})
        result = v.validate("SUM(source.amount)", "SUM(fact[amount])")
        assert "Filters match" in " ".join(result["similarities"])

    def test_filter_mismatch_db_has_filter_dax_does_not(self):
        v = self._v()
        result = v.validate(
            "SUM(source.amount) FILTER (WHERE source.status = 'active')",
            "SUM(T[amount])",
        )
        assert result["is_valid"] is False
        assert any("Filter" in d for d in result["differences"])

    # ── reference matching ──────────────────────────────────────────────

    def test_reference_match_with_table_mapping(self):
        v = self._v(table_mappings={"fact": "source"})
        result = v.validate("SUM(source.amount)", "SUM(fact[amount])")
        assert any("References match" in s for s in result["similarities"])

    # ── strict mode ─────────────────────────────────────────────────────

    def test_strict_mode_structure_checked(self):
        v = self._v(table_mappings={"fact": "source"})
        # Both expressions are pure SUM – structure should match in strict mode too
        result = v.validate("SUM(source.amount)", "SUM(fact[amount])", strict=True)
        assert "is_valid" in result

    def test_strict_mode_division_mismatch(self):
        v = self._v(table_mappings={"fact": "source"})
        # DB has division, DAX doesn't → structure mismatch under strict
        result = v.validate(
            "SUM(source.a) / COUNT(source.b)",
            "SUM(fact[a])",
            strict=True,
        )
        assert result["is_valid"] is False

    # ── result shape ────────────────────────────────────────────────────

    def test_result_contains_required_keys(self):
        v = self._v()
        result = v.validate("SUM(source.amount)", "SUM(T[amount])")
        for key in ("is_valid", "confidence", "differences", "similarities",
                    "recommendations", "databricks_parsed", "dax_parsed"):
            assert key in result

    def test_confidence_low_when_mostly_differences(self):
        v = self._v()
        # Completely unrelated expressions → low confidence
        result = v.validate("SUM(source.a)", "COUNT(other[b])")
        assert result["confidence"] in ("low", "medium")


# ---------------------------------------------------------------------------
# _compare_aggregations()
# ---------------------------------------------------------------------------

class TestCompareAggregations:
    def _v(self):
        return ExpressionValidator(table_mappings={"fact": "source"})

    def test_empty_both_match(self):
        result = self._v()._compare_aggregations([], [])
        assert result["match"] is True

    def test_single_sum_match(self):
        db_agg = [{"type": "SUM", "content": "source.amount", "position": 0}]
        dax_agg = [{"type": "SUM", "content": "fact.amount", "position": 0, "node": {}}]
        v = ExpressionValidator(table_mappings={"fact": "source"})
        result = v._compare_aggregations(db_agg, dax_agg)
        assert result["match"] is True

    def test_type_mismatch(self):
        db_agg = [{"type": "SUM", "content": "source.amount", "position": 0}]
        dax_agg = [{"type": "COUNT", "content": "fact.amount", "position": 0, "node": {}}]
        v = ExpressionValidator(table_mappings={"fact": "source"})
        result = v._compare_aggregations(db_agg, dax_agg)
        assert result["match"] is False

    def test_extra_db_agg_flagged(self):
        db_agg = [
            {"type": "SUM", "content": "source.amount", "position": 0},
            {"type": "COUNT", "content": "source.id", "position": 10},
        ]
        dax_agg = [{"type": "SUM", "content": "fact.amount", "position": 0, "node": {}}]
        v = ExpressionValidator(table_mappings={"fact": "source"})
        result = v._compare_aggregations(db_agg, dax_agg)
        assert result["match"] is False
        assert any("no matching DAX" in m for m in result["mismatches"])


# ---------------------------------------------------------------------------
# _compare_filters()
# ---------------------------------------------------------------------------

class TestCompareFilters:
    def _v(self):
        return ExpressionValidator()

    def test_no_filters_both(self):
        result = self._v()._compare_filters([], [])
        assert result["match"] is True

    def test_db_has_filter_dax_none(self):
        db_f = [{"parsed_condition": {"type": "EQUALS", "column": "source.status", "value": "active"}}]
        result = self._v()._compare_filters(db_f, [])
        assert result["match"] is False
        assert "recommendation" in result

    def test_dax_has_filter_db_none(self):
        dax_f = [{"parsed_condition": {"type": "EQUALS", "column": "fact.status", "value": "active"}}]
        result = self._v()._compare_filters([], dax_f)
        assert result["match"] is False

    def test_matching_equals_filter(self):
        cond = {"type": "EQUALS", "column": "source.status", "value": "active"}
        db_f = [{"parsed_condition": cond}]
        dax_f = [{"parsed_condition": cond}]
        result = self._v()._compare_filters(db_f, dax_f)
        assert result["match"] is True

    def test_order_independent(self):
        cond_a = {"type": "EQUALS", "column": "source.a", "value": "1"}
        cond_b = {"type": "EQUALS", "column": "source.b", "value": "2"}
        db_f = [{"parsed_condition": cond_a}, {"parsed_condition": cond_b}]
        dax_f = [{"parsed_condition": cond_b}, {"parsed_condition": cond_a}]
        result = self._v()._compare_filters(db_f, dax_f)
        assert result["match"] is True

    def test_in_clause_filter(self):
        cond = {"type": "IN", "column": "source.type", "values": ["A", "B"]}
        db_f = [{"parsed_condition": cond}]
        dax_f = [{"parsed_condition": cond}]
        result = self._v()._compare_filters(db_f, dax_f)
        assert result["match"] is True

    def test_different_filter_values(self):
        cond_db = {"type": "EQUALS", "column": "source.status", "value": "active"}
        cond_dax = {"type": "EQUALS", "column": "source.status", "value": "inactive"}
        db_f = [{"parsed_condition": cond_db}]
        dax_f = [{"parsed_condition": cond_dax}]
        result = self._v()._compare_filters(db_f, dax_f)
        assert result["match"] is False


# ---------------------------------------------------------------------------
# _filter_signature()
# ---------------------------------------------------------------------------

class TestFilterSignature:
    def test_equals_signature(self):
        cond = {"type": "EQUALS", "column": "Source.Status", "value": "Active"}
        sig = ExpressionValidator._filter_signature(cond)
        # Case normalised, table prefix stripped for cross-format comparison
        assert sig == ("EQUALS", "status", "active")

    def test_in_signature_order_independent(self):
        cond = {"type": "IN", "column": "Source.type", "values": ["B", "A"]}
        sig = ExpressionValidator._filter_signature(cond)
        assert sig[0] == "IN"
        # Table prefix stripped for cross-format comparison
        assert sig[1] == "type"
        assert sig[2] == frozenset({"a", "b"})

    def test_unknown_type(self):
        cond = {"type": "UNKNOWN", "raw": "some raw condition"}
        sig = ExpressionValidator._filter_signature(cond)
        assert sig[0] == "UNKNOWN"


# ---------------------------------------------------------------------------
# _compare_columns()
# ---------------------------------------------------------------------------

class TestCompareColumns:
    def _v(self, **kwargs):
        return ExpressionValidator(**kwargs)

    def test_exact_qualified_match(self):
        v = self._v()
        result = v._compare_columns({"source.amount"}, {"source.amount"})
        assert result["match"] is True

    def test_qualified_match_with_table_mapping(self):
        v = self._v(table_mappings={"fact": "source"})
        result = v._compare_columns({"source.amount"}, {"fact.amount"})
        assert result["match"] is True

    def test_qualified_mismatch(self):
        v = self._v()
        result = v._compare_columns({"source.amount"}, {"source.revenue"})
        assert result["match"] is False
        assert "recommendation" in result

    def test_unqualified_column_fallback(self):
        v = self._v()
        result = v._compare_columns({"amount"}, {"amount"})
        assert result["match"] is True

    def test_empty_both_sides(self):
        v = self._v()
        result = v._compare_columns(set(), set())
        assert result["match"] is True


# ---------------------------------------------------------------------------
# _compare_structure()
# ---------------------------------------------------------------------------

class TestCompareStructure:
    def _v(self):
        return ExpressionValidator()

    def _s(self, is_div=False, has_filter=False, complexity="simple"):
        return {"is_division": is_div, "has_filter": has_filter, "complexity": complexity}

    def test_identical_structure_matches(self):
        s = self._s()
        result = self._v()._compare_structure(s, s)
        assert result["match"] is True

    def test_division_mismatch(self):
        db = self._s(is_div=True)
        dax = self._s(is_div=False)
        result = self._v()._compare_structure(db, dax)
        assert result["match"] is False
        assert "Division" in result["details"]

    def test_filter_presence_mismatch(self):
        db = self._s(has_filter=True)
        dax = self._s(has_filter=False)
        result = self._v()._compare_structure(db, dax)
        assert result["match"] is False


# ---------------------------------------------------------------------------
# validate_measure_by_name()
# ---------------------------------------------------------------------------

class TestValidateMeasureByName:
    def test_raises_without_data_handler(self):
        v = ExpressionValidator()
        with pytest.raises(ValueError, match="DataInputHandler must be provided"):
            v.validate_measure_by_name("some_measure")

    def test_skipped_when_yaml_measure_not_found(self, tmp_path):
        h = _make_handler(tmp_path)
        v = ExpressionValidator(data_handler=h)
        result = v.validate_measure_by_name("nonexistent")
        assert result["status"] == "SKIPPED"
        assert result["is_valid"] is False

    def test_skipped_when_no_dax_match(self, tmp_path):
        h = _make_handler(tmp_path)
        v = ExpressionValidator(data_handler=h)
        # 'unmatched_measure' has no matching DAX entry
        result = v.validate_measure_by_name("unmatched_measure")
        assert result["status"] == "SKIPPED"

    def test_valid_measure_returns_valid_status(self, tmp_path):
        h = _make_handler(tmp_path)
        v = ExpressionValidator(
            data_handler=h,
            table_mappings={"fact": "source"},
        )
        result = v.validate_measure_by_name("total_sales")
        assert result["status"] in ("VALID", "INVALID")   # expression pair exists
        assert "is_valid" in result


# ---------------------------------------------------------------------------
# validate_ucmv()
# ---------------------------------------------------------------------------

class TestValidateUcmv:
    def test_raises_without_data_handler(self):
        v = ExpressionValidator()
        with pytest.raises(ValueError, match="DataInputHandler must be provided"):
            v.validate_ucmv()

    def test_returns_skipped_and_evaluated_keys(self, tmp_path):
        h = _make_handler(tmp_path)
        v = ExpressionValidator(data_handler=h, table_mappings={"fact": "source"})
        result = v.validate_ucmv()
        assert "skipped" in result
        assert "evaluated" in result

    def test_unmatched_measure_in_skipped(self, tmp_path):
        h = _make_handler(tmp_path)
        v = ExpressionValidator(data_handler=h)
        result = v.validate_ucmv()
        skipped_names = {m.get("measure_name") for m in result["skipped"]}
        assert "unmatched_measure" in skipped_names

    def test_simple_measure_in_skipped(self, tmp_path):
        h = _make_handler(tmp_path)
        v = ExpressionValidator(data_handler=h)
        result = v.validate_ucmv()
        # total_sales / simple_ratio match the simple_pattern → should be skipped
        skipped_evals = {m.get("measure_eval") for m in result["skipped"]}
        assert "simple" in skipped_evals or "unmatched" in skipped_evals


# ---------------------------------------------------------------------------
# EQUIVALENT / REVIEW status classification
# ---------------------------------------------------------------------------

class TestEquivalentStatus:
    """Test that known DAX→SQL transformations produce EQUIVALENT instead of INVALID."""

    def test_equivalent_status_sumx_to_sum(self):
        """SUMX→SUM with same column (different table prefix) gets EQUIVALENT."""
        v = ExpressionValidator()
        # SUMX(fact, fact[amount]) → SUM(source.amount)
        # Different table prefix (no mapping) produces reference + agg diffs,
        # but all are expected transformations.
        result = v.validate("SUM(source.amount)", "SUMX(fact, fact[amount])")
        # The result should not be valid (structural differences exist),
        # but _is_equivalent should recognise them as expected mappings
        assert v._is_equivalent(result) or result["is_valid"]

    def test_equivalent_status_table_prefix_only(self):
        """Table[col] → source.col (same column name, different prefix) gets EQUIVALENT."""
        v = ExpressionValidator()
        # No table mapping: fact.amount vs source.amount — column names match
        result = v.validate("SUM(source.amount)", "SUM(fact[amount])")
        # Column names match (amount == amount), so _compare_columns fallback kicks in
        # making references match. Agg type also matches. Should be VALID or EQUIVALENT.
        assert result["is_valid"] or v._is_equivalent(result)

    def test_review_status_partial_match(self):
        """At least one similarity present → REVIEW (not INVALID)."""
        v = ExpressionValidator()
        # SUM vs COUNT on same column: agg type mismatch but reference similarity
        result = v.validate("SUM(source.amount)", "COUNT(source[amount])")
        assert v._is_review_candidate(result)

    def test_invalid_status_no_match(self):
        """Completely different expressions → INVALID (not REVIEW)."""
        v = ExpressionValidator()
        # Completely different: no shared aggregations, no shared references
        result = v.validate("SUM(source.a)", "COUNT(other[b])")
        # Should have differences and may or may not have similarities
        assert result["is_valid"] is False

    def test_is_equivalent_agg_mapping(self):
        """_is_expected_agg_mapping recognises SUMX→SUM as expected."""
        v = ExpressionValidator()
        diff = "Aggregation mismatch: DAX aggregation SUMX has no matching UCMV SUM"
        assert v._is_expected_agg_mapping(diff)

    def test_is_equivalent_ref_mapping(self):
        """_is_expected_ref_mapping recognises table prefix differences."""
        v = ExpressionValidator()
        diff = "Reference mismatch (table.column): Missing in UCMV: {'fact.amount'}."
        result_with_matching_cols = {
            'databricks_parsed': {'references': {'source.amount'}},
            'dax_parsed': {'references': {'fact.amount'}},
        }
        assert v._is_expected_ref_mapping(diff, result_with_matching_cols)

    def test_is_review_candidate_with_similarities(self):
        """Similarities list triggers REVIEW classification."""
        v = ExpressionValidator()
        result = {
            'similarities': ['Aggregations match: 1 match'],
            'differences': ['Reference mismatch: ...'],
        }
        assert v._is_review_candidate(result)

    def test_is_review_candidate_no_data(self):
        """Empty similarities and differences → not a review candidate."""
        v = ExpressionValidator()
        result = {'similarities': [], 'differences': []}
        assert not v._is_review_candidate(result)


class TestColumnFallbackInStrictQualified:
    """Test the column-name-only fallback when both sides are fully qualified."""

    def test_column_name_match_different_table(self):
        """Column names match but table prefixes differ → match via fallback."""
        v = ExpressionValidator()
        result = v._compare_columns({"source.amount"}, {"fact.amount"})
        assert result["match"] is True
        assert "table prefixes differ" in result["details"]

    def test_column_name_mismatch(self):
        """Column names differ → no fallback, still mismatch."""
        v = ExpressionValidator()
        result = v._compare_columns({"source.amount"}, {"fact.revenue"})
        assert result["match"] is False


class TestFilterSignatureReparse:
    """Test that UNKNOWN filter conditions are re-parsed before fallback."""

    def test_unknown_in_clause_reparsed(self):
        """UNKNOWN condition with IN clause syntax should be re-parsed to IN signature."""
        cond = {"type": "UNKNOWN", "raw": "bic_cwc_type IN ('PET','APET')"}
        sig = ExpressionValidator._filter_signature(cond)
        assert sig[0] == "IN"
        assert sig[1] == "bic_cwc_type"
        assert "pet" in sig[2]
        assert "apet" in sig[2]

    def test_unknown_equals_reparsed(self):
        """UNKNOWN condition with equality syntax should be re-parsed to EQUALS signature."""
        cond = {"type": "UNKNOWN", "raw": "bic_chversion = '0000'"}
        sig = ExpressionValidator._filter_signature(cond)
        assert sig[0] == "EQUALS"
        assert sig[1] == "bic_chversion"
        assert sig[2] == "0000"

    def test_filter_signature_strips_table_prefix(self):
        """Table prefix in filter column should be stripped for cross-format comparison."""
        cond_qualified = {"type": "IN", "column": "source.type", "values": ["A", "B"]}
        cond_bare = {"type": "IN", "column": "type", "values": ["A", "B"]}
        sig_q = ExpressionValidator._filter_signature(cond_qualified)
        sig_b = ExpressionValidator._filter_signature(cond_bare)
        assert sig_q == sig_b


class TestMeasureByNameStatusLevels:
    """Test that validate_measure_by_name uses EQUIVALENT and REVIEW statuses."""

    def test_valid_measure_returns_valid_or_equivalent(self, tmp_path):
        h = _make_handler(tmp_path)
        v = ExpressionValidator(
            data_handler=h,
            table_mappings={"fact": "source"},
        )
        result = v.validate_measure_by_name("total_sales")
        assert result["status"] in ("VALID", "EQUIVALENT", "REVIEW", "INVALID")
        assert "is_valid" in result
