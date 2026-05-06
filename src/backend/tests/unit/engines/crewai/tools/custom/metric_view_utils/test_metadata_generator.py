"""Tests for metadata generator."""
import pytest
from src.engines.crewai.tools.custom.metric_view_utils.metadata_generator import MetadataGenerator


class TestMetadataGeneratorDefaults:
    def test_default_no_prefix_stripping(self):
        gen = MetadataGenerator()
        meta = gen.get_measure_meta("bic_revenue")
        assert meta['display_name'] == "Bic Revenue"

    def test_measure_display_name_humanized(self):
        gen = MetadataGenerator()
        meta = gen.get_measure_meta("total_net_value")
        assert meta['display_name'] == "Total Net Value"


class TestMetadataGeneratorPrefixStripping:
    def test_configured_prefix_stripping(self):
        gen = MetadataGenerator(name_prefixes_to_strip=("bic_",))
        meta = gen.get_measure_meta("bic_revenue")
        assert meta['display_name'] == "Revenue"

    def test_short_name_not_stripped(self):
        """Names too short after stripping are NOT stripped (guard: len > prefix + 3)."""
        gen = MetadataGenerator(name_prefixes_to_strip=("bic_",))
        meta = gen.get_measure_meta("bic_ab")
        # "bic_ab" -> len("bic_ab")=6, len("bic_")=4, 6 > 4+3=7? No -> not stripped
        assert meta['display_name'] == "Bic Ab"

    def test_multiple_prefixes(self):
        gen = MetadataGenerator(name_prefixes_to_strip=("bic_", "zz_"))
        meta = gen.get_measure_meta("zz_total_cost")
        assert meta['display_name'] == "Total Cost"


class TestMetadataGeneratorFormats:
    def test_percentage_detection_pct(self):
        gen = MetadataGenerator()
        meta = gen.get_measure_meta("turnover_pct")
        assert 'format' in meta
        assert meta['format']['type'] == 'percentage'

    def test_percentage_detection_ratio(self):
        gen = MetadataGenerator()
        meta = gen.get_measure_meta("conversion_ratio")
        assert meta['format']['type'] == 'percentage'

    def test_percentage_detection_rate(self):
        gen = MetadataGenerator()
        meta = gen.get_measure_meta("fill_rate")
        assert meta['format']['type'] == 'percentage'

    def test_percentage_detection_share(self):
        gen = MetadataGenerator()
        meta = gen.get_measure_meta("market_share")
        assert meta['format']['type'] == 'percentage'

    def test_currency_detection_cost(self):
        gen = MetadataGenerator()
        meta = gen.get_measure_meta("total_cost")
        assert 'format' in meta
        assert meta['format']['type'] == 'number'

    def test_currency_detection_revenue(self):
        gen = MetadataGenerator()
        meta = gen.get_measure_meta("net_revenue")
        assert meta['format']['type'] == 'number'

    def test_currency_detection_amount(self):
        gen = MetadataGenerator()
        meta = gen.get_measure_meta("order_amount")
        assert meta['format']['type'] == 'number'

    def test_currency_detection_budget(self):
        gen = MetadataGenerator()
        meta = gen.get_measure_meta("annual_budget")
        assert meta['format']['type'] == 'number'

    def test_no_format_for_count(self):
        gen = MetadataGenerator()
        meta = gen.get_measure_meta("order_count")
        assert 'format' not in meta

    def test_decimal_places_in_format(self):
        gen = MetadataGenerator()
        meta = gen.get_measure_meta("fill_rate")
        assert meta['format']['decimal_places']['places'] == 2


class TestMetadataGeneratorDimensions:
    def test_dimension_meta_default(self):
        gen = MetadataGenerator()
        meta = gen.get_dimension_meta("comp_code")
        assert 'display_name' in meta
        assert meta['display_name'] == "Comp Code"

    def test_dimension_override_by_table(self):
        gen = MetadataGenerator(dimension_metadata={
            "fact": {"region": {"display_name": "Region Name", "comment": "Sales region"}}
        })
        meta = gen.get_dimension_meta("region", table_key="fact")
        assert meta['display_name'] == "Region Name"

    def test_dimension_override_missing_table(self):
        gen = MetadataGenerator(dimension_metadata={
            "fact": {"region": {"display_name": "Region Name"}}
        })
        meta = gen.get_dimension_meta("region", table_key="other")
        assert meta['display_name'] == "Region"

    def test_column_metadata_global(self):
        gen = MetadataGenerator(column_metadata={
            "plant": ("Plant Name", ["factory", "site"]),
        })
        meta = gen.get_dimension_meta("plant")
        assert meta['display_name'] == "Plant Name"
        assert meta['synonyms'] == ["factory", "site"]


class TestMetadataGeneratorHelpers:
    def test_comment_override(self):
        gen = MetadataGenerator(comment_overrides={"fact_sales": "Custom comment"})
        assert gen.get_comment_override("fact_sales") == "Custom comment"

    def test_comment_override_missing(self):
        gen = MetadataGenerator()
        assert gen.get_comment_override("fact_sales") is None

    def test_dimension_exclusions(self):
        gen = MetadataGenerator(dimension_exclusions={"fact": {"internal_id", "tmp_col"}})
        excl = gen.get_dimension_exclusions("fact")
        assert "internal_id" in excl
        assert "tmp_col" in excl

    def test_dimension_exclusions_empty(self):
        gen = MetadataGenerator()
        assert gen.get_dimension_exclusions("fact") == set()

    def test_dimension_order(self):
        gen = MetadataGenerator(dimension_order={"fact": ["region", "year", "plant"]})
        order = gen.get_dimension_order("fact")
        assert order == ["region", "year", "plant"]

    def test_dimension_order_empty(self):
        gen = MetadataGenerator()
        assert gen.get_dimension_order("fact") == []


class TestHumanize:
    def test_numeric_prefix_stripped(self):
        gen = MetadataGenerator()
        meta = gen.get_measure_meta("12345_total_sales")
        # _humanize strips 3-6 digit numeric prefixes
        assert meta['display_name'] == "Total Sales"

    def test_empty_after_strip_returns_measure(self):
        gen = MetadataGenerator(name_prefixes_to_strip=("all_",))
        # "all_" prefix, remainder "" -> falls back to "Measure"
        # Actually "all_" len=4, "all_x" len=5, 5 > 4+3=7? No, so not stripped
        meta = gen.get_measure_meta("all_x")
        assert meta['display_name'] == "All X"

    def test_pure_numeric_name(self):
        gen = MetadataGenerator()
        meta = gen.get_measure_meta("12345")
        # 5 digits stripped, nothing left -> "Measure"
        assert meta['display_name'] == "Measure"
