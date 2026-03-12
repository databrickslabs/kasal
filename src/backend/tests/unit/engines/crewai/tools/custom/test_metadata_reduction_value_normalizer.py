"""Unit tests for ValueNormalizer in metadata_reduction package."""

import pytest
from src.engines.crewai.tools.custom.metadata_reduction.value_normalizer import (
    ValueNormalizer,
)


class TestDemonymStemMatch:
    def setup_method(self):
        self.normalizer = ValueNormalizer()

    def test_italian_to_italy(self):
        result = self.normalizer._demonym_stem_match("Italian", ["Italy", "Austria", "Germany"])
        assert result == "Italy"

    def test_german_to_germany(self):
        result = self.normalizer._demonym_stem_match("German", ["Italy", "Germany", "France"])
        assert result == "Germany"

    def test_japanese_to_japan(self):
        result = self.normalizer._demonym_stem_match("Japanese", ["Japan", "China", "Korea"])
        assert result == "Japan"

    def test_ambiguous_returns_none(self):
        # "Chin" stem matches both "China" and "Chinook" — ambiguous
        result = self.normalizer._demonym_stem_match("Chinese", ["China", "Chinook"])
        assert result is None

    def test_no_match_returns_none(self):
        result = self.normalizer._demonym_stem_match("Martian", ["Italy", "Germany"])
        assert result is None

    def test_short_stem_skipped(self):
        # "ic" suffix on "ic" → stem "" → too short
        result = self.normalizer._demonym_stem_match("ic", ["Iceland"])
        assert result is None

    def test_case_insensitive(self):
        result = self.normalizer._demonym_stem_match("ITALIAN", ["Italy", "France"])
        assert result == "Italy"


class TestTokenWordMatch:
    def setup_method(self):
        self.normalizer = ValueNormalizer()

    def test_exact_word_match(self):
        result = self.normalizer._token_word_match(
            "greek BU", ["BU Greece and Cyprus", "BU Germany", "BU Italy"]
        )
        assert result == "BU Greece and Cyprus"

    def test_no_match_returns_none(self):
        result = self.normalizer._token_word_match(
            "martian base", ["BU Greece", "BU Germany"]
        )
        assert result is None

    def test_stopwords_filtered(self):
        result = self.normalizer._token_word_match(
            "and the of", ["Value A", "Value B"]
        )
        assert result is None

    def test_short_tokens_filtered(self):
        result = self.normalizer._token_word_match(
            "ab cd", ["Something", "Another"]
        )
        assert result is None

    def test_ambiguous_returns_none(self):
        result = self.normalizer._token_word_match(
            "value", ["Value A", "Value B"]
        )
        assert result is None


class TestCorrectSingleValue:
    def setup_method(self):
        self.normalizer = ValueNormalizer()
        self.candidates = ["Austria", "Germany", "Italy", "France", "Spain"]

    def test_exact_match(self):
        result = self.normalizer._correct_single_value("Austria", self.candidates)
        assert result["corrected"] == "Austria"
        assert result["status"] == "exact_match"
        assert result["method"] == "exact"

    def test_case_insensitive_exact(self):
        result = self.normalizer._correct_single_value("austria", self.candidates)
        assert result["corrected"] == "Austria"
        assert result["status"] == "exact_match"

    def test_typo_correction(self):
        result = self.normalizer._correct_single_value("Austira", self.candidates)
        assert result["corrected"] == "Austria"
        assert result["status"] == "corrected"
        assert result["method"] == "difflib_close_match"

    def test_demonym_correction(self):
        # "Italian" is close enough to "Italy" for difflib to catch it first
        result = self.normalizer._correct_single_value("Italian", self.candidates)
        assert result["corrected"] == "Italy"
        assert result["status"] == "corrected"
        # difflib catches this before demonym step
        assert result["method"] in ("difflib_close_match", "demonym_stem")

    def test_unresolved(self):
        result = self.normalizer._correct_single_value("Mars", self.candidates)
        assert result["corrected"] == "Mars"
        assert result["status"] == "unresolved"
        assert result["method"] == "none"


class TestParseFilterKey:
    def setup_method(self):
        self.normalizer = ValueNormalizer()

    def test_dot_format(self):
        table, col = self.normalizer._parse_filter_key("Geography.Country")
        assert table == "Geography"
        assert col == "Country"

    def test_bracket_format(self):
        table, col = self.normalizer._parse_filter_key("'Geography'[Country]")
        assert table == "Geography"
        assert col == "Country"

    def test_bracket_no_quotes(self):
        table, col = self.normalizer._parse_filter_key("Geography[Country]")
        assert table == "Geography"
        assert col == "Country"

    def test_invalid_format(self):
        table, col = self.normalizer._parse_filter_key("JustAString")
        assert table is None
        assert col is None


class TestCollectKnownValues:
    def setup_method(self):
        self.normalizer = ValueNormalizer()

    def test_collects_from_sample_data(self):
        sample_data = {
            "Geography": [
                {"Country": "Austria"},
                {"Country": "Germany"},
            ]
        }
        values = self.normalizer._collect_known_values(
            "Geography", "Country", sample_data, []
        )
        assert "Austria" in values
        assert "Germany" in values

    def test_collects_from_slicers(self):
        slicers = [
            {"table": "Geography", "column": "Country", "values": ["Austria", "Germany"]},
        ]
        values = self.normalizer._collect_known_values(
            "Geography", "Country", {}, slicers
        )
        assert "Austria" in values
        assert "Germany" in values

    def test_case_insensitive_slicer_match(self):
        slicers = [
            {"table": "geography", "column": "country", "values": ["Austria"]},
        ]
        values = self.normalizer._collect_known_values(
            "Geography", "Country", {}, slicers
        )
        assert "Austria" in values

    def test_deduplicates(self):
        sample_data = {"Geography": [{"Country": "Austria"}]}
        slicers = [{"table": "Geography", "column": "Country", "values": ["Austria"]}]
        values = self.normalizer._collect_known_values(
            "Geography", "Country", sample_data, slicers
        )
        assert values.count("Austria") == 1

    def test_returns_sorted(self):
        sample_data = {
            "Geography": [
                {"Country": "Germany"},
                {"Country": "Austria"},
                {"Country": "France"},
            ]
        }
        values = self.normalizer._collect_known_values(
            "Geography", "Country", sample_data, []
        )
        assert values == sorted(values)

    def test_empty_returns_empty(self):
        values = self.normalizer._collect_known_values("X", "Y", {}, [])
        assert values == []


class TestNormalizeFilterValues:
    def setup_method(self):
        self.normalizer = ValueNormalizer()
        self.sample_data = {
            "Geography": [
                {"Country": "Austria", "Region": "Europe"},
                {"Country": "Germany", "Region": "Europe"},
                {"Country": "Italy", "Region": "Europe"},
                {"Country": "Japan", "Region": "Asia"},
            ]
        }

    def test_corrects_typo(self):
        result = self.normalizer.normalize_filter_values(
            {"Geography.Country": "Austira"}, self.sample_data, []
        )
        assert result["Geography.Country"] == "Austria"

    def test_corrects_demonym(self):
        result = self.normalizer.normalize_filter_values(
            {"Geography.Country": "Italian"}, self.sample_data, []
        )
        assert result["Geography.Country"] == "Italy"

    def test_passes_through_exact_match(self):
        result = self.normalizer.normalize_filter_values(
            {"Geography.Country": "Germany"}, self.sample_data, []
        )
        assert result["Geography.Country"] == "Germany"

    def test_handles_list_values(self):
        result = self.normalizer.normalize_filter_values(
            {"Geography.Country": ["Austira", "Germany"]}, self.sample_data, []
        )
        assert result["Geography.Country"] == ["Austria", "Germany"]

    def test_includes_normalization_log(self):
        result = self.normalizer.normalize_filter_values(
            {"Geography.Country": "Austria"}, self.sample_data, []
        )
        assert "_normalization_log" in result
        assert len(result["_normalization_log"]) > 0

    def test_empty_filters_returns_empty_log(self):
        result = self.normalizer.normalize_filter_values({}, {}, [])
        assert result == {"_normalization_log": []}

    def test_unparseable_key_passed_through(self):
        result = self.normalizer.normalize_filter_values(
            {"JustAString": "value"}, self.sample_data, []
        )
        assert result["JustAString"] == "value"

    def test_no_reference_values_passed_through(self):
        result = self.normalizer.normalize_filter_values(
            {"Unknown.Column": "value"}, {}, []
        )
        assert result["Unknown.Column"] == "value"

    def test_unresolved_value_passed_through(self):
        result = self.normalizer.normalize_filter_values(
            {"Geography.Country": "Mars"}, self.sample_data, []
        )
        # Mars doesn't match anything — passes through unchanged
        assert result["Geography.Country"] == "Mars"
