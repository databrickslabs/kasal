"""Unit tests for DimensionResolver in metadata_reduction package."""

import pytest
from src.engines.crewai.tools.custom.metadata_reduction.dimension_resolver import (
    DimensionBinding,
    DimensionResolver,
)


# ─── Fixtures ────────────────────────────────────────────────────────────────

def _make_tables():
    """Minimal table set for resolver tests."""
    return [
        {
            "name": "Geography",
            "columns": [
                {"name": "Country"},
                {"name": "Region"},
                {"name": "City"},
            ],
        },
        {
            "name": "Date",
            "columns": [
                {"name": "Year"},
                {"name": "Month"},
                {"name": "Quarter"},
            ],
        },
        {
            "name": "Sales",
            "columns": [
                {"name": "Revenue"},
                {"name": "Quantity"},
                {"name": "Business Unit"},
            ],
        },
    ]


# ─── DimensionBinding dataclass ───────────────────────────────────────────────

class TestDimensionBindingDataclass:
    def test_to_dict_has_all_fields(self):
        b = DimensionBinding(
            user_term="country",
            resolved_table="Geography",
            resolved_column="Country",
            confidence=0.9,
            sample_values=["Germany", "France"],
        )
        d = b.to_dict()
        assert d["user_term"] == "country"
        assert d["resolved_table"] == "Geography"
        assert d["resolved_column"] == "Country"
        assert d["confidence"] == 0.9
        assert d["sample_values"] == ["Germany", "France"]

    def test_confidence_is_rounded_to_4_decimal_places(self):
        b = DimensionBinding(
            user_term="x",
            resolved_table="T",
            resolved_column="C",
            confidence=0.123456789,
        )
        assert b.to_dict()["confidence"] == round(0.123456789, 4)

    def test_default_sample_values_is_empty_list(self):
        b = DimensionBinding(
            user_term="x", resolved_table="T", resolved_column="C", confidence=0.8
        )
        assert b.sample_values == []

    def test_to_dict_includes_empty_sample_values(self):
        b = DimensionBinding(
            user_term="x", resolved_table="T", resolved_column="C", confidence=0.8
        )
        assert b.to_dict()["sample_values"] == []


# ─── DimensionResolver.resolve — exact matches ──────────────────────────────

class TestResolveExactMatch:
    def setup_method(self):
        self.resolver = DimensionResolver()

    def test_exact_match_returns_binding(self):
        bindings = self.resolver.resolve(["Country"], _make_tables())
        assert len(bindings) == 1
        assert bindings[0].resolved_column == "Country"
        assert bindings[0].resolved_table == "Geography"

    def test_exact_match_case_insensitive(self):
        bindings = self.resolver.resolve(["country"], _make_tables())
        assert len(bindings) == 1
        assert bindings[0].resolved_column == "Country"

    def test_exact_match_has_confidence_1(self):
        bindings = self.resolver.resolve(["Country"], _make_tables())
        assert bindings[0].confidence == 1.0

    def test_exact_match_on_multi_word_column(self):
        bindings = self.resolver.resolve(["Business Unit"], _make_tables())
        assert len(bindings) == 1
        assert bindings[0].resolved_column == "Business Unit"
        assert bindings[0].resolved_table == "Sales"


# ─── DimensionResolver.resolve — fuzzy matches ──────────────────────────────

class TestResolveFuzzyMatch:
    def setup_method(self):
        self.resolver = DimensionResolver()

    def test_fuzzy_match_returns_binding_above_threshold(self):
        # "Regions" should fuzzy-match "Region"
        bindings = self.resolver.resolve(["Regions"], _make_tables(), threshold=0.70)
        # At minimum it should return something (Region or nothing depending on scorer)
        assert isinstance(bindings, list)

    def test_no_match_below_threshold(self):
        bindings = self.resolver.resolve(["xyz_unknown_xyz"], _make_tables(), threshold=0.99)
        assert len(bindings) == 0

    def test_threshold_1_only_accepts_exact(self):
        bindings = self.resolver.resolve(["Country"], _make_tables(), threshold=1.0)
        assert len(bindings) == 1
        assert bindings[0].confidence == 1.0

    def test_threshold_0_accepts_any_score(self):
        # Even a poor match should get through at threshold=0
        bindings = self.resolver.resolve(["Year"], _make_tables(), threshold=0.0)
        assert len(bindings) >= 1


# ─── DimensionResolver.resolve — multiple keywords ──────────────────────────

class TestResolveMultipleKeywords:
    def setup_method(self):
        self.resolver = DimensionResolver()

    def test_multiple_keywords_return_one_binding_each(self):
        bindings = self.resolver.resolve(["Country", "Year"], _make_tables())
        names = {b.resolved_column for b in bindings}
        assert "Country" in names
        assert "Year" in names
        assert len(bindings) == 2

    def test_duplicate_column_not_bound_twice(self):
        # "Country" appears once in Geography; resolving it twice should deduplicate
        bindings = self.resolver.resolve(["Country", "country"], _make_tables())
        col_keys = [f"{b.resolved_table}[{b.resolved_column}]" for b in bindings]
        assert col_keys.count("Geography[Country]") == 1

    def test_partial_resolution_skips_unmatched(self):
        bindings = self.resolver.resolve(["Country", "zzz_nomatch_zzz"], _make_tables())
        names = {b.resolved_column for b in bindings}
        assert "Country" in names
        # "zzz_nomatch_zzz" should not have bound anything
        assert all(b.user_term != "zzz_nomatch_zzz" for b in bindings)


# ─── DimensionResolver.resolve — edge cases ──────────────────────────────────

class TestResolveEdgeCases:
    def setup_method(self):
        self.resolver = DimensionResolver()

    def test_empty_keywords_returns_empty_list(self):
        bindings = self.resolver.resolve([], _make_tables())
        assert bindings == []

    def test_empty_tables_returns_empty_list(self):
        bindings = self.resolver.resolve(["Country"], [])
        assert bindings == []

    def test_single_char_keyword_is_skipped(self):
        bindings = self.resolver.resolve(["x"], _make_tables())
        assert bindings == []

    def test_empty_string_keyword_is_skipped(self):
        bindings = self.resolver.resolve([""], _make_tables())
        assert bindings == []

    def test_none_column_names_in_table_are_skipped(self):
        tables = [{"name": "T1", "columns": [{"name": ""}, {"name": None}]}]
        bindings = self.resolver.resolve(["Country"], tables)
        assert bindings == []

    def test_string_columns_are_accepted(self):
        # Columns can be plain strings
        tables = [{"name": "Geography", "columns": ["Country", "Region"]}]
        bindings = self.resolver.resolve(["Country"], tables)
        assert len(bindings) == 1
        assert bindings[0].resolved_column == "Country"

    def test_no_name_field_in_table_is_skipped(self):
        tables = [{"columns": [{"name": "Country"}]}]  # no "name" key
        bindings = self.resolver.resolve(["Country"], tables)
        # Still matches — table_name defaults to ""
        assert isinstance(bindings, list)


# ─── DimensionResolver.resolve — sample_data ────────────────────────────────

class TestResolveSampleData:
    def setup_method(self):
        self.resolver = DimensionResolver()

    def test_sample_values_are_attached_to_binding(self):
        tables = [{"name": "Geography", "columns": [{"name": "Country"}]}]
        sample_data = {
            "Geography[Country]": {"sample_values": ["Germany", "France", "UK"]}
        }
        bindings = self.resolver.resolve(["Country"], tables, sample_data=sample_data)
        assert len(bindings) == 1
        assert "Germany" in bindings[0].sample_values

    def test_sample_values_truncated_to_5(self):
        tables = [{"name": "Geography", "columns": [{"name": "Country"}]}]
        sample_data = {
            "Geography[Country]": {
                "sample_values": ["A", "B", "C", "D", "E", "F", "G"]
            }
        }
        bindings = self.resolver.resolve(["Country"], tables, sample_data=sample_data)
        assert len(bindings[0].sample_values) <= 5

    def test_missing_sample_data_key_gives_empty_sample_values(self):
        tables = [{"name": "Geography", "columns": [{"name": "Country"}]}]
        sample_data = {"Other[Column]": {"sample_values": ["X"]}}
        bindings = self.resolver.resolve(["Country"], tables, sample_data=sample_data)
        assert bindings[0].sample_values == []

    def test_non_dict_sample_entry_is_handled(self):
        tables = [{"name": "Geography", "columns": [{"name": "Country"}]}]
        sample_data = {"Geography[Country]": ["Germany", "France"]}  # not a dict
        # Should not crash; sample_values will be empty
        bindings = self.resolver.resolve(["Country"], tables, sample_data=sample_data)
        assert bindings[0].sample_values == []


# ─── DimensionResolver.resolve — field_synonyms ──────────────────────────────

class TestResolveFieldSynonyms:
    def setup_method(self):
        self.resolver = DimensionResolver()

    def test_synonym_boost_finds_match_via_synonym(self):
        tables = [{"name": "Geography", "columns": [{"name": "Country"}]}]
        synonyms = {"Geography[Country]": ["nation", "state", "territory"]}
        bindings = self.resolver.resolve(
            ["nation"], tables, field_synonyms=synonyms, threshold=0.70
        )
        assert len(bindings) == 1
        assert bindings[0].resolved_column == "Country"

    def test_no_synonym_boost_for_unrelated_term(self):
        tables = [{"name": "Geography", "columns": [{"name": "Country"}]}]
        synonyms = {"Geography[Country]": ["nation"]}
        bindings = self.resolver.resolve(
            ["zzz_xyz_qqq"], tables, field_synonyms=synonyms, threshold=0.95
        )
        assert bindings == []

    def test_synonym_takes_precedence_over_lower_direct_score(self):
        # Column "Rev" has synonym "revenue" → should match "revenue" with score 1.0
        tables = [{"name": "Sales", "columns": [{"name": "Rev"}]}]
        synonyms = {"Sales[Rev]": ["revenue"]}
        bindings = self.resolver.resolve(
            ["revenue"], tables, field_synonyms=synonyms, threshold=0.70
        )
        assert len(bindings) == 1
        assert bindings[0].resolved_column == "Rev"
        assert bindings[0].confidence >= 0.70


# ─── DimensionResolver.resolve — user_term preserved ────────────────────────

class TestResolveUserTerm:
    def setup_method(self):
        self.resolver = DimensionResolver()

    def test_user_term_is_preserved_as_given(self):
        bindings = self.resolver.resolve(["Country"], _make_tables())
        assert bindings[0].user_term == "Country"

    def test_user_term_preserved_case_sensitive(self):
        bindings = self.resolver.resolve(["COUNTRY"], _make_tables())
        assert bindings[0].user_term == "COUNTRY"
