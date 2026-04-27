"""Unit tests for FuzzyScorer in metadata_reduction package."""

import pytest
from src.engines.crewai.tools.custom.metadata_reduction.fuzzy_scorer import (
    FuzzyScorer,
    _fuzzy_score,
)


class TestNormalizeText:
    def test_strips_dim_prefix(self):
        assert "business unit" in FuzzyScorer.normalize_text("Dim_Business_Unit")

    def test_strips_fact_prefix(self):
        assert FuzzyScorer.normalize_text("FactSales_Monthly") == "sales monthly"

    def test_strips_fct_prefix(self):
        result = FuzzyScorer.normalize_text("Fct_Revenue")
        assert "revenue" in result
        assert "fct" not in result

    def test_strips_lkp_prefix(self):
        result = FuzzyScorer.normalize_text("Lkp_Country")
        assert "country" in result

    def test_strips_tbl_prefix(self):
        result = FuzzyScorer.normalize_text("tbl_Users")
        assert "users" in result

    def test_lowercases(self):
        assert FuzzyScorer.normalize_text("MyTable") == "mytable"

    def test_replaces_underscores_with_spaces(self):
        result = FuzzyScorer.normalize_text("first_name")
        assert " " in result or result == "first name"

    def test_replaces_hyphens_with_spaces(self):
        result = FuzzyScorer.normalize_text("first-name")
        assert " " in result

    def test_strips_accents(self):
        result = FuzzyScorer.normalize_text("café")
        assert result == "cafe"

    def test_removes_special_chars(self):
        result = FuzzyScorer.normalize_text("revenue ($)")
        assert "$" not in result
        assert "(" not in result

    def test_collapses_whitespace(self):
        result = FuzzyScorer.normalize_text("a   b   c")
        assert result == "a b c"

    def test_empty_string(self):
        assert FuzzyScorer.normalize_text("") == ""

    def test_none_returns_empty(self):
        assert FuzzyScorer.normalize_text(None) == ""


class TestExtractQuestionTokens:
    def setup_method(self):
        self.scorer = FuzzyScorer()

    def test_removes_stopwords(self):
        tokens = self.scorer.extract_question_tokens("What is the total revenue?")
        assert "what" not in tokens
        assert "is" not in tokens
        assert "the" not in tokens
        assert "total" in tokens
        assert "revenue" in tokens

    def test_returns_unique_tokens(self):
        tokens = self.scorer.extract_question_tokens("revenue revenue revenue")
        assert tokens.count("revenue") == 1

    def test_filters_short_tokens(self):
        tokens = self.scorer.extract_question_tokens("a b cd efg")
        assert "a" not in tokens
        assert "b" not in tokens
        assert "cd" in tokens
        assert "efg" in tokens

    def test_normalizes_tokens(self):
        tokens = self.scorer.extract_question_tokens("Revenue_Amount by Country")
        assert "revenue" in tokens or "revenue amount" in tokens

    def test_empty_question(self):
        tokens = self.scorer.extract_question_tokens("")
        assert tokens == []


class TestFuzzyScore:
    def test_exact_match(self):
        score = _fuzzy_score("revenue", "revenue", threshold=0)
        assert score == 100.0

    def test_case_insensitive(self):
        score = _fuzzy_score("Revenue", "revenue", threshold=0)
        assert score == 100.0

    def test_partial_match_above_threshold(self):
        score = _fuzzy_score("revenue", "total revenue", threshold=50)
        assert score >= 50.0

    def test_no_match_below_threshold(self):
        score = _fuzzy_score("xyz", "abc", threshold=70)
        assert score == 0.0

    def test_threshold_zero_always_returns_score(self):
        score = _fuzzy_score("hello", "world", threshold=0)
        assert isinstance(score, float)


class TestScoreTable:
    def setup_method(self):
        self.scorer = FuzzyScorer(synonym_threshold=70, boost_min=60.0)

    def test_matches_table_name(self):
        table = {"name": "Sales", "columns": [], "measures": []}
        score = self.scorer.score_table(table, ["sales"])
        assert score > 70

    def test_matches_column_name(self):
        table = {
            "name": "Transactions",
            "columns": [{"name": "Revenue"}],
            "measures": [],
        }
        score = self.scorer.score_table(table, ["revenue"])
        assert score > 70

    def test_matches_measure_name(self):
        table = {
            "name": "Facts",
            "columns": [],
            "measures": [{"name": "Total Revenue"}],
        }
        score = self.scorer.score_table(table, ["revenue"])
        assert score > 50

    def test_matches_column_description(self):
        table = {
            "name": "T1",
            "columns": [{"name": "Col1", "description": "Total annual revenue"}],
            "measures": [],
        }
        score = self.scorer.score_table(table, ["revenue"])
        assert score > 50

    def test_matches_synonyms(self):
        table = {
            "name": "T1",
            "columns": [{"name": "Col1", "synonyms": ["turnover", "income"]}],
            "measures": [],
        }
        score = self.scorer.score_table(table, ["turnover"])
        assert score > 70

    def test_matches_purpose(self):
        table = {
            "name": "T1",
            "purpose": "Contains all sales transaction data",
            "columns": [],
            "measures": [],
        }
        score = self.scorer.score_table(table, ["sales"])
        assert score > 50

    def test_no_tokens_returns_zero(self):
        table = {"name": "Sales", "columns": [], "measures": []}
        score = self.scorer.score_table(table, [])
        assert score == 0.0

    def test_empty_table_returns_zero(self):
        table = {"name": "", "columns": [], "measures": []}
        score = self.scorer.score_table(table, ["revenue"])
        assert score == 0.0


class TestScoreMeasure:
    def setup_method(self):
        self.scorer = FuzzyScorer()

    def test_matches_measure_name(self):
        measure = {"name": "Total Revenue"}
        score = self.scorer.score_measure(measure, ["revenue"])
        assert score > 50

    def test_matches_description(self):
        measure = {"name": "M1", "description": "Sum of all revenue"}
        score = self.scorer.score_measure(measure, ["revenue"])
        assert score > 50

    def test_matches_synonyms(self):
        measure = {"name": "M1", "synonyms": ["turnover", "income"]}
        score = self.scorer.score_measure(measure, ["income"])
        assert score > 70


class TestRankTables:
    def setup_method(self):
        self.scorer = FuzzyScorer(synonym_threshold=70, boost_min=60.0)
        self.tables = [
            {"name": "Sales", "columns": [{"name": "Revenue"}], "measures": []},
            {"name": "Geography", "columns": [{"name": "Country"}], "measures": []},
            {"name": "DateTable", "columns": [{"name": "Date"}], "measures": []},
            {"name": "Warehouses", "columns": [{"name": "Warehouse ID"}], "measures": []},
        ]

    def test_returns_sorted_by_score(self):
        ranked = self.scorer.rank_tables(self.tables, "total revenue by country")
        scores = [r["score"] for r in ranked]
        assert scores == sorted(scores, reverse=True)

    def test_marks_likely_relevant(self):
        ranked = self.scorer.rank_tables(self.tables, "total revenue by country")
        relevant = [r for r in ranked if r["likely_relevant"]]
        assert len(relevant) >= 1

    def test_returns_all_tables(self):
        ranked = self.scorer.rank_tables(self.tables, "anything")
        assert len(ranked) == len(self.tables)

    def test_includes_table_dict(self):
        ranked = self.scorer.rank_tables(self.tables, "revenue")
        assert "table" in ranked[0]
        assert "name" in ranked[0]["table"]


class TestRankMeasures:
    def setup_method(self):
        self.scorer = FuzzyScorer()
        self.measures = [
            {"name": "Total Revenue", "table": "Sales"},
            {"name": "Country Count", "table": "Geography"},
            {"name": "Average Date", "table": "Dates"},
        ]

    def test_returns_sorted_by_score(self):
        ranked = self.scorer.rank_measures(self.measures, "total revenue")
        scores = [r["score"] for r in ranked]
        assert scores == sorted(scores, reverse=True)

    def test_returns_all_measures(self):
        ranked = self.scorer.rank_measures(self.measures, "anything")
        assert len(ranked) == len(self.measures)
