"""Tests for period_folding: detecting a report's closed-period calendar columns
and folding canonical period dimensions into every metric view."""

from src.services.tools.metric_view_utils.period_folding import (
    PeriodConfig,
    detect_period_columns,
    fold_period_dimensions,
    has_closed_period_logic,
)

# A representative Power BI calendar column set (mixed casing / separators).
PBI_CALENDAR_COLS = [
    "date_id",
    "Year",
    "Month",
    "MonthName",
    "Past_flag",
    "Latest_Month_Label",
    "Latest_Year_Label",
    "fiscper",
    "Fiscal_Year",
    "Fiscal_Month",
]


class TestDetectPeriodColumns:
    def test_maps_closed_period_columns(self):
        resolved = detect_period_columns(PBI_CALENDAR_COLS)
        assert resolved["past_flag"] == "Past_flag"
        assert resolved["latest_month_label"] == "Latest_Month_Label"
        assert resolved["latest_year_label"] == "Latest_Year_Label"

    def test_maps_fiscal_columns(self):
        resolved = detect_period_columns(PBI_CALENDAR_COLS)
        assert resolved["fiscper"] == "fiscper"
        assert resolved["fiscal_year"] == "Fiscal_Year"
        assert resolved["fiscal_month"] == "Fiscal_Month"

    def test_normalisation_handles_spaces_and_case(self):
        resolved = detect_period_columns(
            ["latest month label", "PASTFLAG", "LatestYearLabel"]
        )
        assert resolved["latest_month_label"] == "latest month label"
        assert resolved["past_flag"] == "PASTFLAG"
        assert resolved["latest_year_label"] == "LatestYearLabel"

    def test_each_source_column_claimed_once(self):
        # 'fiscper' must not also be consumed as fiscal_year, and vice versa.
        resolved = detect_period_columns(["fiscper", "fiscal_year"])
        assert resolved["fiscper"] == "fiscper"
        assert resolved["fiscal_year"] == "fiscal_year"

    def test_unrelated_columns_ignored(self):
        resolved = detect_period_columns(["comp_code", "plant_name", "revenue"])
        assert resolved == {}

    def test_empty_input(self):
        assert detect_period_columns([]) == {}
        assert detect_period_columns(None) == {}


class TestHasClosedPeriodLogic:
    def test_true_when_closed_period_column_present(self):
        assert has_closed_period_logic(["date_id", "Past_flag"]) is True
        assert has_closed_period_logic(["Latest_Month_Label"]) is True

    def test_false_for_plain_date_table(self):
        # Fiscal columns alone are not the report's period calendar.
        assert has_closed_period_logic(["date_id", "Year", "Month", "fiscper"]) is False

    def test_false_for_empty(self):
        assert has_closed_period_logic([]) is False


class TestFoldPeriodDimensions:
    def test_folds_all_period_dims_with_alias(self):
        dims = fold_period_dimensions("dim_calendar", PBI_CALENDAR_COLS)
        names = {d["name"] for d in dims}
        assert {
            "past_flag",
            "latest_month_label",
            "latest_year_label",
            "fiscper",
            "fiscal_year",
            "fiscal_month",
        } <= names

    def test_expr_uses_calendar_alias_and_source_column(self):
        dims = fold_period_dimensions("cal", PBI_CALENDAR_COLS)
        by_name = {d["name"]: d for d in dims}
        assert by_name["past_flag"]["expr"] == "cal.Past_flag"
        assert by_name["latest_month_label"]["expr"] == "cal.Latest_Month_Label"

    def test_dims_carry_metadata(self):
        dims = fold_period_dimensions("cal", PBI_CALENDAR_COLS)
        pf = next(d for d in dims if d["name"] == "past_flag")
        assert pf["display_name"] == "Closed Period Flag"
        assert "closed period" in pf["synonyms"]
        assert pf["comment"]

    def test_skips_existing_dimensions_case_insensitive(self):
        dims = fold_period_dimensions(
            "cal", PBI_CALENDAR_COLS, existing_dim_names={"Fiscal_Year", "fiscper"}
        )
        names = {d["name"] for d in dims}
        assert "fiscal_year" not in names
        assert "fiscper" not in names
        # closed-period labels still folded in
        assert "past_flag" in names

    def test_returns_empty_without_closed_period_logic(self):
        # A plain date table → nothing folded, even though fiscal cols match.
        assert fold_period_dimensions("cal", ["Year", "Month", "fiscper"]) == []

    def test_returns_empty_without_alias(self):
        assert fold_period_dimensions("", PBI_CALENDAR_COLS) == []

    def test_no_duplicate_names(self):
        dims = fold_period_dimensions("cal", PBI_CALENDAR_COLS)
        names = [d["name"] for d in dims]
        assert len(names) == len(set(names))


# ─── PeriodConfig (crew-supplied column overrides) ───────────────────────────


class TestPeriodConfigOverride:
    def test_override_maps_nonstandard_flag_name(self):
        """A model whose closed-period flag is named 'IsCurrentMonth' is detected
        via an explicit override, though the pattern would miss it."""
        cols = ["date_id", "IsCurrentMonth", "fiscper"]
        assert has_closed_period_logic(cols) is False  # pattern misses it
        cfg = PeriodConfig.from_dict(
            {"column_overrides": {"latest_month_label": "IsCurrentMonth"}}
        )
        assert has_closed_period_logic(cols, cfg) is True
        resolved = detect_period_columns(cols, cfg)
        assert resolved["latest_month_label"] == "IsCurrentMonth"

    def test_override_used_in_fold(self):
        cols = ["IsCurrentMonth", "fiscper"]
        cfg = PeriodConfig.from_dict(
            {"column_overrides": {"latest_month_label": "IsCurrentMonth"}}
        )
        dims = fold_period_dimensions("cal", cols, config=cfg)
        by_name = {d["name"]: d for d in dims}
        assert by_name["latest_month_label"]["expr"] == "cal.IsCurrentMonth"

    def test_from_dict_ignores_unknown_canonical_and_junk(self):
        cfg = PeriodConfig.from_dict(
            {"column_overrides": {"not_a_period": "X", "past_flag": ""}}
        )
        assert cfg.column_overrides == {}

    def test_from_dict_none_returns_defaults(self):
        cfg = PeriodConfig.from_dict(None)
        assert cfg.column_overrides == {}
        # defaults still detect standard columns
        assert has_closed_period_logic(["Past_flag"], cfg) is True


class TestDetectPriorPeriodColumn:
    """Prior-period key detection (phase-2 time-intelligence self-join)."""

    def test_detects_date_py_and_variants(self):
        from src.services.tools.metric_view_utils.period_folding import (
            detect_prior_period_column,
        )

        assert detect_prior_period_column(["date_id", "date_py"]) == "date_py"
        assert detect_prior_period_column(["Fiscper_PY", "x"]) == "Fiscper_PY"
        assert detect_prior_period_column(["py_fiscper"]) == "py_fiscper"
        assert detect_prior_period_column(["prev_year_date"]) == "prev_year_date"

    def test_none_when_no_prior_period_column(self):
        from src.services.tools.metric_view_utils.period_folding import (
            detect_prior_period_column,
        )

        assert detect_prior_period_column(["date_id", "fiscper", "year"]) is None
        assert detect_prior_period_column([]) is None
        assert detect_prior_period_column(None) is None
