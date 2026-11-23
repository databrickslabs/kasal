"""Which calculated columns become real source-view columns (``calc_column_views``)."""

from src.services.tools.metric_view_utils.calc_column_views import (
    calculated_column_views_enabled,
    plan_calc_columns,
    translated_calc_names,
)

DATE_KEY = "YEAR(Fact[posting_date]) * 10000 + MONTH(Fact[posting_date]) * 100"
EXPOSED = ["plant_code", "posting_date", "amount"]
KINDS = {"Fact": {"posting_date": "date", "amount": "number"}}
OLD_TODO = "DAX calculated column — verify/translate before use"


def _config(*columns: dict, kinds: dict | None = KINDS, **extra) -> dict:
    cfg = {"calculated_columns": {"Fact": list(columns)}, **extra}
    if kinds is not None:
        cfg["calculated_column_kinds"] = kinds
    return cfg


def _plan(config: dict, exposed=EXPOSED) -> list:
    return plan_calc_columns("Fact", config, exposed)


class TestSwitch:
    def test_default_is_on(self):
        assert calculated_column_views_enabled({}) is True
        assert calculated_column_views_enabled(None) is True

    def test_off_values_are_recognised(self):
        for raw in (False, "false", "False", "0", "no", "off"):
            assert (
                calculated_column_views_enabled(
                    {"calculated_columns_in_source_layer": raw}
                )
                is False
            )

    def test_on_values_are_recognised(self):
        assert (
            calculated_column_views_enabled(
                {"calculated_columns_in_source_layer": "true"}
            )
            is True
        )


class TestTranslated:
    def test_translatable_formula_becomes_a_translated_column(self):
        [col] = _plan(_config({"name": "date_key", "expression": DATE_KEY}))
        assert col.translation_status == "translated"
        assert col.name == "date_key"
        assert "year(" in col.sql_expr and "`posting_date`" in col.sql_expr

    def test_date_function_reads_its_argument_as_a_date_without_a_kind(self):
        # YEAR() takes a date in DAX, so the translator reads the argument that way itself.
        [col] = _plan(_config({"name": "date_key", "expression": DATE_KEY}, kinds=None))
        assert col.translation_status == "translated"
        assert "`posting_date`" in col.sql_expr

    def test_kinds_of_columns_the_view_lacks_are_ignored(self):
        kinds = {"Fact": {"posting_date": "date", "ghost": "number"}}
        [col] = _plan(
            _config({"name": "ok", "expression": "Fact[amount] + 1"}, kinds=kinds)
        )
        assert col.translation_status == "translated"


class TestTodo:
    def test_switch_off_keeps_the_old_todo_text(self):
        cfg = _config(
            {"name": "date_key", "expression": DATE_KEY},
            calculated_columns_in_source_layer=False,
        )
        [col] = _plan(cfg)
        assert (col.translation_status, col.comment) == ("todo", OLD_TODO)

    def test_unknown_view_columns_keep_the_old_todo_text(self):
        [col] = _plan(
            _config({"name": "date_key", "expression": DATE_KEY}), exposed=None
        )
        assert (col.translation_status, col.comment) == ("todo", OLD_TODO)

    def test_refused_formula_keeps_todo_with_its_reason(self):
        [col] = _plan(
            _config(
                {
                    "name": "date_key",
                    "expression": "DATEDIFF(Fact[posting_date], TODAY(), DAY)",
                }
            )
        )
        assert col.translation_status == "todo"
        assert col.comment.startswith("DAX calculated column not translated: ")
        assert "TODAY()" in col.comment

    def test_reference_to_another_calculated_column_is_refused(self):
        cfg = _config(
            {"name": "date_key", "expression": "Fact[other_key] + 1"},
            {"name": "other_key", "expression": "Fact[amount] * 2"},
        )
        cols = {c.name: c for c in _plan(cfg)}
        assert cols["date_key"].translation_status == "todo"
        assert cols["other_key"].translation_status == "translated"

    def test_related_is_refused_because_the_source_view_cannot_join(self):
        [col] = _plan(
            _config({"name": "label", "expression": "RELATED(Dim_Cal[month_name])"})
        )
        assert col.translation_status == "todo"

    def test_name_that_is_not_an_identifier_is_not_emitted(self):
        [col] = _plan(_config({"name": "Date Key", "expression": "amount + 1"}))
        assert col.translation_status == "todo"
        assert "plain identifier" in col.comment

    def test_name_of_an_existing_column_is_not_emitted_again(self):
        [col] = _plan(_config({"name": "AMOUNT", "expression": "amount + 1"}))
        assert col.translation_status == "todo"
        assert "already has a column" in col.comment

    def test_empty_formula_is_todo(self):
        cfg = {"calculated_columns": {"Fact": [{"name": "x", "expression": "  "}]}}
        [col] = _plan(cfg)
        assert col.translation_status == "todo"


class TestNoCalculatedColumns:
    def test_table_without_entries_gives_nothing(self):
        assert _plan({"calculated_columns": {}}) == []
        assert _plan({}) == []

    def test_planned_names_are_only_the_translated_ones(self):
        cfg = _config(
            {"name": "date_key", "expression": DATE_KEY},
            {"name": "label", "expression": "TODAY()"},
        )
        names = translated_calc_names(cfg, {"Fact": EXPOSED, "Other": []})
        assert names == {"Fact": ["date_key"]}

    def test_no_known_columns_means_no_translated_names(self):
        cfg = _config({"name": "date_key", "expression": DATE_KEY})
        assert translated_calc_names(cfg, {"Fact": []}) == {}
