"""The source-view column list: parsing, the rename of a column named like the fact alias, and the
rewrite of the references to it."""

from src.services.tools.metric_view_utils.source_columns import (
    bare_columns_read,
    column_renames,
    columns_read,
    known_name_fixes,
    parse_source_columns,
    rename_references,
    renamed_columns,
)


def test_parse_accepts_typed_entries_and_plain_names():
    raw = '{"fact_a": [{"name": "Qty", "type": "DOUBLE"}, {"name": "qty"}, "k"], "dim": []}'
    # An empty list is a view whose columns are unknown, not a missing view.
    assert parse_source_columns(raw) == {"fact_a": ["Qty", "qty", "k"], "dim": []}


def test_parse_is_empty_on_missing_or_malformed_input():
    assert parse_source_columns(None) == {}
    assert parse_source_columns("") == {}
    assert parse_source_columns("not json") == {}
    assert parse_source_columns("[1, 2]") == {}
    assert parse_source_columns({"t": "x"}) == {}


def test_a_column_named_like_the_fact_alias_is_renamed_once():
    renames = column_renames({"fact": ["id", "Source"], "dim": ["id"]})
    assert renames == {"fact": {"source": "source_"}}


def test_the_new_name_avoids_a_column_that_already_has_it():
    renames = column_renames({"fact": ["source", "source_"]})
    assert renames == {"fact": {"source": "source__"}}


def test_renamed_columns_apply_the_renames():
    cols = {"fact": ["id", "source"], "dim": ["source"]}
    # The rule is per view: a joined view with such a column is renamed the same way.
    assert renamed_columns(cols, column_renames(cols)) == {
        "fact": ["id", "source_"],
        "dim": ["source_"],
    }


def test_references_to_the_renamed_column_are_rewritten_only_on_the_alias():
    renames = {"source": "source_"}
    assert (
        rename_references("SUM(source.source)", "source", renames)
        == "SUM(source.source_)"
    )
    assert rename_references("COUNT(`Source`)", "source", renames) == "COUNT(`Source`)"
    assert (
        rename_references("source.`source` = 1", "source", renames)
        == "source.`source_` = 1"
    )
    assert rename_references("other.source", "source", renames) == "other.source"
    assert rename_references("source.amount", "source", renames) == "source.amount"


def test_a_join_alias_is_rewritten_with_its_own_renames():
    assert (
        rename_references("dim_x.source", "dim_x", {"source": "source_"})
        == "dim_x.source_"
    )


def test_columns_read_lists_lower_cased_names_of_one_alias():
    assert columns_read("SUM(source.Amount) + source.`fiscper`", "source") == {
        "amount",
        "fiscper",
    }
    assert columns_read("dim.month", "source") == set()


def test_bare_columns_read_finds_unqualified_fact_columns_only():
    expr = (
        "sum(case when filter_code in ('A1', 'B2') and `quoted name` is not null"
        " then value_amount end) + MEASURE(other) + dim_plant.plant_name + source.amount"
        " + CAST(qty AS INT) + DATE_TRUNC('MONTH', event_date)"
    )
    assert bare_columns_read(expr) == {
        "filter_code",
        "quoted name",
        "value_amount",
        "qty",
        "event_date",
    }
    assert bare_columns_read("") == set()


class TestKnownNameFixes:
    """A reference spelled differently from the view's column (case, underscores) is read as
    that column; a name that matches none, or more than one, is left alone."""

    def test_underscore_and_case_variants_resolve_to_the_view_column(self):
        texts = [
            "source.order_date = dim_cal.date_id",
            "SUM(source.amount) FILTER (WHERE source.acc_code = 'A1')",
        ]
        fixes = known_name_fixes(texts, "source", ["OrderDate", "acccode", "amount"])
        assert fixes == {"order_date": "OrderDate", "acc_code": "acccode"}

    def test_a_column_the_view_has_is_not_touched(self):
        assert known_name_fixes(["source.acc_code"], "source", ["acc_code", "acccode"]) == {}

    def test_an_ambiguous_or_unknown_name_is_left_as_written(self):
        texts = ["source.acc_code + source.missing"]
        assert known_name_fixes(texts, "source", ["AccCode", "acccode", "amount"]) == {}

    def test_only_the_given_alias_is_read(self):
        assert known_name_fixes(["dim_cal.order_date"], "source", ["OrderDate"]) == {}
