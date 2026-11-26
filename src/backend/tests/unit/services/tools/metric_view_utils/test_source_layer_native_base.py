"""Source-layer views built from a table's native SQL and its Power Query steps in order."""

from src.services.tools.metric_view_utils.m_transform_folder import MTransformFolder
from src.services.tools.metric_view_utils.source_artifacts import (
    _fold_dimension_read,
    _native_base,
)

CALENDAR = '''let
    v = Value.NativeQuery(conn, "select distinct fiscper, fiscper3 as Month, fiscyear as Year from md.ca_calendar
where fiscyear>=2022", null, [EnableFolding=true]),
    #"Added MonthName" = Table.AddColumn(v, "MonthName", each if [Month] = "001" then "Jan" else null)
in
    #"Added MonthName"'''

PLANT = '''let
    v = Value.NativeQuery(conn, "select distinct * from md.ca_dim_plant", null, [EnableFolding=true]),
    #"Filtered Rows" = Table.SelectRows(v, each ([comp_code] <> null)),
    #"Renamed Columns" = Table.RenameColumns(#"Filtered Rows",{{"plant_txtmd", "plant_txtmd_ref"}}),
    #"Added Custom" = Table.AddColumn(#"Renamed Columns", "plant_txtmd", each [plant]&" - "&[plant_txtmd_ref])
in
    #"Added Custom"'''

PARAMETERISED = """let
    v = Value.NativeQuery(conn, "SELECT a FROM md.t WHERE (CASE WHEN '"& Filter &"' = 'x' THEN 1 END) = 1", null)
in v"""


def test_the_native_sql_is_the_base_with_its_catalog():
    assert _native_base(CALENDAR, "prod.md.ca_calendar").startswith(
        "select distinct fiscper, fiscper3 as Month, fiscyear as Year from prod.md.ca_calendar"
    )


def test_sql_assembled_from_parameters_uses_known_values_and_reports_unknown_ones():
    """Was rejected (cut off at the splice), so the view fell back to the raw table and lost
    the native SQL's columns (sum(fltp) AS value)."""
    assumed = []
    sql = _native_base(PARAMETERISED, "prod.md.t", {}, assumed)
    assert "CASE WHEN '' = 'x' THEN 1 END" in sql and assumed == ["Filter"]
    assumed = []
    sql = _native_base(PARAMETERISED, "prod.md.t", {"Filter": "x"}, assumed)
    assert "CASE WHEN 'x' = 'x'" in sql and assumed == []


def test_the_native_projection_survives_and_the_added_column_reads_it():
    sql = _fold_dimension_read(
        "C_Dim_calendar", "prod.md.ca_calendar", None, CALENDAR, {}
    )
    assert sql.startswith(
        "SELECT\n  fiscper,\n  Month,\n  Year,\n  CASE WHEN (Month = '001')"
    )
    assert "from prod.md.ca_calendar" in sql


def test_filter_rename_and_added_column_in_order():
    sql = _fold_dimension_read("Dim_Plant", "prod.md.ca_dim_plant", None, PLANT, {})
    assert "* EXCEPT (plant_txtmd)" in sql
    assert "plant_txtmd AS plant_txtmd_ref" in sql
    assert "concat(concat(plant, ' - '), plant_txtmd_ref) AS plant_txtmd" in sql
    assert sql.rstrip().endswith(
        "WHERE (comp_code IS NOT NULL)"
    )  # was "comp_code <> null"


def test_select_distinct_columns_are_read_and_star_is_unknown():
    assert MTransformFolder._extract_select_columns(
        "select distinct a, b as c from t"
    ) == ["a", "c"]
    assert MTransformFolder._extract_select_columns("select distinct * from t") == []


UNION_FACT = '''let
    v = Value.NativeQuery(conn, "select fiscper, comp_code, sum(a) as value from md.t1 group by all
UNION ALL
select fiscper, comp_code, sum(b) as value from md.t2 group by all", null, [EnableFolding=true]),
    #"Duplicated Column" = Table.DuplicateColumn(v, "fiscper", "fiscper - Copy"),
    #"Renamed Columns" = Table.RenameColumns(#"Duplicated Column",{{"fiscper - Copy", "Year"}}),
    #"Split Column by Position" = Table.SplitColumn(#"Renamed Columns", "Year", Splitter.SplitTextByPositions({0, 4}, false), {"Year.1", "Year.2"}),
    #"Changed Type" = Table.TransformColumnTypes(#"Split Column by Position",{{"Year.1", Int64.Type}, {"Year.2", Int64.Type}, {"comp_code", Int64.Type}}),
    #"Removed Columns" = Table.RemoveColumns(#"Changed Type",{"Year.2"}),
    #"Renamed Columns1" = Table.RenameColumns(#"Removed Columns",{{"Year.1", "Year"}})
in
    #"Renamed Columns1"'''


def test_steps_over_a_union_wrap_the_whole_union_and_resolve_the_split_chain():
    """The steps were written into the first arm only (arms with different columns), and the
    cast read the split part Year.1, which no arm has."""
    sql = _fold_dimension_read("Fact", "prod.md.t1", None, UNION_FACT, {})
    outer, inner = sql.split("FROM (", 1)
    assert "CAST(SUBSTRING(fiscper, 1, 4) AS BIGINT) AS Year" in outer
    assert "CAST(comp_code AS BIGINT) AS comp_code" in outer
    assert "Year.1" not in sql and inner.count("UNION ALL") == 1
    assert "CAST(" not in inner  # the arms are untouched


PARENTHESISED_UNION = """let
    v = Value.NativeQuery(conn, "
(With base as (SELECT comp_code, SUM(qty) AS qty FROM mart.orders GROUP BY all)
SELECT * FROM base)
union
(SELECT comp_code, SUM(qty) AS qty FROM mart.orders_old GROUP BY all)
", null, [EnableFolding=true])
in v"""


def test_a_union_of_parenthesised_queries_is_the_base():
    """Was rejected for not starting with SELECT/WITH, so the view read SELECT * from the
    last table of the query and every measure summed the wrong table."""
    sql = _native_base(PARENTHESISED_UNION, "prod.mart.orders")
    assert sql.startswith("(With base as") and "FROM prod.mart.orders_old" in sql


def test_only_parentheses_around_the_whole_statement_are_removed():
    from src.services.tools.metric_view_utils.m_native_sql import (
        without_outer_parentheses,
    )

    assert without_outer_parentheses("((SELECT ')' AS x));") == "SELECT ')' AS x"
    assert without_outer_parentheses("(SELECT 1) UNION (SELECT 2)") == (
        "(SELECT 1) UNION (SELECT 2)"
    )


def test_power_query_cut_off_by_config_generation_is_restored_in_full():
    """Config generation ships at most 2000 characters of a raw M; a long native query then
    lost its FROM, the table got no view and its measures were summed on another table.
    """
    from src.services.tools.uc_metric_view_generator_utils.model_inputs import (
        restore_full_mquery,
    )

    full = PARENTHESISED_UNION
    entries = [
        {"table_name": "Fact_Orders", "transpiled_sql": full[:40]},
        {"table_name": "Fact_Other", "transpiled_sql": "SELECT a FROM t"},
    ]
    config = {
        "table_mquery_expressions": {"Fact_Orders": full, "Fact_Other": "let x in x"}
    }
    restored = restore_full_mquery(entries, config)
    assert restored[0]["transpiled_sql"] == full.strip()
    assert restored[1]["transpiled_sql"] == "SELECT a FROM t"  # compiled SQL stays
