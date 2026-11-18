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


def test_sql_assembled_from_parameters_is_not_used():
    assert _native_base(PARAMETERISED, "prod.md.t") == ""


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
