"""Two Power BI tables over one warehouse table (two calendars, geography and region), and
Power Query steps on a native query whose columns are not known."""

from src.services.tools.metric_view_utils.m_transform_folder import MTransformFolder
from src.services.tools.metric_view_utils.source_artifacts import _fold_dimension_read
from src.services.tools.metric_view_utils.source_layer_binding import (
    resolve_join_target,
    unique_alias_tables,
)

RAW = {
    "C_Dim_calendar": "prod.md.ca_calendar",  # qualified by its own view
    "Dim_calendar_dummy": "md.ca_calendar",  # as the M names it
    "Dim_Plant": "prod.md.ca_plant",
}


def test_a_join_over_a_shared_table_is_bound_by_its_alias():
    """The dummy calendar's join was bound to the other calendar's view, because only that
    one's raw name equalled the join source: the period then came from the wrong table.
    """
    aliases = unique_alias_tables(
        {
            "C_Dim_calendar": {"alias": "dim_calendar"},
            "Dim_calendar_dummy": {"alias": "dim_calendar_dummy"},
            "Dim_Plant": {"alias": "dim_plant"},
            "C_Dim_Plant": {"alias": "dim_plant"},  # shared alias: not unique
        }
    )
    assert aliases == {
        "dim_calendar": "C_Dim_calendar",
        "dim_calendar_dummy": "Dim_calendar_dummy",
    }
    source = "prod.md.ca_calendar"
    assert resolve_join_target(source, RAW, aliases["dim_calendar_dummy"]) == (
        "Dim_calendar_dummy"
    )
    assert resolve_join_target(source, RAW) is None  # two tables, no alias: not guessed
    assert resolve_join_target("prod.md.ca_plant", RAW) == "Dim_Plant"


CTE_M = """let
    Source = Value.NativeQuery(conn, "WITH a AS (SELECT comp_code, version, SUM(v) AS v FROM md.pl GROUP BY all) SELECT * FROM a", null, [EnableFolding=true]),
    r = Table.ReplaceValue(Source, "C", "", Replacer.ReplaceText, {"comp_code"}),
    t = Table.TransformColumns(r, {{"version", each if Text.StartsWith(_, "R") then "RE" else _, type text}})
in
    t"""


def test_power_query_steps_are_applied_to_a_native_query_with_unknown_columns():
    """The fold was handed the parser's group-by names as a column list and raised; the error
    was swallowed, so the view kept the unreplaced codes and Power BI's rows were not found.
    """

    class _Info:
        group_by_columns = ["comp_code", "null", "Row"]
        source_table = "md.pl"

    todos: dict[str, list] = {}
    sql = _fold_dimension_read("Fact_PL", "prod.md.pl", _Info(), CTE_M, todos)  # type: ignore[arg-type]
    assert "REPLACE(comp_code, 'C', '') AS comp_code" in sql
    assert "CASE WHEN version LIKE 'R%' THEN 'RE' ELSE version END AS version" in sql
    assert not todos


def test_a_value_transform_that_is_not_reproduced_is_named():
    folder = MTransformFolder()
    m = (
        'let\n    Source = x,\n    t = Table.TransformColumns(Source, {{"a", each '
        "Text.Upper(_), type text}})\nin\n    t"
    )
    folder.fold_from_mquery("SELECT a FROM md.t", m, None)
    assert folder.transform_todos
