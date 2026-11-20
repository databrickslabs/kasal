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


def test_a_shared_alias_binds_to_the_table_that_has_the_columns_the_measures_read():
    """Two plant tables share one alias and one warehouse table. The join stayed on the raw
    table, which lacks the column Power Query renames, and every measure filtering on it was
    removed on deploy."""
    from types import SimpleNamespace

    from src.services.tools.metric_view_utils.source_layer_binding import (
        _table_with_measure_columns,
        shared_alias_columns,
    )

    shared = shared_alias_columns(
        {
            "C_Dim_Plant": {
                "alias": "dim_plant",
                "dim_columns": ["plant", "plant_desc"],
            },
            "Dim_Plant": {
                "alias": "dim_plant",
                "dim_columns": ["plant", "Plant_Txtmd_Ref"],
            },
            "C_Dim_Geography": {"alias": "dim_geography", "dim_columns": ["comp_code"]},
        }
    )
    assert sorted(shared) == ["dim_plant"]  # only aliases more than one table uses
    views = {"C_Dim_Plant": "c.s.src_c_dim_plant", "Dim_Plant": "c.s.src_dim_plant"}

    def spec(*exprs: str) -> SimpleNamespace:
        return SimpleNamespace(measures=[SimpleNamespace(sql_expr=e) for e in exprs])

    filtered = spec(
        "SUM(source.qty) FILTER (WHERE dim_plant.plant_txtmd_ref NOT IN ('A'))"
    )
    assert (
        _table_with_measure_columns(filtered, "dim_plant", shared, views) == "Dim_Plant"
    )
    # both tables have the column, or no measure reads the alias: not guessed
    assert (
        _table_with_measure_columns(
            spec("SUM(source.qty) FILTER (WHERE dim_plant.plant = 'A')"),
            "dim_plant",
            shared,
            views,
        )
        is None
    )
    assert (
        _table_with_measure_columns(spec("SUM(source.qty)"), "dim_plant", shared, views)
        is None
    )
    assert _table_with_measure_columns(filtered, "dim_geography", shared, views) is None


def test_a_placeholder_override_does_not_stand_in_for_a_translation():
    """Config generation writes a note per complex measure (expr: "TODO: complex measure
    ..."). Taken as an override it replaced the measure with that text and kept it away from
    the DAX translation: 27 measures of one view were never translated."""
    from src.services.tools.uc_metric_view_generator_utils.pipeline_config_assembly import (
        apply_config_overlays,
    )

    config: dict[str, object] = {
        "manual_overrides": {
            "Fact_Orders": [
                {
                    "name": "ordered_py",
                    "expr": "TODO: complex measure 'Ordered PY' — DAX: CALCULATE(...)",
                },
                {"name": "ordered_fixed", "expr": "SUM(source.ordered)"},
            ]
        }
    }
    apply_config_overlays(
        config,
        in_recon_loop=False,
        naming_raw=None,
        implicit_column_measures_raw=None,
        manual_overrides_raw='{"Fact_Orders": [{"name": "kept", "expr": "SUM(source.kept)"}]}',
    )
    overrides = config["manual_overrides"]
    assert isinstance(overrides, dict)
    assert [o["name"] for o in overrides["Fact_Orders"]] == ["ordered_fixed", "kept"]
