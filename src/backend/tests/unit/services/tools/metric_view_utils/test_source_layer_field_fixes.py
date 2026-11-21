"""Source layer on a production Power BI model: dimensions read only through native SQL, and
Power Query steps on a SELECT * read whose columns are unknown."""

from src.services.tools.metric_view_utils.m_transform_folder import MTransformFolder
from src.services.tools.metric_view_utils.source_layer_binding import (
    placeholder_join_target,
)

VIEWS = {
    "C_Dim_Geography": "`acme-dev`.sb.src_c_dim_geography",
    "Dim_Region": "`acme-dev`.sb.src_dim_region",
}


def test_a_placeholder_join_reads_the_dimensions_source_layer_view():
    assert (
        placeholder_join_target("acme-dev.sb.c_dim_geography", VIEWS)
        == "C_Dim_Geography"
    )
    assert (
        placeholder_join_target("`acme-dev`.`sb`.`dim_region`", VIEWS) == "Dim_Region"
    )


def test_only_a_placeholder_in_the_views_own_schema_is_bound():
    assert placeholder_join_target("prod.md.c_dim_geography", VIEWS) is None
    assert placeholder_join_target("acme-dev.sb.c_dim_plant", VIEWS) is None
    assert placeholder_join_target("(SELECT 1)", VIEWS) is None


def test_steps_on_unknown_columns_keep_every_other_column():
    """Was "SELECT\\n\\nFROM (...)" (invalid) for removals only, and a SELECT of the
    transformed columns alone (every other column lost) for replacements."""
    sql = MTransformFolder()._apply_with_wrapper(
        "SELECT * FROM `c`.`s`.`t`", ["kind = 'A'"], {"a": None, "b": "UPPER(b)"}, []
    )
    assert sql == (
        "SELECT\n  * EXCEPT (a, b),\n  UPPER(b) AS b\nFROM (\nSELECT * FROM `c`.`s`.`t`\n) _src\nWHERE kind = 'A'"
    )
    only_removed = MTransformFolder()._apply_with_wrapper(
        "SELECT * FROM t", [], {"a": None}, []
    )
    assert only_removed.startswith("SELECT\n  * EXCEPT (a)\nFROM (")


def test_known_columns_are_still_folded():
    sql = MTransformFolder()._apply_with_wrapper(
        "SELECT x, y FROM t", [], {"y": None}, []
    )
    assert sql.startswith("SELECT\n  x\nFROM (")
