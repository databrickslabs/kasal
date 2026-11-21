"""The measure allocator with the real columns of the source views: a fact whose columns the parser
could not read (a wildcard) becomes judgeable, a correctly homed measure stays where it is, and
without the columns nothing changes."""

import dataclasses

from src.services.tools.metric_view_utils.data_classes import TableInfo
from src.services.tools.metric_view_utils.measure_allocator import reallocate_by_columns


def _table(
    name: str, *, fact: bool = True, parsed: list[str] | None = None
) -> TableInfo:
    return TableInfo(
        table_name=name,
        source_table=f"cat.sch.{name.lower()}",
        aggregate_columns=[{"name": c, "source_col": c} for c in (parsed or [])],
        group_by_columns=[],
        calculated_columns=[],
        is_fact=fact,
        full_sql="",
    )


def _measure(name: str, dax: str, home: str) -> dict:
    return {
        "measure_name": name,
        "original_name": name,
        "dax_expression": dax,
        "proposed_allocation": home,
        "all_allocations": [{"table": home, "role": "primary"}],
    }


def _home(entry: dict) -> str | None:
    for alloc in entry.get("all_allocations") or []:
        if alloc.get("role") == "primary":
            return alloc.get("table")
    return None


# Fact_A's columns cannot be parsed (a wildcard); Fact_B's are known.
TABLES = {
    "Fact_A": _table("Fact_A"),
    "Fact_B": _table("Fact_B", parsed=["plant", "amount"]),
}


def test_without_real_columns_a_wildcard_fact_blocks_the_verdicts():
    mapping = [_measure("m", "SUM(Fact_A[volume])", "Fact_B")]
    report = reallocate_by_columns(mapping, TABLES)
    assert report["rehomed"] == [] and report["broken"] == []
    assert _home(mapping[0]) == "Fact_B"


def test_real_columns_make_the_wildcard_fact_judgeable_and_rehome_the_measure():
    mapping = [_measure("m", "SUM(Fact_A[volume])", "Fact_B")]
    report = reallocate_by_columns(
        mapping, TABLES, source_columns={"Fact_A": ["id", "volume"]}
    )
    assert _home(mapping[0]) == "Fact_A"
    assert report["rehomed"] == [
        {"measure": "m", "from": "Fact_B", "to": "Fact_A", "columns": ["volume"]}
    ]


def test_a_correctly_homed_measure_is_not_moved_when_its_home_owns_the_columns():
    mapping = [_measure("m", "SUM(Fact_B[amount])", "Fact_B")]
    report = reallocate_by_columns(
        mapping,
        TABLES,
        source_columns={"Fact_A": ["id", "volume"], "Fact_B": ["plant", "amount"]},
    )
    assert _home(mapping[0]) == "Fact_B"
    assert report["rehomed"] == [] and report["rescued"] == []


def test_a_column_no_view_has_breaks_the_measure_when_all_views_are_known():
    mapping = [_measure("m", "SUM(Fact_B[missing])", "Fact_B")]
    report = reallocate_by_columns(
        mapping,
        TABLES,
        source_columns={"Fact_A": ["id", "volume"], "Fact_B": ["plant", "amount"]},
    )
    assert _home(mapping[0]) is None
    assert report["broken"] and report["broken"][0]["measure"] == "m"


def test_a_column_on_two_known_facts_is_ambiguous_and_not_moved():
    tables = {"Fact_A": _table("Fact_A"), "Fact_B": _table("Fact_B")}
    mapping = [_measure("m", "SUM(Fact_B[amount])", "Fact_A")]
    report = reallocate_by_columns(
        mapping,
        tables,
        source_columns={"Fact_A": ["amount"], "Fact_B": ["amount"]},
    )
    assert _home(mapping[0]) == "Fact_A"
    assert report["ambiguous"]


def test_an_ambiguous_measure_homed_on_a_table_with_no_view_goes_to_the_viewed_fact():
    # Fact_C is a mapping target with no source view: it emits nothing, so the measure goes
    # to the one owning fact that has a view.
    tables = {
        "Fact_A": _table("Fact_A"),
        "Fact_C": _table("Fact_C", fact=False, parsed=["amount"]),
    }
    mapping = [_measure("m", "SUM(Fact_A[amount])", "Fact_C")]
    report = reallocate_by_columns(
        mapping, tables, source_columns={"Fact_A": ["amount"]}
    )
    assert _home(mapping[0]) == "Fact_A"
    assert report["rehomed"] == [
        {"measure": "m", "from": "Fact_C", "to": "Fact_A", "columns": ["amount"]}
    ]


def _no_view_home() -> TableInfo:
    """A table the measure is homed on, with no source table: it has no view and emits nothing."""
    return dataclasses.replace(_table("Fact_P", fact=False), source_table="")


def _dax_measure(name: str, dax: str, home: str | None) -> dict:
    entry = _measure(name, dax, home or "")
    if home is None:
        entry["all_allocations"] = []
        entry["proposed_allocation"] = "__unassigned__"
    return entry


def test_a_placement_the_real_columns_confirm_is_kept_although_a_dimension_now_counts():
    # Without columns: Fact_A owns `volume` (the calendar Dim_C is unknown, so it owns nothing).
    # With real columns Dim_C owns `month`, the verdict turns cross-fact, and the measure would leave
    # Fact_A. Fact_A's real columns hold `volume`, so the placement stays.
    tables = {
        "Fact_A": _table("Fact_A", parsed=["volume"]),
        "Dim_C": _table("Dim_C"),
        "Fact_P": _no_view_home(),
    }
    mapping = [_dax_measure("m", "SUM(Fact_A[volume]) + SUM(Dim_C[month])", "Fact_P")]
    without = [dict(m) for m in mapping]
    reallocate_by_columns(without, tables)
    assert _home(without[0]) == "Fact_A"

    report = reallocate_by_columns(
        mapping,
        tables,
        source_columns={"Fact_A": ["id", "volume"], "Dim_C": ["month"]},
    )
    assert _home(mapping[0]) == "Fact_A"
    (entry,) = report["rehomed"]
    assert entry["to"] == "Fact_A" and entry["from"] == "Fact_P"
    assert "confirmed" in entry["basis"]


def test_a_placement_the_real_columns_do_not_confirm_is_judged_by_them():
    # Fact_A's real view lacks `volume`: the placement is not confirmed, so the real verdict applies
    # and the measure does not stay on a view that lacks its column.
    tables = {
        "Fact_A": _table("Fact_A", parsed=["volume"]),
        "Dim_C": _table("Dim_C"),
        "Fact_P": _no_view_home(),
    }
    mapping = [_dax_measure("m", "SUM(Fact_A[volume]) + SUM(Dim_C[month])", "Fact_P")]
    reallocate_by_columns(
        mapping,
        tables,
        source_columns={"Fact_A": ["id"], "Dim_C": ["month"]},
    )
    assert _home(mapping[0]) != "Fact_A"


def test_a_measure_with_no_home_gets_the_placement_the_real_columns_confirm():
    tables = {"Fact_A": _table("Fact_A", parsed=["volume"])}
    mapping = [_dax_measure("m", "SUM(Fact_A[volume])", None)]
    report = reallocate_by_columns(
        mapping, tables, source_columns={"Fact_A": ["volume"]}
    )
    assert _home(mapping[0]) == "Fact_A"
    assert report["rescued"] and report["rescued"][0]["to"] == "Fact_A"
