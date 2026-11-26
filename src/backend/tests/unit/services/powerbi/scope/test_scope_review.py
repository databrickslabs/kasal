"""Scope review: open scope rows grouped into buckets with one proposed decision each, applied per bucket.
Ported with the reference workflow's own cases (repo files -> metric view specs as evidence)."""

import pytest
import yaml

from src.services.powerbi.scope import scope_review

VIEW = """version: 1.1
source: sales.fact
dimensions:
  - name: plant_name
    expr: source.plant_name
measures:
  - name: sales_uc
    display_name: Sales UC
    expr: SUM(source.sales)
"""
SPECS = {"ucm_fact": yaml.safe_load(VIEW)}


def _row(object_type, table, name, decision, reason="", users="", mirrored_as=""):
    return {
        "object_type": object_type,
        "pbi_table": table,
        "name": name,
        "decision": decision,
        "reason": reason,
        "load_bearing_for": users,
        "mirrored_as": mirrored_as,
    }


def _buckets(rows):
    return scope_review.buckets(rows, scope_review.evidence(rows, SPECS))


def _rows():
    return [
        _row("measure", "Fact", "Sales UC", "keep", "drawn"),
        _row("measure", "Fact", "Helper", "keep", "building block of a reachable measure", "Sales UC"),
        _row("measure", "Fact", "Colour", "keep", "building block of a reachable measure", "Sales UC CondForm"),
        _row("measure", "Fact", "Old", "drop"),
        _row("measure", "Fact", "Old Helper", "keep", "building block of a reachable measure", "Old"),
        _row("measure", "Fact", "Missing", "keep", "drawn"),
        _row("column", "Dim", "Plant Name", "review", "not used"),
        _row("column", "Dim", "Plant Key", "review", "not used"),
        _row("measure", "Fact", "Done", "keep", "drawn", mirrored_as="ucm_fact.done"),
    ]


def test_open_rows_land_in_buckets_with_proposals():
    rows = _rows()
    rows[0]["mirrored_as"] = "ucm_fact.sales_uc"  # linked; Helper's user is then mirrored
    result = {b.id: b for b in _buckets(rows)}
    names = {key: [r["name"] for r in b.rows] for key, b in result.items()}
    assert names == {
        "B1": ["Colour"],
        "B2": ["Old Helper"],
        "B3": ["Helper"],
        "C1": ["Plant Name"],
        "C2": ["Plant Key"],
        "G0": ["Missing"],
    }
    assert (result["B3"].proposal, result["C2"].proposal, result["G0"].proposal) == ("fold", "drop", "")


def test_a_mirror_found_by_display_name_is_linked_and_buckets_apply():
    rows = _rows()
    result = _buckets(rows)
    [linked] = [b for b in result if b.id == "L1"]
    assert linked.rows[0]["_mirror"] == "ucm_fact.sales_uc"
    assert scope_review.apply(rows, result, {"L1": "accept", "C2": "accept", "G0": "genie"}) == 3
    assert (rows[0]["decision"], rows[0]["mirrored_as"]) == ("keep", "ucm_fact.sales_uc")
    assert rows[5]["decision"] == "genie" and rows[7]["decision"] == "drop"
    with pytest.raises(ValueError):
        scope_review.apply(rows, result, {"G0": "accept"})  # a bucket without a proposal needs a decision
    with pytest.raises(ValueError):
        scope_review.apply(rows, result, {"NOPE": "drop"})


def test_a_dropped_wrapper_with_mirrors_folds_and_a_dropped_built_measure_is_kept():
    rows = [
        _row(
            "measure",
            "W",
            "KPI Wrapper",
            "drop",
            "SWITCH / wrapper plumbing: the branch measures it selects are mirrored",
            mirrored_as="ucm_fact.a; ucm_fact.b",
        ),
        _row("measure", "W", "Unreached", "drop", "unreached (no visual uses it)", mirrored_as="ucm_fact.c"),
    ]
    result = {b.id: (b.proposal, [r["name"] for r in b.rows]) for b in _buckets(rows)}
    assert result == {"M1": ("fold", ["KPI Wrapper"]), "M2": ("keep", ["Unreached"])}


def test_a_branch_of_a_wrapper_is_never_dropped_with_it():
    rows = [
        _row("measure", "W", "Line_Item_Wrapper_Actual", "drop", "SWITCH / wrapper plumbing: branches are mirrored"),
        _row(
            "measure", "B", "Fill_Rate_Actual", "keep", "building block of a reachable measure", "Line_Item_Wrapper_Actual"
        ),
    ]
    [bucket] = _buckets(rows)
    assert (bucket.id, bucket.proposal, [r["name"] for r in bucket.rows]) == ("B5", "", ["Fill_Rate_Actual"])


def test_a_dropped_step_of_a_mirrored_measure_is_flagged_and_its_own_steps_follow():
    rows = [
        _row("measure", "F", "wFA", "keep", "drawn", mirrored_as="ucm_fact.wfa"),
        _row("measure", "F", "FA wkly", "fold", "building block", "wFA"),
        _row("measure", "F", "AE wkly", "drop", "display helper (format / colour / label)", "FA wkly"),
        _row("measure", "F", "FCT - Sales", "keep", "building block of a reachable measure", "AE wkly"),
        _row("measure", "F", "Trend", "drop", "display helper"),
        _row("measure", "F", "Trend Step", "keep", "building block of a reachable measure", "Trend"),
    ]
    result = {b.id: [r["name"] for r in b.rows] for b in _buckets(rows)}
    assert result == {"M3": ["AE wkly"], "B2": ["Trend Step"], "B3": ["FCT - Sales"]}


def test_tables_link_to_a_view_of_the_same_name_and_summary_names_only():
    rows = [_row("table", "Fact Orders", "", "keep", "materialized view")]
    ev = scope_review.evidence(rows, {}, views=["cat.sch.fact_orders"])
    [bucket] = scope_review.buckets(rows, ev)
    assert (bucket.id, bucket.rows[0]["_mirror"]) == ("L2", "cat.sch.fact_orders")
    [entry] = scope_review.summary([bucket])
    assert entry["examples"] == ["table Fact Orders -> cat.sch.fact_orders"]
    assert "L2" in scope_review.markdown("example", [bucket])
