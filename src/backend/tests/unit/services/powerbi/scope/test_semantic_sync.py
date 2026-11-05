"""Semantic sync: snapshot objects, normalized hashes, changes between snapshots and their impact on the mirror."""

import pytest

from src.services.powerbi.scope import semantic_sync
from src.services.powerbi.scope.parts import PartTree
from src.services.powerbi.scope.tmdl_model import Model, Table

DATABRICKS_M = 'Value.NativeQuery(Databricks.Catalogs("h", "/p", []), "select 1")'


def _model(measure_dax="SUM(Fact[x])", extra_measure=None):
    measures = {"Sales": measure_dax, "Helper": "1"}
    if extra_measure:
        measures[extra_measure] = "2"
    columns = {"x": None, "k": None, "flag": "IF(Fact[x] > 0, 1)"}
    fact = Table("Fact", "m", DATABRICKS_M, columns=columns, measures=measures)
    dim = Table("Dim", "m", DATABRICKS_M, columns={"k": None})
    return Model(
        {"Fact": fact, "Dim": dim},
        [("Fact", "k", "Dim", "k", True)],
        roles=[("AT", "Dim", 'Dim[k] = "AT"')],
    )


def _snapshot(model):
    return {"objects": [vars(o) for o in semantic_sync.model_objects(model)]}


def _change(object_type, name, change_type="changed", table="Fact"):
    return {
        "change_type": change_type,
        "object_type": object_type,
        "pbi_table": table,
        "name": name,
    }


def test_objects_cover_the_model_and_ignore_formatting_and_comments():
    keys = {o.key for o in semantic_sync.model_objects(_model())}
    assert {
        ("table", "Fact", ""),
        ("column", "Fact", "flag"),
        ("measure", "Fact", "Sales"),
        ("relationship", "Fact", "Fact[k] -> Dim[k]"),
        ("rls_filter", "Dim", "AT"),
    } <= keys
    same = semantic_sync.make("measure", "Fact", "Sales", "SUM(Fact[x])")
    reformatted = semantic_sync.make(
        "measure", "Fact", "Sales", "SUM(\n    Fact[x]  ) // total"
    )
    assert same.definition_hash == reformatted.definition_hash
    assert reformatted.definition == "SUM(Fact[x])"
    other = semantic_sync.make("measure", "Fact", "Sales", "SUM(Fact[y])")
    assert same.definition_hash != other.definition_hash


def test_compare_and_impact_follow_the_scope_decision():
    old = _snapshot(_model())
    new = _snapshot(_model(measure_dax="SUM(Fact[x]) * 2", extra_measure="Brand New"))
    new["objects"] = [o for o in new["objects"] if o["name"] != "Helper"]
    changes = semantic_sync.compare(old, new)
    assert [(c["change_type"], c["name"]) for c in changes] == [
        ("added", "Brand New"),
        ("removed", "Helper"),
        ("changed", "Sales"),
    ]
    scope = [
        {
            "object_type": "measure",
            "pbi_table": "Fact",
            "name": "Sales",
            "decision": "keep",
            "mirrored_as": "ucm_fact.sales",
        },
        {
            "object_type": "measure",
            "pbi_table": "Fact",
            "name": "Helper",
            "decision": "drop",
            "mirrored_as": "",
        },
    ]
    actions = {
        c["name"]: c["action"] for c in semantic_sync.with_impact(changes, scope)
    }
    assert actions == {
        "Brand New": "triage: not in the scope table (rebuild the scope draft for a decision)",
        "Helper": "none (not mirrored)",
        "Sales": "update the mirror: ucm_fact.sales",
    }


def test_a_decision_outside_the_vocabulary_is_triaged_not_hidden():
    scope = [
        {
            "object_type": "measure",
            "pbi_table": "Fact",
            "name": "Sales",
            "decision": "genie pattern",
        }
    ]
    [change] = semantic_sync.with_impact([_change("measure", "Sales")], scope)
    assert change["action"].startswith(
        "triage: decision 'genie pattern' is not in the scope vocabulary"
    )


def test_the_scope_draft_decides_new_and_newly_reachable_objects():
    scope = [
        {
            "object_type": "measure",
            "pbi_table": "Fact",
            "name": "Old",
            "decision": "drop",
        }
    ]
    draft = [
        {
            "object_type": "measure",
            "pbi_table": "Fact",
            "name": "Old",
            "decision": "keep",
            "reason": "drawn",
        },
        {
            "object_type": "measure",
            "pbi_table": "Fact",
            "name": "New",
            "decision": "keep",
            "reason": "drawn",
        },
    ]
    changes = [_change("measure", "Old"), _change("measure", "New", "added")]
    actions = [c["action"] for c in semantic_sync.with_impact(changes, scope, draft)]
    assert actions == [
        "triage: dropped in the scope table, now keep in the scope draft (drawn)",
        "triage: not in the scope table; scope draft: keep (drawn)",
    ]


def test_rls_and_column_changes_find_their_scope_rows():
    scope = [
        {
            "object_type": "rls_filter",
            "pbi_table": "Dim",
            "name": "AT; DE",
            "decision": "fold",
            "mirrored_as": "row_filter_fn",
        },
        {
            "object_type": "table",
            "pbi_table": "Fact",
            "name": "",
            "decision": "keep",
            "mirrored_as": "sales.fact",
        },
    ]
    changes = [
        _change("rls_filter", "DE", table="Dim"),
        _change("column", "flag", "removed"),
        _change("page_field", "[Sales]", "added", table="Home"),
    ]
    actions = [c["action"] for c in semantic_sync.with_impact(changes, scope)]
    assert actions[0] == "update the mirror: row_filter_fn"
    assert actions[1] == "removed in Power BI: retire or rework sales.fact"
    assert actions[2].startswith("triage: a field drawn on")


def test_an_ignored_object_keeps_its_change_but_needs_no_person():
    rules = semantic_sync.ignore_rules(
        [
            {
                "object_type": "expression",
                "name": "LoadDate param",
                "reason": "set by every data load",
            }
        ]
    )
    action = "check the tables loaded through this shared query"
    changes = [
        {**_change("expression", "LoadDate param", table=""), "action": action},
        {**_change("expression", "Other", table=""), "action": action},
    ]
    actions = [c["action"] for c in semantic_sync.with_ignores(changes, rules)]
    assert actions == ["none (ignored: set by every data load)", action]


def test_an_ignore_entry_without_a_reason_is_refused():
    with pytest.raises(ValueError):
        semantic_sync.ignore_rules([{"object_type": "expression", "name": "x"}])


def test_rows_of_a_tracked_table_are_objects_and_point_to_its_snapshot():
    before = [
        {"T[Kpi]": "Orders %", "T[Status]": "On Hold"},
        {"T[Kpi]": "Fill %", "T[Status]": "Confirmed"},
    ]
    after = [
        {"T[Kpi]": "Orders %", "T[Status]": "Confirmed"},
        {"T[Kpi]": "Share", "T[Status]": 0.25},
    ]
    objects = semantic_sync.tracked_objects("T", after)
    assert [(o.name, o.definition) for o in objects] == [
        ("Orders %", "Status=Confirmed"),
        ("Share", "Status=<number>"),  # a fractional value is never stored
    ]
    old = {"objects": [vars(o) for o in semantic_sync.tracked_objects("T", before)]}
    new = {"objects": [vars(o) for o in objects]}
    scope = [
        {
            "object_type": "table",
            "pbi_table": "T",
            "name": "",
            "decision": "snapshot",
            "mirrored_as": "x.pbi_t",
        }
    ]
    changes = semantic_sync.with_impact(semantic_sync.compare(old, new), scope)
    assert [(c["change_type"], c["name"], c["mirrored_as"]) for c in changes] == [
        ("removed", "Fill %", "x.pbi_t"),
        ("changed", "Orders %", "x.pbi_t"),
        ("added", "Share", "x.pbi_t"),
    ]
    assert changes[1]["action"].startswith("data of a tracked table changed")


def test_build_snapshot_reads_tmdl_parts_and_report_pages():
    tmdl = {
        "tables/Fact.tmdl": "table Fact\n\tmeasure Sales = SUM(Fact[x])\n\tcolumn x\n\t\tdataType: double\n"
        f"\tpartition Fact = m\n\t\tmode: import\n\t\tsource = {DATABRICKS_M}\n",
        "relationships.tmdl": "relationship r1\n\tfromColumn: Fact.x\n\ttoColumn: 'Dim Table'.'x key'\n\tisActive: false\n",
    }
    visual = {
        "name": "v",
        "visual": {
            "visualType": "card",
            "query": {
                "queryState": {
                    "Values": {
                        "projections": [
                            {
                                "field": {
                                    "Measure": {
                                        "Expression": {"SourceRef": {"Entity": "Fact"}},
                                        "Property": "Sales",
                                    }
                                }
                            }
                        ]
                    }
                }
            },
        },
    }
    report = {
        "pages/p/page.json": {"displayName": "Home"},
        "pages/p/visuals/v/visual.json": visual,
    }
    snapshot = semantic_sync.build_snapshot(PartTree(tmdl), PartTree(report))
    keys = {
        (o["object_type"], o["pbi_table"], o["name"]): o for o in snapshot["objects"]
    }
    assert ("measure", "Fact", "Sales") in keys
    assert (
        keys[("relationship", "Fact", "Fact[x] -> Dim Table[x key]")]["definition"]
        == "inactive"
    )
    assert keys[("page", "", "Home")]["definition"] == "visible; reachable"
    assert keys[("page_field", "Home", "[Sales]")]["definition"] == "drawn"


def test_a_change_to_an_undecided_review_object_needs_a_person():
    scope = [{"object_type": "measure", "pbi_table": "Fact", "name": "Sales", "decision": "review", "mirrored_as": ""}]
    [change] = semantic_sync.with_impact([_change("measure", "Sales")], scope)
    assert change["action"].startswith("triage: open decision in the scope table")
    assert semantic_sync.needs_action(change)
