"""Reachability closure: seeds from reachable pages, DAX lineage incl. calculated columns / tables,
draft decisions. Ported with the reference workflow's own test cases (dump folders -> in-memory parts)."""

import pytest

from src.services.powerbi.scope import closure, decisions, tmdl_model
from src.services.powerbi.scope.parts import PartTree
from src.services.powerbi.scope.scope_table import build_report_scope, build_scope
from src.services.powerbi.scope.scope_table import analyse as analyse_parts


class _Work:
    """Stands in for the reference tool's dump folder: model TMDL, report PBIR, relationships."""

    def __init__(self):
        self.model = {}
        self.report = {}
        self.relationships = []


def _analyse(work, include_measure_dt=False):
    rels = tmdl_model.relationships_from_rows(work.relationships)
    model, seeds, result = analyse_parts(
        PartTree(work.model),
        PartTree(work.report),
        relationships=rels,
        include_measure_dt=include_measure_dt,
    )
    return model, seeds, result, build_scope(model, seeds, result)


DATABRICKS_M = 'Value.NativeQuery(Databricks.Catalogs("host", "/sql/1.0/warehouses/x", []), "select 1")'

TABLES = {
    "Fact": f"""table Fact
\tmeasure Kpi = SUM(Fact[amount]) // + [Commented]
\tmeasure Commented = 4
\tmeasure 'Kpi PY' = CALCULATE([Kpi], SAMEPERIODLASTYEAR(Dim[date]))
\tmeasure Switcher = SWITCH(SELECTEDVALUE(Sel[key]), 1, [Kpi], 2, [Kpi PY])
\tmeasure 'Kpi Title' = "Sales " & FORMAT([Kpi], "0")
\tmeasure 'Kpi by Size Groups' = CALCULATE([Kpi], FILTER(VALUES(Dim[key]), [Kpi] > MIN(Bands[min])))
\tmeasure 'MTD Inv. Days' = DIVIDE([Kpi], 30.4)
\tmeasure Old = 1
\tmeasure CurWeek = MAX(Dim[week])
\tmeasure 'Only On Detail' = 2
\tmeasure Moved = 3
\tcolumn amount
\t\tdataType: double
\t\tsourceColumn: amount
\tcolumn key
\t\tdataType: string
\t\tsourceColumn: key
\tcolumn technical_id
\t\tdataType: string
\t\tsourceColumn: technical_id
\tpartition Fact = m
\t\tmode: import
\t\tsource = {DATABRICKS_M}
""",
    "Dim": f"""table Dim
\tcolumn key
\t\tdataType: string
\t\tsourceColumn: key
\tcolumn date
\t\tdataType: dateTime
\t\tsourceColumn: date
\tcolumn week
\t\tdataType: string
\t\tsourceColumn: week
\tcolumn region_name
\t\tdataType: string
\t\tsourceColumn: region_name
\tcolumn 'Week Label' = "W" & [CurWeek] & [week]
\t\tdataType: string
\tpartition Dim = m
\t\tmode: import
\t\tsource = {DATABRICKS_M}
""",
    "Sel": """table Sel
\tcolumn key
\t\tdataType: int64
\t\tsourceColumn: key
\tpartition Sel = m
\t\tmode: import
\t\tsource = Table.FromRows(Json.Document(Binary.Decompress(Binary.FromText("x"))))
""",
    "Bands": """table Bands
\tcolumn min
\t\tdataType: double
\t\tsourceColumn: min
\tpartition Bands = calculated
\t\tmode: import
\t\tsource = DATATABLE("min", DOUBLE, {{0}, {10}})
""",
    "Plan": """table Plan
\tcolumn key
\t\tdataType: string
\t\tsourceColumn: key
\tpartition Plan = m
\t\tmode: import
\t\tsource = Excel.Workbook(SharePoint.Contents("https://x/sites/y"))
""",
    "Calc": """table Calc
\tcolumn key
\t\tdataType: string
\t\tsourceColumn: key
\tpartition Calc = calculated
\t\tmode: import
\t\tsource = SUMMARIZE(Plan, Plan[key], "n", [Old])
""",
    "Measures": """table Measures
\tmeasure Helper = [Kpi] * 2
\tcolumn Column1
\t\tdataType: string
\t\tisHidden
\t\tsourceColumn: Column1
\tpartition Measures = m
\t\tmode: import
\t\tsource = Table.FromRows(Json.Document(Binary.Decompress(Binary.FromText("x"))))
""",
}

RELATIONSHIPS = [
    {
        "[FromTable]": "Fact",
        "[FromColumn]": "key",
        "[ToTable]": "Dim",
        "[ToColumn]": "key",
        "[IsActive]": True,
    },
    {
        "[FromTable]": "Plan",
        "[FromColumn]": "key",
        "[ToTable]": "Dim",
        "[ToColumn]": "key",
        "[IsActive]": True,
    },
]


def _measure(name, entity="Fact"):
    return {
        "Measure": {"Expression": {"SourceRef": {"Entity": entity}}, "Property": name}
    }


def _column(table, name):
    return {
        "Column": {"Expression": {"SourceRef": {"Entity": table}}, "Property": name}
    }


def _visual(*fields, kind="tableEx", filters=()):
    projections = [{"field": f} for f in fields]
    return {
        "visual": {
            "visualType": kind,
            "query": {"queryState": {"Values": {"projections": projections}}},
        },
        "filterConfig": {"filters": [{"field": f} for f in filters]},
    }


def _write(work, path, content):
    if path == "model/relationships.json":
        work.relationships = content
    elif path.startswith("model/tmdl/"):
        work.model[path[len("model/tmdl/") :]] = content
    else:
        work.report[path[len("report/") :]] = content


@pytest.fixture
def work():
    tmp_path = _Work()
    for name, tmdl in TABLES.items():
        _write(tmp_path, f"model/tmdl/definition/tables/{name}.tmdl", tmdl)
    _write(tmp_path, "model/relationships.json", RELATIONSHIPS)
    definition = "report/definition"
    pages = {
        "home": {"displayName": "Home"},
        "detail": {
            "displayName": "Detail",
            "visibility": "HiddenInViewMode",
            "pageBinding": {
                "type": "Drillthrough",
                "parameters": [{"fieldExpr": _measure("Kpi")}],
            },
        },
    }
    for pid, page in pages.items():
        _write(tmp_path, f"{definition}/pages/{pid}/page.json", page)
    visuals = {
        "home/v1": _visual(
            _measure("Kpi"),
            _measure("Switcher"),
            _measure("Kpi Title"),
            _measure("Kpi by Size Groups"),
            _measure("MTD Inv. Days"),
            _measure("Helper", "Measures"),
            _measure("Moved", "Old Home Table"),
            _column("Dim", "Week Label"),
            filters=[_column("Plan", "key")],
        ),
        "home/v2": _visual(
            {"Aggregation": {"Expression": _column("Dim", "week"), "Function": 3}},
            kind="card",
        ),
        "home/v3": _visual(_column("Sel", "key"), kind="slicer"),
        "home/v4": _visual(_column("Calc", "key")),
        "detail/v5": _visual(_measure("Only On Detail")),
    }
    for path, visual in visuals.items():
        page, vid = path.split("/")
        _write(
            tmp_path,
            f"{definition}/pages/{page}/visuals/{vid}/visual.json",
            {"name": vid, **visual},
        )
    return tmp_path


def _scope(work, include_measure_dt=False):
    _model, _seeds, _closure, rows = _analyse(work, include_measure_dt)
    return {(r["object_type"], r["pbi_table"], r["name"]): r for r in rows}


def _decision(scope, kind, table, name=""):
    return scope[(kind, table, name)]["decision"]


def test_measures_are_parsed_from_one_line_and_fenced_bodies():
    tmdl = "table T\n\tmeasure A = 1\n\tmeasure 'B c' = ```\n\t\t\tSUM(T[x])\n\t\t\t```\n\tmeasure Empty\n\t\tlineageTag: x\n"
    assert tmdl_model.measures_of(tmdl) == {
        "A": "1",
        "B c": "\t\t\tSUM(T[x])",
        "Empty": "",
    }


def test_a_reference_in_a_comment_is_not_followed(work):
    scope = _scope(work)
    assert _decision(scope, "measure", "Fact", "Kpi") == "keep"
    assert _decision(scope, "measure", "Fact", "Commented") == "drop"


def test_a_field_drawn_on_the_page_is_no_filter_there_whatever_the_visual_order(work):
    # "a0" sorts before "v2", which draws Dim[week] on the same page: its filter on Dim[week] must not count
    _write(
        work,
        "report/definition/pages/home/visuals/a0/visual.json",
        {"name": "a0", **_visual(_measure("Kpi"), filters=[_column("Dim", "week")])},
    )
    _model, seeds, _closure, _rows = _analyse(work)
    assert "Home" in seeds.drawn[("c", "Dim", "week")]
    assert "Home" not in seeds.filters.get(("c", "Dim", "week"), set())


def test_a_hidden_visual_that_no_bookmark_shows_seeds_nothing(work):
    hidden = {"name": "v6", "isHidden": True, **_visual(_measure("Only On Detail"))}
    _write(work, "report/definition/pages/home/visuals/v6/visual.json", hidden)
    assert _decision(_scope(work), "measure", "Fact", "Only On Detail") == "drop"
    bookmark = {
        "name": "b",
        "explorationState": {
            "activeSection": "home",
            "sections": {
                "home": {
                    "visualContainers": {
                        "v6": {"singleVisual": {"visualType": "tableEx"}}
                    }
                }
            },
        },
    }
    _write(work, "report/definition/bookmarks/b.bookmark.json", bookmark)
    assert _decision(_scope(work), "measure", "Fact", "Only On Detail") == "keep"


def test_calculated_columns_and_tables_are_followed(work):
    scope = _scope(work)
    cur_week = scope[("measure", "Fact", "CurWeek")]
    assert cur_week["decision"] == "keep"
    assert "calc column Dim[Week Label]" in cur_week["load_bearing_for"]
    assert "calc table Calc" in scope[("measure", "Fact", "Old")]["load_bearing_for"]


def test_a_page_reached_only_through_a_measure_drillthrough_is_left_out(work):
    assert _decision(_scope(work), "measure", "Fact", "Only On Detail") == "drop"
    assert (
        _decision(
            _scope(work, include_measure_dt=True), "measure", "Fact", "Only On Detail"
        )
        == "keep"
    )


def test_a_measure_is_found_by_name_when_the_visual_names_an_old_home_table(work):
    assert _decision(_scope(work), "measure", "Fact", "Moved") == "keep"


def test_measure_categories(work):
    scope = _scope(work)
    assert _decision(scope, "measure", "Fact", "Kpi PY") == "genie"
    assert _decision(scope, "measure", "Fact", "Switcher") == "drop"
    assert scope[("measure", "Fact", "Switcher")]["reason"].startswith(
        "selector plumbing"
    )
    assert _decision(scope, "measure", "Fact", "Kpi by Size Groups") == "review"
    assert _decision(scope, "measure", "Fact", "MTD Inv. Days") == "review"
    assert scope[("measure", "Fact", "MTD Inv. Days")]["reason"].startswith(
        "KPI defined over a fixed window"
    )
    assert _decision(scope, "measure", "Fact", "Kpi Title") == "drop"


def test_table_decisions_follow_the_source(work):
    scope = _scope(work)
    assert _decision(scope, "table", "Fact") == "keep"
    assert (
        _decision(scope, "table", "Plan") == "snapshot"
    )  # SharePoint Excel, filtered on a reachable page
    assert _decision(scope, "table", "Measures") == "n/a"
    assert (
        _decision(scope, "table", "Sel") == "drop"
    )  # typed-in, disconnected, only a slicer and a dropped selector
    assert (
        _decision(scope, "table", "Bands") == "fold"
    )  # read by a segmentation measure
    assert _decision(scope, "table", "Calc") == "fold"


EXPRESSIONS = f"""expression Raw =
\t\tlet
\t\t    Source = #"Desc - DataBricks SQL"
\t\tin
\t\t    Source
\tlineageTag: a

expression 'Desc - DataBricks SQL' =
\t\tlet
\t\t    Source = {DATABRICKS_M}
\t\tin
\t\t    Source
\tlineageTag: b

expression RowLimit = 0 meta [IsParameterQuery = true]
"""


def test_a_table_loaded_through_a_shared_query_has_that_querys_source():
    expressions = tmdl_model.named_expressions(EXPRESSIONS, tmdl_model.EXPRESSION, 0)
    assert set(expressions) == {"Raw", "Desc - DataBricks SQL", "RowLimit"}
    joined = tmdl_model.Table(
        "Dim_Customer", "m", 'Source = Table.NestedJoin(Raw, {"k"}, Other, {"k"}, "x")'
    )
    local = tmdl_model.Table("Local", "m", "Source = Table.Combine({Unknown})")
    model = tmdl_model.Model(
        {t.name: t for t in (joined, local)}, [], expressions=expressions
    )
    assert (
        tmdl_model.source_class(joined, model) == "databricks"
    )  # Raw -> 'Desc - DataBricks SQL' -> Databricks
    assert tmdl_model.source_class(local, model) == "local"


def test_a_data_table_without_relationships_is_disconnected_but_no_selector():
    lookup = tmdl_model.Table("Config", "m", DATABRICKS_M, columns={"weight": None})
    fact = tmdl_model.Table("Fact", "m", DATABRICKS_M, columns={"key": None})
    dim = tmdl_model.Table("Dim", "m", DATABRICKS_M, columns={"key": None})
    model = tmdl_model.Model(
        {t.name: t for t in (lookup, fact, dim)}, [("Fact", "key", "Dim", "key", True)]
    )
    assert model.connected() == {"Fact", "Dim"}
    assert decisions.selector_tables(model) == set()


def test_a_table_merged_in_power_query_is_reached_through_the_merging_table():
    slicer_m = "\n".join(
        [
            "let",
            "    Source = Table.FromRows({}),",
            '    #"Merged Queries" = Table.NestedJoin(Source, {"k"}, #"Status Sheet", {"k"}, "s", JoinKind.LeftOuter),',
            '    // Table.NestedJoin(Source, {"k"}, Unused, {"k"}, "u", JoinKind.LeftOuter)',
            '    Expanded = Table.ExpandTableColumn(#"Merged Queries", "s", {"Status"}),',
            '    Added = Table.AddColumn(Expanded, "Unused", each [Unused] & "https://x//Unused")',
            "in",
            "    Added",
        ]
    )
    slicer = tmdl_model.Table("Slicer", "m", slicer_m, columns={"Status": None})
    status = tmdl_model.Table(
        "Status Sheet",
        "m",
        'Source = SharePoint.Files("https://x")',
        columns={"Status": None},
    )
    unused = tmdl_model.Table(
        "Unused", "m", 'Source = SharePoint.Files("https://y")', columns={"k": None}
    )
    expanded = tmdl_model.Table(
        "Expanded", "m", DATABRICKS_M, columns={"k": None}
    )  # a step of the same name
    model = tmdl_model.Model(
        {t.name: t for t in (slicer, status, unused, expanded)}, []
    )
    assert tmdl_model.power_query_tables(slicer, model) == {"Status Sheet"}
    seeds = closure.Seeds({"p"})
    seeds.filters[("c", "Slicer", "Status")].add("(report filter)")
    result = closure.close(model, seeds)
    assert "Status Sheet" in result.tables and "Unused" not in result.tables
    assert result.used_by[("t", "Status Sheet")] == {"power query Slicer"}


def test_a_calculated_relationship_key_is_followed():
    lookup = "VAR c = [comp_code] RETURN MAXX(FILTER(ALL(Other[comp_code], Other[country]), Other[comp_code] = c), Other[country])"
    fact = tmdl_model.Table(
        "Fact",
        "m",
        DATABRICKS_M,
        columns={"comp_code": None, "amount": None, "country_lookup": lookup},
    )
    dim = tmdl_model.Table(
        "Dim", "m", DATABRICKS_M, columns={"country_id": None, "name": None}
    )
    other = tmdl_model.Table(
        "Other", "m", DATABRICKS_M, columns={"comp_code": None, "country": None}
    )
    model = tmdl_model.Model(
        {t.name: t for t in (fact, dim, other)},
        [("Fact", "country_lookup", "Dim", "country_id", True)],
    )
    seeds = closure.Seeds({"p"})
    seeds.drawn[("c", "Fact", "amount")].add("p")
    seeds.drawn[("c", "Dim", "name")].add("p")
    result = closure.close(model, seeds)
    assert {
        ("Fact", "country_lookup"),
        ("Fact", "comp_code"),
        ("Other", "country"),
    } <= result.columns
    assert result.used_by[("c", "Fact", "country_lookup")] == {"relationship Dim"}
    assert (
        decisions.column_decision(
            ("Fact", "comp_code"), result, seeds, model.relationship_keys()
        )[0]
        == "keep"
    )


def test_columns_of_kept_tables(work):
    scope = _scope(work)
    assert scope[("column", "Dim", "week")]["reason"].startswith(
        "drawn/filter"
    )  # implicit Min on a card
    assert (
        _decision(scope, "column", "Dim", "region_name") == "keep"
    )  # descriptor for Genie
    assert _decision(scope, "column", "Fact", "technical_id") == "drop"
    assert "relationship key" in scope[("column", "Fact", "key")]["reason"]


def test_build_report_scope_returns_rows_csv_and_counts(work):
    rels = tmdl_model.relationships_from_rows(work.relationships)
    scope = build_report_scope(
        PartTree(work.model), PartTree(work.report), relationships=rels
    )
    assert scope["csv"].startswith("object_type,pbi_table,name,")
    assert scope["summary"]["pages_total"] == 2
    assert scope["summary"]["pages_measure_drillthrough_only"] == 1
    assert scope["summary"]["decisions"]["measure"]["keep"] >= 1
    assert {row["page"] for row in scope["pages"]} == {"Home", "Detail"}


def test_build_report_scope_without_a_report_is_empty(work):
    assert build_report_scope(PartTree(work.model), None) == {}


CALC_GROUP = """table CG
\tcalculationGroup
\t\tprecedence: 1

\t\tcalculationItem Ratio = ```
\t\t\t\tDIVIDE([Base A], [Kpi])
\t\t\t\t```

\t\tcalculationItem PY =
\t\t\t\tCALCULATE(SELECTEDMEASURE(), SAMEPERIODLASTYEAR(Dim[date]))

\t\tcalculationItem 'As is' = SELECTEDMEASURE()
\t\t\tformatStringDefinition = "0.0"

\tcolumn Name
\t\tdataType: string
\t\tsourceColumn: Name
"""

EXTRA_MEASURES = """table Extra
\tmeasure 'Base A' = SUM(Fact[amount])
\tmeasure 'Base B' = 1
\tmeasure 'Base C' = 2
\tmeasure 'Via Fx' = fx_ratio ("code") // fx_Label() in a comment
\tmeasure 'Fx Label' = fx_Label()
"""

FUNCTIONS = """function fx_Ratio = ```
\t\t(
\t\t\tCode: string
\t\t) =>
\t\tDIVIDE([Base B], [Kpi])
\t\t```
\tlineageTag: 1

function fx_Label =
\t\t"Label " & [Base C]
\tlineageTag: 2
"""

ROLES = {
    "R1": "role R1\n\tmodelPermission: read\n\n\ttablePermission Dim = ```\n\t\t\tDim[key] IN VALUES(Plan[key])\n\t\t\t```\n",
    "R2": 'role R2\n\tmodelPermission: read\n\n\ttablePermission Dim = Dim[region_name] = "x"\n',
}


@pytest.fixture
def work_plus(work):
    """`work` plus a calculation group, user-defined functions, RLS roles and a report-level measure."""
    _write(work, "model/tmdl/definition/tables/CG.tmdl", CALC_GROUP)
    _write(work, "model/tmdl/definition/tables/Extra.tmdl", EXTRA_MEASURES)
    _write(work, "model/tmdl/definition/functions.tmdl", FUNCTIONS)
    for role, tmdl in ROLES.items():
        _write(work, f"model/tmdl/definition/roles/{role}.tmdl", tmdl)
    definition = "report/definition"
    _write(
        work,
        f"{definition}/reportExtensions.json",
        {
            "entities": [
                {
                    "name": "Report Measures",
                    "measures": [{"name": "Status", "expression": "[Kpi] * 1"}],
                }
            ]
        },
    )
    visual = _visual(
        _column("CG", "Name"),
        _measure("Via Fx", "Extra"),
        _measure("Fx Label", "Extra"),
        _measure("Status", "Report Measures"),
    )
    _write(
        work,
        f"{definition}/pages/home/visuals/v6/visual.json",
        {"name": "v6", **visual},
    )
    return work


def test_calculation_items_and_functions_are_parsed():
    table = tmdl_model.parse_table(CALC_GROUP)
    assert table.calc_items == {
        "Ratio": "\t\t\t\tDIVIDE([Base A], [Kpi])",
        "PY": "CALCULATE(SELECTEDMEASURE(), SAMEPERIODLASTYEAR(Dim[date]))",
        "As is": "SELECTEDMEASURE()",
    }
    functions = tmdl_model.named_expressions(FUNCTIONS, tmdl_model.FUNCTION, 0)
    assert set(functions) == {"fx_Ratio", "fx_Label"}
    assert "DIVIDE([Base B], [Kpi])" in functions["fx_Ratio"]
    assert functions["fx_Label"] == '"Label " & [Base C]'
    assert tmdl_model.parse_roles(ROLES["R1"]) == [
        ("R1", "Dim", "\t\t\tDim[key] IN VALUES(Plan[key])")
    ]


def test_a_function_body_is_followed_and_kept_only_for_kept_callers(work_plus):
    scope = _scope(work_plus)
    assert _decision(scope, "measure", "Extra", "Base B") == "keep"
    assert (
        "function fx_Ratio" in scope[("measure", "Extra", "Base B")]["load_bearing_for"]
    )
    assert _decision(scope, "function", "", "fx_Ratio") == "keep"
    assert (
        _decision(scope, "function", "", "fx_Label") == "drop"
    )  # only the display measure 'Fx Label' calls it
    assert scope[("function", "", "fx_Label")]["reason"].startswith(
        "user-defined function called only by dropped"
    )


def test_calculation_items_are_decided_like_measures(work_plus):
    scope = _scope(work_plus)
    assert _decision(scope, "calculation_item", "CG", "Ratio") == "keep"
    assert _decision(scope, "calculation_item", "CG", "PY") == "genie"
    assert _decision(scope, "calculation_item", "CG", "As is") == "review"
    assert scope[("calculation_item", "CG", "Ratio")]["drawn"] == "Home"
    assert "calc group CG" in scope[("measure", "Extra", "Base A")]["load_bearing_for"]
    assert _decision(scope, "table", "CG") == "n/a"


def test_report_level_measures_join_the_model(work_plus):
    scope = _scope(work_plus)
    assert _decision(scope, "measure", "Report Measures", "Status") == "keep"
    assert scope[("table", "Report Measures", "")]["reason"].startswith(
        "report-level measures"
    )


def test_rls_roles_give_one_review_row_per_table(work_plus):
    scope = _scope(work_plus)
    row = scope[("rls_filter", "Dim", "R1; R2")]
    assert row["decision"] == "review"
    assert "2 role(s)" in row["reason"]
    assert row["reason"].endswith("reads Plan")


def test_references_are_case_insensitive_like_dax():
    fact = tmdl_model.Table(
        "Fact", "m", "", columns={"Week": None}, measures={"Orders in kg (W+1)": "1"}
    )
    calc = tmdl_model.Table("Calc", "m", "", columns={"Label": '[week] & ""'})
    model = tmdl_model.Model({"Fact": fact, "Calc": calc}, [])
    refs = closure.References(model)
    measures, columns, _ = refs.of("[Orders In KG (W+1)] + SUM(fact[WEEK])")
    assert measures == {"Orders in kg (W+1)"}
    assert columns == {("Fact", "Week")}
    calc.columns["week"] = None
    _, own, _ = closure.References(model).of("[WEEK]", own_table="Calc")
    assert own == {("Calc", "week")}
