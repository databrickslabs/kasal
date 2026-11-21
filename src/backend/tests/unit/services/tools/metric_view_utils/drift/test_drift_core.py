"""Unit tests for the UCMV drift monitor core (metric_view_utils/drift).

Everything is offline: SQL, generator and LLM are fakes. The baseline YAML mimics
what Unity Catalog actually returns in DESCRIBE … AS JSON ``view_text`` (re-serialised:
quoted ``"on"``, wrapped lines, no ``#`` comments).
"""

import json

import pytest
import yaml

from src.services.tools.metric_view_utils.drift import (
    dax_fingerprint,
)
from src.services.tools.metric_view_utils.drift import diff as D
from src.services.tools.metric_view_utils.drift import (
    extract_fingerprint,
    normalize_dax,
    run_drift_check,
)
from src.services.tools.metric_view_utils.drift.baseline import (
    fetch_baseline,
    parse_view_names,
    split_full_name,
)
from src.services.tools.metric_view_utils.drift.candidates import (
    dependency_closure,
    referenced_aliases,
)
from src.services.tools.metric_view_utils.drift.fingerprint import extract_pbi_name
from src.services.tools.metric_view_utils.drift.matcher import to_pbi_measures
from src.services.tools.metric_view_utils.drift.patcher import (
    Replacement,
    apply_patch,
    verify_invariant,
)
from src.services.tools.metric_view_utils.drift.semantic_check import (
    judge_legacy_measures,
)

DAX_REVENUE = "SUM(Sales[Amount])"
DAX_MARGIN = "DIVIDE([Revenue], [Cost])"
DAX_COST = "SUM(Sales[Cost])"
FP_REVENUE = dax_fingerprint(DAX_REVENUE)
FP_MARGIN_OLD = dax_fingerprint("DIVIDE([Revenue] - [Cost], [Revenue])")

BASELINE_YAML = f"""version: 1.1

source: prod.sales.fact_sales

joins:
  - name: customer
    source: prod.sales.dim_customer
    "on": customer.customer_id = source.customer_id

dimensions:
  - name: region
    expr: customer.region
    comment: Customer region

measures:
  - name: revenue
    expr: SUM(source.amount)
    comment: "PBI: Revenue · {FP_REVENUE} · LLM[high/translatable_direct]"
    display_name: Revenue
    synonyms:
      - turnover

  - name: margin_pct
    expr: SUM(source.amount - source.cost) / SUM(source.amount)
    comment: "PBI: Margin % · {FP_MARGIN_OLD}"
    display_name: Margin %

  - name: units
    expr: SUM(source.qty)
    comment: "PBI: Units"

  - name: old_kpi
    expr: SUM(source.old)
    comment: "PBI: Old KPI · dax#00000000"

  - name: row_count
    expr: COUNT(1)
    comment: Row count"""


def _describe_payload(view_text: str) -> dict:
    doc = {"type": "METRIC_VIEW", "view_text": view_text}
    return {"success": True, "data": {"result": {"data_array": [[json.dumps(doc)]]}}}


def _sql_fn_for(views: dict):
    def _sql(statement: str) -> dict:
        for name, text in views.items():
            quoted = ".".join(f"`{p}`" for p in name.split("."))
            if quoted in statement and statement.startswith("DESCRIBE"):
                return _describe_payload(text)
        return {"success": False, "error": "TABLE_OR_VIEW_NOT_FOUND"}

    return _sql


def _pbi(name, dax, alloc="FactSales"):
    return {
        "measure_name": name,
        "original_name": name,
        "dax_expression": dax,
        "proposed_allocation": alloc,
    }


# ── fingerprint ──────────────────────────────────────────────────────────────


class TestFingerprint:
    def test_formatting_and_comments_do_not_change_fingerprint(self):
        a = "CALCULATE(SUM(Sales[Amount]), Sales[Year] = 2024)"
        b = "calculate (\n  sum( Sales[Amount] ) , -- yearly\n  Sales[Year]=2024 )"
        assert normalize_dax(a) == normalize_dax(b)
        assert dax_fingerprint(a) == dax_fingerprint(b)

    def test_string_literal_case_is_significant(self):
        assert dax_fingerprint('Sales[Cur] = "EUR"') != dax_fingerprint(
            'Sales[Cur] = "eur"'
        )

    def test_logic_change_changes_fingerprint(self):
        assert dax_fingerprint("SUM(Sales[Amount])") != dax_fingerprint(
            "SUM(Sales[Cost])"
        )

    def test_empty_dax_has_no_fingerprint(self):
        assert dax_fingerprint("") == ""
        assert dax_fingerprint("   ") == ""

    def test_extract_from_comment(self):
        fp = dax_fingerprint("SUM(x[y])")
        assert extract_fingerprint(f"PBI: X · {fp} · Used on: Page 1") == fp
        assert extract_fingerprint("PBI: X") is None

    @pytest.mark.parametrize(
        "comment,expected",
        [
            ("PBI: Margin % · dax#12345678", "Margin %"),
            ("PBI: Revenue YTD [LLM high]", "Revenue YTD"),
            ("PBI: Net Sales", "Net Sales"),
            ("Row count", None),
        ],
    )
    def test_extract_pbi_name(self, comment, expected):
        assert extract_pbi_name(comment) == expected


# ── baseline ─────────────────────────────────────────────────────────────────


class TestBaseline:
    def test_parse_view_names_accepts_lines_json_and_urls(self):
        assert parse_view_names("a.b.c\n a.b.d ,a.b.c") == ["a.b.c", "a.b.d"]
        assert parse_view_names('["x.y.z"]') == ["x.y.z"]
        url = "https://example.com/explore/data/cat/sch/mv_sales?o=1"
        assert parse_view_names(url) == ["cat.sch.mv_sales"]

    def test_split_full_name(self):
        assert split_full_name("`my-cat`.sch.view") == ("my-cat", "sch", "view")
        assert split_full_name("sch.view") is None

    def test_fetch_reads_view_text(self):
        v = fetch_baseline(
            "prod.sales.mv_sales", _sql_fn_for({"prod.sales.mv_sales": BASELINE_YAML})
        )
        assert v.ok
        assert v.spec["source"] == "prod.sales.fact_sales"
        assert v.yaml_text == BASELINE_YAML

    def test_fetch_falls_back_to_show_create(self):
        ddl = f"CREATE VIEW x WITH METRICS LANGUAGE YAML AS $$\n{BASELINE_YAML}\n$$"

        def sql(statement):
            if statement.startswith("SHOW CREATE"):
                return {"success": True, "data": {"result": {"data_array": [[ddl]]}}}
            return {"success": False, "error": "unsupported"}

        assert fetch_baseline("a.b.c", sql).ok

    def test_fetch_missing_view_reports_error(self):
        v = fetch_baseline("a.b.missing", _sql_fn_for({}))
        assert not v.ok
        assert "NOT_FOUND" in v.error


# ── classification ───────────────────────────────────────────────────────────


def _classify(measures, views=None):
    from src.services.tools.metric_view_utils.drift.matcher import match_measures

    views = views or {"prod.sales.mv_sales": BASELINE_YAML}
    baselines = [fetch_baseline(n, _sql_fn_for(views)) for n in views]
    pbi = to_pbi_measures(measures)
    return D.classify(match_measures(baselines, pbi), pbi)


def _items(user_prompt):
    """The JSON item list the semantic check sends after its instructions."""
    return json.loads(user_prompt.rsplit("\n\n", 1)[1])


def _by(drifts):
    return {d.original_name: d for d in drifts}


class TestClassify:
    MEASURES = [
        _pbi("Revenue", DAX_REVENUE),
        _pbi("Margin %", DAX_MARGIN),
        _pbi("Units", "SUM(Sales[Qty])"),
        _pbi("Cost", DAX_COST),
        _pbi("Other Fact KPI", "SUM(Stock[Qty])", alloc="FactStock"),
    ]

    def test_every_bucket(self):
        got = _by(_classify(self.MEASURES))
        assert got["Revenue"].status == D.UNCHANGED
        assert got["Margin %"].status == D.CHANGED
        assert got["Units"].status == D.LEGACY  # no fingerprint in its comment
        assert got["Cost"].status == D.NEW
        assert got["Cost"].view == "prod.sales.mv_sales"  # placed by allocation vote
        assert got["Other Fact KPI"].status == D.UNASSIGNED
        assert got["Old KPI"].status == D.REMOVED
        assert got["row_count"].status == D.BASELINE_ONLY

    def test_new_measure_without_dax_is_flagged(self):
        got = _by(_classify(self.MEASURES[:1] + [_pbi("Blank", "")]))
        assert got["Blank"].status == D.NEW
        assert "no DAX" in got["Blank"].note

    def test_matches_by_snake_name_without_tag(self):
        text = BASELINE_YAML.replace('"PBI: Units"', "Units of product")
        got = _by(_classify([_pbi("Units", "SUM(Sales[Qty])")], {"a.b.c": text}))
        assert got["Units"].status == D.LEGACY
        assert got["Units"].measure_name == "units"


# ── semantic check ───────────────────────────────────────────────────────────


class TestSemanticCheck:
    def test_relabels_legacy_measures(self):
        drifts = _classify(TestClassify.MEASURES)

        def complete(system, user):
            items = _items(user)
            return json.dumps(
                {
                    "results": [
                        {"id": i["id"], "verdict": "CHANGED", "reason": "qty→units"}
                        for i in items
                    ]
                }
            )

        counts = judge_legacy_measures(drifts, complete)
        units = _by(drifts)["Units"]
        assert units.status == D.POSSIBLY_CHANGED
        assert units.llm_reason == "qty→units"
        assert counts["judged"] == 1

    def test_failed_call_leaves_measures_unverified(self):
        drifts = _classify(TestClassify.MEASURES)
        counts = judge_legacy_measures(drifts, lambda s, u: None)
        assert _by(drifts)["Units"].status == D.LEGACY
        assert counts["unverified"] == 1

    def test_batches_calls(self):
        measures = [_pbi(f"M{i}", f"SUM(T[c{i}])") for i in range(5)]
        lines = "\n".join(
            f"  - name: m{i}\n    expr: SUM(source.c{i})" for i in range(5)
        )
        text = f"version: 1.1\nsource: a.b.t\nmeasures:\n{lines}"
        drifts = _classify(measures, {"a.b.c": text})
        calls = []

        def complete(system, user):
            calls.append(user)
            items = _items(user)
            return json.dumps(
                {"results": [{"id": i["id"], "verdict": "SAME"} for i in items]}
            )

        judge_legacy_measures(drifts, complete, batch_size=2)
        assert len(calls) == 3
        assert all(d.status == D.UNCHANGED_LLM for d in drifts)


# ── patcher ──────────────────────────────────────────────────────────────────


class TestPatcher:
    def test_append_preserves_every_baseline_line(self):
        new = {"name": "cost", "expr": "SUM(source.cost)", "comment": "PBI: Cost"}
        res = apply_patch(BASELINE_YAML, [new], [], [])
        assert res.ok, res.problems
        base_lines = BASELINE_YAML.split("\n")
        prop_lines = res.yaml_text.split("\n")
        # Insertion only: the baseline is a subsequence, in order, of the proposal.
        it = iter(prop_lines)
        assert all(line in it for line in base_lines)
        spec = yaml.safe_load(res.yaml_text)
        assert spec["measures"][-1]["name"] == "cost"

    def test_replace_changes_only_expr_and_fingerprint(self):
        new_fp = dax_fingerprint(DAX_MARGIN)
        r = Replacement(
            "margin_pct",
            {"name": "x", "expr": "MEASURE(revenue) / MEASURE(cost)", "comment": "new"},
            new_fp,
        )
        res = apply_patch(BASELINE_YAML, [], [r], [])
        assert res.ok, res.problems
        m = {x["name"]: x for x in yaml.safe_load(res.yaml_text)["measures"]}[
            "margin_pct"
        ]
        assert m["expr"] == "MEASURE(revenue) / MEASURE(cost)"
        assert m["display_name"] == "Margin %"  # baseline metadata kept
        assert m["comment"] == f"PBI: Margin % · {new_fp}"

    def test_adds_join_block_when_missing(self):
        text = "version: 1.1\n\nsource: a.b.f\n\nmeasures:\n  - name: m\n    expr: SUM(source.x)"
        join = {"name": "dim_d", "source": "a.b.d", "on": "dim_d.id = source.d_id"}
        res = apply_patch(text, [], [], [join])
        assert res.ok, res.problems
        spec = yaml.safe_load(res.yaml_text)
        assert (
            spec["joins"][0]["on"] == "dim_d.id = source.d_id"
        )  # 'on' stays a string key

    def test_invariant_catches_tampering(self):
        base = yaml.safe_load(BASELINE_YAML)
        tampered = BASELINE_YAML.replace("SUM(source.qty)", "SUM(source.qty2)")
        problems = verify_invariant(base, tampered, set(), set(), set())
        assert any("units" in p for p in problems)
        tampered_src = BASELINE_YAML.replace("fact_sales", "fact_other")
        assert any(
            "source" in p
            for p in verify_invariant(base, tampered_src, set(), set(), set())
        )

    def test_unknown_replacement_is_a_problem(self):
        res = apply_patch(BASELINE_YAML, [], [Replacement("nope", {"expr": "1"})], [])
        assert not res.ok
        assert res.yaml_text is None


# ── candidates ───────────────────────────────────────────────────────────────


def test_dependency_closure_pulls_referenced_measures():
    pbi = to_pbi_measures(
        [
            _pbi("Revenue", DAX_REVENUE),
            _pbi("Cost", DAX_COST),
            _pbi("Margin %", DAX_MARGIN),
        ]
    )
    margin = [p for p in pbi if p.original_name == "Margin %"]
    names = {p.original_name for p in dependency_closure(margin, pbi)}
    assert names == {"Margin %", "Revenue", "Cost"}


def test_referenced_aliases_skips_source():
    assert referenced_aliases("SUM(source.a) + MAX(dim_d.b)") == {"dim_d"}


# ── end to end ───────────────────────────────────────────────────────────────


def _fake_generator(calls):
    """Mimics the UC Metric View Generator's JSON-mode output for a measure subset."""

    sql = {
        "Revenue": "SUM(source.amount)",
        "Cost": "SUM(source.cost)",
        "Margin %": "MEASURE(revenue) / MEASURE(cost)",
        "Segment Sales": "SUM(source.amount) FILTER (WHERE segment.name = 'B2B')",
    }

    def _gen(measures):
        calls.append([m["original_name"] for m in measures])
        entries = [
            {
                "name": m["original_name"]
                .lower()
                .replace(" ", "_")
                .replace("%", "pct"),
                "expr": sql[m["original_name"]],
                "comment": f"PBI: {m['original_name']} · LLM[high/composed]",
            }
            for m in measures
            if m["original_name"] in sql
        ]
        spec = {
            "version": 1.1,
            "source": "prod.sales.fact_sales",
            "joins": [
                {
                    "name": "customer",
                    "source": "prod.sales.dim_customer",
                    "on": "customer.customer_id = source.customer_id",
                },
                {
                    "name": "segment",
                    "source": "prod.sales.dim_segment",
                    "on": "segment.id = source.segment_id",
                },
            ],
            "measures": entries,
        }
        return {
            "yaml": {"FactSales": yaml.safe_dump(spec, sort_keys=False)},
            "resolved_measures_by_table": {
                "FactSales": [
                    {
                        "measure_name": e["name"],
                        "original_name": e["comment"][5:].split(" ·")[0],
                    }
                    for e in entries
                ]
            },
        }

    return _gen


def test_end_to_end_proposal():
    calls: list = []
    extraction = {
        "measures_json": [
            _pbi("Revenue", DAX_REVENUE),
            _pbi("Margin %", DAX_MARGIN),
            _pbi("Cost", DAX_COST),
            _pbi("Segment Sales", 'CALCULATE([Revenue], Segment[Name] = "B2B")'),
        ]
    }
    report = run_drift_check(
        ["prod.sales.mv_sales"],
        extraction,
        sql_fn=_sql_fn_for({"prod.sales.mv_sales": BASELINE_YAML}),
        generate_fn=_fake_generator(calls),
    )
    s = report["summary"]
    assert s["changed_in_pbi"] == 1 and s["new_in_pbi"] == 2
    assert (
        s["measures_added"] == 2
        and s["measures_updated"] == 1
        and s["joins_added"] == 1
    )
    # Only drifted measures + their dependencies went to the generator, once.
    assert len(calls) == 1
    assert set(calls[0]) == {"Margin %", "Cost", "Segment Sales", "Revenue"}

    view = report["views"][0]
    assert view["invariant_problems"] == []
    spec = yaml.safe_load(view["proposed_yaml"])
    names = [m["name"] for m in spec["measures"]]
    assert names[:5] == ["revenue", "margin_pct", "units", "old_kpi", "row_count"]
    assert set(names[5:]) == {"cost", "segment_sales"}
    assert [j["name"] for j in spec["joins"]] == ["customer", "segment"]
    added = {m["name"]: m for m in spec["measures"][5:]}
    assert dax_fingerprint(DAX_COST) in added["cost"]["comment"]

    statuses = {m["original_name"]: m for m in view["measures"]}
    assert statuses["Cost"]["applied"] and statuses["Cost"]["translation"] == "llm"
    assert statuses["Old KPI"]["status"] == D.REMOVED  # flagged, kept in YAML
    assert "old_kpi" in names
    # Deployer handoff
    assert report["yaml"] == {"mv_sales": view["proposed_yaml"]}
    assert report["catalog"] == "prod" and report["schema_name"] == "sales"
    assert report["deploy_ddl"]["prod.sales.mv_sales"].startswith(
        "CREATE OR REPLACE VIEW prod.sales.mv_sales"
    )


def test_no_drift_means_no_generator_call_and_no_proposal():
    calls: list = []
    report = run_drift_check(
        ["prod.sales.mv_sales"],
        {"measures_json": [_pbi("Revenue", DAX_REVENUE)]},
        sql_fn=_sql_fn_for({"prod.sales.mv_sales": BASELINE_YAML}),
        generate_fn=_fake_generator(calls),
    )
    assert calls == []
    assert report["yaml"] == {}
    assert report["views"][0]["proposed_yaml"] == BASELINE_YAML


def test_suspected_change_is_shown_not_applied_by_default():
    calls: list = []
    text = BASELINE_YAML.replace(f" · {FP_REVENUE}", "")  # revenue becomes legacy

    def judge(system, user):
        items = _items(user)
        return json.dumps(
            {"results": [{"id": i["id"], "verdict": "CHANGED"} for i in items]}
        )

    common = dict(
        sql_fn=_sql_fn_for({"prod.sales.mv_sales": text}),
        generate_fn=_fake_generator(calls),
        complete_fn=judge,
    )
    extraction = {"measures_json": [_pbi("Revenue", DAX_REVENUE)]}
    report = run_drift_check(["prod.sales.mv_sales"], extraction, **common)
    m = {x["original_name"]: x for x in report["views"][0]["measures"]}["Revenue"]
    assert m["status"] == D.POSSIBLY_CHANGED and not m.get("applied")
    assert m["proposed_expr"] == "SUM(source.amount)"
    assert report["yaml"] == {}

    report = run_drift_check(
        ["prod.sales.mv_sales"], extraction, apply_suspected_changes=True, **common
    )
    assert report["summary"]["measures_updated"] == 1


def test_unreadable_view_is_reported_not_fatal():
    report = run_drift_check(
        ["prod.sales.mv_sales", "prod.sales.missing"],
        {"measures_json": [_pbi("Revenue", DAX_REVENUE)]},
        sql_fn=_sql_fn_for({"prod.sales.mv_sales": BASELINE_YAML}),
        generate_fn=_fake_generator([]),
    )
    assert report["summary"]["views_unreadable"] == 1
    missing = [v for v in report["views"] if v["name"] == "missing"][0]
    assert missing["error"]


def test_patch_referencing_missing_measure_is_held_back():
    # Margin % changed and its new SQL uses MEASURE(cost) — but Cost is not in
    # today's model, so no `cost` measure will exist in the view.
    report = run_drift_check(
        ["prod.sales.mv_sales"],
        {"measures_json": [_pbi("Revenue", DAX_REVENUE), _pbi("Margin %", DAX_MARGIN)]},
        sql_fn=_sql_fn_for({"prod.sales.mv_sales": BASELINE_YAML}),
        generate_fn=_fake_generator([]),
    )
    margin = {m["original_name"]: m for m in report["views"][0]["measures"]}["Margin %"]
    assert not margin.get("applied")
    assert "['cost']" in margin["note"]
    assert report["yaml"] == {}
