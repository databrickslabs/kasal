"""Power BI extraction and configuration derivation, independent of tool wiring."""

import base64
from unittest.mock import MagicMock, patch

from src.services.powerbi import pipeline_config as gc

TENANT_ID = "tenant-test"
CLIENT_ID = "client-test"
CLIENT_SECRET = "test-secret"


def _gen():
    return gc


class TestParseTmdlToAdminTables:
    """parse_tmdl_to_admin_tables produces the same shape as parse_admin_tables."""

    def _part(self, name, body):
        return {
            "path": f"definition/tables/{name}.tmdl",
            "payload": base64.b64encode(body.encode()).decode(),
        }

    def test_extracts_columns_measures_and_mquery(self):
        gen = _gen()
        tmdl = (
            "table Fact_Sales\n"
            "\tcolumn amount\n\t\tdataType: double\n"
            "\tcolumn region_key\n"
            "\tmeasure 'Total Revenue' = SUM(Fact_Sales[amount])\n\t\tformatString: 0.00\n"
            "\tmeasure Margin = DIVIDE([Profit],[Revenue])\n"
            "\tpartition Fact_Sales = m\n\t\tsource =\n\t\t\tlet S = Value.NativeQuery(x) in S\n"
        )
        out = gen.parse_tmdl_to_admin_tables(
            [self._part("Fact_Sales", tmdl)], dataset_id="ds1"
        )
        assert "Fact_Sales" in out
        t = out["Fact_Sales"]
        assert {c["name"] for c in t["columns"]} == {"amount", "region_key"}
        assert {m["name"] for m in t["measures"]} == {"Total Revenue", "Margin"}
        # formatString line must be stripped from the measure expression
        rev = next(m for m in t["measures"] if m["name"] == "Total Revenue")
        assert rev["expression"] == "SUM(Fact_Sales[amount])"
        assert "formatString" not in rev["expression"]
        assert "Value.NativeQuery" in t["mquery_expression"]

    def test_multiline_let_in_mquery_not_truncated(self):
        """Regression: a multi-line `let ... in` partition source must be captured
        in full, not truncated to just 'let'.

        The old regex stopped at the first `\\n<word> =`, but M-Query's own let-body
        is full of `Source = ...` / `Filtered = ...` bindings, so the source was cut
        to its first token ('let'). Customer symptom: mquery_expression == 'let'.
        """
        gen = _gen()
        tmdl = (
            "table DCC_Customer\n"
            "\tcolumn CustomerID\n\t\tdataType: string\n"
            "\tpartition DCC_Customer = m\n"
            "\t\tmode: import\n"
            "\t\tsource =\n"
            "\t\t\tlet\n"
            '\t\t\t    Source = Databricks.Catalogs("h", "p", null),\n'
            '\t\t\t    db = Source{[Name="sales"]}[Data],\n'
            "\t\t\t    Filtered = Table.SelectRows(db, each [Active] = true)\n"
            "\t\t\tin\n"
            "\t\t\t    Filtered\n"
            "\t\tannotation PBI_ResultType = Table\n"
        )
        out = gen.parse_tmdl_to_admin_tables([self._part("DCC_Customer", tmdl)])
        mq = out["DCC_Customer"]["mquery_expression"]
        assert mq != "let", "M-Query truncated to just 'let'"
        assert "Databricks.Catalogs" in mq
        assert "Table.SelectRows" in mq
        assert "Filtered" in mq
        # the TMDL directive after the source block must NOT leak in
        assert "annotation" not in mq

    def test_skips_local_date_tables(self):
        gen = _gen()
        parts = [
            self._part(
                "LocalDateTable_abc", "table LocalDateTable_abc\n\tcolumn Date\n"
            )
        ]
        assert gen.parse_tmdl_to_admin_tables(parts) == {}

    def test_ignores_non_table_parts(self):
        gen = _gen()
        parts = [
            {
                "path": "definition/model.tmdl",
                "payload": base64.b64encode(b"model M").decode(),
            }
        ]
        assert gen.parse_tmdl_to_admin_tables(parts) == {}

    def test_empty_or_none(self):
        gen = _gen()
        assert gen.parse_tmdl_to_admin_tables([]) == {}
        assert gen.parse_tmdl_to_admin_tables(None) == {}


class TestGetFabricTokenGrants:
    """get_fabric_token uses SP or SA grant with the Fabric scope."""

    def test_sp_grant_fabric_scope(self):
        gen = _gen()
        cap = {}

        def _post(url, data=None, timeout=None):
            cap["data"] = data
            r = MagicMock()
            r.status_code = 200
            r.json.return_value = {"access_token": "fab-sp"}
            return r

        with patch("requests.post", side_effect=_post):
            tok = gen.get_fabric_token(TENANT_ID, CLIENT_ID, CLIENT_SECRET)
        assert tok == "fab-sp"
        assert cap["data"]["grant_type"] == "client_credentials"
        assert cap["data"]["scope"] == "https://api.fabric.microsoft.com/.default"

    def test_sa_grant_fabric_scope(self):
        gen = _gen()
        cap = {}

        def _post(url, data=None, timeout=None):
            cap["data"] = data
            r = MagicMock()
            r.status_code = 200
            r.json.return_value = {"access_token": "fab-sa"}
            return r

        with patch("requests.post", side_effect=_post):
            tok = gen.get_fabric_token(
                TENANT_ID, CLIENT_ID, None, username="sa@x.com", password="pw"
            )
        assert tok == "fab-sa"
        assert cap["data"]["grant_type"] == "password"
        assert cap["data"]["username"] == "sa@x.com"
        assert cap["data"]["scope"] == "https://api.fabric.microsoft.com/.default"


class TestExtractMeasuresKeyTolerance:
    """extract_measures must read DMV columns whether keys are bracketed or not.

    Regression: the Power BI executeQueries API returns column keys either
    bracketed ('[Expression]') or unbracketed ('Expression'). Reading only the
    bracketed form yielded 471 measures with 0 DAX (empty switch_decompositions).
    """

    def _gen(self):
        return gc

    def _mock_resp(self, rows):
        r = MagicMock()
        r.status_code = 200
        r.json.return_value = {"results": [{"tables": [{"rows": rows}]}]}
        return r

    def test_unbracketed_keys_are_read(self):
        gen = self._gen()
        rows = [
            {
                "Measure Name": "Rev",
                "Expression": "SUM(x)",
                "Table": "Fact",
                "Description": "",
            }
        ]
        with patch("requests.post", return_value=self._mock_resp(rows)):
            measures = gen.extract_measures("tok", "ws", "ds")
        assert len(measures) == 1
        assert measures[0]["measure_name"] == "Rev"
        assert measures[0]["expression"] == "SUM(x)"  # <-- was '' before the fix

    def test_bracketed_keys_still_work(self):
        gen = self._gen()
        rows = [
            {
                "[Measure Name]": "Margin",
                "[Expression]": "DIVIDE(a,b)",
                "[Table]": "Fact",
                "[Description]": "",
            }
        ]
        with patch("requests.post", return_value=self._mock_resp(rows)):
            measures = gen.extract_measures("tok", "ws", "ds")
        assert measures[0]["measure_name"] == "Margin"
        assert measures[0]["expression"] == "DIVIDE(a,b)"

    def test_row_get_helper(self):
        gen = self._gen()
        assert gen._row_get({"Expression": "X"}, "Expression") == "X"
        assert gen._row_get({"[Expression]": "Y"}, "Expression") == "Y"
        assert gen._row_get({}, "Expression", "def") == "def"


class TestEtlColumnCuration:
    """P2 curation: pure ETL plumbing columns are demoted from dimensions."""

    def test_etl_columns_excluded_business_columns_kept(self):
        from src.services.powerbi.pipeline_config import (
            derive_dimension_exclusions,
        )

        admin_tables = {
            "Fact_X": {
                "columns": [
                    {"name": "Region"},  # business — keep
                    {"name": "ObjVers"},  # ETL — exclude
                    {"name": "LogSys"},  # ETL — exclude
                    {"name": "process_run_id"},  # ETL — exclude
                    {"name": "YearCard"},  # ETL — exclude
                    {"name": "SalesAmount", "isHidden": True},  # hidden — exclude
                ]
            }
        }
        excl = derive_dimension_exclusions(admin_tables)["Fact_X"]
        assert "obj_vers" in excl
        assert "log_sys" in excl
        assert "process_run_id" in excl
        assert "year_card" in excl
        assert "sales_amount" in excl  # hidden
        assert "region" not in excl


class TestMeasureRefResolution:
    """PROP-1: referenced-measure DAX is transpiled into base_expr, not a TODO."""

    def _R(self, dax):
        from src.services.powerbi.pipeline_config import (
            _resolve_referenced_measure_dax,
        )

        return _resolve_referenced_measure_dax(dax)

    def test_bare_sum(self):
        assert self._R("SUM(FT_QSE[kbi_value])") == {
            "base_expr": "SUM(source.kbi_value)",
            "base_filters": [],
        }

    def test_calculate_with_filters(self):
        r = self._R('CALCULATE(SUM(T[val]), T[ver]="B000")')
        assert r["base_expr"] == "SUM(source.val)"
        assert r["base_filters"] == ["ver = 'B000'"]

    def test_constant(self):
        assert self._R("1") == {"base_expr": "1", "base_filters": []}

    def test_switch_picks_first_calculate_branch_no_leak(self):
        plant = (
            "Switch(TRUE(),\n"
            "  Or(ISFILTERED(C_Dim_Plant[plant_desc]),HASONEVALUE(C_Dim_Plant[plant])),\n"
            '  CALCULATE(SUM(FT_QSE[kbi_value]), FT_QSE[bic_chversion]="0000", FT_QSE[bic_creg_type]="Plant"),\n'
            '  CALCULATE(SUM(FT_QSE[kbi_value]), FT_QSE[bic_chversion]="0000", FT_QSE[bic_creg_type]="Company Code"))'
        )
        r = self._R(plant)
        assert r["base_expr"] == "SUM(source.kbi_value)"
        # only the FIRST (plant) branch filters — Company Code must NOT leak in
        assert r["base_filters"] == [
            "bic_chversion = '0000'",
            "bic_creg_type = 'Plant'",
        ]

    def test_untranslatable_returns_none(self):
        assert self._R("var x = SELECTEDVALUE(a) return x + 1") is None

    def test_var_scaffolding_switch_sumx_filter_bp(self):
        """Dependency-cascade fix: the _BP twin carries `var std/etd` date-window
        scaffolding AND wraps the aggregate in SUMX(FILTER(...)). Both broke
        resolution (first CALCULATE found was the scaffolding CALCULATE([F_Start_
        date]), and the agg regex didn't handle SUMX(FILTER,col)) → the base
        dropped → all its _BP dependents cascaded out. Must now resolve."""
        bp = (
            "var std = CALCULATE([F_Start_date]) var etd= CALCULATE([F_End_date]) "
            "return Switch(TRUE(), "
            "Or(ISFILTERED(C_Dim_Plant[plant_desc]),HASONEVALUE(C_Dim_Plant[plant])), "
            'CALCULATE(SUMX(FILTER(FT_QSE, FT_QSE[bic_chversion] = "B000" && '
            'FT_QSE[bic_creg_type] = "Plant"),FT_QSE[kbi_value])), '
            'CALCULATE(SUMX(FILTER(FT_QSE, FT_QSE[bic_chversion] = "B000" && '
            'FT_QSE[bic_creg_type] = "Company Code"),FT_QSE[kbi_value])) )'
        )
        r = self._R(bp)
        assert (
            r is not None
        ), "BP base measure must resolve (else _BP dependents cascade out)"
        assert r["base_expr"] == "SUM(source.kbi_value)"
        assert r["base_filters"] == [
            "bic_chversion = 'B000'",
            "bic_creg_type = 'Plant'",
        ]

    def test_var_scaffolding_plain_sumx_filter(self):
        totbp = (
            "var std = CALCULATE([F_Start_date]) var etd= CALCULATE([F_End_date]) "
            'return CALCULATE(SUMX(FILTER(FT_QSE, FT_QSE[bic_chversion] = "B000" ),'
            "FT_QSE[kbi_value]))"
        )
        r = self._R(totbp)
        assert r["base_expr"] == "SUM(source.kbi_value)"
        assert r["base_filters"] == ["bic_chversion = 'B000'"]

    def test_end_to_end_no_todo_literal(self):
        from src.services.powerbi.pipeline_config import (
            derive_measure_resolutions,
        )

        measures = [
            {
                "measure_name": "BaseKBI",
                "table_name": "FT_QSE",
                "expression": 'CALCULATE(SUM(FT_QSE[kbi_value]), FT_QSE[bic_chversion]="0000")',
            },
            {
                "measure_name": "CC",
                "table_name": "FT_QSE",
                "expression": 'CALCULATE([BaseKBI], FT_QSE[bic_csubkbi]="KEMAA0011")',
            },
        ]
        res = derive_measure_resolutions(measures)
        assert "BaseKBI" in res
        assert res["BaseKBI"]["base_expr"] == "SUM(source.kbi_value)"
        assert "TODO" not in res["BaseKBI"]["base_expr"]


class TestGeoSwitchDecomposition:
    """Geo-selector SWITCH → two static measures (plant_ + company_).

    A UC metric view has no slicer context, so a PBI measure that is
    SWITCH(TRUE(), Or(ISFILTERED/HASONEVALUE(plant)), plantBranch, companyBranch)
    must emit BOTH branches. Recovers the company variant the single PBI measure
    would otherwise collapse."""

    def _G(self, measures):
        from src.services.powerbi.pipeline_config import (
            derive_geo_switch_decompositions,
        )

        return derive_geo_switch_decompositions(measures)

    def test_actual_simple_calculate_branches(self):
        ms = [
            {
                "original_name": "Plant_Comp KBI_Value_Actual",
                "table_name": "FT_QSE",
                "expression": (
                    "Switch(TRUE(), Or(ISFILTERED(C_Dim_Plant[plant_desc]),"
                    "HASONEVALUE(C_Dim_Plant[plant])), "
                    'CALCULATE(SUM(FT_QSE[kbi_value]), FT_QSE[bic_chversion]="0000", '
                    'FT_QSE[bic_creg_type]="Plant"), '
                    'CALCULATE(SUM(FT_QSE[kbi_value]), FT_QSE[bic_chversion]="0000", '
                    'FT_QSE[bic_creg_type]="Company Code"))'
                ),
            }
        ]
        entries = self._G(ms)["FT_QSE"]
        by = {e["name"]: e["raw_expr"] for e in entries}
        assert by["plant_kbi_value_actual"] == (
            "SUM(source.kbi_value) FILTER (WHERE bic_chversion = '0000' "
            "AND bic_creg_type = 'Plant')"
        )
        assert by["company_kbi_value_actual"] == (
            "SUM(source.kbi_value) FILTER (WHERE bic_chversion = '0000' "
            "AND bic_creg_type = 'Company Code')"
        )

    def test_bp_sumx_filter_with_scaffolding(self):
        ms = [
            {
                "original_name": "Plant_Comp KBI_Value_BP",
                "table_name": "FT_QSE",
                "expression": (
                    "var std = CALCULATE([F_Start_date]) var etd= CALCULATE([F_End_date]) "
                    "return Switch(TRUE(), Or(ISFILTERED(C_Dim_Plant[plant_desc]),"
                    "HASONEVALUE(C_Dim_Plant[plant])), "
                    'CALCULATE(SUMX(FILTER(FT_QSE, FT_QSE[bic_chversion]="B000" && '
                    'FT_QSE[bic_creg_type]="Plant"),FT_QSE[kbi_value])), '
                    'CALCULATE(SUMX(FILTER(FT_QSE, FT_QSE[bic_chversion]="B000" && '
                    'FT_QSE[bic_creg_type]="Company Code"),FT_QSE[kbi_value])))'
                ),
            }
        ]
        by = {e["name"]: e["raw_expr"] for e in self._G(ms)["FT_QSE"]}
        assert "bic_creg_type = 'Plant'" in by["plant_kbi_value_bp"]
        assert "bic_creg_type = 'Company Code'" in by["company_kbi_value_bp"]

    def test_non_geo_switch_ignored(self):
        # A SELECTEDVALUE+SWITCH (parameterized) is NOT a geo selector — skip.
        ms = [
            {
                "original_name": "Sw",
                "table_name": "T",
                "expression": 'SWITCH(SELECTEDVALUE(D[k]), "a", [A], "b", [B])',
            }
        ]
        assert self._G(ms) == {}

    def test_half_resolving_switch_emits_nothing(self):
        # If only one branch resolves, emit neither (no half-decomposition).
        ms = [
            {
                "original_name": "Plant_Comp X",
                "table_name": "T",
                "expression": (
                    "SWITCH(TRUE(), ISFILTERED(D[p]), "
                    'CALCULATE(SUM(T[v]), T[a]="1"), SELECTEDVALUE(D[weird]))'
                ),
            }
        ]
        assert self._G(ms).get("T", []) == []


class TestReportIdAutoDiscovery:
    """PROP-7: discover the report bound to the dataset when none supplied."""

    def _discover(self, reports, dataset_id="ds1", status=200):
        from unittest.mock import MagicMock, patch

        from src.services.powerbi import pipeline_config as gc

        resp = MagicMock(status_code=status)
        resp.json.return_value = {"value": reports}
        with patch.object(gc, "requests") as rq:
            rq.get.return_value = resp
            return gc.discover_report_id("tok", "ws", dataset_id)

    def test_matches_dataset_case_insensitive(self):
        rid = self._discover([{"id": "r2", "name": "SC Report", "datasetId": "DS1"}])
        assert rid == "r2"

    def test_prefers_real_report_over_usage(self):
        rid = self._discover(
            [
                {"id": "u", "name": "Usage Metrics Report", "datasetId": "ds1"},
                {"id": "real", "name": "SC - Total Supply Chain", "datasetId": "ds1"},
            ]
        )
        assert rid == "real"

    def test_no_match_returns_none(self):
        assert self._discover([{"id": "x", "datasetId": "other"}]) is None

    def test_api_failure_returns_none(self):
        assert self._discover([], status=403) is None
