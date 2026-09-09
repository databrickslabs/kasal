"""Tests for mquery_let_evaluator — the M `let` block evaluator that resolves
parameter-driven table sources (string concatenation of PBI parameters)
without an LLM."""

from src.services.tools.metric_view_utils.mquery_let_evaluator import (
    extract_parameter_defaults,
    resolve_via_let_evaluation,
)


class TestExtractParameterDefaults:
    def test_extracts_simple_parameter(self):
        exprs = {
            "Catalog_Name": '"dc_datalake_prod_001" meta [IsParameterQuery = true, Type = "Text"]'
        }
        assert extract_parameter_defaults(exprs) == {
            "Catalog_Name": "dc_datalake_prod_001"
        }

    def test_extracts_multiple_parameters_ignores_non_parameters(self):
        exprs = {
            "Catalog_Name": '"dc_prod_001" meta [IsParameterQuery=true, Type="Text"]',
            "Database": '"golden_schema" meta [IsParameterQuery = true, IsParameterQueryRequired=false]',
            "SomeSharedQuery": "let Source = Databricks.Catalogs() in Source",
        }
        params = extract_parameter_defaults(exprs)
        assert params == {"Catalog_Name": "dc_prod_001", "Database": "golden_schema"}

    def test_unquotes_doubled_quotes_in_value(self):
        exprs = {"P": '"a""b" meta [IsParameterQuery = true]'}
        assert extract_parameter_defaults(exprs) == {"P": 'a"b'}

    def test_empty_or_none_expressions(self):
        assert extract_parameter_defaults({}) == {}
        assert extract_parameter_defaults(None) == {}


class TestResolveViaLetEvaluation:
    def _params(self):
        return {
            "Catalog_Name": "dc_datalake_prod_001",
            "Database": "application_golden",
            "Table_Version": "v2",
            "Version_Type": "official",
            "Country": "gr",
        }

    def test_resolves_real_paat_style_pattern(self):
        # Mirrors the real customer M this module was built to unblock:
        # a table name AND a WHERE filter built entirely from parameters.
        m = (
            "let\n"
            'Object = "hub_product_" & Table_Version,\n'
            'FromClause = Catalog_Name & "." & Database & "." & Object,\n'
            'VersionFilter = if Table_Version = "v2" then " AND version = \'" & Version_Type & "\'" else "",\n'
            "NativeQuery = Value.NativeQuery(Data_Mesh,\n"
            '"select * from "& FromClause &" WHERE bu = \'"& Text.From(Country) &"\'"'
            '& VersionFilter & ""'
            "\n,null, [EnableFolding=true])\n"
            "in\n"
            '    #"NativeQuery"'
        )
        sql = resolve_via_let_evaluation(m, self._params())
        assert sql == (
            "select * from dc_datalake_prod_001.application_golden.hub_product_v2 "
            "WHERE bu = 'gr' AND version = 'official'"
        )

    def test_if_condition_false_branch(self):
        m = (
            "let\n"
            'Filt = if Table_Version = "v99" then " AND x=1" else "",\n'
            'NativeQuery = Value.NativeQuery(Data_Mesh, "select 1" & Filt, null, [EnableFolding=true])\n'
            "in\n"
            "    NativeQuery"
        )
        sql = resolve_via_let_evaluation(m, self._params())
        assert sql == "select 1"

    def test_no_native_query_call_returns_none(self):
        m = 'let X = Catalog_Name & "." & Database in X'
        assert resolve_via_let_evaluation(m, self._params()) is None

    def test_unresolvable_reference_returns_none(self):
        # `Unknown_Param` isn't in the params map — must fail, not guess.
        m = (
            "let\n"
            'FromClause = Unknown_Param & ".t",\n'
            'NativeQuery = Value.NativeQuery(Data_Mesh, "select * from " & FromClause, null, [EnableFolding=true])\n'
            "in NativeQuery"
        )
        assert resolve_via_let_evaluation(m, self._params()) is None

    def test_nested_let_returns_none(self):
        m = "let X = (let Y = 1 in Y) in X"
        assert resolve_via_let_evaluation(m, self._params()) is None

    def test_not_a_let_block_returns_none(self):
        assert (
            resolve_via_let_evaluation("GENERATESERIES(1,10)", self._params()) is None
        )

    def test_empty_or_no_params(self):
        assert resolve_via_let_evaluation("", self._params()) is None
        assert resolve_via_let_evaluation(None, self._params()) is None
        assert resolve_via_let_evaluation("let X = 1 in X", {}) is None


class TestExtractSourceTableParametricIntegration:
    """extract_source_table falls back to the let-evaluator when a source's
    physical table is BUILT from parameters (nehme's literal extractor can't)."""

    _M = (
        'let Object = "hub_product_" & Table_Version, '
        'FromClause = Catalog_Name & "." & Database & "." & Object, '
        'Q = Value.NativeQuery(Src, "select * from " & FromClause & " where 1=1", '
        "null, [EnableFolding=true]) in Q"
    )
    _EXPRS = {
        "Catalog_Name": '"prod_cat" meta [IsParameterQuery = true, Type = "Text"]',
        "Database": '"gold" meta [IsParameterQuery = true, Type = "Text"]',
        "Table_Version": '"v2" meta [IsParameterQuery = true, Type = "Text"]',
    }

    def test_resolves_parametric_source_with_expressions(self):
        from src.services.tools.metric_view_utils.mquery_parser import (
            extract_source_table,
        )

        assert extract_source_table(self._M) is None  # no expressions → can't resolve
        assert (
            extract_source_table(self._M, expressions=self._EXPRS)
            == "prod_cat.gold.hub_product_v2"
        )
