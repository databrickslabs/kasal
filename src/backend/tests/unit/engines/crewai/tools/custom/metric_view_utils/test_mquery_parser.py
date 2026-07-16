"""Tests for MQueryParser."""
import pytest
from src.engines.crewai.tools.custom.metric_view_utils.mquery_parser import MQueryParser


@pytest.fixture
def parser():
    return MQueryParser()


class TestParseJson:
    def test_basic_fact_table(self, parser):
        entries = [{
            'table_name': 'fact_sales',
            'transpiled_sql': 'SELECT region, SUM(amount) AS amount FROM catalog.schema.sales GROUP BY region',
            'validation_passed': 'Yes',
        }]
        tables = parser.parse_json(entries)
        assert 'fact_sales' in tables
        info = tables['fact_sales']
        assert info.is_fact is True
        assert info.source_table == 'catalog.schema.sales'
        assert len(info.aggregate_columns) == 1
        assert info.aggregate_columns[0]['name'] == 'amount'
        assert 'region' in info.group_by_columns

    def test_dim_table_no_aggregates(self, parser):
        entries = [{
            'table_name': 'dim_region',
            'transpiled_sql': 'SELECT code, name FROM catalog.schema.regions',
            'validation_passed': 'Yes',
        }]
        tables = parser.parse_json(entries)
        assert 'dim_region' in tables
        assert tables['dim_region'].is_fact is False

    def test_skips_failed_validation(self, parser):
        entries = [{
            'table_name': 'bad_table',
            'transpiled_sql': 'SELECT * FROM catalog.schema.x',
            'validation_passed': 'No',
        }]
        tables = parser.parse_json(entries)
        assert 'bad_table' not in tables

    def test_accepts_list_input(self, parser):
        entries = [{
            'table_name': 'test',
            'transpiled_sql': 'SELECT col, SUM(val) AS val FROM cat.sch.tbl GROUP BY col',
            'validation_passed': 'Yes',
        }]
        tables = parser.parse_json(entries)
        assert 'test' in tables

    def test_extracts_where_filters(self, parser):
        entries = [{
            'table_name': 'fact',
            'transpiled_sql': "SELECT col, SUM(val) AS val FROM cat.sch.tbl WHERE status = 'active' GROUP BY col",
            'validation_passed': 'Yes',
        }]
        tables = parser.parse_json(entries)
        assert len(tables['fact'].static_filters) >= 1

    def test_extracts_left_joins(self, parser):
        sql = (
            "SELECT t.col, SUM(t.val) AS val "
            "FROM cat.sch.fact t "
            "LEFT JOIN cat.sch.dim d ON t.key = d.key "
            "GROUP BY t.col"
        )
        entries = [{'table_name': 'fact', 'transpiled_sql': sql, 'validation_passed': 'Yes'}]
        tables = parser.parse_json(entries)
        assert 'd' in tables['fact'].dim_source_tables


class TestGroupByAll:
    def test_infer_group_by_all(self, parser):
        sql = "SELECT region, country, SUM(amount) AS amount FROM cat.sch.tbl GROUP BY ALL"
        entries = [{'table_name': 'test', 'transpiled_sql': sql, 'validation_passed': 'Yes'}]
        tables = parser.parse_json(entries)
        gb = tables['test'].group_by_columns
        assert 'region' in gb
        assert 'country' in gb


class TestMLanguageTokenNormalization:
    """M-language escape tokens (#(lf) etc.) must not leak into parsed SQL/columns."""

    def test_lf_token_stripped_from_source(self):
        from src.engines.crewai.tools.custom.metric_view_utils.mquery_parser import MQueryParser
        sql = ("SELECT a, b,#(lf) c AS flag#(lf) FROM cat.sch.t "
               "WHERE y = YEAR(CURRENT_DATE)")
        info = MQueryParser()._parse_sql('t', sql)
        assert info.source_table == 'cat.sch.t'
        assert '#(lf)' not in str(info.group_by_columns)
        assert '#(lf)' not in (info.full_sql or '')

    def test_doubled_quotes_collapsed(self):
        from src.engines.crewai.tools.custom.metric_view_utils.mquery_parser import MQueryParser
        sql = 'SELECT a, if(x=1,""yes"",""no"") AS f FROM cat.sch.t'
        info = MQueryParser()._parse_sql('t', sql)
        assert '""' not in (info.full_sql or '')


class TestClassifyMquerySource:
    """classify_mquery_source: route no-SQL M sources to a skip-with-reason
    category so the report can emit them as a note (mirrors untranslatable
    measures) instead of routing benign non-facts to broken SQL recovery."""

    def _C(self, m):
        from src.engines.crewai.tools.custom.metric_view_utils.mquery_parser import (
            classify_mquery_source)
        return classify_mquery_source(m)

    def test_inline_constant_table(self):
        m = ('let Source = Table.FromRows(Json.Document(Binary.Decompress('
             'Binary.FromText("i45W", BinaryEncoding.Base64), Compression.Deflate))) '
             'in Source')
        assert self._C(m)[0] == 'inline_const'

    def test_dax_calc_generateseries(self):
        assert self._C('GENERATESERIES(-5, 20, 5)')[0] == 'dax_calc'

    def test_dax_calc_summarizecolumns(self):
        assert self._C('SUMMARIZECOLUMNS(Dim_Date[YearMonth])')[0] == 'dax_calc'

    def test_dax_calc_parameter_query(self):
        assert self._C('"N" meta [IsParameterQuery=true, Type="Text"]')[0] == 'dax_calc'

    def test_external_access_database(self):
        m = 'let Source = Access.Database(File.Contents("x.accdb")) in Source'
        assert self._C(m)[0] == 'external'

    def test_extractable_native_query(self):
        m = 'let Source = Value.NativeQuery(db, "SELECT a FROM t") in Source'
        assert self._C(m)[0] == 'extractable'

    def test_extractable_databricks_connector(self):
        m = 'let Source = Databricks.Catalogs("h","p"){[Name="c"]}[Data] in Source'
        assert self._C(m)[0] == 'extractable'

    def test_unknown_shape(self):
        assert self._C('let Source = List.Numbers(1,10) in Source')[0] == 'unknown'

    def test_empty_input(self):
        assert self._C('')[0] == 'unknown'
        assert self._C(None)[0] == 'unknown'

    def test_reason_is_human_readable(self):
        cat, reason = self._C('GENERATESERIES(1,10)')
        assert isinstance(reason, str) and len(reason) > 10

    def test_inline_const_precedence_over_dax(self):
        # a base64 inline table that also mentions VALUES must classify as inline_const
        m = ('Table.FromRows(Json.Document(Binary.Decompress('
             'Binary.FromText("x", BinaryEncoding.Base64))))  // VALUES helper')
        assert self._C(m)[0] == 'inline_const'


class TestExtractSourceTable:
    """extract_source_table: parse catalog.schema.table from an *extractable* M
    source (Databricks connector nav / native query / Sql.Database). Returns None
    when it can't resolve confidently — never guesses (a wrong source_table would
    silently wire a bad join)."""

    def _E(self, m):
        from src.engines.crewai.tools.custom.metric_view_utils.mquery_parser import (
            extract_source_table)
        return extract_source_table(m)

    def test_databricks_catalogs_navigation_chain(self):
        m = ('let Source = Databricks.Catalogs("h.databricks.com","/sql/1.0/wh/abc")'
             '{[Name="dc_datalake_prod_001"]}[Data]{[Name="udm_example_md"]}[Data]'
             '{[Name="ca_dim_workcenter"]}[Data] in Source')
        assert self._E(m) == 'dc_datalake_prod_001.udm_example_md.ca_dim_workcenter'

    def test_databricks_keyed_navigation(self):
        m = ('Databricks.Catalogs(){[Catalog="c1"]}[Data]{[Schema="s1"]}[Data]'
             '{[Item="t1"]}[Data]')
        assert self._E(m) == 'c1.s1.t1'

    def test_native_query_from_clause(self):
        m = 'let Source = Value.NativeQuery(db, "SELECT a, b FROM cat_x.sch_y.tbl_z WHERE 1=1") in Source'
        assert self._E(m) == 'cat_x.sch_y.tbl_z'

    def test_sql_database_schema_item(self):
        m = 'let Source = Sql.Database("myserver","mydb"){[Schema="dbo",Item="Orders"]} in Source'
        assert self._E(m) == 'mydb.dbo.Orders'

    def test_inline_const_returns_none(self):
        m = ('let Source = Table.FromRows(Json.Document(Binary.Decompress('
             'Binary.FromText("i45W",BinaryEncoding.Base64)))) in Source')
        assert self._E(m) is None

    def test_dax_calc_returns_none(self):
        assert self._E('GENERATESERIES(-5,20,5)') is None

    def test_external_returns_none(self):
        assert self._E('let Source = Access.Database(File.Contents("x.accdb")) in Source') is None

    def test_incomplete_databricks_chain_returns_none(self):
        # only one [Name=…] segment — not enough to form a 3-level name
        m = 'let Source = Databricks.Catalogs("h","p"){[Name="onlycat"]}[Data] in Source'
        assert self._E(m) is None

    def test_none_and_empty_input(self):
        assert self._E(None) is None
        assert self._E('') is None
