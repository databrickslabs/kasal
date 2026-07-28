"""
Extended unit tests for converters/services/sql/models.py

Targets uncovered lines: 83, 100, 108, 121-127, 131-136, 140-146, 150, 156-194,
199, 206-245, 300-307, 352, 354, 360-365, 420-428, 478-480, 484-497, 502, 512-515,
522-524, 536
"""

import pytest
from src.converters.services.sql.models import (
    SQLDialect,
    SQLAggregationType,
    SQLJoinType,
    SQLQuery,
    SQLMeasure,
    SQLStructure,
    SQLDefinition,
    SQLTranslationOptions,
    SQLTranslationResult,
)
from src.converters.base.models import KPI


def make_kpi(technical_name="sales", formula="amount", **kwargs):
    return KPI(
        description=kwargs.pop("description", "Test KPI"),
        technical_name=technical_name,
        formula=formula,
        source_table=kwargs.pop("source_table", "Sales"),
        **kwargs,
    )


class TestSQLDialect:
    def test_databricks_value(self):
        assert SQLDialect.DATABRICKS.value == "DATABRICKS"

    def test_standard_value(self):
        assert SQLDialect.STANDARD.value == "ANSI_SQL"


class TestSQLAggregationType:
    def test_all_values_are_strings(self):
        for agg in SQLAggregationType:
            assert isinstance(agg.value, str)


class TestSQLJoinType:
    def test_inner_join(self):
        assert SQLJoinType.INNER.value == "INNER JOIN"

    def test_left_join(self):
        assert SQLJoinType.LEFT.value == "LEFT JOIN"

    def test_full_outer_join(self):
        assert SQLJoinType.FULL.value == "FULL OUTER JOIN"


class TestSQLQuery:
    """Cover SQLQuery.to_sql and helper methods"""

    def test_to_sql_formatted_simple(self):
        q = SQLQuery(
            select_clause=["SUM(sales.amount) AS total"],
            from_clause="sales",
        )
        sql = q.to_sql(formatted=True)
        assert "SELECT" in sql
        assert "FROM" in sql
        assert sql.strip().endswith(";")

    def test_to_sql_compact(self):
        q = SQLQuery(
            select_clause=["SUM(sales.amount) AS total"],
            from_clause="sales",
        )
        sql = q.to_sql(formatted=False)
        assert "SELECT" in sql
        assert "FROM" in sql

    def test_to_sql_empty_select_star(self):
        """Line 100: no select_clause -> SELECT *"""
        q = SQLQuery(from_clause="sales")
        sql = q.to_sql(formatted=True)
        assert "SELECT *" in sql

    def test_to_sql_multiple_select_items(self):
        """Lines 95-98: multiple columns uses indented format"""
        q = SQLQuery(
            select_clause=["a", "b", "c"],
            from_clause="tbl",
        )
        sql = q.to_sql(formatted=True)
        assert "SELECT" in sql
        assert "a" in sql
        assert "b" in sql

    def test_to_sql_with_joins(self):
        """Line 108: join clauses"""
        q = SQLQuery(
            select_clause=["*"],
            from_clause="orders",
            join_clauses=["LEFT JOIN customers ON orders.customer_id = customers.id"],
        )
        sql = q.to_sql(formatted=True)
        assert "LEFT JOIN" in sql

    def test_to_sql_with_where_single(self):
        """Line 121-127: single WHERE condition"""
        q = SQLQuery(
            select_clause=["*"],
            from_clause="sales",
            where_clause=["region = 'West'"],
        )
        sql = q.to_sql(formatted=True)
        assert "WHERE" in sql
        assert "region = 'West'" in sql

    def test_to_sql_with_where_multiple(self):
        """Lines 123-126: multiple WHERE conditions use AND"""
        q = SQLQuery(
            select_clause=["*"],
            from_clause="sales",
            where_clause=["region = 'West'", "status = 'Active'"],
        )
        sql = q.to_sql(formatted=True)
        assert "AND" in sql

    def test_to_sql_with_group_by_one(self):
        """Line 122: single group by"""
        q = SQLQuery(
            select_clause=["region", "SUM(amount)"],
            from_clause="sales",
            group_by_clause=["region"],
        )
        sql = q.to_sql(formatted=True)
        assert "GROUP BY" in sql
        assert "region" in sql

    def test_to_sql_with_group_by_many(self):
        """Lines 124-127: many group by columns"""
        q = SQLQuery(
            select_clause=["a", "b", "c", "SUM(d)"],
            from_clause="tbl",
            group_by_clause=["a", "b", "c"],
        )
        sql = q.to_sql(formatted=True)
        assert "GROUP BY" in sql

    def test_to_sql_with_having(self):
        """Lines 130-136: HAVING clause"""
        q = SQLQuery(
            select_clause=["SUM(amount)"],
            from_clause="sales",
            having_clause=["SUM(amount) > 1000"],
        )
        sql = q.to_sql(formatted=True)
        assert "HAVING" in sql

    def test_to_sql_with_having_multiple(self):
        q = SQLQuery(
            select_clause=["SUM(a)", "SUM(b)"],
            from_clause="tbl",
            having_clause=["SUM(a) > 0", "SUM(b) > 0"],
        )
        sql = q.to_sql(formatted=True)
        assert "AND SUM(b) > 0" in sql

    def test_to_sql_with_order_by_few(self):
        """Line 140-141: few order by columns inline"""
        q = SQLQuery(
            select_clause=["*"],
            from_clause="sales",
            order_by_clause=["region ASC"],
        )
        sql = q.to_sql(formatted=True)
        assert "ORDER BY" in sql

    def test_to_sql_with_order_by_many(self):
        """Lines 143-146: many order by columns expanded"""
        q = SQLQuery(
            select_clause=["*"],
            from_clause="sales",
            order_by_clause=["a", "b", "c"],
        )
        sql = q.to_sql(formatted=True)
        assert "ORDER BY" in sql

    def test_to_sql_with_limit(self):
        """Line 150: LIMIT clause"""
        q = SQLQuery(
            select_clause=["*"],
            from_clause="sales",
            limit_clause=100,
        )
        sql = q.to_sql(formatted=True)
        assert "LIMIT 100" in sql

    def test_to_sql_compact_with_where(self):
        """Lines 174-175: compact WHERE uses AND"""
        q = SQLQuery(
            select_clause=["a"],
            from_clause="tbl",
            where_clause=["x = 1", "y = 2"],
        )
        sql = q.to_sql(formatted=False)
        assert "WHERE" in sql
        assert "AND" in sql

    def test_to_sql_compact_with_having(self):
        """Line 184-185: compact HAVING"""
        q = SQLQuery(
            select_clause=["SUM(a)"],
            from_clause="tbl",
            having_clause=["SUM(a) > 0", "SUM(a) < 100"],
        )
        sql = q.to_sql(formatted=False)
        assert "HAVING" in sql

    def test_to_sql_compact_no_limit(self):
        q = SQLQuery(select_clause=["*"], from_clause="tbl")
        sql = q.to_sql(formatted=False)
        assert "LIMIT" not in sql

    def test_format_sql_union_all(self):
        """Line 202-204: _format_sql delegates to _format_union_sql for UNION ALL"""
        q = SQLQuery(select_clause=["*"], from_clause="tbl")
        q._custom_sql = "SELECT a FROM t1 UNION ALL SELECT b FROM t2"
        sql = q.to_sql(formatted=True)
        assert "UNION ALL" in sql

    def test_format_sql_basic(self):
        """Lines 206-245: _format_sql basic path"""
        q = SQLQuery(select_clause=["*"], from_clause="tbl")
        q._custom_sql = "SELECT a, b FROM tbl WHERE x = 1 GROUP BY a"
        sql = q.to_sql(formatted=True)
        assert "SELECT" in sql


class TestSQLMeasure:
    """Cover SQLMeasure methods (lines 346-365)"""

    def test_to_sql_expression_positive_sign(self):
        """Line 350: display_sign=1 returns unmodified expression"""
        measure = SQLMeasure(
            name="Total Sales",
            sql_expression="SUM(sales.amount)",
            aggregation_type=SQLAggregationType.SUM,
            source_table="sales",
            display_sign=1,
        )
        expr = measure.to_sql_expression()
        assert expr == "SUM(sales.amount)"

    def test_to_sql_expression_negative_sign(self):
        """Line 352: display_sign=-1 negates the expression"""
        measure = SQLMeasure(
            name="Negative Sales",
            sql_expression="SUM(sales.amount)",
            aggregation_type=SQLAggregationType.SUM,
            source_table="sales",
            display_sign=-1,
        )
        expr = measure.to_sql_expression()
        assert "(-1) *" in expr

    def test_to_sql_expression_other_sign(self):
        """Line 353-354: non-standard display_sign"""
        measure = SQLMeasure(
            name="Doubled Sales",
            sql_expression="SUM(sales.amount)",
            aggregation_type=SQLAggregationType.SUM,
            source_table="sales",
            display_sign=2,
        )
        expr = measure.to_sql_expression()
        assert "2 *" in expr

    def test_to_case_statement_no_filters(self):
        """Line 360: no filters returns to_sql_expression"""
        measure = SQLMeasure(
            name="Sales",
            sql_expression="SUM(amount)",
            aggregation_type=SQLAggregationType.SUM,
            source_table="sales",
        )
        stmt = measure.to_case_statement()
        assert stmt == measure.to_sql_expression()

    def test_to_case_statement_with_filters(self):
        """Lines 362-365: filters build a CASE WHEN statement"""
        measure = SQLMeasure(
            name="Filtered Sales",
            sql_expression="SUM(amount)",
            aggregation_type=SQLAggregationType.SUM,
            source_table="sales",
            filters=["region = 'West'", "status = 'Active'"],
        )
        stmt = measure.to_case_statement()
        assert "CASE WHEN" in stmt
        assert "THEN" in stmt
        assert "ELSE NULL" in stmt


class TestSQLDefinition:
    """Cover SQLDefinition.get_full_table_name (lines 418-428)"""

    def test_get_full_table_name_no_db_schema(self):
        """Line 428: no database/schema -> just table name"""
        defn = SQLDefinition(
            description="Test",
            technical_name="test",
        )
        assert defn.get_full_table_name("orders") == "orders"

    def test_get_full_table_name_with_schema(self):
        """Line 421-428: with schema -> schema.table"""
        defn = SQLDefinition(
            description="Test",
            technical_name="test",
            database_schema="myschema",
        )
        result = defn.get_full_table_name("orders")
        assert result == "myschema.orders"

    def test_get_full_table_name_with_database_and_schema(self):
        """All three parts: catalog.schema.table"""
        defn = SQLDefinition(
            description="Test",
            technical_name="test",
            database="mydb",
            database_schema="myschema",
        )
        result = defn.get_full_table_name("orders")
        assert result == "mydb.myschema.orders"

    def test_get_full_table_name_database_only(self):
        defn = SQLDefinition(
            description="Test",
            technical_name="test",
            database="mydb",
        )
        result = defn.get_full_table_name("orders")
        assert result == "mydb.orders"


class TestSQLTranslationResult:
    """Cover SQLTranslationResult methods (lines 476-540)"""

    def make_result(self, queries=None, measures=None) -> SQLTranslationResult:
        sql_def = SQLDefinition(description="Test", technical_name="test")
        options = SQLTranslationOptions()
        return SQLTranslationResult(
            sql_queries=queries or [],
            sql_measures=measures or [],
            sql_definition=sql_def,
            translation_options=options,
        )

    def test_get_primary_query_no_queries(self):
        """Line 478: no queries returns None"""
        result = self.make_result()
        assert result.get_primary_query() is None

    def test_get_primary_query_with_query(self):
        """Line 479: returns first query's SQL"""
        q = SQLQuery(select_clause=["*"], from_clause="sales")
        result = self.make_result(queries=[q])
        sql = result.get_primary_query()
        assert "SELECT" in sql

    def test_get_primary_query_unformatted(self):
        q = SQLQuery(select_clause=["*"], from_clause="sales")
        result = self.make_result(queries=[q])
        sql = result.get_primary_query(formatted=False)
        assert "SELECT" in sql

    def test_get_all_sql_statements_no_create_view(self):
        """Lines 484-495: get_all_sql_statements without create_view"""
        q = SQLQuery(select_clause=["*"], from_clause="sales")
        result = self.make_result(queries=[q])
        statements = result.get_all_sql_statements()
        assert len(statements) == 1
        assert "SELECT" in statements[0]

    def test_get_all_sql_statements_with_create_view(self):
        """Lines 487-491: create_view_statements=True"""
        kpi = make_kpi()
        q = SQLQuery(
            select_clause=["*"],
            from_clause="sales",
            original_kbi=kpi,
        )
        sql_def = SQLDefinition(description="Test", technical_name="test")
        options = SQLTranslationOptions(create_view_statements=True)
        result = SQLTranslationResult(
            sql_queries=[q],
            sql_measures=[],
            sql_definition=sql_def,
            translation_options=options,
        )
        statements = result.get_all_sql_statements()
        # Should have a CREATE VIEW statement and the query
        assert any("CREATE OR REPLACE VIEW" in s for s in statements)

    def test_get_formatted_sql_output_no_queries(self):
        """Line 502: no queries returns comment"""
        result = self.make_result()
        output = result.get_formatted_sql_output()
        assert "No SQL queries generated" in output

    def test_get_formatted_sql_output_with_queries(self):
        """Lines 504-531: formatted output with queries"""
        q1 = SQLQuery(
            select_clause=["*"],
            from_clause="sales",
            description="Sales Query",
        )
        q2 = SQLQuery(select_clause=["*"], from_clause="orders")
        result = self.make_result(queries=[q1, q2])
        result.optimization_suggestions = ["Use an index"]
        output = result.get_formatted_sql_output()
        assert "Generated SQL for" in output
        assert "SELECT" in output
        assert "Use an index" in output

    def test_get_measures_summary_empty(self):
        """Line 534-540: measures summary with empty measures"""
        result = self.make_result()
        summary = result.get_measures_summary()
        assert summary["total_measures"] == 0
        assert summary["aggregation_types"] == []

    def test_get_measures_summary_with_measures(self):
        """Lines 536-540: measures summary calculation"""
        m1 = SQLMeasure(
            name="Sales",
            sql_expression="SUM(amount)",
            aggregation_type=SQLAggregationType.SUM,
            source_table="sales",
            filters=["region = 'West'"],
            group_by_columns=["region"],
        )
        m2 = SQLMeasure(
            name="Orders",
            sql_expression="COUNT(*)",
            aggregation_type=SQLAggregationType.COUNT,
            source_table="orders",
        )
        result = self.make_result(measures=[m1, m2])
        summary = result.get_measures_summary()
        assert summary["total_measures"] == 2
        assert "SUM" in summary["aggregation_types"]
        assert "COUNT" in summary["aggregation_types"]
        assert summary["has_filters"] == 1
        assert summary["has_grouping"] == 1


class TestSQLQueryFormatSingleSelect:
    """Cover _format_single_select (lines 267-320)"""

    def test_format_single_select_basic(self):
        q = SQLQuery(select_clause=["*"], from_clause="tbl")
        q._custom_sql = "SELECT a, b FROM tbl WHERE x = 1"
        result = q.to_sql(formatted=True)
        assert "SELECT" in result
        assert "FROM" in result

    def test_format_union_sql(self):
        q = SQLQuery(select_clause=["*"], from_clause="tbl")
        q._custom_sql = "SELECT a FROM t1 UNION ALL SELECT b FROM t2"
        result = q.to_sql(formatted=True)
        assert "UNION ALL" in result
        # Both SELECT statements should appear
        assert result.count("SELECT") >= 2

    def test_format_single_select_with_group_by(self):
        """GROUP BY handling in _format_single_select"""
        q = SQLQuery(select_clause=["*"], from_clause="tbl")
        q._custom_sql = "SELECT region, SUM(amount) FROM sales GROUP BY region"
        result = q.to_sql(formatted=True)
        assert "GROUP BY" in result

    def test_format_single_select_with_order_by(self):
        """ORDER BY handling in _format_single_select"""
        q = SQLQuery(select_clause=["*"], from_clause="tbl")
        q._custom_sql = "SELECT * FROM tbl ORDER BY created_at DESC"
        result = q.to_sql(formatted=True)
        assert "ORDER BY" in result

    def test_format_single_select_with_and_in_where(self):
        """AND outside SELECT/FROM is pushed to new line in _format_single_select"""
        q = SQLQuery(select_clause=["*"], from_clause="tbl")
        q._custom_sql = "SELECT * FROM tbl WHERE x = 1 AND y = 2"
        result = q.to_sql(formatted=True)
        assert "AND" in result

    def test_format_single_select_adds_semicolon(self):
        """Ensure semicolon is added when missing"""
        q = SQLQuery(select_clause=["*"], from_clause="tbl")
        q._custom_sql = "SELECT * FROM tbl"
        result = q.to_sql(formatted=True)
        assert result.strip().endswith(";")
