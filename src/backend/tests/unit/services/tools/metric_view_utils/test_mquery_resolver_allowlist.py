"""The deterministic M -> SQL resolver only resolves steps it reproduces.

Any step that changes which rows a table holds (a row filter, a dedup, a
group, ...) makes the M "not resolvable" (``None``), so the caller falls back
to the raw M instead of shipping ``SELECT * FROM <table>`` and every measure
aggregating rows Power BI excluded.
"""

from src.services.tools.metric_view_utils.mquery_parser import (
    MQueryParser,
    resolve_mquery_to_sql,
    resolve_mquery_with_context,
)
from src.services.tools.metric_view_utils.mquery_steps import classify_m_steps

_NAV = (
    'Source = Databricks.Catalogs("host.example.com", "/sql/1.0/warehouses/x", '
    "[Catalog=null, Database=null, EnableAutomaticProxyDiscovery=null]),\n"
    '    main_Database = Source{[Name="main",Kind="Database"]}[Data],\n'
    '    sales_Schema = main_Database{[Name="sales",Kind="Schema"]}[Data],\n'
    '    orders_Table = sales_Schema{[Name="orders",Kind="Table"]}[Data]'
)


def _m(*steps: str, result: str) -> str:
    body = ",\n    ".join([_NAV, *steps])
    return f"let\n    {body}\nin\n    {result}"


FILTERED = _m(
    '#"Filtered Rows" = Table.SelectRows(orders_Table, each [status] = "Completed")',
    result='#"Filtered Rows"',
)
DEDUPED = _m(
    "Deduped = Table.Distinct(orders_Table)",
    result="Deduped",
)
FILTERED_AND_DEDUPED = _m(
    '#"Filtered Rows" = Table.SelectRows(orders_Table, each [status] = "Completed")',
    'Deduped = Table.Distinct(#"Filtered Rows")',
    result="Deduped",
)


class TestRowLevelStepsAreNotResolvable:
    def test_row_filter_is_not_resolvable(self):
        assert resolve_mquery_to_sql(FILTERED) is None

    def test_dedup_is_not_resolvable(self):
        assert resolve_mquery_to_sql(DEDUPED) is None

    def test_filter_plus_dedup_is_not_resolvable(self):
        assert resolve_mquery_to_sql(FILTERED_AND_DEDUPED) is None
        assert resolve_mquery_with_context(FILTERED_AND_DEDUPED, {"x": "y"}) is None

    def test_unhandled_steps_are_named(self):
        _, unhandled = classify_m_steps(FILTERED_AND_DEDUPED)
        assert any("Table.SelectRows" in u for u in unhandled)
        assert any("Table.Distinct" in u for u in unhandled)

    def test_skip_reason_names_the_refused_steps(self):
        from src.services.tools.metric_view_utils.mquery_parser import (
            classify_mquery_source,
        )

        category, reason = classify_mquery_source(FILTERED_AND_DEDUPED)
        assert category == "extractable"
        assert "Table.SelectRows" in reason and "Table.Distinct" in reason

    def test_group_and_added_column_are_not_resolvable(self):
        grouped = _m(
            'Grouped = Table.Group(orders_Table, {"region"}, '
            '{{"amount", each List.Sum([amount]), type number}})',
            result="Grouped",
        )
        added = _m(
            'Added = Table.AddColumn(orders_Table, "net", each [amount] - [tax])',
            result="Added",
        )
        assert resolve_mquery_to_sql(grouped) is None
        assert resolve_mquery_to_sql(added) is None

    def test_native_query_is_not_compiled_to_select_star(self):
        m = (
            'let Source = Value.NativeQuery(Databricks.Query("h","p"), '
            "\"SELECT region, SUM(amount) AS amount FROM main.sales.orders WHERE status = 'Completed' "
            'GROUP BY region", null, [EnableFolding=true]) in Source'
        )
        assert resolve_mquery_to_sql(m) is None

    def test_sql_database_with_query_option_is_not_resolvable(self):
        m = (
            'let Source = Sql.Database("server.example.com", "db", '
            "[Query=\"SELECT * FROM dbo.Sales WHERE Region = 'EU'\"]) in Source"
        )
        assert resolve_mquery_to_sql(m) is None

    def test_reference_with_own_row_filter_is_not_resolvable(self):
        staging = _m(result="orders_Table")
        table = (
            'let Source = #"Orders Staging",\n'
            '    Kept = Table.SelectRows(Source, each [status] <> "Cancelled")\n'
            "in Kept"
        )
        assert resolve_mquery_with_context(table, {"Orders Staging": staging}) is None


class TestColumnOnlyStepsStillResolve:
    def test_plain_navigation(self):
        assert (
            resolve_mquery_to_sql(_m(result="orders_Table"))
            == "SELECT * FROM main.sales.orders"
        )

    def test_inline_navigation_chain(self):
        m = 'Databricks.Catalogs(){[Name="a"]}[Data]{[Name="b"]}[Data]{[Name="c"]}[Data]'
        assert resolve_mquery_to_sql(m) == "SELECT * FROM a.b.c"

    def test_types_select_and_rename(self):
        m = _m(
            '#"Changed Type" = Table.TransformColumnTypes(orders_Table, '
            '{{"amount", type number}}, "en-US")',
            'Selected = Table.SelectColumns(#"Changed Type", {"region", "amount"})',
            'Renamed = Table.RenameColumns(Selected, {{"amount", "order_amount"}})',
            result="Renamed",
        )
        assert (
            resolve_mquery_to_sql(m)
            == "SELECT region, amount AS order_amount FROM main.sales.orders"
        )

    def test_rename_before_select_maps_back_to_source(self):
        m = _m(
            'Renamed = Table.RenameColumns(orders_Table, {{"amount", "order_amount"}})',
            'Selected = Table.SelectColumns(Renamed, {"order_amount", "region"})',
            result="Selected",
        )
        assert (
            resolve_mquery_to_sql(m)
            == "SELECT amount AS order_amount, region FROM main.sales.orders"
        )

    def test_rename_without_projection_is_kept(self):
        m = _m(
            'Renamed = Table.RenameColumns(orders_Table, {{"amount", "order_amount"}})',
            result="Renamed",
        )
        assert resolve_mquery_to_sql(m) == (
            "SELECT * EXCEPT (amount), amount AS order_amount FROM main.sales.orders"
        )

    def test_reference_with_type_change_resolves(self):
        staging = _m(result="orders_Table")
        table = (
            'let Source = #"Orders Staging",\n'
            "    Typed = Table.TransformColumnTypes(Source, "
            '{{"amount", type number}})\n'
            "in Typed"
        )
        assert (
            resolve_mquery_with_context(table, {"Orders Staging": staging})
            == "SELECT * FROM main.sales.orders"
        )


class TestHandoffKeepsTheFilter:
    def test_ucmv_handoff_ships_raw_m_not_select_star(self):
        from src.services.tools.pipeline_config_generator_tool import (
            PipelineConfigGeneratorTool,
        )

        out = PipelineConfigGeneratorTool._build_ucmv_mquery(
            {"Orders": {"mquery_expression": FILTERED_AND_DEDUPED}}, {}
        )
        assert out[0]["transpiled_sql"] == FILTERED_AND_DEDUPED.strip()

    def test_native_query_where_survives_to_the_parser(self):
        from src.services.tools.pipeline_config_generator_tool import (
            PipelineConfigGeneratorTool,
        )

        m = (
            'let Source = Value.NativeQuery(Databricks.Query("h","p"), '
            '"SELECT region, SUM(amount) AS amount FROM main.sales.orders '
            "WHERE status = 'Completed' GROUP BY region\", null) in Source"
        )
        out = PipelineConfigGeneratorTool._build_ucmv_mquery(
            {"Orders": {"mquery_expression": m}}, {}
        )
        tables = MQueryParser().parse_json(out)
        assert tables["Orders"].static_filters == ["status = 'Completed'"]
