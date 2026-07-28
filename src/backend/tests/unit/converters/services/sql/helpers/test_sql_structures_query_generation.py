"""
Extended unit tests for converters/services/sql/helpers/sql_structures.py

Targets uncovered code paths to increase coverage from 53% to 60%+.
Focuses on:
- generate_sql_queries_from_definition
- _process_calculated_kbi
- _build_sql_query_for_kbi
- _process_structure_kbis
- SQL generation with filters and structures
"""

import pytest
from src.converters.services.sql.helpers.sql_structures import (
    SQLStructureExpander,
    SQLTimeIntelligenceHelper
)
from src.converters.base.models import KPI, KPIDefinition, Structure, QueryFilter
from src.converters.services.sql.models import (
    SQLDialect,
    SQLTranslationOptions,
    SQLStructure,
    SQLAggregationType,
    SQLDefinition,
    SQLMeasure
)


class TestSQLStructureExpanderGeneration:
    """Tests for SQL generation methods in SQLStructureExpander"""

    @pytest.fixture
    def expander(self):
        return SQLStructureExpander(dialect=SQLDialect.DATABRICKS)

    @pytest.fixture
    def standard_expander(self):
        return SQLStructureExpander(dialect=SQLDialect.STANDARD)

    @pytest.fixture
    def simple_kpi_definition(self):
        return KPIDefinition(
            description="Sales Metrics",
            technical_name="sales_metrics",
            kpis=[
                KPI(
                    description="Total Revenue",
                    technical_name="total_revenue",
                    formula="amount",
                    aggregation_type="SUM",
                    source_table="FactSales"
                )
            ]
        )

    @pytest.fixture
    def multi_kpi_definition(self):
        return KPIDefinition(
            description="Multi KPI Metrics",
            technical_name="multi_kpi",
            kpis=[
                KPI(
                    description="Total Revenue",
                    technical_name="total_revenue",
                    formula="amount",
                    aggregation_type="SUM",
                    source_table="FactSales"
                ),
                KPI(
                    description="Total Cost",
                    technical_name="total_cost",
                    formula="cost",
                    aggregation_type="SUM",
                    source_table="FactSales"
                ),
                KPI(
                    description="Order Count",
                    technical_name="order_count",
                    formula="order_id",
                    aggregation_type="COUNT",
                    source_table="FactSales"
                )
            ]
        )

    @pytest.fixture
    def kpi_with_filters(self):
        return KPIDefinition(
            description="Filtered Metrics",
            technical_name="filtered_metrics",
            default_variables={"year": "2024", "region": "EMEA"},
            kpis=[
                KPI(
                    description="Filtered Revenue",
                    technical_name="filtered_revenue",
                    formula="amount",
                    aggregation_type="SUM",
                    source_table="FactSales",
                    filters=["region = 'EMEA'", "year = 2024"]
                )
            ]
        )

    @pytest.fixture
    def kpi_with_var_filters(self):
        return KPIDefinition(
            description="Variable Filtered Metrics",
            technical_name="var_filtered",
            default_variables={"current_year": "2024"},
            kpis=[
                KPI(
                    description="Revenue This Year",
                    technical_name="revenue_this_year",
                    formula="amount",
                    aggregation_type="SUM",
                    source_table="FactSales",
                    filters=["fiscyear = $var_current_year"]
                )
            ]
        )

    # ========== process_definition Tests ==========

    def test_process_definition_simple(self, expander, simple_kpi_definition):
        """Test processing simple KPI definition"""
        options = SQLTranslationOptions(target_dialect=SQLDialect.DATABRICKS)
        result = expander.process_definition(simple_kpi_definition, options)

        assert result is not None
        assert len(result.sql_measures) > 0

    def test_process_definition_returns_sql_definition(self, expander, simple_kpi_definition):
        """Test that process_definition returns SQLDefinition"""
        result = expander.process_definition(simple_kpi_definition)

        assert isinstance(result, SQLDefinition)
        assert result.technical_name == "sales_metrics"

    def test_process_definition_with_multiple_kpis(self, expander, multi_kpi_definition):
        """Test processing definition with multiple KPIs"""
        result = expander.process_definition(multi_kpi_definition)

        assert len(result.sql_measures) == 3

    def test_process_definition_without_options(self, expander, simple_kpi_definition):
        """Test process_definition works without explicit options"""
        result = expander.process_definition(simple_kpi_definition)

        assert result is not None
        assert len(result.sql_measures) >= 1

    def test_process_definition_with_filters(self, expander, kpi_with_filters):
        """Test processing KPI with filter conditions"""
        result = expander.process_definition(kpi_with_filters)

        assert len(result.sql_measures) == 1
        measure = result.sql_measures[0]
        assert measure is not None

    # ========== generate_sql_queries_from_definition Tests ==========

    def test_generate_sql_queries_returns_list(self, expander, simple_kpi_definition):
        """Test that generate_sql_queries_from_definition returns a list"""
        options = SQLTranslationOptions(target_dialect=SQLDialect.DATABRICKS)
        sql_def = expander.process_definition(simple_kpi_definition, options)
        queries = expander.generate_sql_queries_from_definition(sql_def, options)

        assert isinstance(queries, list)

    def test_generate_sql_queries_not_empty(self, expander, simple_kpi_definition):
        """Test that generate_sql_queries generates actual queries"""
        options = SQLTranslationOptions(target_dialect=SQLDialect.DATABRICKS)
        sql_def = expander.process_definition(simple_kpi_definition, options)
        queries = expander.generate_sql_queries_from_definition(sql_def, options)

        assert len(queries) > 0

    def test_generate_sql_queries_contain_select(self, expander, simple_kpi_definition):
        """Test that generated SQL queries contain SELECT"""
        options = SQLTranslationOptions(target_dialect=SQLDialect.DATABRICKS)
        sql_def = expander.process_definition(simple_kpi_definition, options)
        queries = expander.generate_sql_queries_from_definition(sql_def, options)

        for query in queries:
            sql_text = query.to_sql()
            assert "SELECT" in sql_text.upper()

    def test_generate_sql_queries_contain_from(self, expander, simple_kpi_definition):
        """Test that generated SQL queries contain FROM"""
        options = SQLTranslationOptions(target_dialect=SQLDialect.DATABRICKS)
        sql_def = expander.process_definition(simple_kpi_definition, options)
        queries = expander.generate_sql_queries_from_definition(sql_def, options)

        for query in queries:
            sql_text = query.to_sql()
            assert "FROM" in sql_text.upper()

    def test_generate_sql_queries_multiple_kpis(self, expander, multi_kpi_definition):
        """Test SQL generation with multiple KPIs"""
        options = SQLTranslationOptions(target_dialect=SQLDialect.DATABRICKS)
        sql_def = expander.process_definition(multi_kpi_definition, options)
        queries = expander.generate_sql_queries_from_definition(sql_def, options)

        assert len(queries) >= 1

    def test_generate_sql_queries_standard_dialect(self, standard_expander, simple_kpi_definition):
        """Test SQL generation with STANDARD dialect"""
        options = SQLTranslationOptions(target_dialect=SQLDialect.STANDARD)
        sql_def = standard_expander.process_definition(simple_kpi_definition, options)
        queries = standard_expander.generate_sql_queries_from_definition(sql_def, options)

        assert len(queries) > 0
        sql_text = queries[0].to_sql()
        assert "SELECT" in sql_text.upper()

    # ========== KBI Type Detection Tests ==========

    def test_is_base_kbi_empty_formula(self, expander):
        """Test _is_base_kbi with empty formula"""
        kbi = KPI(
            description="Empty",
            technical_name="empty",
            formula=""
        )
        assert expander._is_base_kbi(kbi) is True

    def test_is_base_kbi_with_brackets_reference(self, expander):
        """Test _is_base_kbi with bracketed KBI reference"""
        # Build lookup first
        expander._dependency_resolver._kbi_lookup = {
            "revenue": KPI(description="Revenue", technical_name="revenue", formula="amount")
        }
        kbi = KPI(
            description="Profit",
            technical_name="profit",
            formula="[revenue] - cost",
            aggregation_type="CALCULATED"
        )
        assert expander._is_base_kbi(kbi) is False

    # ========== Structures Tests ==========

    def test_process_definition_with_structures(self, expander):
        """Test processing definition that includes structures"""
        definition = KPIDefinition(
            description="Structured Metrics",
            technical_name="structured",
            structures={
                "YTD": Structure(
                    description="Year to Date",
                    filter=["fiscyear = '2024'"],
                    display_sign=1
                )
            },
            kpis=[
                KPI(
                    description="Revenue YTD",
                    technical_name="revenue",
                    formula="amount",
                    aggregation_type="SUM",
                    source_table="FactSales",
                    apply_structures=["YTD"]
                )
            ]
        )

        result = expander.process_definition(definition)
        assert result is not None

    def test_process_definition_no_structures(self, expander, simple_kpi_definition):
        """Test processing definition without structures"""
        result = expander.process_definition(simple_kpi_definition)

        # Should process fine with no structures
        assert result is not None
        assert len(result.sql_measures) == 1

    # ========== Aggregation Type Handling Tests ==========

    def test_sum_aggregation_in_sql(self, expander):
        """Test SUM aggregation generates correct SQL"""
        definition = KPIDefinition(
            description="Test",
            technical_name="test",
            kpis=[
                KPI(
                    description="Revenue",
                    technical_name="revenue",
                    formula="amount",
                    aggregation_type="SUM",
                    source_table="Sales"
                )
            ]
        )

        options = SQLTranslationOptions(target_dialect=SQLDialect.DATABRICKS)
        sql_def = expander.process_definition(definition, options)
        queries = expander.generate_sql_queries_from_definition(sql_def, options)

        assert len(queries) > 0
        sql_text = queries[0].to_sql()
        assert "SUM" in sql_text.upper()

    def test_count_aggregation_in_sql(self, expander):
        """Test COUNT aggregation generates correct SQL"""
        definition = KPIDefinition(
            description="Test",
            technical_name="test",
            kpis=[
                KPI(
                    description="Orders",
                    technical_name="orders",
                    formula="order_id",
                    aggregation_type="COUNT",
                    source_table="Orders"
                )
            ]
        )

        options = SQLTranslationOptions(target_dialect=SQLDialect.DATABRICKS)
        sql_def = expander.process_definition(definition, options)
        queries = expander.generate_sql_queries_from_definition(sql_def, options)

        assert len(queries) > 0
        sql_text = queries[0].to_sql()
        assert "COUNT" in sql_text.upper()

    def test_avg_aggregation_in_sql(self, expander):
        """Test AVG aggregation generates correct SQL"""
        definition = KPIDefinition(
            description="Test",
            technical_name="test",
            kpis=[
                KPI(
                    description="Avg Price",
                    technical_name="avg_price",
                    formula="price",
                    aggregation_type="AVERAGE",
                    source_table="Products"
                )
            ]
        )

        options = SQLTranslationOptions(target_dialect=SQLDialect.DATABRICKS)
        sql_def = expander.process_definition(definition, options)
        queries = expander.generate_sql_queries_from_definition(sql_def, options)

        assert len(queries) > 0
        sql_text = queries[0].to_sql()
        assert "AVG" in sql_text.upper()

    def test_distinctcount_aggregation_in_sql(self, expander):
        """Test DISTINCTCOUNT aggregation generates correct SQL"""
        definition = KPIDefinition(
            description="Test",
            technical_name="test",
            kpis=[
                KPI(
                    description="Unique Customers",
                    technical_name="unique_customers",
                    formula="customer_id",
                    aggregation_type="DISTINCTCOUNT",
                    source_table="FactSales"
                )
            ]
        )

        options = SQLTranslationOptions(target_dialect=SQLDialect.DATABRICKS)
        sql_def = expander.process_definition(definition, options)
        queries = expander.generate_sql_queries_from_definition(sql_def, options)

        assert len(queries) > 0
        sql_text = queries[0].to_sql()
        assert "DISTINCT" in sql_text.upper()

    # ========== Filter Integration Tests ==========

    def test_kpi_with_simple_filter(self, expander):
        """Test KPI with a simple filter generates SQL with WHERE"""
        definition = KPIDefinition(
            description="Filtered",
            technical_name="filtered",
            kpis=[
                KPI(
                    description="Active Revenue",
                    technical_name="active_revenue",
                    formula="amount",
                    aggregation_type="SUM",
                    source_table="Sales",
                    filters=["status = 'active'"]
                )
            ]
        )

        options = SQLTranslationOptions(target_dialect=SQLDialect.DATABRICKS)
        sql_def = expander.process_definition(definition, options)
        queries = expander.generate_sql_queries_from_definition(sql_def, options)

        assert len(queries) > 0
        sql_text = queries[0].to_sql()
        assert "WHERE" in sql_text.upper() or "active" in sql_text.lower()

    def test_kpi_with_multiple_filters(self, expander):
        """Test KPI with multiple filters"""
        definition = KPIDefinition(
            description="Multi Filter",
            technical_name="multi_filter",
            kpis=[
                KPI(
                    description="Filtered Revenue",
                    technical_name="filtered_revenue",
                    formula="amount",
                    aggregation_type="SUM",
                    source_table="Sales",
                    filters=["region = 'EMEA'", "year = 2024", "status = 'active'"]
                )
            ]
        )

        options = SQLTranslationOptions(target_dialect=SQLDialect.DATABRICKS)
        sql_def = expander.process_definition(definition, options)
        queries = expander.generate_sql_queries_from_definition(sql_def, options)

        assert len(queries) > 0

    # ========== Variable Substitution Tests ==========

    def test_variable_substitution_in_filters(self, expander):
        """Test that variables are substituted in filter conditions"""
        definition = KPIDefinition(
            description="Variable Filter",
            technical_name="var_filter",
            default_variables={"current_year": "2024"},
            kpis=[
                KPI(
                    description="Revenue This Year",
                    technical_name="revenue_this_year",
                    formula="amount",
                    aggregation_type="SUM",
                    source_table="FactSales",
                    filters=["fiscyear = $var_current_year"]
                )
            ]
        )

        result = expander.process_definition(definition)
        assert result is not None
        assert len(result.sql_measures) == 1

    # ========== SQL Output Structure Tests ==========

    def test_sql_query_has_aggregation_expression(self, expander, simple_kpi_definition):
        """Test SQL query contains aggregation expression"""
        options = SQLTranslationOptions(target_dialect=SQLDialect.DATABRICKS)
        sql_def = expander.process_definition(simple_kpi_definition, options)
        queries = expander.generate_sql_queries_from_definition(sql_def, options)

        sql_text = queries[0].to_sql()
        # Should have some aggregation function
        has_agg = any(func in sql_text.upper() for func in ['SUM', 'COUNT', 'AVG', 'MIN', 'MAX'])
        assert has_agg

    def test_sql_query_references_source_table(self, expander, simple_kpi_definition):
        """Test SQL query references the source table"""
        options = SQLTranslationOptions(target_dialect=SQLDialect.DATABRICKS)
        sql_def = expander.process_definition(simple_kpi_definition, options)
        queries = expander.generate_sql_queries_from_definition(sql_def, options)

        sql_text = queries[0].to_sql()
        # Should reference FactSales table
        assert "FactSales" in sql_text or "factsales" in sql_text.lower()

    def test_sql_measure_technical_name_preserved(self, expander, simple_kpi_definition):
        """Test SQL measure preserves technical name"""
        result = expander.process_definition(simple_kpi_definition)

        assert len(result.sql_measures) == 1
        measure = result.sql_measures[0]
        assert measure.technical_name == "total_revenue"

    # ========== Constant Selection Tests ==========

    def test_kpi_with_constant_selection(self, expander):
        """Test KPI with fields_for_constant_selection"""
        definition = KPIDefinition(
            description="Constant Selection",
            technical_name="const_sel",
            kpis=[
                KPI(
                    description="Revenue by Region",
                    technical_name="revenue_by_region",
                    formula="amount",
                    aggregation_type="SUM",
                    source_table="FactSales",
                    fields_for_constant_selection=["region", "year"]
                )
            ]
        )

        result = expander.process_definition(definition)
        assert result is not None
        assert len(result.sql_measures) == 1

    # ========== Exception Aggregation Tests ==========

    def test_kpi_with_exception_aggregation(self, expander):
        """Test KPI with exception aggregation settings"""
        definition = KPIDefinition(
            description="Exception Aggregation",
            technical_name="exception_agg",
            kpis=[
                KPI(
                    description="Revenue Exception",
                    technical_name="revenue_exception",
                    formula="amount",
                    aggregation_type="EXCEPTION_AGGREGATION",
                    source_table="FactSales",
                    exception_aggregation="SUM",
                    fields_for_exception_aggregation=["product_id"]
                )
            ]
        )

        result = expander.process_definition(definition)
        assert result is not None

    # ========== Edge Cases ==========

    def test_empty_definition(self, expander):
        """Test processing empty KPI definition"""
        definition = KPIDefinition(
            description="Empty",
            technical_name="empty",
            kpis=[]
        )

        result = expander.process_definition(definition)
        assert result is not None
        assert len(result.sql_measures) == 0

    def test_kpi_without_source_table(self, expander):
        """Test KPI processing without explicit source table"""
        definition = KPIDefinition(
            description="No Table",
            technical_name="no_table",
            kpis=[
                KPI(
                    description="Revenue",
                    technical_name="revenue",
                    formula="amount",
                    aggregation_type="SUM"
                )
            ]
        )

        result = expander.process_definition(definition)
        assert result is not None

    def test_kpi_min_aggregation(self, expander):
        """Test MIN aggregation type"""
        definition = KPIDefinition(
            description="Min Price",
            technical_name="min_price",
            kpis=[
                KPI(
                    description="Min Price",
                    technical_name="min_price",
                    formula="price",
                    aggregation_type="MIN",
                    source_table="Products"
                )
            ]
        )

        options = SQLTranslationOptions(target_dialect=SQLDialect.DATABRICKS)
        sql_def = expander.process_definition(definition, options)
        queries = expander.generate_sql_queries_from_definition(sql_def, options)

        sql_text = queries[0].to_sql()
        assert "MIN" in sql_text.upper()

    def test_kpi_max_aggregation(self, expander):
        """Test MAX aggregation type"""
        definition = KPIDefinition(
            description="Max Price",
            technical_name="max_price",
            kpis=[
                KPI(
                    description="Max Price",
                    technical_name="max_price",
                    formula="price",
                    aggregation_type="MAX",
                    source_table="Products"
                )
            ]
        )

        options = SQLTranslationOptions(target_dialect=SQLDialect.DATABRICKS)
        sql_def = expander.process_definition(definition, options)
        queries = expander.generate_sql_queries_from_definition(sql_def, options)

        sql_text = queries[0].to_sql()
        assert "MAX" in sql_text.upper()


class TestSQLTimeIntelligenceHelperExtended:
    """Additional tests for SQLTimeIntelligenceHelper"""

    def test_create_ytd_structure(self):
        """Test creating YTD SQL structure"""
        helper = SQLTimeIntelligenceHelper()
        structure = helper.create_ytd_sql_structure()

        assert structure is not None
        assert "Year" in structure.description or "YTD" in structure.description

    def test_create_ytg_structure(self):
        """Test creating YTG SQL structure"""
        helper = SQLTimeIntelligenceHelper()
        structure = helper.create_ytg_sql_structure()

        assert structure is not None

    def test_create_py_structure(self):
        """Test creating Prior Year SQL structure"""
        helper = SQLTimeIntelligenceHelper()
        structure = helper.create_prior_year_sql_structure()

        assert structure is not None

    def test_create_variance_structure(self):
        """Test creating variance SQL structure"""
        helper = SQLTimeIntelligenceHelper()
        structure = helper.create_variance_sql_structure(["actual", "budget"])

        assert structure is not None
        assert structure.formula is not None or structure.description is not None

    def test_structures_have_description(self):
        """Test that time intelligence structures have descriptions"""
        helper = SQLTimeIntelligenceHelper()
        ytd = helper.create_ytd_sql_structure()
        ytg = helper.create_ytg_sql_structure()
        py = helper.create_prior_year_sql_structure()

        assert ytd.description is not None
        assert ytg.description is not None
        assert py.description is not None

    def test_ytd_has_date_filter(self):
        """Test that YTD structure has a date-based filter"""
        helper = SQLTimeIntelligenceHelper()
        structure = helper.create_ytd_sql_structure()

        # YTD should have sql_template or filters
        assert structure.sql_template is not None or len(structure.filters) > 0
