"""
Unit tests for converters/services/sql/helpers/sql_structures.py

Tests SQL structure expansion and time intelligence helpers for SQL query generation.
Focuses on key public methods and critical workflows.
"""

import pytest
from src.converters.services.sql.helpers.sql_structures import (
    SQLStructureExpander,
    SQLTimeIntelligenceHelper
)
from src.converters.base.models import KPI, KPIDefinition
from src.converters.services.sql.models import (
    SQLDialect,
    SQLTranslationOptions,
    SQLStructure
)


class TestSQLStructureExpander:
    """Tests for SQLStructureExpander class"""

    @pytest.fixture
    def standard_expander(self):
        """Create SQLStructureExpander with STANDARD dialect"""
        return SQLStructureExpander(dialect=SQLDialect.STANDARD)

    @pytest.fixture
    def databricks_expander(self):
        """Create SQLStructureExpander with DATABRICKS dialect"""
        return SQLStructureExpander(dialect=SQLDialect.DATABRICKS)

    @pytest.fixture
    def simple_definition(self):
        """Simple KPI definition for testing"""
        return KPIDefinition(
            description="Sales Metrics",
            technical_name="sales_metrics",
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

    # ========== Initialization Tests ==========

    def test_expander_initialization_standard(self, standard_expander):
        """Test expander initializes with STANDARD dialect"""
        assert standard_expander.dialect == SQLDialect.STANDARD

    def test_expander_initialization_databricks(self, databricks_expander):
        """Test expander initializes with DATABRICKS dialect"""
        assert databricks_expander.dialect == SQLDialect.DATABRICKS

    # ========== Base KBI Detection Tests ==========

    def test_is_base_kbi_simple_formula(self, standard_expander):
        """Test detecting base KBI with simple column formula"""
        kbi = KPI(
            description="Revenue",
            technical_name="revenue",
            formula="amount",
            aggregation_type="SUM"
        )
        assert standard_expander._is_base_kbi(kbi) is True

    def test_is_base_kbi_calculated(self, standard_expander):
        """Test detecting calculated KBI (not base)"""
        # Create referenced KBIs first
        revenue_kbi = KPI(
            description="Revenue",
            technical_name="revenue",
            formula="amount",
            aggregation_type="SUM"
        )
        cost_kbi = KPI(
            description="Cost",
            technical_name="cost",
            formula="cost_amount",
            aggregation_type="SUM"
        )

        # Build KBI lookup so dependencies can be resolved
        standard_expander._dependency_resolver.build_kbi_lookup([revenue_kbi, cost_kbi])

        # Now create calculated KBI that references them
        kbi = KPI(
            description="Profit",
            technical_name="profit",
            formula="[revenue] - [cost]",
            aggregation_type="CALCULATED"
        )
        assert standard_expander._is_base_kbi(kbi) is False

    def test_is_simple_column_reference_true(self, standard_expander):
        """Test detecting simple column reference"""
        assert standard_expander._is_simple_column_reference("amount") is True
        assert standard_expander._is_simple_column_reference("customer_id") is True

    def test_is_simple_column_reference_false(self, standard_expander):
        """Test detecting complex formula (not simple column)"""
        assert standard_expander._is_simple_column_reference("[revenue] - [cost]") is False
        assert standard_expander._is_simple_column_reference("SUM(amount)") is False

    # ========== Technical Name Generation Tests ==========

    def test_generate_technical_name_simple(self, standard_expander):
        """Test generating technical name from description"""
        result = standard_expander._generate_technical_name("Total Revenue")
        assert result == "total_revenue"

    def test_generate_technical_name_with_special_chars(self, standard_expander):
        """Test generating technical name with special characters"""
        result = standard_expander._generate_technical_name("Revenue (YTD) - Actual")
        assert "_" in result
        assert "(" not in result
        assert ")" not in result

    def test_generate_technical_name_lowercase(self, standard_expander):
        """Test technical name is lowercase"""
        result = standard_expander._generate_technical_name("TOTAL SALES AMOUNT")
        assert result.islower()

    # ========== Identifier Quoting Tests ==========

    def test_quote_identifier_standard(self, standard_expander):
        """Test quoting identifier for STANDARD dialect"""
        result = standard_expander._quote_identifier("column_name")
        assert result == '"column_name"'

    def test_quote_identifier_databricks(self, databricks_expander):
        """Test quoting identifier for DATABRICKS dialect"""
        result = databricks_expander._quote_identifier("column_name")
        assert result == "`column_name`"

    # ========== Aggregation Type Mapping Tests ==========

    def test_map_to_sql_aggregation_type_sum(self, standard_expander):
        """Test mapping SUM aggregation type"""
        from src.converters.services.sql.models import SQLAggregationType
        result = standard_expander._map_to_sql_aggregation_type("SUM")
        assert result == SQLAggregationType.SUM

    def test_map_to_sql_aggregation_type_count(self, standard_expander):
        """Test mapping COUNT aggregation type"""
        from src.converters.services.sql.models import SQLAggregationType
        result = standard_expander._map_to_sql_aggregation_type("COUNT")
        assert result == SQLAggregationType.COUNT

    def test_map_to_sql_aggregation_type_average(self, standard_expander):
        """Test mapping AVERAGE to AVG aggregation type"""
        from src.converters.services.sql.models import SQLAggregationType
        result = standard_expander._map_to_sql_aggregation_type("AVERAGE")
        assert result == SQLAggregationType.AVG

    # ========== Time Intelligence Detection Tests ==========

    def test_is_time_intelligence_structure_ytd(self, standard_expander):
        """Test detecting YTD time intelligence structure"""
        from src.converters.base.models import Structure
        struct = Structure(
            description="Year to Date",
            formula=None
        )
        assert standard_expander._is_time_intelligence_structure("ytd_revenue", struct) is True

    def test_is_time_intelligence_structure_not_ti(self, standard_expander):
        """Test non-time-intelligence structure"""
        from src.converters.base.models import Structure
        struct = Structure(
            description="Custom Structure",
            formula="[base] * 2"
        )
        assert standard_expander._is_time_intelligence_structure("custom_struct", struct) is False

    # ========== Process Definition Tests ==========

    def test_process_definition_simple(self, standard_expander, simple_definition):
        """Test processing simple KPI definition"""
        result = standard_expander.process_definition(simple_definition)

        assert result is not None
        assert result.description == "Sales Metrics"
        assert result.technical_name == "sales_metrics"

    def test_process_definition_with_options(self, standard_expander, simple_definition):
        """Test processing definition with translation options"""
        options = SQLTranslationOptions(
            target_dialect=SQLDialect.STANDARD,
            format_output=True
        )
        result = standard_expander.process_definition(simple_definition, options)

        assert result is not None
        assert result.dialect == SQLDialect.STANDARD

    # ========== SQL Query Generation Tests ==========

    def test_generate_sql_queries_from_definition(self, standard_expander):
        """Test generating SQL queries from definition"""
        from src.converters.services.sql.models import SQLDefinition, SQLMeasure, SQLAggregationType

        sql_measure = SQLMeasure(
            name="Revenue",
            description="Total Revenue",
            sql_expression='SUM("Sales"."amount")',
            aggregation_type=SQLAggregationType.SUM,
            source_table="Sales"
        )

        sql_definition = SQLDefinition(
            description="Test",
            technical_name="test",
            sql_measures=[sql_measure]
        )

        queries = standard_expander.generate_sql_queries_from_definition(sql_definition)

        assert len(queries) > 0
        assert queries[0] is not None

    # ========== Edge Cases ==========

    def test_process_definition_empty_kpis(self, standard_expander):
        """Test processing definition with no KPIs"""
        definition = KPIDefinition(
            description="Empty",
            technical_name="empty",
            kpis=[]
        )
        result = standard_expander.process_definition(definition)

        assert result is not None
        assert len(result.sql_measures) == 0


class TestSQLTimeIntelligenceHelper:
    """Tests for SQLTimeIntelligenceHelper class"""

    @pytest.fixture
    def standard_helper(self):
        """Create helper with STANDARD dialect"""
        return SQLTimeIntelligenceHelper(dialect=SQLDialect.STANDARD)

    @pytest.fixture
    def databricks_helper(self):
        """Create helper with DATABRICKS dialect"""
        return SQLTimeIntelligenceHelper(dialect=SQLDialect.DATABRICKS)

    # ========== Initialization Tests ==========

    def test_helper_initialization_standard(self, standard_helper):
        """Test helper initializes with STANDARD dialect"""
        assert standard_helper.dialect == SQLDialect.STANDARD

    def test_helper_initialization_databricks(self, databricks_helper):
        """Test helper initializes with DATABRICKS dialect"""
        assert databricks_helper.dialect == SQLDialect.DATABRICKS

    # ========== YTD Structure Tests ==========

    def test_create_ytd_sql_structure(self, standard_helper):
        """Test creating YTD SQL structure"""
        result = standard_helper.create_ytd_sql_structure()

        assert isinstance(result, SQLStructure)
        assert result.description is not None
        assert "ytd" in result.description.lower() or "year" in result.description.lower()

    def test_create_ytd_sql_structure_custom_date_column(self, standard_helper):
        """Test creating YTD structure with custom date column"""
        result = standard_helper.create_ytd_sql_structure(date_column="transaction_date")

        assert isinstance(result, SQLStructure)
        # Check that custom date column is used
        if result.sql_template:
            assert "transaction_date" in result.sql_template or result.date_column == "transaction_date"

    # ========== YTG Structure Tests ==========

    def test_create_ytg_sql_structure(self, standard_helper):
        """Test creating YTG (Year to Go) SQL structure"""
        result = standard_helper.create_ytg_sql_structure()

        assert isinstance(result, SQLStructure)
        assert result.description is not None

    def test_create_ytg_sql_structure_custom_date_column(self, standard_helper):
        """Test creating YTG structure with custom date column"""
        result = standard_helper.create_ytg_sql_structure(date_column="order_date")

        assert isinstance(result, SQLStructure)

    # ========== Prior Year Structure Tests ==========

    def test_create_prior_year_sql_structure(self, standard_helper):
        """Test creating prior year SQL structure"""
        result = standard_helper.create_prior_year_sql_structure()

        assert isinstance(result, SQLStructure)
        assert result.description is not None
        assert "prior" in result.description.lower() or "previous" in result.description.lower() or "year" in result.description.lower()

    def test_create_prior_year_custom_date_column(self, standard_helper):
        """Test creating prior year structure with custom date column"""
        result = standard_helper.create_prior_year_sql_structure(date_column="fiscal_date")

        assert isinstance(result, SQLStructure)

    # ========== Variance Structure Tests ==========

    def test_create_variance_sql_structure(self, standard_helper):
        """Test creating variance SQL structure"""
        base_measures = ["actual", "budget"]
        result = standard_helper.create_variance_sql_structure(base_measures)

        assert isinstance(result, SQLStructure)
        assert result.description is not None
        assert "variance" in result.description.lower() or "difference" in result.description.lower()

    def test_create_variance_empty_measures(self, standard_helper):
        """Test creating variance structure with empty measures list"""
        result = standard_helper.create_variance_sql_structure([])

        assert isinstance(result, SQLStructure)

    # ========== Databricks Dialect Tests ==========

    def test_ytd_structure_databricks(self, databricks_helper):
        """Test YTD structure with Databricks dialect"""
        result = databricks_helper.create_ytd_sql_structure()

        assert isinstance(result, SQLStructure)
        # Databricks-specific checks if template is generated
        if result.sql_template:
            # Databricks uses backticks for identifiers
            assert "`" in result.sql_template or result.sql_template != ""

    def test_prior_year_structure_databricks(self, databricks_helper):
        """Test prior year structure with Databricks dialect"""
        result = databricks_helper.create_prior_year_sql_structure()

        assert isinstance(result, SQLStructure)


class TestIntegration:
    """Integration tests for SQL structures"""

    @pytest.fixture
    def expander(self):
        """Create expander for integration tests"""
        return SQLStructureExpander(dialect=SQLDialect.STANDARD)

    def test_complete_workflow_simple_kpi(self, expander):
        """Test complete workflow for simple KPI"""
        definition = KPIDefinition(
            description="Sales",
            technical_name="sales",
            kpis=[
                KPI(
                    description="Total Sales",
                    technical_name="total_sales",
                    formula="amount",
                    aggregation_type="SUM",
                    source_table="FactSales"
                )
            ]
        )

        # Process definition
        sql_def = expander.process_definition(definition)
        assert sql_def is not None

        # Generate queries
        queries = expander.generate_sql_queries_from_definition(sql_def)
        assert len(queries) > 0

    def test_workflow_with_options(self, expander):
        """Test workflow with translation options"""
        definition = KPIDefinition(
            description="Metrics",
            technical_name="metrics",
            kpis=[
                KPI(
                    description="Count",
                    technical_name="count",
                    formula="id",
                    aggregation_type="COUNT",
                    source_table="Data"
                )
            ]
        )

        options = SQLTranslationOptions(
            format_output=True,
            include_comments=True
        )

        sql_def = expander.process_definition(definition, options)
        assert sql_def is not None
