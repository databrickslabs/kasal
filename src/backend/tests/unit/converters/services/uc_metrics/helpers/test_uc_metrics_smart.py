"""
Unit tests for converters/services/uc_metrics/helpers/uc_metrics_smart.py

Tests SmartUCMetricsGenerator for automatic strategy selection and routing.
"""

import pytest
from src.converters.services.uc_metrics.helpers.uc_metrics_smart import SmartUCMetricsGenerator
from src.converters.base.models import KPI, KPIDefinition


class TestSmartUCMetricsGenerator:
    """Tests for SmartUCMetricsGenerator class"""

    @pytest.fixture
    def generator(self):
        """Create SmartUCMetricsGenerator for testing"""
        return SmartUCMetricsGenerator(dialect="spark")

    @pytest.fixture
    def simple_definition(self):
        """Simple KPI definition without dependencies"""
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
                ),
                KPI(
                    description="Count",
                    technical_name="count",
                    formula="id",
                    aggregation_type="COUNT",
                    source_table="Sales"
                )
            ]
        )

    @pytest.fixture
    def definition_with_dependencies(self):
        """KPI definition with CALCULATED measures"""
        return KPIDefinition(
            description="Financial Metrics",
            technical_name="financial_metrics",
            kpis=[
                KPI(
                    description="Revenue",
                    technical_name="revenue",
                    formula="amount",
                    aggregation_type="SUM",
                    source_table="Finance"
                ),
                KPI(
                    description="Cost",
                    technical_name="cost",
                    formula="cost_amount",
                    aggregation_type="SUM",
                    source_table="Finance"
                ),
                KPI(
                    description="Profit",
                    technical_name="profit",
                    formula="[revenue] - [cost]",
                    aggregation_type="CALCULATED"
                )
            ]
        )

    @pytest.fixture
    def metadata(self):
        """Metadata for UC Metrics generation"""
        return {
            "name": "test_metrics",
            "catalog": "main",
            "schema": "default"
        }

    # ========== Initialization Tests ==========

    def test_generator_initialization_spark(self, generator):
        """Test generator initializes with Spark dialect"""
        assert generator.dialect == "spark"

    def test_generator_initialization_default(self):
        """Test generator initializes with default dialect"""
        generator = SmartUCMetricsGenerator()
        assert generator.dialect == "spark"

    def test_generator_has_basic_generator(self, generator):
        """Test generator has basic generator"""
        assert hasattr(generator, 'basic_generator')
        assert generator.basic_generator is not None

    def test_generator_has_tree_generator(self, generator):
        """Test generator has tree parsing generator"""
        assert hasattr(generator, 'tree_generator')
        assert generator.tree_generator is not None

    # ========== Has Dependencies Tests ==========

    def test_has_dependencies_false_for_simple(self, generator, simple_definition):
        """Test has_dependencies returns False for simple measures"""
        result = generator.has_dependencies(simple_definition)
        assert result is False

    def test_has_dependencies_true_for_calculated(self, generator, definition_with_dependencies):
        """Test has_dependencies returns True for CALCULATED measures"""
        result = generator.has_dependencies(definition_with_dependencies)
        assert result is True

    def test_has_dependencies_false_for_empty(self, generator):
        """Test has_dependencies returns False for empty definition"""
        definition = KPIDefinition(
            description="Empty",
            technical_name="empty",
            kpis=[]
        )
        result = generator.has_dependencies(definition)
        assert result is False

    def test_has_dependencies_case_insensitive(self, generator):
        """Test has_dependencies is case-insensitive for aggregation_type"""
        definition = KPIDefinition(
            description="Test",
            technical_name="test",
            kpis=[
                KPI(
                    description="Base",
                    technical_name="base",
                    formula="amount",
                    aggregation_type="sum",
                    source_table="Data"
                ),
                KPI(
                    description="Calc",
                    technical_name="calc",
                    formula="[base] * 2",
                    aggregation_type="calculated"  # lowercase
                )
            ]
        )
        result = generator.has_dependencies(definition)
        assert result is True

    def test_has_dependencies_mixed_types(self, generator):
        """Test has_dependencies with mix of SUM and CALCULATED"""
        definition = KPIDefinition(
            description="Mixed",
            technical_name="mixed",
            kpis=[
                KPI(
                    description="Sum",
                    technical_name="sum_measure",
                    formula="amount",
                    aggregation_type="SUM",
                    source_table="Data"
                ),
                KPI(
                    description="Avg",
                    technical_name="avg_measure",
                    formula="value",
                    aggregation_type="AVERAGE",
                    source_table="Data"
                ),
                KPI(
                    description="Calc",
                    technical_name="calc_measure",
                    formula="[sum_measure] / [avg_measure]",
                    aggregation_type="CALCULATED"
                )
            ]
        )
        result = generator.has_dependencies(definition)
        assert result is True

    # ========== Get Recommended Strategy Tests ==========

    def test_get_recommended_strategy_basic(self, generator, simple_definition):
        """Test get_recommended_strategy returns 'basic' for simple measures"""
        result = generator.get_recommended_strategy(simple_definition)
        assert result == "basic"

    def test_get_recommended_strategy_tree_parsing(self, generator, definition_with_dependencies):
        """Test get_recommended_strategy returns 'tree_parsing' for CALCULATED measures"""
        result = generator.get_recommended_strategy(definition_with_dependencies)
        assert result == "tree_parsing"

    def test_get_recommended_strategy_empty(self, generator):
        """Test get_recommended_strategy returns 'basic' for empty definition"""
        definition = KPIDefinition(
            description="Empty",
            technical_name="empty",
            kpis=[]
        )
        result = generator.get_recommended_strategy(definition)
        assert result == "basic"

    def test_get_recommended_strategy_all_aggregations(self, generator):
        """Test strategy for all aggregation types except CALCULATED"""
        definition = KPIDefinition(
            description="All Aggs",
            technical_name="all_aggs",
            kpis=[
                KPI(
                    description="Sum",
                    technical_name="sum_m",
                    formula="a",
                    aggregation_type="SUM",
                    source_table="Data"
                ),
                KPI(
                    description="Count",
                    technical_name="count_m",
                    formula="b",
                    aggregation_type="COUNT",
                    source_table="Data"
                ),
                KPI(
                    description="Avg",
                    technical_name="avg_m",
                    formula="c",
                    aggregation_type="AVERAGE",
                    source_table="Data"
                ),
                KPI(
                    description="Distinct",
                    technical_name="distinct_m",
                    formula="d",
                    aggregation_type="DISTINCTCOUNT",
                    source_table="Data"
                )
            ]
        )
        result = generator.get_recommended_strategy(definition)
        assert result == "basic"

    # ========== Generate Consolidated UC Metrics Tests ==========

    def test_generate_consolidated_uc_metrics_basic_strategy(self, generator, simple_definition, metadata):
        """Test generating UC Metrics with basic strategy"""
        result = generator.generate_consolidated_uc_metrics(simple_definition, metadata)

        assert result is not None
        assert "version" in result
        assert "description" in result
        assert "measures" in result
        # Basic strategy does not include 'source' field
        assert len(result["measures"]) == 2

    def test_generate_consolidated_uc_metrics_tree_parsing_strategy(self, generator, definition_with_dependencies, metadata):
        """Test generating UC Metrics with tree parsing strategy"""
        result = generator.generate_consolidated_uc_metrics(definition_with_dependencies, metadata)

        assert result is not None
        assert "version" in result
        assert "description" in result
        assert "source" in result
        assert "measures" in result
        assert len(result["measures"]) == 3

    def test_generate_consolidated_uc_metrics_source_table_qualified(self, generator, definition_with_dependencies, metadata):
        """Test source table is properly qualified (tree parsing strategy)"""
        result = generator.generate_consolidated_uc_metrics(definition_with_dependencies, metadata)

        # Tree parsing strategy includes source field with catalog.schema.table format
        assert "source" in result
        assert "main.default.Finance" in result["source"]

    def test_generate_consolidated_uc_metrics_source_table_already_qualified(self, generator, metadata):
        """Test source table that's already qualified is not modified (tree parsing strategy)"""
        # Need CALCULATED measure to trigger tree parsing strategy
        definition = KPIDefinition(
            description="Test",
            technical_name="test",
            kpis=[
                KPI(
                    description="Base",
                    technical_name="base",
                    formula="value",
                    aggregation_type="SUM",
                    source_table="catalog.schema.table"
                ),
                KPI(
                    description="Calc",
                    technical_name="calc",
                    formula="[base] * 2",
                    aggregation_type="CALCULATED"
                )
            ]
        )
        result = generator.generate_consolidated_uc_metrics(definition, metadata)

        assert "source" in result
        assert result["source"] == "catalog.schema.table"

    def test_generate_consolidated_uc_metrics_no_source_table(self, generator, metadata):
        """Test default source table when none specified (tree parsing strategy)"""
        # Need CALCULATED measure to trigger tree parsing strategy
        definition = KPIDefinition(
            description="Test",
            technical_name="test",
            kpis=[
                KPI(
                    description="Base",
                    technical_name="base",
                    formula="value",
                    aggregation_type="SUM"
                ),
                KPI(
                    description="Calc",
                    technical_name="calc",
                    formula="[base] * 2",
                    aggregation_type="CALCULATED"
                )
            ]
        )
        result = generator.generate_consolidated_uc_metrics(definition, metadata)

        assert "source" in result
        assert "default_table" in result["source"]

    def test_generate_consolidated_uc_metrics_description(self, generator, definition_with_dependencies, metadata):
        """Test UC Metrics description includes metadata name"""
        result = generator.generate_consolidated_uc_metrics(definition_with_dependencies, metadata)

        assert "test_metrics" in result["description"]

    def test_generate_consolidated_uc_metrics_version(self, generator, simple_definition, metadata):
        """Test UC Metrics has version field"""
        result = generator.generate_consolidated_uc_metrics(simple_definition, metadata)

        assert result["version"] == "0.1"

    def test_generate_consolidated_uc_metrics_circular_dependency_error(self, generator, metadata):
        """Test circular dependency detection raises error"""
        # Create circular dependency: A -> B -> C -> A
        definition = KPIDefinition(
            description="Circular",
            technical_name="circular",
            kpis=[
                KPI(
                    description="A",
                    technical_name="a",
                    formula="[b]",
                    aggregation_type="CALCULATED",
                    source_table="Data"
                ),
                KPI(
                    description="B",
                    technical_name="b",
                    formula="[c]",
                    aggregation_type="CALCULATED"
                ),
                KPI(
                    description="C",
                    technical_name="c",
                    formula="[a]",
                    aggregation_type="CALCULATED"
                )
            ]
        )

        with pytest.raises(ValueError) as exc_info:
            generator.generate_consolidated_uc_metrics(definition, metadata)

        assert "Circular dependencies detected" in str(exc_info.value)

    # ========== Build Consolidated Format Tests ==========

    def test_build_consolidated_format_basic(self, generator, simple_definition, metadata):
        """Test _build_consolidated_format with basic measures"""
        measures = [
            {"name": "revenue", "expr": "SUM(amount)", "description": "Revenue"},
            {"name": "count", "expr": "COUNT(id)", "description": "Count"}
        ]

        result = generator._build_consolidated_format(measures, simple_definition, metadata)

        assert result["version"] == "0.1"
        assert "test_metrics" in result["description"]
        assert result["measures"] == measures
        assert "main.default.Sales" in result["source"]

    def test_build_consolidated_format_no_source_table(self, generator, metadata):
        """Test _build_consolidated_format with no source table"""
        definition = KPIDefinition(
            description="Test",
            technical_name="test",
            kpis=[]
        )
        measures = [{"name": "m1", "expr": "SUM(a)", "description": "M1"}]

        result = generator._build_consolidated_format(measures, definition, metadata)

        assert "default_table" in result["source"]

    def test_build_consolidated_format_uses_first_source_table(self, generator, metadata):
        """Test _build_consolidated_format uses first KPI's source table"""
        definition = KPIDefinition(
            description="Test",
            technical_name="test",
            kpis=[
                KPI(
                    description="M1",
                    technical_name="m1",
                    formula="a",
                    aggregation_type="SUM",
                    source_table="Table1"
                ),
                KPI(
                    description="M2",
                    technical_name="m2",
                    formula="b",
                    aggregation_type="SUM",
                    source_table="Table2"
                )
            ]
        )
        measures = []

        result = generator._build_consolidated_format(measures, definition, metadata)

        assert "Table1" in result["source"]
        assert "Table2" not in result["source"]

    def test_build_consolidated_format_custom_catalog_schema(self, generator, simple_definition):
        """Test _build_consolidated_format with custom catalog and schema"""
        metadata = {
            "name": "custom_metrics",
            "catalog": "custom_catalog",
            "schema": "custom_schema"
        }
        measures = []

        result = generator._build_consolidated_format(measures, simple_definition, metadata)

        assert "custom_catalog.custom_schema.Sales" in result["source"]

    # ========== Get Analysis Report Tests ==========

    def test_get_analysis_report_simple(self, generator, simple_definition):
        """Test get_analysis_report for simple definition"""
        result = generator.get_analysis_report(simple_definition)

        assert result["total_measures"] == 2
        assert result["has_dependencies"] is False
        assert result["recommended_strategy"] == "basic"
        assert result["calculated_measures"] == 0
        assert result["simple_measures"] == 2

    def test_get_analysis_report_with_dependencies(self, generator, definition_with_dependencies):
        """Test get_analysis_report for definition with dependencies"""
        result = generator.get_analysis_report(definition_with_dependencies)

        assert result["total_measures"] == 3
        assert result["has_dependencies"] is True
        assert result["recommended_strategy"] == "tree_parsing"
        assert result["calculated_measures"] == 1
        assert result["simple_measures"] == 2

    def test_get_analysis_report_dependency_analysis(self, generator, definition_with_dependencies):
        """Test get_analysis_report includes dependency analysis for tree parsing strategy"""
        result = generator.get_analysis_report(definition_with_dependencies)

        assert "dependency_analysis" in result
        # Dependency analysis should have structure
        dep_analysis = result["dependency_analysis"]
        assert isinstance(dep_analysis, dict)

    def test_get_analysis_report_empty(self, generator):
        """Test get_analysis_report for empty definition"""
        definition = KPIDefinition(
            description="Empty",
            technical_name="empty",
            kpis=[]
        )
        result = generator.get_analysis_report(definition)

        assert result["total_measures"] == 0
        assert result["has_dependencies"] is False
        assert result["recommended_strategy"] == "basic"
        assert result["calculated_measures"] == 0
        assert result["simple_measures"] == 0

    def test_get_analysis_report_all_calculated(self, generator):
        """Test get_analysis_report for all CALCULATED measures"""
        definition = KPIDefinition(
            description="All Calc",
            technical_name="all_calc",
            kpis=[
                KPI(
                    description="Base",
                    technical_name="base",
                    formula="amount",
                    aggregation_type="SUM",
                    source_table="Data"
                ),
                KPI(
                    description="Calc1",
                    technical_name="calc1",
                    formula="[base] * 2",
                    aggregation_type="CALCULATED"
                ),
                KPI(
                    description="Calc2",
                    technical_name="calc2",
                    formula="[calc1] * 3",
                    aggregation_type="CALCULATED"
                )
            ]
        )
        result = generator.get_analysis_report(definition)

        assert result["total_measures"] == 3
        assert result["calculated_measures"] == 2
        assert result["simple_measures"] == 1

    # ========== Edge Cases ==========

    def test_generate_with_no_metadata_name(self, generator, definition_with_dependencies):
        """Test generating with no name in metadata (tree parsing strategy)"""
        metadata = {"catalog": "main", "schema": "default"}
        result = generator.generate_consolidated_uc_metrics(definition_with_dependencies, metadata)

        assert "measures" in result["description"]

    def test_generate_with_no_metadata_catalog(self, generator, definition_with_dependencies):
        """Test generating with no catalog in metadata (tree parsing strategy)"""
        metadata = {"name": "test", "schema": "default"}
        result = generator.generate_consolidated_uc_metrics(definition_with_dependencies, metadata)

        # Should default to "main"
        assert "source" in result
        assert "main.default" in result["source"]

    def test_generate_with_no_metadata_schema(self, generator, definition_with_dependencies):
        """Test generating with no schema in metadata (tree parsing strategy)"""
        metadata = {"name": "test", "catalog": "main"}
        result = generator.generate_consolidated_uc_metrics(definition_with_dependencies, metadata)

        # Should default to "default"
        assert "source" in result
        assert "main.default" in result["source"]

    # ========== Integration Tests ==========

    def test_full_workflow_simple(self, generator, simple_definition, metadata):
        """Test complete workflow for simple definition"""
        # Analyze
        report = generator.get_analysis_report(simple_definition)
        assert report["recommended_strategy"] == "basic"

        # Generate
        result = generator.generate_consolidated_uc_metrics(simple_definition, metadata)
        assert result is not None
        assert len(result["measures"]) == 2

    def test_full_workflow_with_dependencies(self, generator, definition_with_dependencies, metadata):
        """Test complete workflow for definition with dependencies"""
        # Analyze
        report = generator.get_analysis_report(definition_with_dependencies)
        assert report["recommended_strategy"] == "tree_parsing"

        # Generate
        result = generator.generate_consolidated_uc_metrics(definition_with_dependencies, metadata)
        assert result is not None
        assert len(result["measures"]) == 3
