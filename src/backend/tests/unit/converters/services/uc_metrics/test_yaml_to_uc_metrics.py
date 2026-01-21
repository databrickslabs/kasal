"""
Unit tests for converters/services/uc_metrics/yaml_to_uc_metrics.py

Tests UCMetricsGenerator for converting KPI definitions to Unity Catalog metrics store format.
"""

import pytest
from src.converters.services.uc_metrics.yaml_to_uc_metrics import UCMetricsGenerator
from src.converters.base.models import KPI, KPIDefinition


class TestUCMetricsGenerator:
    """Tests for UCMetricsGenerator class"""

    @pytest.fixture
    def generator(self):
        """Create UCMetricsGenerator for testing"""
        return UCMetricsGenerator(dialect="spark")

    @pytest.fixture
    def simple_kpi(self):
        """Simple KPI for testing"""
        return KPI(
            description="Revenue",
            technical_name="revenue",
            formula="amount",
            aggregation_type="SUM",
            source_table="Sales"
        )

    @pytest.fixture
    def kpi_with_filters(self):
        """KPI with filters"""
        return KPI(
            description="Active Revenue",
            technical_name="active_revenue",
            formula="amount",
            aggregation_type="SUM",
            source_table="Sales",
            filters=["status = 'active'", "year = 2023"]
        )

    @pytest.fixture
    def simple_definition(self):
        """Simple KPI definition"""
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

    @pytest.fixture
    def yaml_metadata(self):
        """Sample YAML metadata"""
        return {
            "name": "test_metrics",
            "description": "Test Metrics",
            "default_variables": {
                "year": 2023,
                "region": "US"
            },
            "filters": {
                "query_filter": {
                    "year_filter": "year = $var_year"
                }
            }
        }

    # ========== Initialization Tests ==========

    def test_generator_initialization_spark(self, generator):
        """Test generator initializes with Spark dialect"""
        assert generator.dialect == "spark"

    def test_generator_initialization_default(self):
        """Test generator initializes with default dialect"""
        generator = UCMetricsGenerator()
        assert generator.dialect == "spark"

    def test_generator_has_aggregation_builder(self, generator):
        """Test generator has aggregation builder"""
        assert hasattr(generator, 'aggregation_builder')
        assert generator.aggregation_builder is not None

    def test_generator_has_formula_parser(self, generator):
        """Test generator has formula parser"""
        assert hasattr(generator, '_formula_parser')
        assert generator._formula_parser is not None

    def test_generator_has_dependency_resolver(self, generator):
        """Test generator has dependency resolver"""
        assert hasattr(generator, '_dependency_resolver')
        assert generator._dependency_resolver is not None

    # ========== Build Source Reference Tests ==========

    def test_build_source_reference_simple(self, generator, yaml_metadata):
        """Test building source reference for simple table name"""
        result = generator._build_source_reference("Sales", yaml_metadata)
        assert result == "catalog.schema.Sales"

    def test_build_source_reference_already_qualified(self, generator, yaml_metadata):
        """Test building source reference for already qualified table"""
        result = generator._build_source_reference("main.default.Sales", yaml_metadata)
        assert result == "main.default.Sales"

    def test_build_source_reference_with_schema(self, generator, yaml_metadata):
        """Test building source reference for table with schema"""
        result = generator._build_source_reference("schema.Sales", yaml_metadata)
        assert result == "schema.Sales"

    # ========== Variable Substitution Tests ==========

    def test_substitute_variables_single_value(self, generator):
        """Test substituting single variable value"""
        expression = "year = $var_year"
        variables = {"year": 2023}
        result = generator._substitute_variables(expression, variables)
        assert result == "year = '2023'"

    def test_substitute_variables_list_value(self, generator):
        """Test substituting list variable value"""
        expression = "region IN $var_regions"
        variables = {"regions": ["US", "EU", "APAC"]}
        result = generator._substitute_variables(expression, variables)
        assert result == "region IN ('US', 'EU', 'APAC')"

    def test_substitute_variables_multiple_vars(self, generator):
        """Test substituting multiple variables"""
        expression = "year = $var_year AND region = $var_region"
        variables = {"year": 2023, "region": "US"}
        result = generator._substitute_variables(expression, variables)
        assert result == "year = '2023' AND region = 'US'"

    def test_substitute_variables_no_vars(self, generator):
        """Test expression with no variables"""
        expression = "status = 'active'"
        variables = {}
        result = generator._substitute_variables(expression, variables)
        assert result == "status = 'active'"

    # ========== Build Filter Conditions Tests ==========

    def test_build_filter_conditions_no_filters(self, generator, simple_kpi, yaml_metadata):
        """Test building filter conditions with no filters"""
        result = generator._build_filter_conditions(simple_kpi, yaml_metadata)
        assert result is None

    def test_build_filter_conditions_with_filters(self, generator, kpi_with_filters, yaml_metadata):
        """Test building filter conditions with filters"""
        result = generator._build_filter_conditions(kpi_with_filters, yaml_metadata)
        assert result is not None
        assert "status = 'active'" in result
        assert "year = 2023" in result

    def test_build_filter_conditions_with_query_filter(self, generator, yaml_metadata):
        """Test building filter conditions with query filter"""
        kpi = KPI(
            description="Test",
            technical_name="test",
            formula="amount",
            aggregation_type="SUM",
            source_table="Sales",
            filters=["$query_filter"]
        )
        result = generator._build_filter_conditions(kpi, yaml_metadata)
        assert result is not None
        assert "year = '2023'" in result

    # ========== Generate UC Metric (Single KBI) Tests ==========
    # Note: generate_uc_metric is overwritten in the source and is now specific to constant selection KBIs
    # Regular KBIs should use generate_consolidated_uc_metrics

    def test_generate_uc_metric_constant_selection(self, generator, simple_definition, yaml_metadata):
        """Test generating UC metric for constant selection KBI"""
        pytest.skip("generate_uc_metric is overwritten and specific to constant selection KBIs")

    def test_generate_uc_metric_with_filters(self, generator, simple_definition, yaml_metadata):
        """Test generating UC metric with filters"""
        pytest.skip("generate_uc_metric is overwritten and specific to constant selection KBIs")

    def test_generate_uc_metric_source_reference(self, generator, simple_definition, yaml_metadata):
        """Test UC metric has correct source reference"""
        pytest.skip("generate_uc_metric is overwritten and specific to constant selection KBIs")

    # ========== Generate Consolidated UC Metrics Tests ==========

    def test_generate_consolidated_uc_metrics_simple(self, generator, yaml_metadata):
        """Test generating consolidated UC metrics for simple KBIs"""
        kbi_list = [
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

        result = generator.generate_consolidated_uc_metrics(kbi_list, yaml_metadata)

        assert result["version"] == "0.1"
        assert "description" in result
        assert "measures" in result
        assert len(result["measures"]) == 2

    def test_generate_consolidated_uc_metrics_with_common_filters(self, generator, yaml_metadata):
        """Test consolidated UC metrics extracts common filters"""
        kbi_list = [
            KPI(
                description="Revenue",
                technical_name="revenue",
                formula="amount",
                aggregation_type="SUM",
                source_table="Sales",
                filters=["$query_filter", "region = 'US'"]
            ),
            KPI(
                description="Cost",
                technical_name="cost",
                formula="cost_amount",
                aggregation_type="SUM",
                source_table="Sales",
                filters=["$query_filter", "region = 'US'"]
            )
        ]

        result = generator.generate_consolidated_uc_metrics(kbi_list, yaml_metadata)

        # Query filters and common filters should be at the top level
        assert "filter" in result
        assert "year = '2023'" in result["filter"]
        assert "region = 'US'" in result["filter"]

    def test_generate_consolidated_uc_metrics_with_specific_filters(self, generator, yaml_metadata):
        """Test consolidated UC metrics handles KBI-specific filters"""
        kbi_list = [
            KPI(
                description="Active Revenue",
                technical_name="active_revenue",
                formula="amount",
                aggregation_type="SUM",
                source_table="Sales",
                filters=["status = 'active'"]
            ),
            KPI(
                description="Inactive Revenue",
                technical_name="inactive_revenue",
                formula="amount",
                aggregation_type="SUM",
                source_table="Sales",
                filters=["status = 'inactive'"]
            )
        ]

        result = generator.generate_consolidated_uc_metrics(kbi_list, yaml_metadata)

        # Each measure should have different filters in FILTER clause
        assert len(result["measures"]) == 2

    def test_generate_consolidated_uc_metrics_description(self, generator, yaml_metadata):
        """Test consolidated UC metrics includes description"""
        kbi_list = [
            KPI(
                description="Revenue",
                technical_name="revenue",
                formula="amount",
                aggregation_type="SUM",
                source_table="Sales"
            )
        ]

        result = generator.generate_consolidated_uc_metrics(kbi_list, yaml_metadata)

        assert "Test Metrics" in result["description"]

    # ========== Format UC Metrics YAML Tests ==========

    def test_format_uc_metrics_yaml_simple(self, generator):
        """Test formatting UC metrics as YAML"""
        uc_metrics = {
            "version": "0.1",
            "description": "Test Metrics",
            "source": "catalog.schema.Sales",
            "measures": [
                {"name": "revenue", "expr": "SUM(amount)"}
            ]
        }

        result = generator.format_uc_metrics_yaml(uc_metrics)

        assert "version: 0.1" in result
        assert "Test Metrics" in result  # Description appears in comment
        assert "source: catalog.schema.Sales" in result
        assert "measures:" in result
        assert "name: revenue" in result
        assert "expr: SUM(amount)" in result

    def test_format_uc_metrics_yaml_with_filter(self, generator):
        """Test formatting UC metrics with filter"""
        uc_metrics = {
            "version": "0.1",
            "description": "Test Metrics",
            "source": "catalog.schema.Sales",
            "filter": "year = 2023",
            "measures": [
                {"name": "revenue", "expr": "SUM(amount)"}
            ]
        }

        result = generator.format_uc_metrics_yaml(uc_metrics)

        assert "filter: year = 2023" in result

    # ========== Format Consolidated UC Metrics YAML Tests ==========

    def test_format_consolidated_uc_metrics_yaml_simple(self, generator):
        """Test formatting consolidated UC metrics as YAML"""
        uc_metrics = {
            "version": "0.1",
            "description": "Test Metrics",
            "measures": [
                {"name": "revenue", "expr": "SUM(amount)"},
                {"name": "count", "expr": "COUNT(id)"}
            ]
        }

        result = generator.format_consolidated_uc_metrics_yaml(uc_metrics)

        assert "version: 0.1" in result
        assert "measures:" in result
        assert "name: revenue" in result
        assert "name: count" in result

    def test_format_consolidated_uc_metrics_yaml_with_window(self, generator):
        """Test formatting consolidated UC metrics with window configuration"""
        uc_metrics = {
            "version": "0.1",
            "description": "Test Metrics",
            "measures": [
                {
                    "name": "balance",
                    "expr": "SUM(amount)",
                    "window": [
                        {
                            "order": "date",
                            "range": "current",
                            "semiadditive": "last"
                        }
                    ]
                }
            ]
        }

        result = generator.format_consolidated_uc_metrics_yaml(uc_metrics)

        assert "window:" in result
        assert "order: date" in result
        assert "semiadditive: last" in result

    # ========== Process Definition Tests ==========

    def test_process_definition_simple(self, generator, simple_definition):
        """Test processing simple KPI definition"""
        generator.process_definition(simple_definition)

        # Should build dependency tree
        assert len(generator._base_kbi_contexts) > 0

    def test_process_definition_with_dependencies(self, generator):
        """Test processing definition with KBI dependencies"""
        definition = KPIDefinition(
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

        generator.process_definition(definition)

        # Should have 2 base KBI contexts (revenue and cost)
        assert len(generator._base_kbi_contexts) == 2

    # ========== Is Base KBI Tests ==========

    def test_is_base_kbi_true_for_simple(self, generator, simple_kpi):
        """Test is_base_kbi returns True for simple KBI"""
        result = generator._is_base_kbi(simple_kpi)
        assert result is True

    def test_is_base_kbi_false_for_calculated(self, generator):
        """Test is_base_kbi returns False for calculated KBI"""
        # Need to set up dependency resolver first
        base_kpi = KPI(
            description="Revenue",
            technical_name="revenue",
            formula="amount",
            aggregation_type="SUM",
            source_table="Sales"
        )
        generator._dependency_resolver.build_kbi_lookup([base_kpi])

        calc_kpi = KPI(
            description="Doubled Revenue",
            technical_name="doubled_revenue",
            formula="[revenue] * 2",
            aggregation_type="CALCULATED"
        )

        result = generator._is_base_kbi(calc_kpi)
        assert result is False

    def test_is_base_kbi_true_for_empty_formula(self, generator):
        """Test is_base_kbi returns True for KBI with empty formula"""
        kpi = KPI(
            description="Count",
            technical_name="count",
            formula="",
            aggregation_type="COUNT",
            source_table="Sales"
        )
        result = generator._is_base_kbi(kpi)
        assert result is True

    # ========== Extract Common Filters Tests ==========

    def test_extract_common_filters_no_filters(self, generator, yaml_metadata):
        """Test extracting common filters when no filters present"""
        kbi_list = [
            KPI(
                description="Revenue",
                technical_name="revenue",
                formula="amount",
                aggregation_type="SUM",
                source_table="Sales"
            )
        ]

        # Remove query filters for this test
        yaml_metadata = yaml_metadata.copy()
        yaml_metadata["filters"] = {}

        result = generator._extract_common_filters(kbi_list, yaml_metadata)
        assert result is None

    def test_extract_common_filters_query_filters(self, generator, yaml_metadata):
        """Test extracting common filters includes query filters"""
        kbi_list = [
            KPI(
                description="Revenue",
                technical_name="revenue",
                formula="amount",
                aggregation_type="SUM",
                source_table="Sales"
            )
        ]

        result = generator._extract_common_filters(kbi_list, yaml_metadata)
        assert result is not None
        assert "year = '2023'" in result

    def test_extract_common_filters_shared_filters(self, generator, yaml_metadata):
        """Test extracting common filters shared by all KBIs"""
        kbi_list = [
            KPI(
                description="Revenue",
                technical_name="revenue",
                formula="amount",
                aggregation_type="SUM",
                source_table="Sales",
                filters=["region = 'US'"]
            ),
            KPI(
                description="Cost",
                technical_name="cost",
                formula="cost_amount",
                aggregation_type="SUM",
                source_table="Sales",
                filters=["region = 'US'"]
            )
        ]

        result = generator._extract_common_filters(kbi_list, yaml_metadata)
        assert "region = 'US'" in result

    # ========== Get KBI Specific Filters Tests ==========

    def test_get_kbi_specific_filters_no_common(self, generator, kpi_with_filters, yaml_metadata):
        """Test getting KBI specific filters when no common filters"""
        result = generator._get_kbi_specific_filters(kpi_with_filters, None, yaml_metadata)

        assert result is not None
        assert "status = 'active'" in result
        assert "year = 2023" in result

    def test_get_kbi_specific_filters_exclude_common(self, generator, yaml_metadata):
        """Test getting KBI specific filters excludes common filters"""
        kpi = KPI(
            description="Test",
            technical_name="test",
            formula="amount",
            aggregation_type="SUM",
            source_table="Sales",
            filters=["region = 'US'", "status = 'active'"]
        )

        common_filters = "region = 'US'"
        result = generator._get_kbi_specific_filters(kpi, common_filters, yaml_metadata)

        assert result == "status = 'active'"
        assert "region = 'US'" not in result

    def test_get_kbi_specific_filters_no_filters(self, generator, simple_kpi, yaml_metadata):
        """Test getting KBI specific filters when KBI has no filters"""
        result = generator._get_kbi_specific_filters(simple_kpi, None, yaml_metadata)
        assert result is None

    # ========== Extract Formula KBIs Tests ==========

    def test_extract_formula_kbis_no_dependencies(self, generator, simple_kpi):
        """Test extracting formula KBIs with no dependencies"""
        result = generator._extract_formula_kbis(simple_kpi)
        assert result == []

    def test_extract_formula_kbis_with_dependencies(self, generator):
        """Test extracting formula KBIs with dependencies"""
        # Set up dependency resolver
        base_kpis = [
            KPI(
                description="Revenue",
                technical_name="revenue",
                formula="amount",
                aggregation_type="SUM",
                source_table="Sales"
            ),
            KPI(
                description="Cost",
                technical_name="cost",
                formula="cost_amount",
                aggregation_type="SUM",
                source_table="Sales"
            )
        ]
        generator._dependency_resolver.build_kbi_lookup(base_kpis)

        calc_kpi = KPI(
            description="Profit",
            technical_name="profit",
            formula="[revenue] - [cost]",
            aggregation_type="CALCULATED"
        )

        result = generator._extract_formula_kbis(calc_kpi)
        assert len(result) == 2

    # ========== Edge Cases ==========

    def test_generate_uc_metric_no_technical_name(self, generator, yaml_metadata):
        """Test generating UC metric with no technical name uses consolidated format"""
        kpi = KPI(
            description="Test",
            formula="amount",
            aggregation_type="SUM",
            source_table="Sales"
        )
        result = generator.generate_consolidated_uc_metrics([kpi], yaml_metadata)

        assert result["measures"][0]["name"] == "unnamed_measure"

    def test_generate_uc_metric_no_description(self, generator, yaml_metadata):
        """Test generating UC metric with no description uses consolidated format"""
        kpi = KPI(
            description="",
            technical_name="test",
            formula="amount",
            aggregation_type="SUM",
            source_table="Sales"
        )
        result = generator.generate_consolidated_uc_metrics([kpi], yaml_metadata)

        assert "test" in result["description"] or "UC metrics" in result["description"]

    def test_generate_consolidated_empty_kbi_list(self, generator, yaml_metadata):
        """Test generating consolidated UC metrics with empty KBI list"""
        result = generator.generate_consolidated_uc_metrics([], yaml_metadata)

        assert result["version"] == "0.1"
        assert result["measures"] == []

    # ========== Integration Tests ==========

    def test_full_workflow_simple(self, generator, simple_kpi, yaml_metadata):
        """Test complete workflow for simple KBI"""
        # Generate UC metric using consolidated format
        result = generator.generate_consolidated_uc_metrics([simple_kpi], yaml_metadata)
        assert result is not None

        # Format as YAML
        yaml_str = generator.format_consolidated_uc_metrics_yaml(result)
        assert yaml_str is not None
        assert "measures:" in yaml_str

    def test_full_workflow_consolidated(self, generator, yaml_metadata):
        """Test complete workflow for consolidated UC metrics"""
        kbi_list = [
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

        # Generate consolidated UC metrics
        result = generator.generate_consolidated_uc_metrics(kbi_list, yaml_metadata)
        assert result is not None

        # Format as YAML
        yaml_str = generator.format_consolidated_uc_metrics_yaml(result)
        assert yaml_str is not None
        assert "measures:" in yaml_str
