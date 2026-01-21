"""
Unit tests for converters/common/translators/filters.py

Tests filter resolution logic including variable substitution, query filter expansion,
and DAX formatting for PowerBI measure generation.
"""

import pytest
from src.converters.common.translators.filters import FilterResolver
from src.converters.base.models import KPI, QueryFilter, KPIDefinition


class TestFilterResolver:
    """Tests for FilterResolver class"""

    @pytest.fixture
    def resolver(self):
        """Create FilterResolver instance for testing"""
        return FilterResolver()

    @pytest.fixture
    def simple_definition(self):
        """Simple KPI definition with default variables"""
        return KPIDefinition(
            description="Simple Definition",
            technical_name="simple_def",
            default_variables={
                'year': 2024,
                'region': 'US',
                'status': 'active'
            },
            kpis=[]
        )

    @pytest.fixture
    def query_filter_definition(self):
        """KPI definition with query filters"""
        return KPIDefinition(
            description="With Query Filters",
            technical_name="query_filter_def",
            default_variables={'year': 2024},
            query_filters=[
                QueryFilter(name='active_only', expression='status = "active"'),
                QueryFilter(name='current_year', expression='year = $var_year')
            ],
            kpis=[]
        )

    @pytest.fixture
    def kpi_with_string_filters(self):
        """KPI with simple string filters"""
        return KPI(
            description="Sales",
            technical_name="sales",
            formula="SUM(sales.amount)",
            filters=['region = "US"', 'year = 2024']
        )

    @pytest.fixture
    def kpi_with_variable_filters(self):
        """KPI with filters containing variables"""
        return KPI(
            description="Sales",
            technical_name="sales",
            formula="SUM(sales.amount)",
            filters=['region = $var_region', 'year = $var_year']
        )

    @pytest.fixture
    def kpi_with_query_filter(self):
        """KPI with query filter reference"""
        return KPI(
            description="Sales",
            technical_name="sales",
            formula="SUM(sales.amount)",
            filters=['$query_filter', 'category = "product"']
        )

    @pytest.fixture
    def kpi_with_complex_filters(self):
        """KPI with complex filter dictionaries"""
        return KPI(
            description="Sales",
            technical_name="sales",
            formula="SUM(sales.amount)",
            filters=[
                {'field': 'bic_region', 'operator': '=', 'value': 'US'},
                {'field': 'bic_year', 'operator': '>=', 'value': 2024}
            ]
        )

    @pytest.fixture
    def kpi_no_filters(self):
        """KPI with no filters"""
        return KPI(
            description="Sales",
            technical_name="sales",
            formula="SUM(sales.amount)"
            # filters not specified - uses default
        )

    # ========== Initialization Tests ==========

    def test_resolver_initialization(self, resolver):
        """Test FilterResolver initializes with compiled patterns"""
        assert resolver.variable_pattern is not None
        assert resolver.query_filter_pattern is not None

    def test_variable_pattern_compilation(self, resolver):
        """Test variable pattern correctly identifies variables"""
        test_string = "year = $var_year AND region = $var_region"
        matches = resolver.variable_pattern.findall(test_string)
        assert len(matches) == 2
        assert 'year' in matches
        assert 'region' in matches

    def test_query_filter_pattern_compilation(self, resolver):
        """Test query filter pattern identifies $query_filter"""
        test_string = "WHERE $query_filter AND status = 'active'"
        match = resolver.query_filter_pattern.search(test_string)
        assert match is not None

    # ========== resolve_filters Tests ==========

    def test_resolve_filters_none(self, resolver, kpi_no_filters, simple_definition):
        """Test resolve_filters returns empty list for None filters"""
        result = resolver.resolve_filters(kpi_no_filters, simple_definition)
        assert result == []

    def test_resolve_filters_simple_strings(self, resolver, kpi_with_string_filters, simple_definition):
        """Test resolve_filters with simple string filters"""
        result = resolver.resolve_filters(kpi_with_string_filters, simple_definition)

        assert len(result) == 2
        assert 'region = "US"' in result
        assert 'year = 2024' in result

    def test_resolve_filters_with_variables(self, resolver, kpi_with_variable_filters, simple_definition):
        """Test resolve_filters substitutes variables"""
        result = resolver.resolve_filters(kpi_with_variable_filters, simple_definition)

        assert len(result) == 2
        assert "region = 'US'" in result
        assert 'year = 2024' in result

    def test_resolve_filters_with_query_filter(self, resolver, kpi_with_query_filter, query_filter_definition):
        """Test resolve_filters expands query filter reference"""
        result = resolver.resolve_filters(kpi_with_query_filter, query_filter_definition)

        assert len(result) == 2
        # Query filter should be expanded
        assert 'status = "active"' in result[0]
        assert 'year = 2024' in result[0]
        # Other filter preserved
        assert 'category = "product"' in result

    def test_resolve_filters_with_complex_dicts(self, resolver, kpi_with_complex_filters, simple_definition):
        """Test resolve_filters handles complex filter dictionaries"""
        result = resolver.resolve_filters(kpi_with_complex_filters, simple_definition)

        assert len(result) == 2
        # Check DAX formatting
        assert 'Region' in result[0]
        assert 'US' in result[0]
        assert 'Year' in result[1]
        assert '2024' in result[1]

    def test_resolve_filters_mixed_types(self, resolver, simple_definition):
        """Test resolve_filters handles mixed filter types"""
        kpi = KPI(
            description="Sales",
            technical_name="sales",
            formula="SUM(sales.amount)",
            filters=[
                'region = "US"',
                {'field': 'bic_year', 'operator': '=', 'value': 2024}
            ]
        )

        result = resolver.resolve_filters(kpi, simple_definition)
        assert len(result) == 2

    # ========== _resolve_variables Tests ==========

    def test_resolve_variables_string_value(self, resolver):
        """Test variable resolution with string values"""
        filter_text = "region = $var_region"
        variables = {'region': 'US'}

        result = resolver._resolve_variables(filter_text, variables)
        assert result == "region = 'US'"

    def test_resolve_variables_numeric_value(self, resolver):
        """Test variable resolution with numeric values"""
        filter_text = "year = $var_year"
        variables = {'year': 2024}

        result = resolver._resolve_variables(filter_text, variables)
        assert result == "year = 2024"

    def test_resolve_variables_list_value(self, resolver):
        """Test variable resolution with list values (IN clause)"""
        filter_text = "region IN $var_regions"
        variables = {'regions': ['US', 'UK', 'CA']}

        result = resolver._resolve_variables(filter_text, variables)
        assert result == "region IN ('US', 'UK', 'CA')"

    def test_resolve_variables_list_numeric(self, resolver):
        """Test variable resolution with numeric list"""
        filter_text = "year IN $var_years"
        variables = {'years': [2023, 2024, 2025]}

        result = resolver._resolve_variables(filter_text, variables)
        assert result == "year IN (2023, 2024, 2025)"

    def test_resolve_variables_already_quoted(self, resolver):
        """Test variable resolution doesn't double-quote already quoted values"""
        filter_text = "'$var_region'"
        variables = {'region': 'US'}

        result = resolver._resolve_variables(filter_text, variables)
        # Should not add extra quotes since original had quotes
        assert result == "'US'"

    def test_resolve_variables_not_found(self, resolver):
        """Test variable resolution leaves unknown variables unchanged"""
        filter_text = "region = $var_unknown"
        variables = {'region': 'US'}

        result = resolver._resolve_variables(filter_text, variables)
        assert result == "region = $var_unknown"

    def test_resolve_variables_multiple(self, resolver):
        """Test variable resolution with multiple variables in one filter"""
        filter_text = "year = $var_year AND region = $var_region"
        variables = {'year': 2024, 'region': 'US'}

        result = resolver._resolve_variables(filter_text, variables)
        assert 'year = 2024' in result
        assert "region = 'US'" in result

    # ========== _resolve_query_filters Tests ==========

    def test_resolve_query_filters_single_filter(self, resolver):
        """Test query filter resolution with single filter"""
        filter_text = "$query_filter"
        query_filters = [QueryFilter(name='active', expression='status = "active"')]

        result = resolver._resolve_query_filters(filter_text, query_filters)
        assert result == '(status = "active")'

    def test_resolve_query_filters_multiple(self, resolver):
        """Test query filter resolution with multiple filters"""
        filter_text = "$query_filter"
        query_filters = [
            QueryFilter(name='active', expression='status = "active"'),
            QueryFilter(name='current_year', expression='year = 2024')
        ]

        result = resolver._resolve_query_filters(filter_text, query_filters)
        assert 'status = "active"' in result
        assert 'year = 2024' in result
        assert ' AND ' in result

    def test_resolve_query_filters_with_variables(self, resolver):
        """Test query filter resolution with variable substitution"""
        filter_text = "$query_filter"
        query_filters = [QueryFilter(name='year_filter', expression='year = $var_year')]
        variables = {'year': 2024}

        result = resolver._resolve_query_filters(filter_text, query_filters, variables)
        assert 'year = 2024' in result

    def test_resolve_query_filters_empty_list(self, resolver):
        """Test query filter resolution with empty filter list"""
        filter_text = "$query_filter"
        query_filters = []

        result = resolver._resolve_query_filters(filter_text, query_filters)
        assert result == "1=1"

    def test_resolve_query_filters_no_reference(self, resolver):
        """Test query filter resolution when no $query_filter in text"""
        filter_text = "region = 'US'"
        query_filters = [QueryFilter(name='active', expression='status = "active"')]

        result = resolver._resolve_query_filters(filter_text, query_filters)
        assert result == "region = 'US'"

    # ========== _resolve_complex_filter Tests ==========

    def test_resolve_complex_filter_equals(self, resolver, simple_definition):
        """Test complex filter resolution with equals operator"""
        filter_dict = {'field': 'bic_region', 'operator': '=', 'value': 'US'}

        result = resolver._resolve_complex_filter(filter_dict, simple_definition)
        assert 'Region' in result
        assert 'bic_region' in result
        assert 'US' in result
        assert '=' in result

    def test_resolve_complex_filter_not_equals(self, resolver, simple_definition):
        """Test complex filter resolution with not equals operator"""
        filter_dict = {'field': 'bic_status', 'operator': '!=', 'value': 'inactive'}

        result = resolver._resolve_complex_filter(filter_dict, simple_definition)
        assert 'Status' in result
        assert '<>' in result
        assert 'inactive' in result

    def test_resolve_complex_filter_greater_than(self, resolver, simple_definition):
        """Test complex filter resolution with greater than operator"""
        filter_dict = {'field': 'bic_amount', 'operator': '>', 'value': 1000}

        result = resolver._resolve_complex_filter(filter_dict, simple_definition)
        assert 'Amount' in result
        assert '>' in result
        assert '1000' in result

    def test_resolve_complex_filter_in_operator(self, resolver, simple_definition):
        """Test complex filter resolution with IN operator"""
        filter_dict = {'field': 'bic_region', 'operator': 'IN', 'value': ['US', 'UK', 'CA']}

        result = resolver._resolve_complex_filter(filter_dict, simple_definition)
        assert 'Region' in result
        assert 'IN' in result
        assert 'US' in result
        assert 'UK' in result
        assert 'CA' in result

    def test_resolve_complex_filter_with_variable(self, resolver, simple_definition):
        """Test complex filter resolution with variable in value"""
        filter_dict = {'field': 'bic_region', 'operator': '=', 'value': '$var_region'}

        result = resolver._resolve_complex_filter(filter_dict, simple_definition)
        assert "'US'" in result or "US" in result

    # ========== _format_dax_filter Tests ==========

    def test_format_dax_filter_equals_string(self, resolver):
        """Test DAX filter formatting for equals with string value"""
        result = resolver._format_dax_filter('bic_region', '=', 'US')

        assert 'Region' in result
        assert 'bic_region' in result
        assert '=' in result
        assert '"US"' in result

    def test_format_dax_filter_equals_numeric(self, resolver):
        """Test DAX filter formatting for equals with numeric value"""
        result = resolver._format_dax_filter('bic_amount', '=', 1000)

        assert 'Amount' in result
        assert '= 1000' in result

    def test_format_dax_filter_not_equals_string(self, resolver):
        """Test DAX filter formatting for not equals with string"""
        result = resolver._format_dax_filter('bic_status', '!=', 'inactive')

        assert 'Status' in result
        assert '<>' in result
        assert '"inactive"' in result

    def test_format_dax_filter_not_equals_numeric(self, resolver):
        """Test DAX filter formatting for not equals with numeric"""
        result = resolver._format_dax_filter('bic_value', '!=', 0)

        assert 'Value' in result
        assert '<> 0' in result

    def test_format_dax_filter_greater_than(self, resolver):
        """Test DAX filter formatting for greater than"""
        result = resolver._format_dax_filter('bic_amount', '>', 500)

        assert 'Amount' in result
        assert '> 500' in result

    def test_format_dax_filter_less_than(self, resolver):
        """Test DAX filter formatting for less than"""
        result = resolver._format_dax_filter('bic_quantity', '<', 100)

        assert 'Quantity' in result
        assert '< 100' in result

    def test_format_dax_filter_greater_equals(self, resolver):
        """Test DAX filter formatting for greater than or equals"""
        result = resolver._format_dax_filter('bic_year', '>=', 2024)

        assert 'Year' in result
        assert '>= 2024' in result

    def test_format_dax_filter_less_equals(self, resolver):
        """Test DAX filter formatting for less than or equals"""
        result = resolver._format_dax_filter('bic_age', '<=', 65)

        assert 'Age' in result
        assert '<= 65' in result

    def test_format_dax_filter_in_string_list(self, resolver):
        """Test DAX filter formatting for IN with string list"""
        result = resolver._format_dax_filter('bic_region', 'IN', ['US', 'UK', 'CA'])

        assert 'Region' in result
        assert 'IN' in result
        assert '"US"' in result
        assert '"UK"' in result
        assert '"CA"' in result

    def test_format_dax_filter_in_numeric_list(self, resolver):
        """Test DAX filter formatting for IN with numeric list"""
        result = resolver._format_dax_filter('bic_year', 'IN', [2023, 2024, 2025])

        assert 'Year' in result
        assert 'IN' in result
        assert '2023' in result
        assert '2024' in result

    def test_format_dax_filter_field_name_cleaning(self, resolver):
        """Test DAX filter cleans field names (removes bic_, converts underscores)"""
        result = resolver._format_dax_filter('bic_customer_name', '=', 'Acme')

        # Should clean up to "Customer Name"
        assert 'Customer Name' in result or 'CustomerName' in result
        assert 'bic_customer_name' in result  # Original field name in brackets

    # ========== combine_filters Tests ==========

    def test_combine_filters_empty_list(self, resolver):
        """Test combine_filters with empty list returns empty string"""
        result = resolver.combine_filters([])
        assert result == ""

    def test_combine_filters_single_filter(self, resolver):
        """Test combine_filters with single filter returns filter as-is"""
        filters = ["region = 'US'"]
        result = resolver.combine_filters(filters)
        assert result == "region = 'US'"

    def test_combine_filters_multiple_and(self, resolver):
        """Test combine_filters with multiple filters using AND"""
        filters = ["region = 'US'", "year = 2024", "status = 'active'"]
        result = resolver.combine_filters(filters)

        assert "region = 'US'" in result
        assert "year = 2024" in result
        assert "status = 'active'" in result
        assert result.count(' AND ') == 2

    def test_combine_filters_multiple_or(self, resolver):
        """Test combine_filters with multiple filters using OR"""
        filters = ["region = 'US'", "region = 'UK'"]
        result = resolver.combine_filters(filters, logical_operator="OR")

        assert "region = 'US'" in result
        assert "region = 'UK'" in result
        assert ' OR ' in result

    def test_combine_filters_parentheses(self, resolver):
        """Test combine_filters wraps each filter in parentheses"""
        filters = ["a = 1", "b = 2"]
        result = resolver.combine_filters(filters)

        assert '(a = 1)' in result
        assert '(b = 2)' in result

    def test_combine_filters_preserves_complex_expressions(self, resolver):
        """Test combine_filters preserves complex filter expressions"""
        filters = ["(a = 1 OR a = 2)", "(b > 3 AND b < 10)"]
        result = resolver.combine_filters(filters)

        assert '((a = 1 OR a = 2))' in result
        assert '((b > 3 AND b < 10))' in result

    # ========== Integration Tests ==========

    def test_full_workflow_simple(self, resolver, kpi_with_string_filters, simple_definition):
        """Test complete workflow with simple filters"""
        # Resolve filters
        resolved = resolver.resolve_filters(kpi_with_string_filters, simple_definition)
        assert len(resolved) == 2

        # Combine filters
        combined = resolver.combine_filters(resolved)
        assert 'region = "US"' in combined
        assert 'year = 2024' in combined
        assert ' AND ' in combined

    def test_full_workflow_with_variables(self, resolver, kpi_with_variable_filters, simple_definition):
        """Test complete workflow with variable substitution"""
        # Resolve filters
        resolved = resolver.resolve_filters(kpi_with_variable_filters, simple_definition)

        # Variables should be substituted
        assert any('US' in f for f in resolved)
        assert any('2024' in str(f) for f in resolved)

        # Combine filters
        combined = resolver.combine_filters(resolved)
        assert 'US' in combined
        assert '2024' in combined

    def test_full_workflow_with_query_filters(self, resolver, kpi_with_query_filter, query_filter_definition):
        """Test complete workflow with query filter expansion"""
        # Resolve filters
        resolved = resolver.resolve_filters(kpi_with_query_filter, query_filter_definition)

        # Query filter should be expanded
        assert any('status = "active"' in f for f in resolved)
        assert any('year = 2024' in f for f in resolved)

        # Combine filters
        combined = resolver.combine_filters(resolved)
        assert 'status' in combined
        assert 'category' in combined

    def test_full_workflow_complex_filters(self, resolver, kpi_with_complex_filters, simple_definition):
        """Test complete workflow with complex DAX filter formatting"""
        # Resolve filters
        resolved = resolver.resolve_filters(kpi_with_complex_filters, simple_definition)

        # Should have DAX-formatted filters
        assert len(resolved) == 2

        # Combine filters
        combined = resolver.combine_filters(resolved)
        assert 'Region' in combined
        assert 'Year' in combined

    def test_edge_case_empty_variables(self, resolver, kpi_with_variable_filters):
        """Test edge case with empty variables dictionary"""
        definition = KPIDefinition(
            description="Empty Vars",
            technical_name="empty_vars",
            default_variables={},
            kpis=[]
        )

        # Should not crash, but variables won't be resolved
        result = resolver.resolve_filters(kpi_with_variable_filters, definition)
        assert len(result) == 2
        # Variables should remain unresolved
        assert any('$var_' in f for f in result)

    def test_edge_case_empty_query_filters(self, resolver, kpi_with_query_filter):
        """Test edge case with empty query filters list"""
        definition = KPIDefinition(
            description="No Query Filters",
            technical_name="no_qf",
            query_filters=[],
            kpis=[]
        )

        result = resolver.resolve_filters(kpi_with_query_filter, definition)
        # $query_filter should be replaced with 1=1
        assert any('1=1' in f for f in result)

    def test_error_handling_invalid_filter_dict(self, resolver, simple_definition):
        """Test error handling for invalid filter dictionary"""
        kpi = KPI(
            description="Sales",
            technical_name="sales",
            formula="SUM(sales.amount)",
            filters=[{'invalid': 'structure'}]
        )

        # Should not crash, returns string representation
        result = resolver.resolve_filters(kpi, simple_definition)
        assert len(result) == 1
