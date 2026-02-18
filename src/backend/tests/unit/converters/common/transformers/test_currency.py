"""
Unit tests for converters/common/transformers/currency.py

Tests currency conversion logic for SQL and DAX measure generation.
"""

import pytest
from src.converters.common.transformers.currency import CurrencyConverter
from src.converters.base.models import KPI


class TestCurrencyConverter:
    """Tests for CurrencyConverter class"""

    @pytest.fixture
    def converter(self):
        """Create CurrencyConverter instance for testing"""
        return CurrencyConverter()

    @pytest.fixture
    def kpi_with_fixed_currency(self):
        """KPI with fixed source currency"""
        return KPI(
            description="USD Sales",
            technical_name="usd_sales",
            formula="SUM(sales.amount)",
            fixed_currency="USD",
            target_currency="EUR"
        )

    @pytest.fixture
    def kpi_with_dynamic_currency(self):
        """KPI with dynamic currency from column"""
        return KPI(
            description="Multi-currency Sales",
            technical_name="multi_sales",
            formula="SUM(sales.amount)",
            source_table="sales",
            currency_column="source_currency",
            target_currency="EUR"
        )

    @pytest.fixture
    def kpi_no_currency(self):
        """KPI without currency conversion"""
        return KPI(
            description="Simple Sales",
            technical_name="simple_sales",
            formula="SUM(sales.amount)"
        )

    # ========== Initialization Tests ==========

    def test_converter_initialization(self, converter):
        """Test CurrencyConverter initializes with default exchange rate table"""
        assert converter.exchange_rate_table == "ExchangeRates"

    def test_supported_currencies_defined(self, converter):
        """Test supported currencies are defined"""
        assert len(converter.SUPPORTED_CURRENCIES) > 0
        assert "USD" in converter.SUPPORTED_CURRENCIES
        assert "EUR" in converter.SUPPORTED_CURRENCIES
        assert "GBP" in converter.SUPPORTED_CURRENCIES

    # ========== should_convert_currency Tests ==========

    def test_should_convert_currency_with_fixed_currency(self, converter, kpi_with_fixed_currency):
        """Test currency conversion is needed for KPI with fixed currency and target"""
        assert converter.should_convert_currency(kpi_with_fixed_currency) is True

    def test_should_convert_currency_with_dynamic_currency(self, converter, kpi_with_dynamic_currency):
        """Test currency conversion is needed for KPI with currency column and target"""
        assert converter.should_convert_currency(kpi_with_dynamic_currency) is True

    def test_should_convert_currency_no_source(self, converter):
        """Test no conversion when source currency is missing"""
        kpi = KPI(
            description="Sales",
            technical_name="sales",
            formula="SUM(amount)",
            target_currency="EUR"  # Has target but no source
        )

        assert converter.should_convert_currency(kpi) is False

    def test_should_convert_currency_no_target(self, converter):
        """Test no conversion when target currency is missing"""
        kpi = KPI(
            description="Sales",
            technical_name="sales",
            formula="SUM(amount)",
            fixed_currency="USD"  # Has source but no target
        )

        assert converter.should_convert_currency(kpi) is False

    def test_should_convert_currency_no_currency_info(self, converter, kpi_no_currency):
        """Test no conversion when no currency information"""
        assert converter.should_convert_currency(kpi_no_currency) is False

    # ========== get_kbi_currency_recursive Tests ==========

    def test_get_kbi_currency_recursive_fixed(self, converter, kpi_with_fixed_currency):
        """Test getting fixed currency from KPI"""
        currency_type, currency_value = converter.get_kbi_currency_recursive(kpi_with_fixed_currency)

        assert currency_type == "fixed"
        assert currency_value == "USD"

    def test_get_kbi_currency_recursive_dynamic(self, converter, kpi_with_dynamic_currency):
        """Test getting dynamic currency from KPI"""
        currency_type, currency_value = converter.get_kbi_currency_recursive(kpi_with_dynamic_currency)

        assert currency_type == "dynamic"
        assert currency_value == "source_currency"

    def test_get_kbi_currency_recursive_no_currency(self, converter, kpi_no_currency):
        """Test getting currency from KPI with no currency info"""
        currency_type, currency_value = converter.get_kbi_currency_recursive(kpi_no_currency)

        assert currency_type is None
        assert currency_value is None

    def test_get_kbi_currency_recursive_from_dependency(self, converter):
        """Test getting currency from KPI formula dependency"""
        # Create base KPI with currency
        base_kpi = KPI(
            description="Base Sales",
            technical_name="base_sales",
            formula="SUM(sales.amount)",
            fixed_currency="USD",
            target_currency="EUR"
        )

        # Create derived KPI that references base KPI
        derived_kpi = KPI(
            description="Derived Sales",
            technical_name="derived_sales",
            formula="[base_sales] * 1.1"  # References base_sales
            # No direct currency info
        )

        kpi_lookup = {
            "base_sales": base_kpi
        }

        currency_type, currency_value = converter.get_kbi_currency_recursive(derived_kpi, kpi_lookup)

        # Should find currency from dependency
        assert currency_type == "fixed"
        assert currency_value == "USD"

    def test_get_kbi_currency_recursive_multiple_dependencies(self, converter):
        """Test getting currency from first dependency with currency info"""
        kpi_with_currency = KPI(
            description="KPI 1",
            technical_name="kpi1",
            formula="SUM(amount)",
            fixed_currency="GBP"
        )

        kpi_no_currency = KPI(
            description="KPI 2",
            technical_name="kpi2",
            formula="SUM(amount)"
        )

        derived = KPI(
            description="Derived",
            technical_name="derived",
            formula="[kpi2] + [kpi1]"
        )

        kpi_lookup = {
            "kpi1": kpi_with_currency,
            "kpi2": kpi_no_currency
        }

        currency_type, currency_value = converter.get_kbi_currency_recursive(derived, kpi_lookup)

        # Should find currency from kpi1
        assert currency_type == "fixed"
        assert currency_value == "GBP"

    def test_get_kbi_currency_recursive_priority_direct_over_dependency(self, converter):
        """Test direct currency info takes priority over dependencies"""
        base_kpi = KPI(
            description="Base",
            technical_name="base",
            formula="SUM(amount)",
            fixed_currency="USD"
        )

        # This KPI has both direct currency and a dependency with different currency
        kpi = KPI(
            description="Derived",
            technical_name="derived",
            formula="[base] * 2",
            currency_column="my_currency"  # Direct dynamic currency
        )

        kpi_lookup = {"base": base_kpi}

        currency_type, currency_value = converter.get_kbi_currency_recursive(kpi, kpi_lookup)

        # Should use direct currency_column, not dependency
        assert currency_type == "dynamic"
        assert currency_value == "my_currency"

    # ========== generate_sql_conversion Tests ==========

    def test_generate_sql_conversion_fixed_currency(self, converter):
        """Test SQL generation for fixed currency conversion"""
        sql = converter.generate_sql_conversion(
            value_expression="sales_amount",
            source_currency="USD",
            target_currency="EUR",
            currency_type="fixed"
        )

        assert "sales_amount" in sql
        assert "ExchangeRates" in sql
        assert "USD" in sql
        assert "EUR" in sql
        assert "SELECT rate" in sql
        assert "from_currency" in sql
        assert "to_currency" in sql

    def test_generate_sql_conversion_fixed_with_custom_table(self, converter):
        """Test SQL generation with custom exchange rate table"""
        sql = converter.generate_sql_conversion(
            value_expression="amount",
            source_currency="GBP",
            target_currency="USD",
            currency_type="fixed",
            exchange_rate_table="CustomRates"
        )

        assert "CustomRates" in sql
        assert "ExchangeRates" not in sql
        assert "GBP" in sql
        assert "USD" in sql

    def test_generate_sql_conversion_dynamic_currency(self, converter):
        """Test SQL generation for dynamic currency conversion"""
        sql = converter.generate_sql_conversion(
            value_expression="sales_amount",
            source_currency=None,
            target_currency="EUR",
            currency_type="dynamic",
            currency_column="source_curr"
        )

        # Dynamic conversion returns a placeholder for query-level handling
        assert "__CURRENCY_CONVERSION__" in sql
        assert "sales_amount" in sql
        assert "source_curr" in sql
        assert "EUR" in sql

    def test_generate_sql_conversion_includes_date_filter(self, converter):
        """Test SQL includes effective date filter for latest rate"""
        sql = converter.generate_sql_conversion(
            value_expression="amount",
            source_currency="USD",
            target_currency="EUR",
            currency_type="fixed"
        )

        assert "effective_date" in sql
        assert "CURRENT_DATE" in sql
        assert "ORDER BY effective_date DESC" in sql
        assert "LIMIT 1" in sql

    # ========== generate_dax_conversion Tests ==========

    def test_generate_dax_conversion_fixed_currency(self, converter):
        """Test DAX generation for fixed currency conversion"""
        dax = converter.generate_dax_conversion(
            value_expression="[Amount]",
            source_currency="USD",
            target_currency="EUR",
            currency_type="fixed"
        )

        assert "[Amount]" in dax
        assert "LOOKUPVALUE" in dax
        assert "ExchangeRates" in dax or "ExchangeRates[Rate]" in dax
        assert "USD" in dax
        assert "EUR" in dax
        assert "FromCurrency" in dax
        assert "ToCurrency" in dax

    def test_generate_dax_conversion_fixed_with_custom_table(self, converter):
        """Test DAX generation with custom exchange rate table"""
        dax = converter.generate_dax_conversion(
            value_expression="[Sales]",
            source_currency="GBP",
            target_currency="USD",
            currency_type="fixed",
            exchange_rate_table="CustomRates"
        )

        assert "CustomRates" in dax
        assert "ExchangeRates" not in dax
        assert "GBP" in dax
        assert "USD" in dax

    def test_generate_dax_conversion_dynamic_currency(self, converter):
        """Test DAX generation for dynamic currency conversion"""
        dax = converter.generate_dax_conversion(
            value_expression="[Amount]",
            source_currency=None,
            target_currency="EUR",
            currency_type="dynamic",
            currency_column="SourceCurrency"
        )

        assert "[Amount]" in dax
        assert "LOOKUPVALUE" in dax
        assert "[SourceCurrency]" in dax
        assert "EUR" in dax
        assert "FromCurrency" in dax

    def test_generate_dax_conversion_structure(self, converter):
        """Test DAX conversion has proper multiplication structure"""
        dax = converter.generate_dax_conversion(
            value_expression="[Value]",
            source_currency="USD",
            target_currency="EUR",
            currency_type="fixed"
        )

        # Should multiply value by lookup
        assert "*" in dax
        assert dax.count("LOOKUPVALUE") >= 1

    # ========== get_required_joins Tests ==========

    def test_get_required_joins_no_currency_kpis(self, converter):
        """Test no joins needed when no KPIs have currency conversion"""
        kpis = [
            KPI(description="KPI 1", technical_name="kpi1", formula="SUM(a)"),
            KPI(description="KPI 2", technical_name="kpi2", formula="SUM(b)")
        ]

        joins = converter.get_required_joins(kpis)

        assert joins == []

    def test_get_required_joins_fixed_currency(self, converter, kpi_with_fixed_currency):
        """Test no joins needed for fixed currency (uses subquery)"""
        joins = converter.get_required_joins([kpi_with_fixed_currency])

        # Fixed currency doesn't need JOIN (uses subquery in SELECT)
        assert joins == []

    def test_get_required_joins_dynamic_currency(self, converter, kpi_with_dynamic_currency):
        """Test JOIN is generated for dynamic currency KPI"""
        joins = converter.get_required_joins([kpi_with_dynamic_currency])

        assert len(joins) == 1

        join = joins[0]
        assert "LEFT JOIN" in join
        assert "ExchangeRates" in join
        assert "source_currency" in join
        assert "EUR" in join
        assert "effective_date" in join

    def test_get_required_joins_multiple_dynamic_kpis(self, converter):
        """Test multiple JOINs generated for multiple dynamic currency KPIs"""
        kpis = [
            KPI(
                description="Sales EUR",
                technical_name="sales_eur",
                formula="SUM(sales.amount)",
                source_table="sales",
                currency_column="curr_code",
                target_currency="EUR"
            ),
            KPI(
                description="Cost USD",
                technical_name="cost_usd",
                formula="SUM(cost.amount)",
                source_table="cost",
                currency_column="currency",
                target_currency="USD"
            )
        ]

        joins = converter.get_required_joins(kpis)

        assert len(joins) == 2
        assert all("LEFT JOIN" in join for join in joins)

    def test_get_required_joins_with_custom_table(self, converter, kpi_with_dynamic_currency):
        """Test JOIN uses custom exchange rate table"""
        joins = converter.get_required_joins(
            [kpi_with_dynamic_currency],
            exchange_rate_table="CustomRates"
        )

        assert len(joins) == 1
        assert "CustomRates" in joins[0]
        assert "ExchangeRates" not in joins[0]

    def test_get_required_joins_includes_latest_rate_logic(self, converter, kpi_with_dynamic_currency):
        """Test JOIN includes logic to get latest exchange rate"""
        joins = converter.get_required_joins([kpi_with_dynamic_currency])

        join = joins[0]
        assert "MAX(effective_date)" in join
        assert "CURRENT_DATE" in join
        # Should have subquery (one SELECT) to find most recent rate
        assert "SELECT MAX(effective_date)" in join
        assert join.count("SELECT") >= 1

    def test_get_required_joins_mixed_currency_types(self, converter, kpi_with_fixed_currency, kpi_with_dynamic_currency):
        """Test JOINs generated only for dynamic currency KPIs"""
        kpis = [kpi_with_fixed_currency, kpi_with_dynamic_currency]

        joins = converter.get_required_joins(kpis)

        # Only 1 JOIN for the dynamic currency KPI
        assert len(joins) == 1
        assert "source_currency" in joins[0]  # The dynamic one

    # ========== Integration Tests ==========

    def test_full_conversion_workflow_fixed(self, converter, kpi_with_fixed_currency):
        """Test complete workflow for fixed currency conversion"""
        # Check if conversion needed
        assert converter.should_convert_currency(kpi_with_fixed_currency) is True

        # Get currency info
        currency_type, currency_value = converter.get_kbi_currency_recursive(kpi_with_fixed_currency)
        assert currency_type == "fixed"
        assert currency_value == "USD"

        # Generate SQL
        sql = converter.generate_sql_conversion(
            "SUM(amount)",
            currency_value,
            kpi_with_fixed_currency.target_currency,
            currency_type
        )
        assert "USD" in sql
        assert "EUR" in sql

        # Generate DAX
        dax = converter.generate_dax_conversion(
            "[Amount]",
            currency_value,
            kpi_with_fixed_currency.target_currency,
            currency_type
        )
        assert "USD" in dax
        assert "EUR" in dax

        # Check JOINs (should be none for fixed)
        joins = converter.get_required_joins([kpi_with_fixed_currency])
        assert joins == []

    def test_full_conversion_workflow_dynamic(self, converter, kpi_with_dynamic_currency):
        """Test complete workflow for dynamic currency conversion"""
        # Check if conversion needed
        assert converter.should_convert_currency(kpi_with_dynamic_currency) is True

        # Get currency info
        currency_type, currency_value = converter.get_kbi_currency_recursive(kpi_with_dynamic_currency)
        assert currency_type == "dynamic"
        assert currency_value == "source_currency"

        # Generate SQL (placeholder)
        sql = converter.generate_sql_conversion(
            "SUM(amount)",
            None,
            kpi_with_dynamic_currency.target_currency,
            currency_type,
            currency_column=currency_value
        )
        assert "__CURRENCY_CONVERSION__" in sql

        # Generate DAX
        dax = converter.generate_dax_conversion(
            "[Amount]",
            None,
            kpi_with_dynamic_currency.target_currency,
            currency_type,
            currency_column=currency_value
        )
        assert "source_currency" in dax
        assert "EUR" in dax

        # Check JOINs (should have one)
        joins = converter.get_required_joins([kpi_with_dynamic_currency])
        assert len(joins) == 1
