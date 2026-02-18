"""
Unit tests for DAX Context Tracking

Tests context-aware filter tracking, constant selection, and exception aggregation
for Power BI DAX converter.
"""

import pytest
from src.converters.base.models import KPI
from src.converters.outbound.dax.context import DAXBaseKBIContext, DAXKBIContextCache


class TestDAXBaseKBIContext:
    """Test suite for DAXBaseKBIContext class"""

    def test_context_initialization(self):
        """Test basic context initialization"""
        kbi = KPI(
            description="Total Revenue",
            technical_name="revenue",
            formula="sales_amount",
            source_table="fact_sales",
            aggregation_type="SUM"
        )

        context = DAXBaseKBIContext(kbi=kbi, parent_kbis=None)

        assert context.kbi == kbi
        assert context.parent_kbis == []
        assert context.id == "revenue"

    def test_context_id_generation(self):
        """Test context ID generation with parent chain"""
        # Create KBI hierarchy
        kbi_sales = KPI(
            description="Sales",
            technical_name="sales",
            formula="sales_amount",
            source_table="fact_sales",
            aggregation_type="SUM"
        )

        kbi_filtered = KPI(
            description="Filtered Sales",
            technical_name="filtered_sales",
            formula="[sales]",
            filters=["region = 'EMEA'"],
            aggregation_type="CALCULATED"
        )

        kbi_ytd = KPI(
            description="YTD Sales",
            technical_name="ytd_sales",
            formula="[filtered_sales]",
            filters=["fiscal_year = 2024"],
            aggregation_type="CALCULATED"
        )

        # Context with no parents
        ctx1 = DAXBaseKBIContext(kbi_sales, parent_kbis=[])
        assert ctx1.id == "sales"

        # Context with one parent
        ctx2 = DAXBaseKBIContext(kbi_sales, parent_kbis=[kbi_filtered])
        assert ctx2.id == "sales_filtered_sales"

        # Context with two parents
        ctx3 = DAXBaseKBIContext(kbi_sales, parent_kbis=[kbi_filtered, kbi_ytd])
        assert ctx3.id == "sales_filtered_sales_ytd_sales"

    def test_combined_filters(self):
        """Test filter combination from KBI and parent chain"""
        kbi_base = KPI(
            description="Revenue",
            technical_name="revenue",
            formula="revenue_amount",
            filters=["status = 'ACTIVE'"],
            aggregation_type="SUM"
        )

        kbi_parent1 = KPI(
            description="EMEA Revenue",
            technical_name="emea_revenue",
            formula="[revenue]",
            filters=["region = 'EMEA'"],
            aggregation_type="CALCULATED"
        )

        kbi_parent2 = KPI(
            description="YTD EMEA Revenue",
            technical_name="ytd_emea_revenue",
            formula="[emea_revenue]",
            filters=["fiscal_year = 2024"],
            aggregation_type="CALCULATED"
        )

        context = DAXBaseKBIContext(
            kbi=kbi_base,
            parent_kbis=[kbi_parent1, kbi_parent2]
        )

        filters = context.combined_filters

        # Should have all three filters
        assert len(filters) == 3
        assert "status = 'ACTIVE'" in filters
        assert "region = 'EMEA'" in filters
        assert "fiscal_year = 2024" in filters

    def test_dax_filter_expressions_generation(self):
        """Test DAX FILTER function generation"""
        kbi = KPI(
            description="Revenue",
            technical_name="revenue",
            formula="revenue_amount",
            filters=["status = 'ACTIVE'", "region = 'EMEA'"],
            source_table="FactSales",
            aggregation_type="SUM"
        )

        context = DAXBaseKBIContext(kbi)
        filter_exprs = context.get_dax_filter_expressions("FactSales")

        # Should generate FILTER functions for each condition
        assert len(filter_exprs) == 2
        assert "FILTER(FactSales, status = 'ACTIVE')" in filter_exprs
        assert "FILTER(FactSales, region = 'EMEA')" in filter_exprs

    def test_dax_constant_selection_expressions(self):
        """Test DAX REMOVEFILTERS generation for constant selection"""
        kbi = KPI(
            description="Revenue",
            technical_name="revenue",
            formula="revenue_amount",
            fields_for_constant_selection=["Product", "Region"],
            aggregation_type="SUM"
        )

        context = DAXBaseKBIContext(kbi)
        removefilters = context.get_dax_constant_selection_expressions("FactSales")

        # Should generate REMOVEFILTERS for each field
        assert len(removefilters) == 2
        assert "REMOVEFILTERS(FactSales[Product])" in removefilters
        assert "REMOVEFILTERS(FactSales[Region])" in removefilters

    def test_context_equality(self):
        """Test context equality comparison"""
        kbi = KPI(
            description="Sales",
            technical_name="sales",
            formula="sales_amount",
            aggregation_type="SUM"
        )

        parent = KPI(
            description="Filtered Sales",
            technical_name="filtered",
            formula="[sales]",
            filters=["region = 'EMEA'"],
            aggregation_type="CALCULATED"
        )

        ctx1 = DAXBaseKBIContext(kbi, [parent])
        ctx2 = DAXBaseKBIContext(kbi, [parent])
        ctx3 = DAXBaseKBIContext(kbi, [])  # Different parent chain

        assert ctx1 == ctx2
        assert ctx1 != ctx3
        assert hash(ctx1) == hash(ctx2)

    def test_context_validity_check(self):
        """Test is_valid_for_context class method"""
        # KBI with filters - should be valid
        kbi_with_filters = KPI(
            description="Filtered Sales",
            technical_name="filtered",
            formula="sales",
            filters=["region = 'EMEA'"],
            aggregation_type="SUM"
        )
        assert DAXBaseKBIContext.is_valid_for_context(kbi_with_filters) is True

        # Simple KBI - should be invalid (not needed in context chain)
        kbi_simple = KPI(
            description="Simple Sales",
            technical_name="simple",
            formula="sales",
            aggregation_type="SUM"
        )
        assert DAXBaseKBIContext.is_valid_for_context(kbi_simple) is False

    def test_fields_for_constant_selection(self):
        """Test constant selection field aggregation from context chain"""
        kbi_base = KPI(
            description="Revenue",
            technical_name="revenue",
            formula="revenue_amount",
            fields_for_constant_selection=["Product"],
            aggregation_type="SUM"
        )

        kbi_parent = KPI(
            description="Regional Revenue",
            technical_name="regional_revenue",
            formula="[revenue]",
            fields_for_constant_selection=["Region", "Year"],
            aggregation_type="CALCULATED"
        )

        context = DAXBaseKBIContext(kbi_base, [kbi_parent])
        fields = context.fields_for_constant_selection

        # Should have all three unique fields
        assert len(fields) == 3
        assert "Product" in fields
        assert "Region" in fields
        assert "Year" in fields

    def test_fields_for_exception_aggregation(self):
        """Test exception aggregation field aggregation from context chain"""
        kbi_base = KPI(
            description="Revenue",
            technical_name="revenue",
            formula="revenue_amount",
            fields_for_exception_aggregation=["Customer"],
            aggregation_type="SUM"
        )

        kbi_parent = KPI(
            description="Detailed Revenue",
            technical_name="detailed_revenue",
            formula="[revenue]",
            fields_for_exception_aggregation=["Order"],
            aggregation_type="CALCULATED"
        )

        context = DAXBaseKBIContext(kbi_base, [kbi_parent])
        fields = context.fields_for_exception_aggregation

        # Should have both fields
        assert len(fields) == 2
        assert "Customer" in fields
        assert "Order" in fields


class TestDAXKBIContextCache:
    """Test suite for DAXKBIContextCache class"""

    def test_cache_initialization(self):
        """Test cache initialization"""
        cache = DAXKBIContextCache()
        assert len(cache.get_all_contexts()) == 0

    def test_add_and_retrieve_contexts(self):
        """Test adding and retrieving contexts"""
        cache = DAXKBIContextCache()

        kbi1 = KPI(description="Sales", technical_name="sales", formula="sales_amount", aggregation_type="SUM")
        kbi2 = KPI(description="Revenue", technical_name="revenue", formula="revenue_amount", aggregation_type="SUM")

        ctx1 = DAXBaseKBIContext(kbi1)
        ctx2 = DAXBaseKBIContext(kbi2)

        cache.add_context(ctx1)
        cache.add_context(ctx2)

        all_contexts = cache.get_all_contexts()
        assert len(all_contexts) == 2
        assert ctx1 in all_contexts
        assert ctx2 in all_contexts

    def test_get_contexts_for_kbi(self):
        """Test retrieving contexts for specific KBI"""
        cache = DAXKBIContextCache()

        kbi = KPI(description="Sales", technical_name="sales", formula="sales_amount", aggregation_type="SUM")
        kbi_parent1 = KPI(description="Filtered", technical_name="filtered", formula="[sales]",
                          filters=["region = 'EMEA'"], aggregation_type="CALCULATED")
        kbi_parent2 = KPI(description="YTD", technical_name="ytd", formula="[sales]",
                          filters=["year = 2024"], aggregation_type="CALCULATED")

        ctx1 = DAXBaseKBIContext(kbi, [])
        ctx2 = DAXBaseKBIContext(kbi, [kbi_parent1])
        ctx3 = DAXBaseKBIContext(kbi, [kbi_parent2])

        cache.add_context(ctx1)
        cache.add_context(ctx2)
        cache.add_context(ctx3)

        contexts_for_sales = cache.get_contexts_for_kbi("sales")
        assert len(contexts_for_sales) == 3

    def test_cache_clear(self):
        """Test cache clearing"""
        cache = DAXKBIContextCache()

        kbi = KPI(description="Sales", technical_name="sales", formula="sales_amount", aggregation_type="SUM")
        cache.add_context(DAXBaseKBIContext(kbi))

        assert len(cache.get_all_contexts()) == 1

        cache.clear()

        assert len(cache.get_all_contexts()) == 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
