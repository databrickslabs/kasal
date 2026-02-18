"""
Unit tests for UC Metrics Context Tracking

Tests context-aware filter tracking, constant selection, and exception aggregation
for Unity Catalog Metrics converter.
"""

import pytest
from src.converters.base.models import KPI
from src.converters.outbound.uc_metrics.context import UCBaseKBIContext, UCKBIContextCache


class TestUCBaseKBIContext:
    """Test suite for UCBaseKBIContext class"""

    def test_context_initialization(self):
        """Test basic context initialization"""
        kbi = KPI(
            description="Total Revenue",
            technical_name="revenue",
            formula="sales_amount",
            source_table="fact_sales",
            aggregation_type="SUM"
        )

        context = UCBaseKBIContext(kbi=kbi, parent_kbis=None)

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
        ctx1 = UCBaseKBIContext(kbi_sales, parent_kbis=[])
        assert ctx1.id == "sales"

        # Context with one parent
        ctx2 = UCBaseKBIContext(kbi_sales, parent_kbis=[kbi_filtered])
        assert ctx2.id == "sales_filtered_sales"

        # Context with two parents
        ctx3 = UCBaseKBIContext(kbi_sales, parent_kbis=[kbi_filtered, kbi_ytd])
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

        context = UCBaseKBIContext(
            kbi=kbi_base,
            parent_kbis=[kbi_parent1, kbi_parent2]
        )

        filters = context.combined_filters

        # Should have all three filters
        assert len(filters) == 3
        assert "status = 'ACTIVE'" in filters
        assert "region = 'EMEA'" in filters
        assert "fiscal_year = 2024" in filters

    def test_filter_expression_generation(self):
        """Test Spark SQL filter expression generation"""
        kbi = KPI(
            description="Revenue",
            technical_name="revenue",
            formula="revenue_amount",
            filters=["status = 'ACTIVE'", "region = 'EMEA'"],
            aggregation_type="SUM"
        )

        context = UCBaseKBIContext(kbi)
        filter_expr = context.get_filter_expression()

        # Should join filters with AND
        assert "(status = 'ACTIVE')" in filter_expr
        assert "(region = 'EMEA')" in filter_expr
        assert " AND " in filter_expr

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

        ctx1 = UCBaseKBIContext(kbi, [parent])
        ctx2 = UCBaseKBIContext(kbi, [parent])
        ctx3 = UCBaseKBIContext(kbi, [])  # Different parent chain

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
        assert UCBaseKBIContext.is_valid_for_context(kbi_with_filters) is True

        # Simple KBI - should be invalid (not needed in context chain)
        kbi_simple = KPI(
            description="Simple Sales",
            technical_name="simple",
            formula="sales",
            aggregation_type="SUM"
        )
        assert UCBaseKBIContext.is_valid_for_context(kbi_simple) is False


class TestUCKBIContextCache:
    """Test suite for UCKBIContextCache class"""

    def test_cache_initialization(self):
        """Test cache initialization"""
        cache = UCKBIContextCache()
        assert len(cache.get_all_contexts()) == 0

    def test_add_and_retrieve_contexts(self):
        """Test adding and retrieving contexts"""
        cache = UCKBIContextCache()

        kbi1 = KPI(description="Sales", technical_name="sales", formula="sales_amount", aggregation_type="SUM")
        kbi2 = KPI(description="Revenue", technical_name="revenue", formula="revenue_amount", aggregation_type="SUM")

        ctx1 = UCBaseKBIContext(kbi1)
        ctx2 = UCBaseKBIContext(kbi2)

        cache.add_context(ctx1)
        cache.add_context(ctx2)

        all_contexts = cache.get_all_contexts()
        assert len(all_contexts) == 2
        assert ctx1 in all_contexts
        assert ctx2 in all_contexts

    def test_cache_clear(self):
        """Test cache clearing"""
        cache = UCKBIContextCache()

        kbi = KPI(description="Sales", technical_name="sales", formula="sales_amount", aggregation_type="SUM")
        cache.add_context(UCBaseKBIContext(kbi))

        assert len(cache.get_all_contexts()) == 1

        cache.clear()

        assert len(cache.get_all_contexts()) == 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
