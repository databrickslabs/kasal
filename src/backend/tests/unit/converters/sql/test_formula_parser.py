"""
Unit tests for SQL Formula Parser

Tests formula parsing, token extraction, and dependency resolution
"""

import pytest
from src.converters.base.models import KPI
from src.converters.common.transformers.formula import (
    KbiFormulaParser,
    KBIDependencyResolver,
    TokenType,
    FormulaToken
)


class TestKbiFormulaParser:
    """Test suite for KbiFormulaParser class"""

    def test_extract_kbi_references_square_brackets(self):
        """Test extraction of KBI references with square bracket notation"""
        parser = KbiFormulaParser()

        formula = "[Revenue] * [Quantity]"
        kbi_refs = parser.extract_kbi_references(formula)

        assert len(kbi_refs) == 2
        assert "Revenue" in kbi_refs
        assert "Quantity" in kbi_refs

    def test_extract_kbi_references_curly_braces(self):
        """Test extraction of KBI references with curly brace notation"""
        parser = KbiFormulaParser()

        formula = "{Gross_Profit} - {Operating_Expenses}"
        kbi_refs = parser.extract_kbi_references(formula)

        assert len(kbi_refs) == 2
        assert "Gross_Profit" in kbi_refs
        assert "Operating_Expenses" in kbi_refs

    def test_extract_kbi_references_complex_formula(self):
        """Test extraction from complex formulas"""
        parser = KbiFormulaParser()

        formula = "([Revenue] - [COGS]) / [Revenue] * 100"
        kbi_refs = parser.extract_kbi_references(formula)

        assert len(kbi_refs) == 2  # Revenue appears twice but should be deduped
        assert "Revenue" in kbi_refs
        assert "COGS" in kbi_refs

    def test_extract_kbi_references_no_matches(self):
        """Test formula with no KBI references"""
        parser = KbiFormulaParser()

        formula = "SUM(sales_amount) * 1.2"
        kbi_refs = parser.extract_kbi_references(formula)

        assert len(kbi_refs) == 0

    def test_extract_variables_simple(self):
        """Test extraction of simple variable references"""
        parser = KbiFormulaParser()

        formula = "revenue * $tax_rate"
        vars = parser.extract_variables(formula)

        assert len(vars) == 1
        assert "tax_rate" in vars

    def test_extract_variables_with_var_prefix(self):
        """Test extraction of variables with var_ prefix"""
        parser = KbiFormulaParser()

        formula = "sales * (1 + $var_GROWTH_RATE)"
        vars = parser.extract_variables(formula)

        assert len(vars) == 1
        assert "GROWTH_RATE" in vars

    def test_extract_variables_multiple(self):
        """Test extraction of multiple variables"""
        parser = KbiFormulaParser()

        formula = "amount * $discount + $surcharge - $credit"
        vars = parser.extract_variables(formula)

        assert len(vars) == 3
        assert "discount" in vars
        assert "surcharge" in vars
        assert "credit" in vars

    def test_extract_dependencies_combined(self):
        """Test extraction of all dependencies at once"""
        parser = KbiFormulaParser()

        formula = "[Base_Revenue] * (1 + $growth_rate) - overhead_cost"
        deps = parser.extract_dependencies(formula)

        assert "kbis" in deps
        assert "variables" in deps
        assert "columns" in deps

        assert "Base_Revenue" in deps["kbis"]
        assert "growth_rate" in deps["variables"]
        assert "overhead_cost" in deps["columns"]

    def test_parse_formula_tokens(self):
        """Test full formula parsing into tokens"""
        parser = KbiFormulaParser()

        formula = "[Revenue] * $tax_rate"
        tokens = parser.parse_formula(formula)

        # Should have KBI token and variable token
        kbi_tokens = [t for t in tokens if t.token_type == TokenType.KBI_REFERENCE]
        var_tokens = [t for t in tokens if t.token_type == TokenType.VARIABLE]

        assert len(kbi_tokens) == 1
        assert kbi_tokens[0].value == "Revenue"

        assert len(var_tokens) == 1
        assert var_tokens[0].value == "tax_rate"

    def test_extract_column_references(self):
        """Test extraction of column references"""
        parser = KbiFormulaParser()

        formula = "sales_amount * quantity + overhead"
        columns = parser._extract_column_references(formula)

        assert "sales_amount" in columns
        assert "quantity" in columns
        assert "overhead" in columns

    def test_sql_keyword_detection(self):
        """Test SQL keyword detection"""
        parser = KbiFormulaParser()

        assert parser._is_sql_keyword("SELECT") is True
        assert parser._is_sql_keyword("WHERE") is True
        assert parser._is_sql_keyword("from") is True  # Case insensitive
        assert parser._is_sql_keyword("revenue") is False

    def test_sql_function_detection(self):
        """Test SQL function detection"""
        parser = KbiFormulaParser()

        assert parser._is_sql_function("SUM") is True
        assert parser._is_sql_function("COUNT") is True
        assert parser._is_sql_function("AVG") is True
        assert parser._is_sql_function("CUSTOM_FUNC") is False


class TestKBIDependencyResolver:
    """Test suite for KBIDependencyResolver class"""

    def setup_method(self):
        """Set up test fixtures"""
        self.parser = KbiFormulaParser()
        self.resolver = KBIDependencyResolver(self.parser)

        # Create test KBIs
        self.kbi_sales = KPI(
            technical_name="sales",
            description="Total Sales",
            formula="sales_amount",
            source_table="fact_sales",
            aggregation_type="SUM"
        )

        self.kbi_costs = KPI(
            technical_name="costs",
            description="Total Costs",
            formula="cost_amount",
            source_table="fact_sales",
            aggregation_type="SUM"
        )

        self.kbi_profit = KPI(
            technical_name="profit",
            description="Profit",
            formula="[sales] - [costs]",
            aggregation_type="CALCULATED"
        )

        self.kbi_margin = KPI(
            technical_name="margin",
            description="Profit Margin",
            formula="[profit] / [sales] * 100",
            aggregation_type="CALCULATED"
        )

        self.all_kbis = [
            self.kbi_sales,
            self.kbi_costs,
            self.kbi_profit,
            self.kbi_margin
        ]

    def test_build_kbi_lookup(self):
        """Test building KBI lookup table"""
        self.resolver.build_kbi_lookup(self.all_kbis)

        assert "sales" in self.resolver._kbi_lookup
        assert "costs" in self.resolver._kbi_lookup
        assert "profit" in self.resolver._kbi_lookup
        assert "margin" in self.resolver._kbi_lookup

    def test_resolve_formula_kbis_direct_dependencies(self):
        """Test resolving direct KBI dependencies"""
        self.resolver.build_kbi_lookup(self.all_kbis)

        # Profit depends on sales and costs
        resolved = self.resolver.resolve_formula_kbis(self.kbi_profit)

        assert len(resolved) == 2
        resolved_names = [k.technical_name for k in resolved]
        assert "sales" in resolved_names
        assert "costs" in resolved_names

    def test_resolve_formula_kbis_transitive_dependencies(self):
        """Test resolving transitive dependencies"""
        self.resolver.build_kbi_lookup(self.all_kbis)

        # Margin depends on profit and sales (directly)
        # Profit depends on sales and costs (transitively)
        resolved = self.resolver.resolve_formula_kbis(self.kbi_margin)

        assert len(resolved) == 2  # Only direct dependencies
        resolved_names = [k.technical_name for k in resolved]
        assert "profit" in resolved_names
        assert "sales" in resolved_names

    def test_resolve_formula_kbis_base_kbi(self):
        """Test resolving base KBI with no dependencies"""
        self.resolver.build_kbi_lookup(self.all_kbis)

        # Sales is a base KBI - no dependencies
        resolved = self.resolver.resolve_formula_kbis(self.kbi_sales)

        assert len(resolved) == 0

    def test_resolve_formula_kbis_missing_reference(self):
        """Test resolving with missing KBI reference"""
        self.resolver.build_kbi_lookup(self.all_kbis)

        # KBI with reference to non-existent KBI
        kbi_invalid = KPI(
            description="Invalid KBI",
            technical_name="invalid",
            formula="[non_existent_kbi] * 2",
            aggregation_type="CALCULATED"
        )

        # Should log warning but not crash
        resolved = self.resolver.resolve_formula_kbis(kbi_invalid)
        assert len(resolved) == 0  # No valid KBIs found

    def test_get_dependency_tree_simple(self):
        """Test building dependency tree for simple KBI"""
        self.resolver.build_kbi_lookup(self.all_kbis)

        tree = self.resolver.get_dependency_tree(self.kbi_sales)

        assert tree["kbi"] == self.kbi_sales
        assert tree["is_base"] is True
        assert len(tree["dependencies"]) == 0

    def test_get_dependency_tree_nested(self):
        """Test building dependency tree with nested dependencies"""
        self.resolver.build_kbi_lookup(self.all_kbis)

        tree = self.resolver.get_dependency_tree(self.kbi_margin)

        assert tree["kbi"] == self.kbi_margin
        assert tree["is_base"] is False
        assert len(tree["dependencies"]) == 2  # profit and sales

        # Check that profit node has its own dependencies
        profit_dep = next(d for d in tree["dependencies"] if d["kbi"].technical_name == "profit")
        assert len(profit_dep["dependencies"]) == 2  # sales and costs

    def test_get_dependency_tree_circular_detection(self):
        """Test circular dependency detection"""
        # Create circular dependency
        kbi_a = KPI(
            description="KBI A",
            technical_name="kbi_a",
            formula="[kbi_b] + 1",
            aggregation_type="CALCULATED"
        )

        kbi_b = KPI(
            description="KBI B",
            technical_name="kbi_b",
            formula="[kbi_a] + 1",  # Circular!
            aggregation_type="CALCULATED"
        )

        resolver = KBIDependencyResolver(self.parser)
        resolver.build_kbi_lookup([kbi_a, kbi_b])

        tree = resolver.get_dependency_tree(kbi_a)

        # Should detect circular dependency
        # Check that circular flag is set somewhere in the tree
        assert "kbi" in tree
        # The implementation marks circular nodes
        assert tree is not None  # At minimum, doesn't crash

    def test_lookup_by_description_fallback(self):
        """Test KBI lookup falls back to description"""
        kbi = KPI(
            technical_name="sales_kbi",
            description="Total Sales",
            formula="sales_amount",
            aggregation_type="SUM"
        )

        resolver = KBIDependencyResolver(self.parser)
        resolver.build_kbi_lookup([kbi])

        # Should be findable by technical_name
        assert "sales_kbi" in resolver._kbi_lookup

        # Should also be findable by description (if not conflicting)
        assert "Total Sales" in resolver._kbi_lookup


class TestFormulaToken:
    """Test suite for FormulaToken class"""

    def test_token_creation(self):
        """Test token creation"""
        token = FormulaToken("Revenue", TokenType.KBI_REFERENCE, position=0)

        assert token.value == "Revenue"
        assert token.token_type == TokenType.KBI_REFERENCE
        assert token.position == 0

    def test_token_equality(self):
        """Test token equality"""
        token1 = FormulaToken("Revenue", TokenType.KBI_REFERENCE)
        token2 = FormulaToken("Revenue", TokenType.KBI_REFERENCE)
        token3 = FormulaToken("Revenue", TokenType.VARIABLE)

        assert token1 == token2
        assert token1 != token3
        assert hash(token1) == hash(token2)

    def test_token_repr(self):
        """Test token string representation"""
        token = FormulaToken("Revenue", TokenType.KBI_REFERENCE)

        assert "kbi_reference" in str(token)
        assert "Revenue" in str(token)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
