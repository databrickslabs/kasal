"""
Unit tests for converters/common/transformers/formula.py

Tests the KBI formula parser and dependency resolver used by all converters.
"""

import pytest
from src.converters.common.transformers.formula import (
    TokenType,
    FormulaToken,
    KbiFormulaParser,
    KBIDependencyResolver,
)
from src.converters.base.models import KPI


class TestTokenType:
    """Tests for TokenType enum"""

    def test_token_type_values(self):
        """Test all token type enum values"""
        assert TokenType.KBI_REFERENCE.value == "kbi_reference"
        assert TokenType.VARIABLE.value == "variable"
        assert TokenType.COLUMN.value == "column"
        assert TokenType.FUNCTION.value == "function"
        assert TokenType.OPERATOR.value == "operator"
        assert TokenType.LITERAL.value == "literal"

    def test_token_type_from_value(self):
        """Test creating TokenType from string value"""
        assert TokenType("kbi_reference") == TokenType.KBI_REFERENCE
        assert TokenType("variable") == TokenType.VARIABLE


class TestFormulaToken:
    """Tests for FormulaToken class"""

    def test_create_token(self):
        """Test creating a FormulaToken"""
        token = FormulaToken("total_sales", TokenType.KBI_REFERENCE, 10)

        assert token.value == "total_sales"
        assert token.token_type == TokenType.KBI_REFERENCE
        assert token.position == 10

    def test_token_equality(self):
        """Test token equality comparison"""
        token1 = FormulaToken("sales", TokenType.KBI_REFERENCE)
        token2 = FormulaToken("sales", TokenType.KBI_REFERENCE)
        token3 = FormulaToken("cost", TokenType.KBI_REFERENCE)
        token4 = FormulaToken("sales", TokenType.VARIABLE)

        assert token1 == token2
        assert token1 != token3  # Different value
        assert token1 != token4  # Different type
        assert token1 != "sales"  # Different class

    def test_token_hash(self):
        """Test token can be used in sets/dicts"""
        token1 = FormulaToken("sales", TokenType.KBI_REFERENCE)
        token2 = FormulaToken("sales", TokenType.KBI_REFERENCE)
        token3 = FormulaToken("cost", TokenType.KBI_REFERENCE)

        token_set = {token1, token2, token3}

        assert len(token_set) == 2  # token1 and token2 are duplicates
        assert token1 in token_set
        assert token3 in token_set

    def test_token_repr(self):
        """Test token string representation"""
        token = FormulaToken("total_sales", TokenType.KBI_REFERENCE)

        assert "kbi_reference" in repr(token)
        assert "total_sales" in repr(token)


class TestKbiFormulaParser:
    """Tests for KbiFormulaParser class"""

    @pytest.fixture
    def parser(self):
        """Create parser instance for testing"""
        return KbiFormulaParser()

    # ========== Extract KBI References Tests ==========

    def test_extract_kbi_references_square_brackets(self, parser):
        """Test extracting KBI references with square bracket notation"""
        formula = "SUM([total_sales] + [total_cost])"

        kbi_refs = parser.extract_kbi_references(formula)

        assert len(kbi_refs) == 2
        assert "total_sales" in kbi_refs
        assert "total_cost" in kbi_refs

    def test_extract_kbi_references_curly_braces(self, parser):
        """Test extracting KBI references with curly brace notation"""
        formula = "SUM({total_sales} + {total_cost})"

        kbi_refs = parser.extract_kbi_references(formula)

        assert len(kbi_refs) == 2
        assert "total_sales" in kbi_refs
        assert "total_cost" in kbi_refs

    def test_extract_kbi_references_mixed_notation(self, parser):
        """Test extracting KBI references with mixed bracket styles"""
        formula = "[total_sales] - {total_cost}"

        kbi_refs = parser.extract_kbi_references(formula)

        assert len(kbi_refs) == 2
        assert "total_sales" in kbi_refs
        assert "total_cost" in kbi_refs

    def test_extract_kbi_references_deduplication(self, parser):
        """Test KBI references are deduplicated"""
        formula = "[total_sales] + [total_sales] + [total_cost]"

        kbi_refs = parser.extract_kbi_references(formula)

        assert len(kbi_refs) == 2  # Should deduplicate total_sales
        assert "total_sales" in kbi_refs
        assert "total_cost" in kbi_refs

    def test_extract_kbi_references_empty_formula(self, parser):
        """Test extracting from empty formula returns empty list"""
        kbi_refs = parser.extract_kbi_references("")

        assert kbi_refs == []

    def test_extract_kbi_references_no_matches(self, parser):
        """Test formula with no KBI references"""
        formula = "SUM(sales.amount)"

        kbi_refs = parser.extract_kbi_references(formula)

        assert kbi_refs == []

    # ========== Extract Variables Tests ==========

    def test_extract_variables_dollar_notation(self, parser):
        """Test extracting variable references with $ notation"""
        formula = "SUM(amount) WHERE year = $year_filter"

        variables = parser.extract_variables(formula)

        assert len(variables) == 1
        assert "year_filter" in variables

    def test_extract_variables_var_prefix(self, parser):
        """Test extracting variables with $var_ prefix"""
        formula = "SUM(amount) WHERE year = $var_YEAR_FILTER"

        variables = parser.extract_variables(formula)

        assert len(variables) == 1
        assert "YEAR_FILTER" in variables

    def test_extract_variables_multiple(self, parser):
        """Test extracting multiple variables"""
        formula = "SUM(amount) WHERE year = $year AND region = $region"

        variables = parser.extract_variables(formula)

        assert len(variables) == 2
        assert "year" in variables
        assert "region" in variables

    def test_extract_variables_empty_formula(self, parser):
        """Test extracting from empty formula returns empty list"""
        variables = parser.extract_variables("")

        assert variables == []

    def test_extract_variables_no_matches(self, parser):
        """Test formula with no variables"""
        formula = "SUM(sales.amount)"

        variables = parser.extract_variables(formula)

        assert variables == []

    # ========== Extract Dependencies Tests ==========

    def test_extract_dependencies_complete(self, parser):
        """Test extracting all dependency types"""
        formula = "[total_sales] + [total_cost] WHERE year = $year_filter"

        deps = parser.extract_dependencies(formula)

        assert "kbis" in deps
        assert "variables" in deps
        assert "columns" in deps

        assert len(deps["kbis"]) == 2
        assert "total_sales" in deps["kbis"]
        assert "total_cost" in deps["kbis"]

        assert len(deps["variables"]) == 1
        assert "year_filter" in deps["variables"]

    def test_extract_dependencies_empty_formula(self, parser):
        """Test extracting dependencies from empty formula"""
        deps = parser.extract_dependencies("")

        assert deps["kbis"] == []
        assert deps["variables"] == []
        assert deps["columns"] == []

    # ========== Parse Formula Tests ==========

    def test_parse_formula_with_kbi_references(self, parser):
        """Test parsing formula with KBI references creates tokens"""
        formula = "[total_sales] + [total_cost]"

        tokens = parser.parse_formula(formula)

        kbi_tokens = [t for t in tokens if t.token_type == TokenType.KBI_REFERENCE]

        assert len(kbi_tokens) == 2
        assert any(t.value == "total_sales" for t in kbi_tokens)
        assert any(t.value == "total_cost" for t in kbi_tokens)

    def test_parse_formula_with_variables(self, parser):
        """Test parsing formula with variables creates tokens"""
        formula = "SUM(amount) WHERE year = $year_filter"

        tokens = parser.parse_formula(formula)

        var_tokens = [t for t in tokens if t.token_type == TokenType.VARIABLE]

        assert len(var_tokens) == 1
        assert var_tokens[0].value == "year_filter"

    def test_parse_formula_empty(self, parser):
        """Test parsing empty formula returns empty list"""
        tokens = parser.parse_formula("")

        assert tokens == []

    def test_parse_formula_token_positions(self, parser):
        """Test tokens contain position information"""
        formula = "[total_sales]"

        tokens = parser.parse_formula(formula)

        kbi_token = next(t for t in tokens if t.token_type == TokenType.KBI_REFERENCE)

        assert kbi_token.position >= 0  # Should have position


class TestKBIDependencyResolver:
    """Tests for KBIDependencyResolver class"""

    @pytest.fixture
    def resolver(self):
        """Create resolver instance for testing"""
        return KBIDependencyResolver()

    @pytest.fixture
    def sample_kpis(self):
        """Create sample KPIs for testing"""
        return [
            KPI(
                description="Total Sales",
                technical_name="total_sales",
                formula="SUM(sales.amount)"
            ),
            KPI(
                description="Total Cost",
                technical_name="total_cost",
                formula="SUM(cost.amount)"
            ),
            KPI(
                description="Profit",
                technical_name="profit",
                formula="[total_sales] - [total_cost]"
            ),
            KPI(
                description="Profit Margin",
                technical_name="profit_margin",
                formula="[profit] / [total_sales]"
            )
        ]

    # ========== Build KBI Lookup Tests ==========

    def test_build_kbi_lookup_by_technical_name(self, resolver, sample_kpis):
        """Test building lookup dictionary indexes by technical_name"""
        resolver.build_kbi_lookup(sample_kpis)

        assert "total_sales" in resolver._kbi_lookup
        assert "total_cost" in resolver._kbi_lookup
        assert "profit" in resolver._kbi_lookup
        assert resolver._kbi_lookup["total_sales"].description == "Total Sales"

    def test_build_kbi_lookup_by_description(self, resolver, sample_kpis):
        """Test lookup also indexes by description as fallback"""
        resolver.build_kbi_lookup(sample_kpis)

        assert "Total Sales" in resolver._kbi_lookup
        assert "Total Cost" in resolver._kbi_lookup
        assert resolver._kbi_lookup["Total Sales"].technical_name == "total_sales"

    def test_build_kbi_lookup_empty_list(self, resolver):
        """Test building lookup with empty KPI list"""
        resolver.build_kbi_lookup([])

        assert resolver._kbi_lookup == {}

    # ========== Resolve Formula KBIs Tests ==========

    def test_resolve_formula_kbis_success(self, resolver, sample_kpis):
        """Test resolving KBI dependencies from formula"""
        resolver.build_kbi_lookup(sample_kpis)

        profit_kpi = sample_kpis[2]  # Profit depends on total_sales and total_cost

        dependencies = resolver.resolve_formula_kbis(profit_kpi)

        assert len(dependencies) == 2
        assert any(kpi.technical_name == "total_sales" for kpi in dependencies)
        assert any(kpi.technical_name == "total_cost" for kpi in dependencies)

    def test_resolve_formula_kbis_no_dependencies(self, resolver, sample_kpis):
        """Test KBI with no dependencies returns empty list"""
        resolver.build_kbi_lookup(sample_kpis)

        sales_kpi = sample_kpis[0]  # Total Sales has no KBI dependencies

        dependencies = resolver.resolve_formula_kbis(sales_kpi)

        assert dependencies == []

    def test_resolve_formula_kbis_unresolved_reference(self, resolver, sample_kpis):
        """Test handling of unresolved KBI references"""
        resolver.build_kbi_lookup(sample_kpis)

        # Create KPI with reference to non-existent KBI
        kpi_with_bad_ref = KPI(
            description="Bad KPI",
            technical_name="bad_kpi",
            formula="[nonexistent_kbi] + 100"
        )

        dependencies = resolver.resolve_formula_kbis(kpi_with_bad_ref)

        assert dependencies == []  # Unresolved references are skipped

    def test_resolve_formula_kbis_empty_formula(self, resolver, sample_kpis):
        """Test KPI with empty formula returns empty list"""
        resolver.build_kbi_lookup(sample_kpis)

        kpi_no_formula = KPI(
            description="No Formula",
            technical_name="no_formula",
            formula=""
        )

        dependencies = resolver.resolve_formula_kbis(kpi_no_formula)

        assert dependencies == []

    # ========== Get Dependency Tree Tests ==========

    def test_get_dependency_tree_simple(self, resolver, sample_kpis):
        """Test building dependency tree for KPI with dependencies"""
        resolver.build_kbi_lookup(sample_kpis)

        profit_kpi = sample_kpis[2]  # Profit depends on sales and cost

        tree = resolver.get_dependency_tree(profit_kpi)

        assert tree["kbi"].technical_name == "profit"
        assert len(tree["dependencies"]) == 2

        # Check dependencies are included
        dep_names = {dep["kbi"].technical_name for dep in tree["dependencies"]}
        assert "total_sales" in dep_names
        assert "total_cost" in dep_names

    def test_get_dependency_tree_nested(self, resolver, sample_kpis):
        """Test building nested dependency tree"""
        resolver.build_kbi_lookup(sample_kpis)

        profit_margin_kpi = sample_kpis[3]  # profit_margin -> profit -> sales/cost

        tree = resolver.get_dependency_tree(profit_margin_kpi)

        assert tree["kbi"].technical_name == "profit_margin"
        assert len(tree["dependencies"]) == 2

        # Find profit dependency
        profit_dep = next(d for d in tree["dependencies"] if d["kbi"].technical_name == "profit")

        # Profit should have its own dependencies
        assert len(profit_dep["dependencies"]) == 2

    def test_get_dependency_tree_no_dependencies(self, resolver, sample_kpis):
        """Test dependency tree for KPI with no dependencies"""
        resolver.build_kbi_lookup(sample_kpis)

        sales_kpi = sample_kpis[0]  # Total Sales has no dependencies

        tree = resolver.get_dependency_tree(sales_kpi)

        assert tree["kbi"].technical_name == "total_sales"
        assert tree["dependencies"] == []

    def test_get_dependency_tree_circular(self, resolver):
        """Test handling of circular dependencies"""
        # Create circular dependency: A -> B -> A
        circular_kpis = [
            KPI(
                description="KPI A",
                technical_name="kpi_a",
                formula="[kpi_b] + 100"
            ),
            KPI(
                description="KPI B",
                technical_name="kpi_b",
                formula="[kpi_a] * 2"
            )
        ]

        resolver.build_kbi_lookup(circular_kpis)

        tree = resolver.get_dependency_tree(circular_kpis[0])

        # Should detect circular dependency somewhere in the tree
        def has_circular(node):
            if "circular" in node and node["circular"]:
                return True
            if "dependencies" in node:
                return any(has_circular(dep) for dep in node["dependencies"])
            return False

        assert has_circular(tree), "Circular dependency should be detected in tree"
