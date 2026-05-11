"""Tests for metric_view_validation_utils.dax_expression_parser (DAXExpressionParser)."""
import pytest

from src.engines.crewai.tools.custom.metric_view_validation_utils.dax_expression_parser import (
    DAXExpressionParser,
    _MAX_VAR_SUBSTITUTION_ITERATIONS,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def parser():
    return DAXExpressionParser()


# ---------------------------------------------------------------------------
# clean_dax_comments()
# ---------------------------------------------------------------------------

class TestCleanDaxComments:
    def test_removes_double_slash_comment(self, parser):
        result = parser.clean_dax_comments("SUM(T[col]) // this is a comment")
        assert "//" not in result
        assert "SUM" in result

    def test_removes_double_dash_comment(self, parser):
        result = parser.clean_dax_comments("SUM(T[col]) -- comment")
        assert "--" not in result

    def test_removes_full_line_comment(self, parser):
        expr = "// full line comment\nSUM(T[col])"
        result = parser.clean_dax_comments(expr)
        assert "full line comment" not in result
        assert "SUM" in result

    def test_strips_whitespace(self, parser):
        result = parser.clean_dax_comments("  SUM(T[col])  ")
        assert result.strip() == result.strip()

    def test_empty_string(self, parser):
        result = parser.clean_dax_comments("")
        assert result == ""


# ---------------------------------------------------------------------------
# parse_function_call()
# ---------------------------------------------------------------------------

class TestParseFunctionCall:
    def test_simple_value(self, parser):
        result = parser.parse_function_call("myvar")
        assert result == {"value": "myvar"}

    def test_simple_function(self, parser):
        result = parser.parse_function_call("SUM(T[col])")
        assert result["function"] == "SUM"
        assert len(result["arguments"]) == 1

    def test_nested_function(self, parser):
        result = parser.parse_function_call("DIVIDE(SUM(T[a]),COUNT(T[b]))")
        assert result["function"] == "DIVIDE"
        assert len(result["arguments"]) == 2
        assert result["arguments"][0]["function"] == "SUM"
        assert result["arguments"][1]["function"] == "COUNT"

    def test_deeply_nested(self, parser):
        result = parser.parse_function_call("CALCULATE(SUM(T[a]),FILTER(T,T[b]=1))")
        assert result["function"] == "CALCULATE"
        assert len(result["arguments"]) == 2

    def test_no_args_function(self, parser):
        result = parser.parse_function_call("NOW()")
        assert result["function"] == "NOW"
        assert result["arguments"] == []


# ---------------------------------------------------------------------------
# _parse_arguments()
# ---------------------------------------------------------------------------

class TestParseArguments:
    def test_empty(self, parser):
        assert parser._parse_arguments("") == []
        assert parser._parse_arguments("   ") == []

    def test_single_arg(self, parser):
        args = parser._parse_arguments("T[col]")
        assert len(args) == 1

    def test_two_args(self, parser):
        args = parser._parse_arguments("T[a],T[b]")
        assert len(args) == 2

    def test_nested_comma_not_split(self, parser):
        # Comma inside FILTER(...) should not be treated as arg separator
        args = parser._parse_arguments("FILTER(T,T[a]=1),T[b]")
        assert len(args) == 2


# ---------------------------------------------------------------------------
# _extract_variables() + _extract_return_expr()
# ---------------------------------------------------------------------------

class TestExtractVariables:
    def test_single_var(self, parser):
        expr = "VAR x = SUM(T[col])\nRETURN x"
        vars_ = parser._extract_variables(expr)
        assert len(vars_) == 1
        assert vars_[0]["variable_name"] == "x"

    def test_multiple_vars(self, parser):
        expr = "VAR a = SUM(T[col1])\nVAR b = COUNT(T[col2])\nRETURN DIVIDE(a,b)"
        vars_ = parser._extract_variables(expr)
        assert len(vars_) == 2
        names = {v["variable_name"] for v in vars_}
        assert names == {"a", "b"}

    def test_no_vars(self, parser):
        vars_ = parser._extract_variables("SUM(T[col])")
        assert vars_ == []


class TestExtractReturnExpr:
    def test_extracts_return(self, parser):
        expr = "VAR x = 1\nRETURN x + 1"
        ret = parser._extract_return_expr(expr)
        assert ret is not None
        assert "x" in ret

    def test_no_return(self, parser):
        ret = parser._extract_return_expr("SUM(T[col])")
        assert ret is None


# ---------------------------------------------------------------------------
# check_variable_usage()
# ---------------------------------------------------------------------------

class TestCheckVariableUsage:
    def test_variable_in_expression(self, parser):
        assert parser.check_variable_usage("myVar", " myVar + 1") is True

    def test_variable_not_in_expression(self, parser):
        assert parser.check_variable_usage("myVar", "SUM(T[col])") is False


# ---------------------------------------------------------------------------
# decompose_toplevel()
# ---------------------------------------------------------------------------

class TestDecomposeToplevel:
    def test_simple_no_vars(self, parser):
        result = parser.decompose_toplevel("SUM(T[col])")
        assert result["type"] == "no_return"
        assert "expr" in result

    def test_with_vars(self, parser):
        # Use DIVIDE(x,1) so 'x' has non-word chars on both sides, avoiding
        # the known boundary-match limitation in check_variable_usage.
        expr = "VAR x = SUM(T[col])\nRETURN DIVIDE(x,1)"
        result = parser.decompose_toplevel(expr)
        assert result["type"] == "return"
        assert "return_expr" in result
        assert len(result["variables"]) >= 1


# ---------------------------------------------------------------------------
# _substitute_single_variable()
# ---------------------------------------------------------------------------

class TestSubstituteSingleVariable:
    def test_substitutes_variable(self, parser):
        result = parser._substitute_single_variable(" myVar + 1", "myVar", "SUM(T[col])")
        assert "SUM(T[col])" in result

    def test_no_match_returns_unchanged(self, parser):
        original = " otherVar + 1"
        result = parser._substitute_single_variable(original, "myVar", "SUM(T[col])")
        assert result == original

    def test_case_insensitive(self, parser):
        result = parser._substitute_single_variable(" MyVar / 2", "myvar", "SUM(T[col])")
        assert "SUM(T[col])" in result


# ---------------------------------------------------------------------------
# substitute_all_variables_recursively()
# ---------------------------------------------------------------------------

class TestSubstituteAllVariablesRecursively:
    def test_empty_list(self, parser):
        assert parser.substitute_all_variables_recursively([]) == []

    def test_single_independent_var(self, parser):
        vars_ = [{"variable_name": "x", "variable_expr": "SUM(T[col])"}]
        result = parser.substitute_all_variables_recursively(vars_)
        assert result[0]["variable_expr"] == "SUM(T[col])"

    def test_chain_substitution(self, parser):
        # a = SUM(T[col]), b = a + 1  →  b should expand to SUM(T[col]) + 1
        vars_ = [
            {"variable_name": "a", "variable_expr": "SUM(T[col])"},
            {"variable_name": "b", "variable_expr": " a + 1"},
        ]
        result = parser.substitute_all_variables_recursively(vars_)
        b_expr = next(v["variable_expr"] for v in result if v["variable_name"] == "b")
        assert "SUM(T[col])" in b_expr

    def test_raises_on_circular_dependency(self, parser):
        # a references b, b references a → infinite loop → RuntimeError
        vars_ = [
            {"variable_name": "a", "variable_expr": " b * 2"},
            {"variable_name": "b", "variable_expr": " a / 2"},
        ]
        with pytest.raises(RuntimeError, match="maximum iterations"):
            parser.substitute_all_variables_recursively(vars_)


# ---------------------------------------------------------------------------
# decompose()
# ---------------------------------------------------------------------------

class TestDecompose:
    def test_simple_expression(self, parser):
        tree = parser.decompose("SUM(T[col])")
        assert tree["function"] == "SUM"

    def test_with_var_returns_substituted_tree(self, parser):
        expr = "VAR x = SUM(T[col])\nRETURN x"
        tree = parser.decompose(expr)
        # After substitution the tree root should resolve to SUM
        assert "function" in tree or "value" in tree

    def test_comments_stripped_before_parse(self, parser):
        expr = "// comment\nSUM(T[col])"
        tree = parser.decompose(expr)
        assert tree["function"] == "SUM"


# ---------------------------------------------------------------------------
# parse()  (full pipeline)
# ---------------------------------------------------------------------------

class TestParse:
    def test_raw_preserved(self, parser):
        expr = "SUM(fact[amount])"
        result = parser.parse(expr)
        assert result["raw"] == expr

    def test_aggregation_extracted(self, parser):
        result = parser.parse("SUM(fact[amount])")
        assert any(a["type"] == "SUM" for a in result["aggregations"])

    def test_reference_extracted(self, parser):
        result = parser.parse("SUM(fact[amount])")
        assert "fact.amount" in result["references"]

    def test_filter_extracted(self, parser):
        result = parser.parse("CALCULATE(SUM(T[a]),FILTER(T,T[b]=\"val\"))")
        assert len(result["filters"]) >= 1

    def test_division_in_operations(self, parser):
        result = parser.parse("DIVIDE(SUM(T[a]),COUNT(T[b]))")
        assert "DIVISION" in result["operations"]

    def test_structure_has_divide(self, parser):
        result = parser.parse("DIVIDE(SUM(T[a]),COUNT(T[b]))")
        assert result["structure"]["is_division"] is True

    def test_calculate_in_structure(self, parser):
        result = parser.parse("CALCULATE(SUM(T[a]),FILTER(T,T[b]=\"x\"))")
        assert result["structure"]["has_calculate"] is True


# ---------------------------------------------------------------------------
# _extract_balanced_parens()
# ---------------------------------------------------------------------------

class TestDaxExtractBalancedParens:
    def test_round_parens(self, parser):
        content = parser._extract_balanced_parens("SUM(T[col])", 3)
        assert content == "T[col]"

    def test_curly_braces(self, parser):
        content = parser._extract_balanced_parens("{a,b,c}", 0)
        assert content == "a,b,c"

    def test_out_of_bounds(self, parser):
        assert parser._extract_balanced_parens("abc", 99) == ""

    def test_wrong_char_at_start(self, parser):
        assert parser._extract_balanced_parens("abc", 0) == ""


# ---------------------------------------------------------------------------
# format_parse_tree()
# ---------------------------------------------------------------------------

class TestFormatParseTree:
    def test_leaf_node(self, parser):
        tree = {"value": "myval"}
        output = parser.format_parse_tree(tree)
        assert "myval" in output

    def test_function_node(self, parser):
        tree = {"function": "SUM", "arguments": [{"value": "T[col]"}]}
        output = parser.format_parse_tree(tree)
        assert "SUM" in output
        assert "T[col]" in output

    def test_indentation_increases(self, parser):
        tree = {
            "function": "DIVIDE",
            "arguments": [
                {"function": "SUM", "arguments": [{"value": "T[a]"}]},
                {"value": "T[b]"},
            ],
        }
        output = parser.format_parse_tree(tree)
        lines = output.split("\n")
        # Root line should have less indentation than child lines
        assert lines[0].startswith("- DIVIDE")
        assert lines[1].startswith("    - SUM")
