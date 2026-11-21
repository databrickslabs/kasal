"""Row-level DAX calculated columns -> Databricks SQL (calc_column_sql).

Made-up model: table ``Sales`` with columns Amount, Cost, Qty, Type, Name, Date, Flag,
and a related table ``Region``. Each construct has a translation test, and refusals and
blank semantics have their own tests.
"""

from __future__ import annotations

import pytest

from src.services.tools.metric_view_utils.calc_column_sql import (
    RelatedTable,
    translate_calculated_column,
)

COLS = ("Amount", "Cost", "Qty", "Type", "Name", "Date", "Flag")
KINDS = {
    "Amount": "number",
    "Cost": "number",
    "Qty": "integer",
    "Type": "text",
    "Name": "text",
    "Date": "date",
    "Flag": "boolean",
}
REGION = RelatedTable(
    alias="rg",
    view="ai.kasal.src_region",
    from_column="Type",
    to_column="Code",
    columns=("Code", "Label"),
    column_kinds={"Label": "text"},
)


def tr(expression, *, kinds=KINDS, related=None, source_alias=None, cols=COLS, calc=()):
    return translate_calculated_column(
        expression,
        "Sales",
        cols,
        related,
        column_kinds=kinds,
        calculated_columns=calc,
        source_alias=source_alias,
    )


def sql_of(expression, **kwargs):
    result = tr(expression, **kwargs)
    assert result.translated, result.refusal
    return result.sql


def refusal_of(expression, **kwargs):
    result = tr(expression, **kwargs)
    assert not result.translated, f"expected a refusal, got {result.sql}"
    return result.refusal


# -- column references ---------------------------------------------------------


def test_qualified_column_is_read_with_its_view_spelling_and_quoted():
    assert sql_of("sales[amount]") == "`Amount`"


def test_bare_bracket_column_is_a_column_of_the_table():
    assert sql_of("[Flag]", kinds={}) == "`Flag`"


def test_column_with_space_in_name_keeps_backticks():
    cols = ("Net Amount",)
    assert sql_of("Sales[Net Amount]", cols=cols, kinds={}) == "`Net Amount`"


def test_quoted_table_name_form_is_accepted():
    assert sql_of("'Sales'[Cost]") == "`Cost`"


def test_source_alias_qualifies_own_columns():
    assert sql_of("Sales[Amount]", source_alias="src") == "src.`Amount`"


def test_reference_to_another_table_outside_related_is_refused():
    assert "another table" in refusal_of('Region[Label] = "x"')


def test_missing_column_is_refused():
    assert "not in the source view" in refusal_of("Sales[Nope]")


def test_calculated_column_reference_is_refused_with_its_reason():
    reason = refusal_of("Sales[Margin] + 1", calc=("Margin",))
    assert "calculated column" in reason


def test_bare_name_is_refused_with_the_valid_forms():
    assert "Table[Column] or [Column]" in refusal_of("Amount + 1")


# -- literals and arithmetic ---------------------------------------------------


@pytest.mark.parametrize(
    ("expression", "expected"),
    [
        (
            "Sales[Amount] - Sales[Cost]",
            "(COALESCE(`Amount`, 0) - COALESCE(`Cost`, 0))",
        ),
        ("Sales[Amount] * 2", "(COALESCE(`Amount`, 0) * 2)"),
        ("Sales[Qty] + 1", "(COALESCE(`Qty`, 0) + 1)"),
        ("-Sales[Cost]", "(-COALESCE(`Cost`, 0))"),
        ("Sales[Amount] / 4", "(COALESCE(`Amount`, 0) / 4)"),
    ],
)
def test_arithmetic_reads_blank_as_zero(expression, expected):
    assert sql_of(expression) == expected


def test_text_literal_is_escaped_and_has_no_null():
    result = tr('"it\'s"')
    assert result.sql == "'it''s'"
    assert result.may_be_blank is False


def test_division_by_non_zero_literal_is_translated():
    assert sql_of("Sales[Amount] / 2").endswith("/ 2)")


@pytest.mark.parametrize(
    "expression",
    ["Sales[Amount] / Sales[Cost]", "Sales[Amount] / 0", "Sales[Amount] ^ 2"],
)
def test_division_with_other_divisor_and_power_are_refused(expression):
    refusal_of(expression)


def test_arithmetic_on_text_is_refused():
    assert "needs a number" in refusal_of('"5" + 1')


# -- text concatenation and text functions -------------------------------------


def test_concatenation_of_text_columns_treats_blank_as_empty_text():
    sql = sql_of('Sales[Name] & " - " & Sales[Type]')
    assert sql == "concat(concat(COALESCE(`Name`, ''), ' - '), COALESCE(`Type`, ''))"


def test_concatenation_of_an_integer_casts_it_to_text():
    assert sql_of('"Qty " & Sales[Qty]') == (
        "concat('Qty ', COALESCE(CAST(`Qty` AS STRING), ''))"
    )


def test_concatenation_of_a_decimal_is_refused():
    assert "number formatting" in refusal_of('"x" & Sales[Amount]')


def test_text_column_of_unknown_type_is_refused_for_concatenation():
    assert "pass its kind" in refusal_of('Sales[Name] & "x"', kinds={})


@pytest.mark.parametrize(
    ("expression", "expected"),
    [
        ("LEFT(Sales[Name], 3)", "left(COALESCE(`Name`, ''), CAST(3 AS INT))"),
        ("RIGHT(Sales[Name], 2)", "right(COALESCE(`Name`, ''), CAST(2 AS INT))"),
        (
            "MID(Sales[Name], 2, 3)",
            "substring(COALESCE(`Name`, ''), 2, CAST(3 AS INT))",
        ),
        ("LEN(Sales[Name])", "length(COALESCE(`Name`, ''))"),
        ("UPPER(Sales[Name])", "upper(COALESCE(`Name`, ''))"),
        ("LOWER(Sales[Name])", "lower(COALESCE(`Name`, ''))"),
        ('CONCATENATE(Sales[Name], "x")', "concat(COALESCE(`Name`, ''), 'x')"),
        (
            'SUBSTITUTE(Sales[Name], "a", "b")',
            "replace(COALESCE(`Name`, ''), 'a', 'b')",
        ),
    ],
)
def test_text_functions_map_to_spark(expression, expected):
    assert sql_of(expression, kinds=KINDS) == expected


def test_trim_collapses_inner_space_runs_as_dax_does():
    sql = sql_of("TRIM(Sales[Name])")
    assert sql == ("regexp_replace(trim(COALESCE(`Name`, '')), ' +', ' ')")


def test_search_is_case_insensitive_with_not_found_value():
    sql = sql_of('SEARCH("cy", Sales[Name], 1, 0)')
    assert sql == (
        "COALESCE(NULLIF(locate('cy', lower(COALESCE(`Name`, '')), 1), 0), 0)"
    )


def test_search_not_found_blank_gives_blank():
    result = tr('SEARCH("cy", Sales[Name], 1, BLANK())')
    assert result.translated and result.may_be_blank is True


def test_search_without_not_found_value_is_refused():
    assert "NotFoundValue" in refusal_of('SEARCH("cy", Sales[Name])')


def test_search_wildcards_are_refused():
    assert "wildcards" in refusal_of('SEARCH("c?", Sales[Name], 1, 0)')


def test_search_column_text_is_refused():
    assert "literal" in refusal_of("SEARCH(Sales[Name], Sales[Name], 1, 0)")


def test_find_is_case_sensitive_and_keeps_its_text():
    sql = sql_of('FIND("cy", Sales[Name], 1, 0)')
    assert sql == "COALESCE(NULLIF(locate('cy', COALESCE(`Name`, ''), 1), 0), 0)"


def test_start_position_must_be_a_literal_of_one_or_more():
    assert "whole-number literal" in refusal_of("MID(Sales[Name], Sales[Qty], 2)")


def test_value_of_numeric_text_literal_and_refusal_for_unknown_text():
    assert sql_of('VALUE("12")') == "try_cast('12' AS DOUBLE)"
    assert sql_of('VALUE("")', kinds=KINDS).endswith("AS DOUBLE)")
    assert sql_of("VALUE(Sales[Name])").startswith("try_cast(")


def test_value_of_blank_is_blank():
    result = tr("VALUE(BLANK())")
    assert result.may_be_blank is True and result.sql == "NULL"


@pytest.mark.parametrize(
    ("pattern", "spark"),
    [
        ("yyyy", "yyyy"),
        ("yyyy-mm", "yyyy-MM"),
        ("yy-m-d", "yy-M-d"),
        ("dd mm", "dd MM"),
    ],
)
def test_format_maps_numeric_date_patterns(pattern, spark):
    sql = sql_of(f'FORMAT(Sales[Date], "{pattern}")')
    assert sql == f"COALESCE(date_format(`Date`, '{spark}'), '')"


@pytest.mark.parametrize("pattern", ["mmm", "dddd", "yyyy/mm/dd", "hh:nn"])
def test_format_refuses_patterns_it_cannot_map_exactly(pattern):
    assert "not translated" in refusal_of(f'FORMAT(Sales[Date], "{pattern}")')


def test_format_of_a_number_is_refused():
    assert "date values only" in refusal_of('FORMAT(Sales[Amount], "yyyy")')


def test_format_of_blank_is_empty_text():
    result = tr('FORMAT(BLANK(), "yyyy")')
    assert result.sql == "''" and result.may_be_blank is False


# -- comparisons, IN, logic ----------------------------------------------------


def test_text_equality_is_two_valued_and_case_insensitive():
    sql = sql_of('Sales[Type] = "a"')
    assert sql == "(lower(COALESCE(`Type`, '')) = 'a')"


def test_text_ordering_is_refused():
    assert "ordering comparison" in refusal_of('Sales[Type] < "a"')


def test_number_comparison_reads_blank_as_zero():
    assert sql_of("Sales[Amount] > 10") == "(COALESCE(`Amount`, 0) > 10)"


def test_comparison_with_unknown_column_takes_the_literal_kind():
    assert (
        sql_of('Sales[Type] = "x"', kinds={}) == "(lower(COALESCE(`Type`, '')) = 'x')"
    )


def test_mixed_type_comparison_is_refused():
    assert "mixed types" in refusal_of('Sales[Amount] = "5"')


def test_comparison_of_two_unknown_columns_is_refused():
    assert "unknown type" in refusal_of("Sales[Qty] = Sales[Cost]", kinds={})


def test_comparison_of_unknown_column_with_blank_is_refused():
    assert "ISBLANK" in refusal_of("Sales[Qty] = BLANK()", kinds={})


def test_blank_equals_blank_is_true():
    assert tr("BLANK() = BLANK()").sql == "TRUE"


def test_and_or_not_are_two_valued_conditions():
    sql = sql_of("(Sales[Amount] > 1 && Sales[Cost] > 1) || NOT(Sales[Flag])")
    assert sql == (
        "(((COALESCE(`Amount`, 0) > 1) AND (COALESCE(`Cost`, 0) > 1)) "
        "OR (NOT COALESCE(`Flag`, FALSE)))"
    )


def test_boolean_column_condition_reads_blank_as_false():
    assert (
        sql_of("IF(Sales[Flag], 1, 0)")
        == "(CASE WHEN COALESCE(`Flag`, FALSE) THEN 1 ELSE 0 END)"
    )


def test_mixing_and_or_without_parentheses_is_refused():
    assert "mixed without parentheses" in refusal_of(
        "Sales[Amount] > 1 && Sales[Cost] > 1 || Sales[Flag]"
    )


def test_strict_equality_is_refused():
    assert "==" in refusal_of("Sales[Amount] == 1")


def test_in_list_of_text_is_case_insensitive_or_of_equalities():
    sql = sql_of('Sales[Type] IN {"A", "B"}')
    assert sql == (
        "((lower(COALESCE(`Type`, '')) = 'a') OR (lower(COALESCE(`Type`, '')) = 'b'))"
    )


def test_in_list_of_numbers():
    assert sql_of("Sales[Qty] IN {1, 2}") == (
        "((COALESCE(`Qty`, 0) = 1) OR (COALESCE(`Qty`, 0) = 2))"
    )


def test_in_list_with_a_column_item_is_refused():
    assert "constants only" in refusal_of("Sales[Qty] IN {Sales[Cost]}")


def test_empty_in_list_is_refused():
    assert "empty IN list" in refusal_of("Sales[Qty] IN {}")


def test_isblank_of_column_and_of_constant():
    assert sql_of("ISBLANK(Sales[Name])") == "(`Name` IS NULL)"
    assert sql_of('ISBLANK("")') == "FALSE"


# -- IF, SWITCH, COALESCE, DIVIDE, BLANK ---------------------------------------


def test_if_with_two_arguments_is_blank_when_false():
    result = tr('IF(Sales[Flag], "yes")')
    assert result.sql == "(CASE WHEN COALESCE(`Flag`, FALSE) THEN 'yes' END)"
    assert result.may_be_blank is True


def test_if_with_three_arguments_is_not_blank_when_both_branches_are():
    result = tr("IF(Sales[Flag], 1, 0)")
    assert result.may_be_blank is False


def test_if_branches_of_different_kinds_are_refused():
    assert "mix text and integer" in refusal_of('IF(Sales[Flag], "x", 1)')


def test_if_branch_blank_makes_result_blank():
    assert tr("IF(Sales[Flag], 1, BLANK())").may_be_blank is True


def test_switch_on_a_value_uses_dax_equality_and_default():
    sql = sql_of('SWITCH(Sales[Type], "A", "Alpha", "B", "Beta", "Other")')
    assert sql == (
        "(CASE WHEN (lower(COALESCE(`Type`, '')) = 'a') THEN 'Alpha' "
        "WHEN (lower(COALESCE(`Type`, '')) = 'b') THEN 'Beta' ELSE 'Other' END)"
    )


def test_switch_without_default_is_blank_when_nothing_matches():
    result = tr('SWITCH(Sales[Type], "A", "Alpha")')
    assert result.may_be_blank is True


def test_switch_true_takes_the_first_true_condition():
    sql = sql_of(
        'SWITCH(TRUE(), Sales[Amount] > 10, "big", Sales[Amount] > 1, "mid", "small")'
    )
    assert sql == (
        "(CASE WHEN (COALESCE(`Amount`, 0) > 10) THEN 'big' "
        "WHEN (COALESCE(`Amount`, 0) > 1) THEN 'mid' ELSE 'small' END)"
    )


def test_switch_with_one_argument_is_refused():
    assert "SWITCH takes" in refusal_of("SWITCH(Sales[Flag])")


def test_coalesce_is_blank_only_when_every_argument_is_blank():
    assert tr("COALESCE(Sales[Name], 0)").may_be_blank is False
    assert tr("COALESCE(BLANK(), BLANK())").may_be_blank is True


def test_coalesce_of_text_and_number_is_refused():
    assert "mix" in refusal_of("COALESCE(Sales[Name], 1)")


def test_divide_two_arguments_is_blank_on_zero_denominator():
    result = tr("DIVIDE(Sales[Amount], Sales[Cost])")
    assert result.sql == "(COALESCE(`Amount`, 0) / NULLIF(COALESCE(`Cost`, 0), 0))"
    assert result.may_be_blank is True


def test_divide_with_constant_alternate_result():
    assert sql_of("DIVIDE(Sales[Amount], Sales[Cost], 0)") == (
        "COALESCE((COALESCE(`Amount`, 0) / NULLIF(COALESCE(`Cost`, 0), 0)), 0)"
    )


def test_divide_with_blank_alternate_result_is_blank():
    assert tr("DIVIDE(Sales[Amount], Sales[Cost], BLANK())").may_be_blank is True


def test_divide_alternate_result_must_be_constant():
    assert "must be a constant" in refusal_of(
        "DIVIDE(Sales[Amount], Sales[Cost], Sales[Qty])"
    )


# -- VAR ------------------------------------------------------------------------


def test_var_is_inlined_in_order():
    assert sql_of("VAR x = Sales[Amount] * 2 VAR y = x + 1 RETURN y") == (
        "((COALESCE(`Amount`, 0) * 2) + 1)"
    )


def test_var_names_are_case_insensitive():
    assert (
        sql_of("VAR Total = Sales[Qty] RETURN total + 1") == "(COALESCE(`Qty`, 0) + 1)"
    )


def test_var_chain_that_would_explode_is_refused_by_length():
    expression = "VAR v0 = Sales[Amount] VAR v1 = v0 + v0 VAR v2 = v1 + v1 "
    expression += " ".join(f"VAR v{i} = v{i - 1} + v{i - 1}" for i in range(3, 16))
    expression += " RETURN v15"
    assert "too large" in refusal_of(expression)


# -- RELATED --------------------------------------------------------------------


def test_related_becomes_left_join_and_blank_capable_column():
    result = tr(
        "RELATED(Region[Label])",
        related={"Region": REGION},
        source_alias="src",
    )
    assert result.translated
    assert result.sql == "rg.`Label`"
    assert result.may_be_blank is True
    assert [j.sql for j in result.joins] == [
        "LEFT JOIN ai.kasal.src_region AS rg ON src.`Type` = rg.`Code`"
    ]
    assert result.joins[0].table == "Region"


def test_related_twice_adds_one_join():
    result = tr(
        'RELATED(Region[Label]) = "x" || RELATED(Region[Label]) = "y"',
        related={"Region": REGION},
        source_alias="src",
    )
    assert len(result.joins) == 1


def test_related_needs_a_source_alias():
    assert "source view read under an alias" in refusal_of(
        "RELATED(Region[Label])", related={"Region": REGION}
    )


def test_related_without_a_supplied_relationship_is_refused():
    assert "no direct many-to-one" in refusal_of(
        "RELATED(Region[Label])", related={}, source_alias="src"
    )


def test_related_to_missing_related_column_is_refused():
    assert "not in the related view" in refusal_of(
        "RELATED(Region[Nope])", related={"Region": REGION}, source_alias="src"
    )


def test_related_of_own_table_is_refused():
    assert "own column" in refusal_of(
        "RELATED(Sales[Amount])", related={"Region": REGION}, source_alias="src"
    )


def test_related_takes_one_qualified_column():
    assert "qualified column" in refusal_of(
        "RELATED(Label)", related={"Region": REGION}, source_alias="src"
    )


# -- date functions ---------------------------------------------------------------


@pytest.mark.parametrize(
    ("expression", "expected"),
    [
        ("YEAR(Sales[Date])", "year(COALESCE(`Date`, DATE '1899-12-30'))"),
        ("MONTH(Sales[Date])", "month(COALESCE(`Date`, DATE '1899-12-30'))"),
        ("DAY(Sales[Date])", "dayofmonth(COALESCE(`Date`, DATE '1899-12-30'))"),
        (
            "EOMONTH(Sales[Date], 1)",
            "last_day(add_months(COALESCE(`Date`, DATE '1899-12-30'), CAST(ROUND(1) AS INT)))",
        ),
        (
            "EDATE(Sales[Date], -2)",
            "add_months(COALESCE(`Date`, DATE '1899-12-30'), CAST(ROUND((-2)) AS INT))",
        ),
    ],
)
def test_date_parts_and_month_shifts(expression, expected):
    assert sql_of(expression) == expected


def test_date_constructor_rolls_over_and_maps_two_digit_years():
    sql = sql_of("DATE(2008, 14, 2)")
    assert "date_add(add_months(make_date(" in sql
    assert "WHEN CAST(ROUND(2008) AS INT) BETWEEN 0 AND 49" in sql


def test_datediff_day_needs_date_kinds_and_keeps_argument_order():
    sql = sql_of(
        "DATEDIFF(Sales[Date], DATE(2024, 1, 1), DAY)", kinds={**KINDS, "Date": "date"}
    )
    assert sql.startswith("datediff(")
    assert sql.index("2024") > sql.index("datediff(")


@pytest.mark.parametrize("unit", ["MONTH", "YEAR", "QUARTER", "WEEK", "HOUR"])
def test_datediff_other_units_are_refused(unit):
    assert "DAY only" in refusal_of(
        f"DATEDIFF(Sales[Date], DATE(2024, 1, 1), {unit})",
        kinds={**KINDS, "Date": "date"},
    )


def test_datediff_of_unknown_kind_column_is_refused():
    assert "needs a date" in refusal_of(
        "DATEDIFF(Sales[Date], DATE(2024, 1, 1), DAY)", kinds={}
    )


@pytest.mark.parametrize(
    ("expression", "fragment"),
    [
        ("WEEKDAY(DATE(2024, 1, 7))", "dayofweek("),
        ("WEEKDAY(DATE(2024, 1, 7), 2)", "(pmod(dayofweek("),
        ("WEEKDAY(DATE(2024, 1, 7), 3)", "pmod(dayofweek("),
    ],
)
def test_weekday_return_types_one_two_three(expression, fragment):
    assert fragment in sql_of(expression)


@pytest.mark.parametrize("return_type", ["11", "12", "17", "0"])
def test_weekday_other_return_types_are_refused(return_type):
    assert "return_type" in refusal_of(f"WEEKDAY(DATE(2024, 1, 7), {return_type})")


def test_weeknum_iso_is_weekofyear():
    assert sql_of("WEEKNUM(DATE(2024, 1, 7), 21)").startswith("weekofyear(")


def test_weeknum_default_is_the_week_containing_january_first():
    sql = sql_of("WEEKNUM(DATE(2024, 1, 7))")
    assert "pmod(dayofweek(make_date(year(" in sql
    assert "- 1 + pmod(dayofweek(make_date(year(" in sql
    assert sql.endswith("/ 7) AS INT) + 1)")


@pytest.mark.parametrize("return_type", ["11", "17"])
def test_weeknum_other_return_types_are_refused(return_type):
    assert "return_type" in refusal_of(f"WEEKNUM(DATE(2024, 1, 7), {return_type})")


@pytest.mark.parametrize(
    "expression",
    ["TODAY()", "NOW()"],
)
def test_volatile_dates_are_refused(expression):
    assert "changes" in refusal_of(expression)


# -- refusals -------------------------------------------------------------------


@pytest.mark.parametrize(
    ("expression", "fragment"),
    [
        ("RELATEDTABLE(Region)", "row context"),
        ("CALCULATE(SUM(Sales[Amount]))", "filter context"),
        ("FILTER(Sales, Sales[Amount] > 1)", "row context"),
        ("SUMX(Sales, Sales[Amount])", "row context"),
        ("EARLIER(Sales[Amount])", "outer row"),
        ("LOOKUPVALUE(Region[Label], Region[Code], Sales[Type])", "LEFT JOIN"),
        ("RANKX(ALL(Sales), Sales[Amount])", "ranks over a table"),
        ("PATHITEM(Sales[Name], 1)", "parent-child"),
        ("USERELATIONSHIP(Sales[Date], Region[Code])", "inactive relationship"),
        ("IFERROR(Sales[Amount], 0)", "not translated"),
        ("SELECTEDVALUE(Sales[Type])", "filter context"),
    ],
)
def test_named_refusals(expression, fragment):
    assert fragment in refusal_of(expression)


# -- parsing and inputs ------------------------------------------------------------


def test_empty_formula_is_refused():
    assert tr("   ").refusal == "the formula is empty"


def test_unterminated_string_is_refused():
    assert "unterminated" in refusal_of('"abc')


def test_power_and_unexpected_characters_are_refused():
    assert "unexpected character" in refusal_of("Sales[Amount] @ 2")


def test_exponent_number_format_is_refused():
    assert "1E3" in refusal_of("Sales[Amount] * 1E3")


def test_comments_are_skipped():
    assert sql_of("Sales[Qty] // a note\n + 1") == "(COALESCE(`Qty`, 0) + 1)"


def test_invalid_column_kind_is_a_caller_error():
    with pytest.raises(ValueError, match="column_kinds"):
        tr("Sales[Qty]", kinds={"Qty": "float"})


def test_invalid_source_alias_is_a_caller_error():
    with pytest.raises(ValueError, match="source_alias"):
        tr("Sales[Qty]", source_alias="src; drop")


def test_related_alias_equal_to_source_alias_is_a_caller_error():
    clash = RelatedTable(
        alias="src",
        view="ai.kasal.src_region",
        from_column="Type",
        to_column="Code",
        columns=("Code", "Label"),
    )
    with pytest.raises(ValueError, match="equals source_alias"):
        tr("RELATED(Region[Label])", related={"Region": clash}, source_alias="src")


def test_translation_result_reports_its_kind():
    assert tr("Sales[Qty] > 1").kind == "boolean"
    assert tr('"a" & "b"').kind == "text"
    assert tr("Sales[Qty] + 1").kind == "integer"
