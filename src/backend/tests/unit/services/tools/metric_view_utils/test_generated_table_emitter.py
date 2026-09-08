"""Tests for generated_table_emitter (KASAL_FIXES Gap 3 materialization) — turning a
Power Query List.Dates calendar into a CREATE VIEW. Fixture mirrors the JTI date dim."""

from src.services.tools.metric_view_utils.generated_table_emitter import (
    emit_view_sql,
)

M_DATE = """let
    StartDate = #date(2020, 1, 1),
    EndDate = #date(2028, 12, 31),
    DateList = List.Dates(StartDate, Duration.Days(EndDate - StartDate) + 1, #duration(1,0,0,0)),
    DateTable = Table.FromList(DateList, Splitter.SplitByNothing(), {"Date"}),
    AddYear = Table.AddColumn(DateTable, "Year", each Date.Year([Date]), Int64.Type),
    AddQuarter = Table.AddColumn(AddYear, "Quarter", each "Q" & Text.From(Date.QuarterOfYear([Date]))),
    AddMonth = Table.AddColumn(AddQuarter, "Month", each Date.ToText([Date], "MMM")),
    AddWeekDay = Table.AddColumn(AddMonth, "Week Day", each Date.ToText([Date], "dddd")),
    AddWeekDayNo = Table.AddColumn(AddWeekDay, "Week Day Number", each Date.DayOfWeek([Date], Day.Monday) + 1, Int64.Type),
    AddDateInteger = Table.AddColumn(AddWeekDayNo, "Date Integer", each Date.Year([Date]) * 10000 + Date.Month([Date]) * 100 + Date.Day([Date]), Int64.Type)
in AddDateInteger"""

M_NAV = 'let Source = Databricks.Catalogs(...), #"Navigation" = Source{[Item="market_dim"]}[Data] in Navigation'


def test_emits_create_view_with_range_and_columns():
    sql = emit_view_sql(M_DATE, "`cat`.`sch`.`date`")
    assert sql is not None
    assert "CREATE OR REPLACE VIEW `cat`.`sch`.`date`" in sql
    assert "sequence(DATE'2020-01-01', DATE'2028-12-31', INTERVAL 1 DAY)" in sql
    # column translations
    assert "year(d) AS `Year`" in sql
    assert "date_format(d, 'MMM') AS `Month`" in sql
    assert "date_format(d, 'EEEE') AS `Week Day`" in sql  # dddd -> EEEE
    assert "weekday(d) + 1 AS `Week Day Number`" in sql
    assert "year(d) * 10000 + month(d) * 100 + day(d) AS `Date Integer`" in sql
    assert "concat" not in sql.lower() or "||" in sql  # '&' handled


def test_quarter_concat_translation():
    sql = emit_view_sql(M_DATE, "`c`.`s`.`date`")
    # "Q" & Text.From(Date.QuarterOfYear([Date])) -> 'Q' || string(quarter(d))
    assert "'Q' || string(quarter(d)) AS `Quarter`" in sql


def test_non_generated_returns_none():
    assert emit_view_sql(M_NAV, "`c`.`s`.`market_dim`") is None
    assert emit_view_sql("", "`c`.`s`.`x`") is None


def test_untranslatable_column_becomes_todo_not_dropped():
    m = """let A = #date(2021,1,1), B = #date(2021,12,31),
        L = List.Dates(A, 10, #duration(1,0,0,0)),
        T = Table.FromList(L, Splitter.SplitByNothing(), {"Date"}),
        C = Table.AddColumn(T, "Weird", each Time.Hour([Date]), Int64.Type)
    in C"""
    sql = emit_view_sql(m, "`c`.`s`.`cal`")
    assert sql is not None
    assert (
        "TODO translate M" in sql and "`Weird`" in sql
    )  # kept as placeholder, not dropped
