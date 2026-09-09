"""Tests for physical_name_resolver (KASAL_FIXES Gaps 1–3) — resolving PBI
friendly table names and model column names to the real physical names from the
Power Query M. Fixtures mirror the JTI ConsumerExperience cases."""

from types import SimpleNamespace

from src.services.tools.metric_view_utils.data_classes import (
    MetricViewSpec,
    TableInfo,
    TranslationResult,
)
from src.services.tools.metric_view_utils.physical_name_resolver import (
    physical_table,
    is_generated,
    column_map,
    resolve_physical_names,
    _norm,
)

# ── M expressions (trimmed to the shape the resolver reads) ──
M_SURVEY_FACT = '''let
    Source = Value.NativeQuery(Databricks.Catalogs(...){[Name=#"Default Catalog",Kind="Database"]}[Data],
    "SELECT id_profile, ResponseId FROM `" & #"Default Catalog" & "`." & #"Schema Market" & ".survey_responses", null, [EnableFolding=true]),
    #"Renamed Columns" = Table.RenameColumns(Source,{{"ResponseId","Response Id"},{"ChoiceId","Choice Id"},{"CSATRespondentCategory","CSAT Respondent Category"}})
in #"Renamed Columns"'''
M_QUESTION = '''let
    Source = Databricks.Catalogs(...),
    #"Navigation" = Source{[Item="survey_questions",Schema=#"Schema Market"]}[Data],
    #"Renamed Columns" = Table.RenameColumns(Navigation,{{"GlobalIdQuestion","Global Question Id"}})
in #"Renamed Columns"'''
M_MARKET = '''let
    Source = Databricks.Catalogs(...),
    #"Navigation" = Source{[Item="market_dim"]}[Data],
    #"Renamed Columns" = Table.RenameColumns(Navigation,{{"_TF_MARKET","Market"}})
in #"Renamed Columns"'''
M_SUPERVISOR = """let Source = Databricks.Catalogs(...), #"Navigation" = Source{[Item="dim_supervisor"]}[Data] in Navigation"""
M_DATE = """let StartDate=#date(2020,1,1), DateList=List.Dates(StartDate,100,#duration(1,0,0,0)) in DateList"""


def test_physical_table_from_navigation_and_nativequery():
    assert physical_table(M_QUESTION) == "survey_questions"
    assert physical_table(M_MARKET) == "market_dim"
    assert physical_table(M_SUPERVISOR) == "dim_supervisor"
    assert physical_table(M_SURVEY_FACT) == "survey_responses"


def test_generated_table_has_no_physical_source():
    assert is_generated(M_DATE) is True
    assert physical_table(M_DATE) is None
    assert is_generated(M_QUESTION) is False


def test_column_map_recovers_renamed_and_camelcase():
    cm = column_map(M_QUESTION, ["Global Question Id", "KeyQuestion", "Survey Id"])
    # renamed: display 'Global Question Id' -> physical 'GlobalIdQuestion'
    assert cm[_norm("global_question_id")] == "GlobalIdQuestion"
    # NOT renamed camelCase: token 'key_question' must resolve to physical 'KeyQuestion'
    assert cm[_norm("key_question")] == "KeyQuestion"


def _spec():
    fact = TranslationResult(
        measure_name="nps",
        original_name="NPS",
        is_translatable=True,
        skip_reason="",
        dax_expression="",
        confidence="high",
        category="dax",
        sql_expr="AVG(TRY_CAST(source.choice_id AS DOUBLE)) FILTER (WHERE dim_question.global_question_id = 'Q1')",
    )
    return MetricViewSpec(
        fact_table_key="Survey Response Online Fact",
        source_table="cat.sch.survey_response_online_fact",
        view_name="mv",
        comment="",
        joins=[
            {
                "name": "dim_question",
                "source": "cat.sch.question",
                "join_on": "source.key_question = dim_question.key_question",
            },
            {
                "name": "dim_market",
                "source": "cat.sch.market",
                "join_on": "source.market = dim_market.market",
            },
        ],
        dimensions=[
            {"name": "gq", "expr": "dim_question.global_question_id"},
            {"name": "mk", "expr": "dim_market.market"},
        ],
        measures=[fact],
        untranslatable=[],
    )


def _tables():
    return {
        "Survey Response Online Fact": TableInfo(
            "Survey Response Online Fact",
            "cat.sch.survey_response_online_fact",
            [{"name": "Choice Id", "source_col": "Choice Id"}],
            ["Profile Id", "Response Id", "Market"],
            [],
            True,
            "",
        ),
        "Question": TableInfo(
            "Question",
            "cat.sch.question",
            [],
            ["Global Question Id", "KeyQuestion", "Survey Id"],
            [],
            False,
            "",
        ),
        "Market": TableInfo("Market", "cat.sch.market", [], ["Market"], [], False, ""),
    }


def _m():
    return {
        "Survey Response Online Fact": M_SURVEY_FACT,
        "Question": M_QUESTION,
        "Market": M_MARKET,
    }


def test_resolve_rewrites_tables_and_columns():
    specs = {"Survey Response Online Fact": _spec()}
    report = resolve_physical_names(specs, _tables(), _m())
    spec = specs["Survey Response Online Fact"]
    # Gap 1: join sources -> physical
    srcs = {j["name"]: j["source"] for j in spec.joins}
    assert srcs["dim_question"].endswith(".survey_questions")
    assert srcs["dim_market"].endswith(".market_dim")
    # Gap 2: columns -> physical (camelCase + rename recovered)
    assert "dim_question.KeyQuestion" in spec.joins[0]["join_on"]
    assert "dim_market._TF_MARKET" in spec.joins[1]["join_on"]
    assert spec.dimensions[0]["expr"] == "dim_question.GlobalIdQuestion"
    assert "dim_question.GlobalIdQuestion = 'Q1'" in spec.measures[0].sql_expr
    # 'Choice Id' (display) was renamed in M from physical 'ChoiceId' → resolve to that
    assert "source.ChoiceId" in spec.measures[0].sql_expr
    assert report["tables_resolved"] >= 2


def test_generated_table_reported():
    specs = {"Survey Response Online Fact": _spec()}
    report = resolve_physical_names(specs, _tables(), {**_m(), "Date": M_DATE})
    assert "Date" in report["generated_tables"]


def test_fail_open_without_m():
    specs = {"Survey Response Online Fact": _spec()}
    before = specs["Survey Response Online Fact"].joins[0]["source"]
    report = resolve_physical_names(specs, _tables(), {})
    assert specs["Survey Response Online Fact"].joins[0]["source"] == before
    assert report["tables_resolved"] == 0


def test_unknown_column_left_untouched():
    specs = {"Survey Response Online Fact": _spec()}
    specs["Survey Response Online Fact"].measures[
        0
    ].sql_expr = "COUNT(source.some_unknown_col)"
    resolve_physical_names(specs, _tables(), _m())
    assert (
        "source.some_unknown_col"
        in specs["Survey Response Online Fact"].measures[0].sql_expr
    )
