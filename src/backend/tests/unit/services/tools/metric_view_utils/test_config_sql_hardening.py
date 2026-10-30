"""Config- and model-derived names stay out of metric-view SQL as code.

Apps security scan follow-up (2026-10-28, group A). Each of these reached SQL a
metric view deploys or a draft a person runs, unquoted or unescaped:

* join_detector: config ``kbi_codes`` became a literal AND an alias; config
  dedup / pivot identifiers went in raw.
* pipeline: ``switch_join_alias`` / ``switch_join_col`` went in raw.
* dax_translator: a ``*/`` in a measure name closed the UNRESOLVED comment;
  config-mapped physical columns went in raw.
* m_transform_folder: ``Text.StartsWith`` prefix / replacement went in raw.
* recovery_recommender / generated_table_emitter: draft names unescaped.
"""

import pytest

from src.services.tools.metric_view_utils.data_classes import MStep
from src.services.tools.metric_view_utils.dax_translator import DaxTranslator
from src.services.tools.metric_view_utils.generated_table_emitter import (
    emit_view_sql,
)
from src.services.tools.metric_view_utils.join_detector import JoinDetector
from src.services.tools.metric_view_utils.m_transform_folder import MTransformFolder
from src.services.tools.metric_view_utils.pipeline import MetricViewPipeline
from src.services.tools.metric_view_utils.recovery_recommender import (
    draft_source_view,
)
from tests.unit.services.tools.metric_view_utils.test_join_detector import (
    _make_table,
)
from tests.unit.services.tools.metric_view_utils.test_sql_literal_security import (
    _outside_literals,
)

TABLES = {
    "fact": _make_table("fact", "cat.sch.fact", ["key"]),
    "kbi": _make_table("kbi", "cat.sch.kbi", ["key", "code", "val"]),
}
PIVOT = {
    "alias": "k",
    "join_key": "key",
    "pivot_col": "code",
    "value_col": "val",
    "grain": ["key"],
}


def _pivot_joins(fact_join):
    detector = JoinDetector(TABLES, {"fact_join_map": {"kbi": fact_join}})
    measures = [{"dax_expression": "", "direct_fact_refs": ["kbi"]}]
    return detector.detect_fact_joins("fact", measures, TABLES["fact"])


class TestJoinDetector:
    def test_a_hostile_config_kbi_code_is_dropped(self):
        joins = _pivot_joins({**PIVOT, "kbi_codes": ["AB1", "x') OR 1=1 --"]})
        source = joins[0]["source"]
        assert "sc_ab1" in source
        assert "OR 1=1" not in source

    def test_a_hostile_pivot_identifier_skips_the_join(self):
        assert _pivot_joins({**PIVOT, "pivot_col": "code; DROP TABLE t"}) == []

    def test_a_hostile_dedup_order_is_not_applied(self):
        detector = JoinDetector(TABLES, {})
        jk = {"dedup_dim": True, "dedup_order_by": "x; DROP TABLE t"}
        assert detector._maybe_dedup_source("cat.sch.dim", "dkey", jk) == "cat.sch.dim"

    def test_a_column_order_still_dedups(self):
        detector = JoinDetector(TABLES, {})
        jk = {"dedup_dim": True, "dedup_order_by": "updated_at DESC, id"}
        sql = detector._maybe_dedup_source("cat.sch.dim", "dkey", jk)
        assert "ORDER BY updated_at DESC, id" in sql


class TestSwitchJoinIdentifiers:
    def test_a_hostile_join_alias_is_refused(self):
        defn = {"name": "m", "num": "a", "den": "b", "num_fs": "A", "den_fs": "A"}
        pipeline = MetricViewPipeline(mapping=[], mquery_tables={})
        with pytest.raises(ValueError):
            pipeline._build_switch_measure(
                defn, join_alias="d) OR (1=1", join_col="code", filter_sets={"A": ["x"]}
            )


class TestDaxTranslator:
    def test_a_measure_name_cannot_close_or_nest_the_unresolved_comment(self):
        # Spark SQL nests /* */ comments, so an inner "/*" must go too.
        sql = DaxTranslator()._resolve_remaining_dax("[x */ DROP TABLE t /* y]")
        assert "*/ DROP" not in sql
        assert sql.count("/*") == 1 and sql.count("*/") == 1


class TestMTransformLiterals:
    def test_starts_with_values_stay_literals(self):
        steps = [
            MStep(
                step_type="TransformColumns",
                raw_expression=(
                    'Table.TransformColumns(prev, {{"col", each if Text.StartsWith(_, '
                    '"a\\") then "\') OR 1=1 --" else _}})'
                ),
            )
        ]
        expr = MTransformFolder()._build_column_transforms(steps, {}, [])["col"]
        assert "OR 1=1" not in _outside_literals(expr)


class TestDrafts:
    def test_fact_names_are_escaped_and_quoted(self):
        dax = "SUM('Fact A'[x]) + SUM('Fact\\B'[y])"
        draft = draft_source_view(dax)
        assert "FROM `Fact A`" in draft
        assert "'Fact\\\\B' AS _src" in draft

    def test_generated_view_names_and_notes_cannot_break_out(self):
        m = (
            "let\n"
            "    StartDate = #date(2020, 1, 1),\n"
            "    EndDate = #date(2020, 12, 31),\n"
            "    DateList = List.Dates(StartDate, Duration.Days(EndDate - StartDate) + 1,"
            " #duration(1,0,0,0)),\n"
            '    DateTable = Table.FromList(DateList, Splitter.SplitByNothing(), {"Date"}),\n'
            '    A = Table.AddColumn(DateTable, "Ye`ar", each Date.Year([Date]), Int64.Type),\n'
            '    B = Table.AddColumn(A, "Odd", each Date.Weird([Date]) */ DROP TABLE t),\n'
            '    C = Table.AddColumn(B, "Label", each if [Date] > 1 then "a\'b" else null)\n'
            "in\n    C"
        )
        sql = emit_view_sql(m, "`cat`.`sch`.`date`") or ""
        assert "`Ye``ar`" in sql
        assert "*/ DROP" not in sql  # untranslatable body, kept as a TODO comment
        assert "'a''b'" in sql  # M string escaped as a SQL literal
