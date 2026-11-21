"""Power BI "enter data" tables: their rows are in the Power Query, so the source layer gets a
deployable view over VALUES without any data access to Power BI."""

import base64
import json
import zlib

from src.services.tools.metric_view_utils.embedded_tables import (
    decode_embedded_table,
    embedded_table_sql,
)
from src.services.tools.metric_view_utils.source_artifacts import _embedded_read


def _deflate(rows):
    raw = zlib.compressobj(9, zlib.DEFLATED, -15)
    return base64.b64encode(
        raw.compress(json.dumps(rows).encode()) + raw.flush()
    ).decode()


ROWS = [["Engaged Talent", "1", "it's"], ["Safety", "2", None]]
M = (
    'let\n    Source = Table.FromRows(Json.Document(Binary.Decompress(Binary.FromText("'
    + _deflate(ROWS)
    + '", BinaryEncoding.Base64), Compression.Deflate)), let _t = ((type nullable text) meta '
    '[Serialized.Text = true]) in type table [Tab = _t, Seq_ID = _t, #"AC_UC_%age" = _t]),\n'
    '    #"Changed Type" = Table.TransformColumnTypes(Source,{{"Tab", type text}, {"Seq_ID", Int64.Type}})\n'
    'in\n    #"Changed Type"'
)


def test_rows_and_columns_are_decoded():
    assert decode_embedded_table(M) == (["Tab", "Seq_ID", "AC_UC_%age"], ROWS)
    assert (
        decode_embedded_table('let Source = Sql.Database("s", "d") in Source') is None
    )


def test_the_rows_become_values_with_escaped_literals():
    sql = embedded_table_sql(M)
    assert sql.startswith("SELECT Tab, Seq_ID, `AC_UC_%age` FROM VALUES")
    assert "('Engaged Talent', '1', 'it''s')" in sql and "('Safety', '2', NULL)" in sql


def test_the_source_layer_read_folds_the_steps_and_keeps_every_column():
    todos = {}
    sql = _embedded_read("Mapping", M, todos)
    assert "CAST(Seq_ID AS BIGINT) AS Seq_ID" in sql
    assert "`AC_UC_%age`" in sql.split("FROM (")[0]  # a quoted column is not lost
    assert "Binary.FromText" not in sql and todos == {}


def test_an_excel_table_is_loaded_where_its_joins_expect_it():
    from src.services.tools.metric_view_utils.source_artifacts import detect_pbi_only_tables

    m = 'let Source = Excel.Workbook(Web.Contents("https://example.com/x.xlsx"), null, true) in Source'
    (task,) = detect_pbi_only_tables({"Dim_Department": m}, "cat", "sch")
    # the join detector's placeholder for this dimension is {catalog}.{schema}.dim_department
    assert task["uc_target"] == "`cat`.`sch`.`dim_department`"
