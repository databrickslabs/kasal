"""Power BI -> SQL never reports a measure with no SQL as translated.

A measure no template matches used to come back ``is_transpilable=True`` with
``transpiled_sql=None``; the pipeline then emitted ``sql: None`` / ``expr: None``
and the run still reported success. It is now a reported gap with no SQL.
"""

from unittest.mock import MagicMock, patch

from src.services.converters.base.connectors import ConnectorType
from src.services.converters.base.models import KPI, KPIDefinition
from src.services.converters.formats.powerbi.dax_parser import DAXExpressionParser
from src.services.converters.formats.powerbi.dax_to_sql import DaxToSqlTranspiler
from src.services.converters.pipeline import (
    ConversionPipeline,
    OutboundFormat,
    translation_gaps,
)

UNMATCHED = "DIVIDE(SUM(Sales[Amount]), SUM(Sales[Qty]))"


def _definition() -> KPIDefinition:
    parser = DAXExpressionParser()
    kpis = []
    for name, dax in (("revenue", "SUM(Sales[Amount])"), ("avg_price", UNMATCHED)):
        parsed = parser.parse_advanced(dax)
        kpi = KPI(
            description=name,
            technical_name=name,
            formula=parsed["base_formula"],
            source_table="Sales",
        )
        kpi._advanced_parsing = {
            "transpiled_sql": parsed["transpiled_sql"],
            "is_transpilable": parsed["is_transpilable"],
            "transpilability_reason": parsed["transpilability_reason"],
            "signature": parsed["generic_signature"],
        }
        kpis.append(kpi)
    return KPIDefinition(description="t", technical_name="t", kpis=kpis)


class TestTranspileNoneIsAGap:
    def test_translate_reports_reason_when_no_template_matches(self):
        sql, reason = DaxToSqlTranspiler().translate("nothing ( matches )", [])
        assert sql is None
        assert reason

    def test_parse_advanced_marks_unmatched_measure_not_transpilable(self):
        parsed = DAXExpressionParser().parse_advanced(UNMATCHED)
        assert parsed["transpiled_sql"] is None
        assert parsed["is_transpilable"] is False
        assert parsed["transpilability_reason"]

    def test_matched_measure_is_still_translated(self):
        parsed = DAXExpressionParser().parse_advanced("SUM(Sales[Amount])")
        assert parsed["is_transpilable"] is True
        assert parsed["transpiled_sql"] == "SUM(`Amount`)"


class TestPipelineReportsGaps:
    def test_sql_output_has_no_sql_for_the_gap(self):
        out = ConversionPipeline()._format_transpiled_sql(_definition(), {})
        by_name = {m["name"]: m for m in out}
        assert by_name["revenue"]["sql"] == "SUM(`Amount`)"
        assert by_name["revenue"]["status"] == "translated"
        gap = by_name["avg_price"]
        assert gap["sql"] is None
        assert gap["is_transpilable"] is False
        assert gap["status"] == "gap"
        assert gap["gap_reason"]

    def test_stale_transpilable_flag_without_sql_is_a_gap(self):
        definition = _definition()
        definition.kpis[1]._advanced_parsing.update(
            {"is_transpilable": True, "transpiled_sql": None}
        )
        out = ConversionPipeline()._format_transpiled_sql(definition, {})
        assert out[1]["sql"] is None and out[1]["status"] == "gap"

    def test_uc_metrics_output_emits_no_expr_for_the_gap(self):
        yaml_text = ConversionPipeline()._format_transpiled_uc_metrics(
            _definition(), {}
        )
        assert "expr: None" not in yaml_text
        assert "name: avg_price" not in yaml_text
        assert "GAP: avg_price" in yaml_text
        assert "expr: SUM(`Amount`)" in yaml_text

    @patch("src.services.converters.pipeline.PowerBIConnector")
    def test_execute_lists_the_gaps(self, mock_connector_class):
        connector = MagicMock()
        connector.extract_to_definition.return_value = _definition()
        connector.__enter__.return_value = connector
        connector.__exit__.return_value = None
        mock_connector_class.return_value = connector

        result = ConversionPipeline().execute(
            inbound_type=ConnectorType.POWERBI,
            inbound_params={"semantic_model_id": "m", "group_id": "g"},
            outbound_format=OutboundFormat.SQL,
        )
        assert [g["name"] for g in result["gaps"]] == ["avg_price"]
        assert translation_gaps(result["definition"]) == result["gaps"]


class TestToolOutputShowsGaps:
    def test_measure_conversion_tool_marks_partial_and_gap(self):
        from src.services.tools.measure_conversion_pipeline_tool import (
            MeasureConversionPipelineTool,
        )

        definition = _definition()
        output = ConversionPipeline()._format_transpiled_sql(definition, {})
        text = MeasureConversionPipelineTool()._format_output(
            output=output,
            inbound_connector="powerbi",
            outbound_format="sql",
            measure_count=2,
            source_id="model",
            gaps=translation_gaps(definition),
        )
        assert "Partial conversion" in text
        assert "1 of 2 measures" in text
        assert "-- GAP: not translated" in text
        assert "None" not in text
