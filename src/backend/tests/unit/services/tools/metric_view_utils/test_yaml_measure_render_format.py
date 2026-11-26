"""Every rendered measure bucket writes its display format."""

from src.services.tools.metric_view_utils.data_classes import TranslationResult
from src.services.tools.metric_view_utils.metadata_generator import MetadataGenerator
from src.services.tools.metric_view_utils.yaml_measure_render import (
    render_base_measures,
    render_dax_measures,
    render_implicit_measures,
    render_switch_measures,
)


def _tr(name, expr, original=None):
    return TranslationResult(
        measure_name=name,
        original_name=original or name,
        sql_expr=expr,
        is_translatable=True,
        skip_reason="",
        dax_expression="",
        confidence="high",
        category="single_table",
    )


def _text(lines):
    return "\n".join(lines)


def test_base_measure_gets_format_from_pbi_format_string():
    lines: list[str] = []
    gen = MetadataGenerator(pbi_format_strings={"fill_rate": "0.00%"})
    render_base_measures(lines, [_tr("fill_rate", "SUM(a)/SUM(b)")], {}, gen)
    text = _text(lines)
    assert "      type: percentage" in text
    assert "        type: exact" in text


def test_dax_measure_format_from_name_and_override_wins():
    lines: list[str] = []
    gen = MetadataGenerator()
    overrides = {
        "total_cost": {
            "format": {"type": "number", "decimal_places": {"type": "max", "places": 0}}
        }
    }
    render_dax_measures(
        lines, [_tr("total_cost", "SUM(a)")], overrides, gen, "_budget", None
    )
    text = _text(lines)
    assert "      type: number" in text
    assert "        places: 0" in text


def test_switch_measure_now_has_a_format():
    lines: list[str] = []
    gen = MetadataGenerator()
    render_switch_measures(lines, [_tr("sales_actual", "SUM(a)")], gen)
    assert "    format:" in _text(lines)


def test_implicit_measure_now_has_a_format():
    lines: list[str] = []
    gen = MetadataGenerator()
    render_implicit_measures(lines, [_tr("order_count", "COUNT(a)")], gen)
    text = _text(lines)
    assert "    format:" in text
    assert "        type: max" in text
