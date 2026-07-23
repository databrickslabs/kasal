"""DAX to SQL Translator Tool for CrewAI — standalone DAX→SQL translation."""
import json
import logging
from typing import Any, Optional, Type

from crewai.tools import BaseTool
from pydantic import BaseModel, Field, PrivateAttr

logger = logging.getLogger(__name__)


class DaxToSqlTranslatorSchema(BaseModel):
    """Input schema for DaxToSqlTranslatorTool."""
    dax_measures_json: Optional[str] = Field(
        None, description="JSON string containing DAX measures to translate. "
        "Each entry: {measure_name, dax_expression, proposed_allocation}")
    table_key: Optional[str] = Field(
        None, description="Target fact table key for context (e.g. 'fact_pe002')")
    config_json: Optional[str] = Field(
        None, description="JSON string with pipeline config overrides "
        "(filter_sets, column_overrides, measure_resolutions, fact_join_map)")


class DaxToSqlTranslatorTool(BaseTool):
    """Translate DAX expressions to Spark SQL using pattern-based rules."""
    name: str = "DAX to SQL Translator"
    description: str = (
        "Translate Power BI DAX measure expressions to Databricks Spark SQL. "
        "Supports 14+ DAX patterns including SUM, SUMX+FILTER, CALCULATE, DIVIDE, "
        "COUNTX, AVERAGEX, SAMEPERIODLASTYEAR, and SELECTEDVALUE+SWITCH detection. "
        "Input: JSON array of measures with dax_expression fields. "
        "Output: JSON array with sql_expr, confidence, and skip_reason per measure."
    )
    args_schema: Type[BaseModel] = DaxToSqlTranslatorSchema
    _default_config: dict = PrivateAttr(default_factory=dict)

    model_config = {"arbitrary_types_allowed": True, "extra": "allow"}

    def __init__(self, **kwargs: Any) -> None:
        default_config = {}
        for key in ('config_json', 'filter_sets', 'column_overrides',
                     'measure_resolutions', 'fact_join_map'):
            val = kwargs.pop(key, None)
            if val is not None:
                default_config[key] = val
        super().__init__(**kwargs)
        self._default_config = default_config

    def _run(self, **kwargs: Any) -> str:
        from src.engines.crewai.tools.custom.metric_view_utils.dax_translator import DaxTranslator
        from src.engines.crewai.tools.custom.metric_view_utils.utils import to_snake_case

        measures_json = kwargs.get('dax_measures_json') or self._default_config.get('dax_measures_json', '[]')
        table_key = kwargs.get('table_key') or self._default_config.get('table_key', '')
        config_json = kwargs.get('config_json') or self._default_config.get('config_json', '{}')

        try:
            measures = json.loads(measures_json) if isinstance(measures_json, str) else measures_json
            config = json.loads(config_json) if isinstance(config_json, str) else config_json
        except json.JSONDecodeError as e:
            return json.dumps({"error": f"Invalid JSON: {e}"})

        translator = DaxTranslator(config=config)
        results = []
        for m in measures:
            result = translator.translate(m, table_key)
            results.append({
                'measure_name': result.measure_name,
                'original_name': result.original_name,
                'sql_expr': result.sql_expr,
                'is_translatable': result.is_translatable,
                'skip_reason': result.skip_reason,
                'confidence': result.confidence,
                'category': result.category,
            })

        translated = sum(1 for r in results if r['is_translatable'])
        total = len(results)
        return json.dumps({
            'results': results,
            'summary': {
                'total': total,
                'translated': translated,
                'untranslatable': total - translated,
                'rate': f'{translated * 100 // total}%' if total else '0%',
            }
        }, indent=2)
