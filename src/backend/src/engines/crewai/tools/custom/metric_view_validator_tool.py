"""Metric View Validator Tool — validates generated UC Metric Views against DAX expressions.

Wraps MetricExpressionValidatorPipeline as a CrewAI tool. Accepts the UCMV
Generator output (YAML + measures JSON) and produces a per-measure validation
report with VALID/EQUIVALENT/REVIEW/INVALID status.
"""
import json
import logging
import os
import tempfile
from typing import Any, Optional, Type

from crewai.tools import BaseTool
from pydantic import BaseModel, Field, PrivateAttr

logger = logging.getLogger(__name__)


class MetricViewValidatorSchema(BaseModel):
    """Input schema for MetricViewValidatorTool."""

    yaml_content: Optional[str] = Field(
        None, description="JSON dict of table_key → YAML string (from UCMV Generator output)")
    measures_json: Optional[str] = Field(
        None, description="JSON string of measure_table_mapping (same input as UCMV Generator)")


class MetricViewValidatorTool(BaseTool):
    """Validate generated UC Metric Views against original DAX expressions."""

    name: str = "Metric View Validator"
    description: str = (
        "Validates generated UC Metric View YAML definitions against the original "
        "DAX expressions. Compares each translated measure's SQL expression with "
        "the source DAX to detect semantic mismatches, missing filters, or "
        "incorrect aggregations. Returns VALID/EQUIVALENT/REVIEW/INVALID per measure."
    )
    args_schema: Type[BaseModel] = MetricViewValidatorSchema
    _default_config: dict = PrivateAttr(default_factory=dict)

    model_config = {"arbitrary_types_allowed": True, "extra": "allow"}

    def __init__(self, **kwargs: Any) -> None:
        config_keys = ('yaml_content', 'measures_json')
        default_config = {}
        for key in config_keys:
            val = kwargs.pop(key, None)
            if val is not None:
                default_config[key] = val
        super().__init__(**kwargs)
        self._default_config = default_config

    def _run(self, **kwargs: Any) -> str:
        """Execute the validation pipeline."""
        from src.engines.crewai.tools.custom.metric_view_validation_utils.pipeline import (
            MetricExpressionValidatorPipeline,
        )

        def _get(key: str) -> Any:
            val = kwargs.get(key)
            if val is not None:
                return val
            return self._default_config.get(key)

        yaml_raw = _get('yaml_content') or '{}'
        measures_raw = _get('measures_json') or '[]'

        try:
            yaml_tables = json.loads(yaml_raw) if isinstance(yaml_raw, str) else yaml_raw
            measures = json.loads(measures_raw) if isinstance(measures_raw, str) else measures_raw
        except json.JSONDecodeError as e:
            return json.dumps({"error": f"Invalid JSON input: {e}"})

        if not yaml_tables:
            return json.dumps({"error": "No YAML content provided. Pass the UCMV Generator output."})

        # Build table→mapping lookup from measures
        table_mapping = {}
        for m in measures:
            table = m.get('proposed_allocation', m.get('table_name', ''))
            if table and table != '__unassigned__':
                table_mapping[table] = 'source'

        results = {}
        total_evaluated = 0
        total_valid = 0
        total_equivalent = 0
        total_review = 0
        total_invalid = 0

        for table_key, yaml_str in yaml_tables.items():
            if not yaml_str or not yaml_str.strip():
                continue

            try:
                # Write YAML to temp file (validator expects file paths)
                with tempfile.NamedTemporaryFile(
                    mode='w', suffix='.yml', delete=False, prefix=f'mv_{table_key}_'
                ) as yf:
                    yf.write(yaml_str)
                    yaml_path = yf.name

                # Write measures for this table to temp JSON
                table_measures = [m for m in measures
                                  if m.get('proposed_allocation') == table_key
                                  or m.get('table_name') == table_key]
                with tempfile.NamedTemporaryFile(
                    mode='w', suffix='.json', delete=False, prefix=f'map_{table_key}_'
                ) as mf:
                    json.dump(table_measures, mf)
                    mapping_path = mf.name

                if not table_measures:
                    results[table_key] = {"skipped": "No measures found for this table"}
                    continue

                pipeline = MetricExpressionValidatorPipeline(
                    table_mappings={table_key: 'source'},
                )
                result = pipeline.run(
                    metrics_view_yaml_path=yaml_path,
                    table_mapping_json_path=mapping_path,
                )

                evaluated = result.get('evaluated', [])
                valid = sum(1 for m in evaluated if m.get('measure_eval_result', {}).get('status') == 'VALID')
                equivalent = sum(1 for m in evaluated if m.get('measure_eval_result', {}).get('status') == 'EQUIVALENT')
                review = sum(1 for m in evaluated if m.get('measure_eval_result', {}).get('status') == 'REVIEW')
                invalid = sum(1 for m in evaluated if m.get('measure_eval_result', {}).get('status') == 'INVALID')

                total_evaluated += len(evaluated)
                total_valid += valid
                total_equivalent += equivalent
                total_review += review
                total_invalid += invalid

                results[table_key] = {
                    "evaluated": len(evaluated),
                    "valid": valid,
                    "equivalent": equivalent,
                    "review": review,
                    "invalid": invalid,
                    "details": evaluated,
                }

            except Exception as e:
                logger.warning(f"[Validator] {table_key}: {e}")
                results[table_key] = {"error": str(e)}
            finally:
                # Cleanup temp files
                for p in [yaml_path, mapping_path]:
                    try:
                        os.unlink(p)
                    except OSError:
                        pass

        output = {
            "summary": {
                "tables_validated": len(results),
                "total_evaluated": total_evaluated,
                "total_valid": total_valid,
                "total_equivalent": total_equivalent,
                "total_review": total_review,
                "total_invalid": total_invalid,
            },
            "per_table": results,
        }

        logger.info(
            f"[Validator] Done: {total_valid} VALID, {total_equivalent} EQUIVALENT, "
            f"{total_review} REVIEW, {total_invalid} INVALID out of {total_evaluated}"
        )
        return json.dumps(output, indent=2, default=str)
