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

    ucmv_output: Optional[str] = Field(
        None, description="Raw UCMV Generator output JSON (contains yaml, sql, stats keys). Preferred input.")
    yaml_content: Optional[str] = Field(
        None, description="JSON dict of table_key → YAML string (alternative to ucmv_output)")
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
        config_keys = ('ucmv_output', 'yaml_content', 'measures_json')
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

        # Try ucmv_output first (raw UCMV Generator output with yaml/sql/stats keys)
        ucmv_raw = _get('ucmv_output')
        yaml_raw = _get('yaml_content') or '{}'
        measures_raw = _get('measures_json') or '[]'

        try:
            # If ucmv_output is provided, extract yaml from it
            if ucmv_raw:
                ucmv_data = json.loads(ucmv_raw) if isinstance(ucmv_raw, str) else ucmv_raw
                if isinstance(ucmv_data, dict):
                    if 'yaml' in ucmv_data:
                        yaml_raw = ucmv_data['yaml']
                    elif 'proposed_config' in ucmv_data:
                        pass  # Config Proposer output, not UCMV
                    else:
                        yaml_raw = ucmv_data
                logger.info(f"[Validator] Extracted YAML from ucmv_output: {len(yaml_raw) if isinstance(yaml_raw, dict) else 'str'}")

            yaml_tables = json.loads(yaml_raw) if isinstance(yaml_raw, str) else yaml_raw
            measures = json.loads(measures_raw) if isinstance(measures_raw, str) else measures_raw
        except json.JSONDecodeError as e:
            return json.dumps({"error": f"Invalid JSON input: {e}"})

        # Fallback: if no YAML provided, fetch from the latest UCMV Generator execution trace
        if not yaml_tables:
            logger.info("[Validator] No YAML in args — attempting to fetch from latest UCMV execution trace")
            try:
                _db_result = self._fetch_latest_ucmv_from_db()
                if isinstance(_db_result, dict) and 'yaml' in _db_result:
                    # Full UCMV output — extract yaml and keep the rest for validation
                    yaml_tables = _db_result['yaml']
                    ucmv_raw = json.dumps(_db_result)  # Store full output for built-in validation
                    logger.info(f"[Validator] Fetched full UCMV output from DB: {len(yaml_tables)} tables")
                elif isinstance(_db_result, dict):
                    yaml_tables = _db_result
            except Exception as db_err:
                logger.warning(f"[Validator] DB fallback failed: {db_err}")

        # Fallback: if no measures, fetch from the latest UCMV Generator execution
        if not measures or measures == []:
            logger.info("[Validator] No measures in args — fetching from latest UCMV execution trace")
            try:
                measures = self._fetch_measures_from_db()
                logger.info(f"[Validator] Fetched {len(measures)} measures from DB")
            except Exception as db_err:
                logger.warning(f"[Validator] Measures DB fallback failed: {db_err}")

        if not yaml_tables:
            return json.dumps({
                "error": "No YAML content provided. Pass ucmv_output (raw UCMV Generator output) "
                "or yaml_content (JSON dict of table_key → YAML string)."
            })

        # Extract measures from the UCMV Generator output if available
        # The UCMV tool stores the measures it used internally — we can
        # reconstruct them from the execution trace
        _builtin_stats = None
        if ucmv_raw:
            try:
                ucmv_full = json.loads(ucmv_raw) if isinstance(ucmv_raw, str) else ucmv_raw
                if isinstance(ucmv_full, dict):
                    _builtin_stats = ucmv_full.get('stats', {})
            except (json.JSONDecodeError, TypeError):
                pass

        # If no measures, try extracting from the UCMV output (measures_with_dax key)
        if not measures or measures == []:
            if ucmv_raw:
                try:
                    ucmv_full = json.loads(ucmv_raw) if isinstance(ucmv_raw, str) else ucmv_raw
                    if isinstance(ucmv_full, dict) and 'measures_with_dax' in ucmv_full:
                        measures = ucmv_full['measures_with_dax']
                        logger.info(f"[Validator] Extracted {len(measures)} measures from UCMV measures_with_dax")
                except (json.JSONDecodeError, TypeError):
                    pass

        if not measures or measures == []:
            logger.info("[Validator] No measures — fetching from latest UCMV execution")
            try:
                measures = self._fetch_measures_from_db()
                logger.info(f"[Validator] Fetched {len(measures)} measures from DB")
            except Exception as db_err:
                logger.warning(f"[Validator] Measures DB fallback failed: {db_err}")

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
            "yaml": yaml_tables,
        }

        logger.info(
            f"[Validator] Done: {total_valid} VALID, {total_equivalent} EQUIVALENT, "
            f"{total_review} REVIEW, {total_invalid} INVALID out of {total_evaluated}"
        )
        return json.dumps(output, indent=2, default=str)

    @staticmethod
    def _fetch_measures_from_db() -> list:
        """Fetch measures from the latest UCMV Generator execution's stats/migration_report."""
        import asyncio
        from sqlalchemy import text

        async def _query():
            from src.db.session import async_session_factory
            async with async_session_factory() as session:
                # The UCMV tool output contains the full result with yaml/sql/stats
                # The stats section has per-table measure info, but we need the raw
                # DAX measures. Try fetching from the UCMV tool's input (execution inputs).
                result = await session.execute(text(
                    "SELECT et.output::text "
                    "FROM execution_trace et "
                    "WHERE et.span_name LIKE 'UC Metric View Generator%run' "
                    "ORDER BY et.created_at DESC LIMIT 1"
                ))
                row = result.fetchone()
                if not row:
                    return []
                data = json.loads(row[0])
                content = data.get('content', '')
                inner = json.loads(content) if isinstance(content, str) else content
                if not isinstance(inner, dict):
                    return []
                # Extract measures from the migration_report or stats
                # First try measures_with_dax (added by UCMV Generator)
                if 'measures_with_dax' in inner and inner['measures_with_dax']:
                    return inner['measures_with_dax']
                return []

        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                import concurrent.futures
                with concurrent.futures.ThreadPoolExecutor() as pool:
                    return pool.submit(lambda: asyncio.run(_query())).result(timeout=10)
            return loop.run_until_complete(_query())
        except Exception:
            return asyncio.run(_query())

    @staticmethod
    def _fetch_latest_ucmv_from_db() -> dict:
        """Fetch YAML from the latest UCMV Generator execution trace in the DB."""
        import asyncio
        from sqlalchemy import text

        async def _query():
            from src.db.session import async_session_factory
            async with async_session_factory() as session:
                result = await session.execute(text(
                    "SELECT et.output::text "
                    "FROM execution_trace et "
                    "WHERE et.span_name LIKE 'UC Metric View Generator%run' "
                    "ORDER BY et.created_at DESC LIMIT 1"
                ))
                row = result.fetchone()
                if not row:
                    return {}
                data = json.loads(row[0])
                content = data.get('content', '')
                inner = json.loads(content) if isinstance(content, str) else content
                # Return full UCMV output (has yaml, sql, stats, validation keys)
                return inner if isinstance(inner, dict) else {}

        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                import concurrent.futures
                with concurrent.futures.ThreadPoolExecutor() as pool:
                    return pool.submit(lambda: asyncio.run(_query())).result(timeout=10)
            return loop.run_until_complete(_query())
        except Exception:
            return asyncio.run(_query())
