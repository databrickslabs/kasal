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
        # Always seed ucmv_output so flow_methods.py injection check
        # (`'ucmv_output' in tool._default_config`) matches this tool.
        default_config: dict = {'ucmv_output': None}
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

        # HIGHEST PRIORITY: check /tmp/ucmv_user_edits.json written by the API
        # when the user saves edits from the UI. This file is accessible to the
        # subprocess (shared /tmp in the container) unlike the DB.
        try:
            _user_edits_path = '/tmp/ucmv_user_edits.json'
            if os.path.exists(_user_edits_path):
                with open(_user_edits_path) as _f:
                    _user_edits = json.load(_f)
                if isinstance(_user_edits, dict) and 'yaml' in _user_edits:
                    logger.info(f"[Validator] Found user-saved UCMV edits in {_user_edits_path} — using over flow output")
                    ucmv_raw = json.dumps(_user_edits)
        except Exception as _ue:
            logger.debug(f"[Validator] Could not read user edits from /tmp: {_ue}")

        # Also check DB for user-saved edits (works locally, fails in deployed env).
        # Only try DB if the /tmp user-edits file wasn't found.
        if not os.path.exists('/tmp/ucmv_user_edits.json'):
            try:
                saved = self._fetch_saved_ucmv_edits_from_db()
                if saved and isinstance(saved, dict) and 'yaml' in saved:
                    logger.info("[Validator] Found user-saved UCMV edits in DB — using edited YAML instead of flow-passed output")
                    ucmv_raw = json.dumps(saved)
            except Exception as _e:
                logger.debug(f"[Validator] Could not check for saved edits: {_e}")

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

        # Final fallback: read from /tmp file written by the UCMV Generator tool
        # (covers the edge case where flow injection into _default_config failed)
        if not yaml_tables:
            logger.info("[Validator] No YAML after DB fallback — checking /tmp for Generator output")
            try:
                import glob as _glob
                _tmp_files = sorted(
                    _glob.glob('/tmp/ucmv_latest_*.json'),
                    key=lambda p: os.path.getmtime(p),
                    reverse=True,
                )
                if _tmp_files:
                    with open(_tmp_files[0]) as _f:
                        _tmp_data = json.load(_f)
                    if isinstance(_tmp_data, dict) and 'yaml' in _tmp_data:
                        yaml_tables = _tmp_data['yaml']
                        if not ucmv_raw:
                            ucmv_raw = json.dumps(_tmp_data)
                        logger.info(f"[Validator] Loaded UCMV output from {_tmp_files[0]} ({len(yaml_tables)} tables)")
            except Exception as _tmp_err:
                logger.warning(f"[Validator] /tmp fallback failed: {_tmp_err}")

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

        # PREFERRED source: resolved_measures_by_table from the UCMV output. This
        # is the translated measures paired with their ORIGINAL DAX, keyed by the
        # FACT TABLE (same key as the YAML). It is the only source with the correct
        # measure↔fact-table pairing the comparison needs — the raw measures list
        # is keyed by PBI holder-table and won't line up with the YAML.
        if not measures or measures == []:
            if ucmv_raw:
                try:
                    ucmv_full = json.loads(ucmv_raw) if isinstance(ucmv_raw, str) else ucmv_raw
                    if isinstance(ucmv_full, dict) and ucmv_full.get('resolved_measures_by_table'):
                        flat = []
                        for _tbl, _rows in ucmv_full['resolved_measures_by_table'].items():
                            flat.extend(_rows)
                        if flat:
                            measures = flat
                            logger.info(
                                f"[Validator] Using {len(measures)} resolved measures "
                                f"(fact-table-keyed, with DAX) from UCMV output"
                            )
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

                # Surface the SKIPPED measures too (with a reason), so the UI
                # breakdown shows EVERY measure — not just the evaluated ones —
                # and the reviewer understands why the rest weren't compared.
                # These do NOT count toward evaluated/valid/etc. The validation
                # pipeline buckets a skipped measure as either 'simple' (a
                # trivial FUNC(table.col) aggregation — correct by construction,
                # nothing to compare) or 'unmatched' (no source DAX measure to
                # compare against).
                skipped_details = []
                for m in result.get('skipped', []):
                    kind = m.get('measure_eval')  # 'simple' | 'unmatched'
                    reason = (
                        "Trivial aggregation — nothing to compare (correct by construction)"
                        if kind == 'simple'
                        else "No source DAX measure to compare against"
                    )
                    skipped_details.append({
                        "measure_name": m.get('measure_name'),
                        "measure_eval_result": {"status": "skipped", "reason": reason},
                    })

                results[table_key] = {
                    "evaluated": len(evaluated),
                    "valid": valid,
                    "equivalent": equivalent,
                    "review": review,
                    "invalid": invalid,
                    # total = evaluated + skipped, so the UI can show "N of M".
                    "total_measures": len(evaluated) + len(skipped_details),
                    # evaluated (with verdicts) FIRST, then skipped (with reasons).
                    "details": evaluated + skipped_details,
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

        # ── Build a COMPACT agent-facing return ──────────────────────────────
        # The crew agent ingests this tool result into its LLM context. Returning
        # the full per-measure `details` for every table PLUS all the YAML docs
        # overflowed the model's context window on large models (28+ views →
        # "Context window exceeded", non-retryable). The full detail is already
        # captured in the execution trace / DB, so the agent only needs:
        #   - the summary totals,
        #   - per-table COUNTS (no per-measure detail),
        #   - the measures that actually need a human (REVIEW/INVALID), capped.
        # yaml is dropped from the return entirely (agent never needs it back).
        _ATTENTION_CAP = 50
        per_table_summary: dict = {}
        attention: list = []
        for table_key, r in results.items():
            if not isinstance(r, dict):
                per_table_summary[table_key] = r
                continue
            if 'skipped' in r or 'error' in r:
                per_table_summary[table_key] = r
                continue
            # SLIM per-measure list for the UI's expandable breakdown: measure
            # name + status (+ a short reason), NOT the heavy differences/
            # similarities/dax/sql comparison arrays. This restores the "every
            # measure and whether it's EQUIVALENT/VALID/…" dropdown at ~11 KB for
            # 28 tables (the full comparison text was the ~110 KB overflow and
            # stays in the trace only).
            slim_details = []
            for m in r.get("details", []):
                mer = m.get("measure_eval_result", {}) or {}
                status = mer.get("status")
                # A skipped measure carries a plain 'reason' string; an evaluated
                # one carries similarities/differences arrays. Keep one short line
                # either way (tiny in tokens).
                if mer.get("reason"):
                    reason_line = str(mer["reason"])[:160]
                else:
                    arr = (mer.get("similarities") or mer.get("differences") or [])
                    reason_line = arr[0][:120] if arr else ""
                slim_details.append({
                    "measure_name": m.get("measure_name"),
                    # keep the nested shape the viewer already reads
                    "measure_eval_result": {
                        "status": status,
                        "similarities": [reason_line] if reason_line else [],
                    },
                })
                # Also surface the actionable ones in the top-level attention list.
                if status in ("REVIEW", "INVALID"):
                    attention.append({
                        "table": table_key,
                        "measure_name": m.get("measure_name"),
                        "status": status,
                        "detail": mer,
                    })
            per_table_summary[table_key] = {
                "evaluated": r.get("evaluated", 0),
                "valid": r.get("valid", 0),
                "equivalent": r.get("equivalent", 0),
                "review": r.get("review", 0),
                "invalid": r.get("invalid", 0),
                "total_measures": r.get("total_measures", r.get("evaluated", 0)),
                "details": slim_details,
            }

        attention_truncated = len(attention) > _ATTENTION_CAP
        output = {
            "summary": {
                "tables_validated": len(results),
                "total_evaluated": total_evaluated,
                "total_valid": total_valid,
                "total_equivalent": total_equivalent,
                "total_review": total_review,
                "total_invalid": total_invalid,
            },
            "per_table_summary": per_table_summary,
            # Only measures needing human attention (REVIEW/INVALID), capped.
            "attention": attention[:_ATTENTION_CAP],
            "attention_truncated": attention_truncated,
            # The generated YAML IS included — it's small (~11 KB even for 28
            # views) and the UI needs it for the 1:1 metric-view downloads. It was
            # NOT the source of the context overflow; the per-measure `details`
            # (~110 KB) were, and those stay stripped (only `attention` +
            # `per_table_summary` counts represent them). Full per-measure detail
            # remains queryable in the execution trace.
            "yaml": yaml_tables,
            "full_detail_in_trace": True,
        }

        logger.info(
            f"[Validator] Done: {total_valid} VALID, {total_equivalent} EQUIVALENT, "
            f"{total_review} REVIEW, {total_invalid} INVALID out of {total_evaluated} "
            f"({len(attention)} need attention"
            f"{', capped at %d' % _ATTENTION_CAP if attention_truncated else ''})"
        )
        return json.dumps(output, indent=2, default=str)

    @staticmethod
    def _fetch_measures_from_db() -> list:
        """Fetch measures from the latest UCMV Generator execution's stats/migration_report."""
        import asyncio
        from sqlalchemy import text

        async def _query():
            from src.engines.crewai.tools.tool_session_provider import ToolSessionProvider
            async with ToolSessionProvider.session() as session:
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
                # PREFERRED: resolved_measures_by_table (fact-table-keyed, with DAX)
                if inner.get('resolved_measures_by_table'):
                    flat = []
                    for _rows in inner['resolved_measures_by_table'].values():
                        flat.extend(_rows)
                    if flat:
                        return flat
                # Fallback: measures_with_dax (raw, holder-table-keyed)
                if 'measures_with_dax' in inner and inner['measures_with_dax']:
                    return inner['measures_with_dax']
                return []

        import time
        from src.engines.crewai.tools.async_bridge import run_sync_with_context
        from src.utils.asyncio_utils import create_and_run_loop

        def _run_with_retry():
            """Retry up to 5x to handle the race condition where the UCMV
            Generator's DB write hasn't committed yet when the validator starts."""
            for attempt in range(5):
                try:
                    result = create_and_run_loop(_query())
                    if result:  # _query returns a (possibly empty) list of measures
                        return result
                    if attempt < 4:
                        logger.info(f"[Validator] Measures not in DB yet (attempt {attempt+1}/5), retrying in 3s...")
                        time.sleep(3)
                except Exception as e:
                    logger.debug(f"[Validator] DB query attempt {attempt+1} failed: {e}")
                    if attempt < 4:
                        time.sleep(2)
            return []

        try:
            return run_sync_with_context(_run_with_retry, timeout=60)
        except Exception:
            return []

    @staticmethod
    def _fetch_saved_ucmv_edits_from_db() -> dict:
        """
        Check executionhistory for the most recent UCMV Generator result that has
        user-saved edits (stored in checkpoint_data.edited_config or result field).

        Returns the edited UCMV result dict if found, otherwise empty dict.
        """
        import asyncio
        from sqlalchemy import text

        async def _query():
            from src.engines.crewai.tools.tool_session_provider import ToolSessionProvider
            async with ToolSessionProvider.session() as session:
                # Look for the dedicated UCMV yaml edits key written by the
                # save button in the UI (separate from Config Generator's
                # edited_config to avoid collisions in a multi-step flow).
                result = await session.execute(text("""
                    SELECT checkpoint_data::text
                    FROM executionhistory
                    WHERE checkpoint_data::text LIKE '%ucmv_yaml_edits%'
                    ORDER BY created_at DESC
                    LIMIT 1
                """))
                row = result.fetchone()
                if row and row[0]:
                    try:
                        cp = json.loads(row[0])
                        edits = cp.get('ucmv_yaml_edits', {})
                        if isinstance(edits, dict) and 'yaml' in edits:
                            logger.info("[Validator] Using user-edited UCMV YAML from checkpoint_data.ucmv_yaml_edits")
                            return edits
                    except Exception:
                        pass
                return {}

        from src.engines.crewai.tools.async_bridge import run_sync_with_context
        from src.utils.asyncio_utils import create_and_run_loop

        try:
            return run_sync_with_context(lambda: create_and_run_loop(_query()), timeout=15)
        except Exception:
            return {}

    @staticmethod
    def _fetch_latest_ucmv_from_db() -> dict:
        """Fetch YAML from the latest UCMV Generator execution trace in the DB."""
        import asyncio
        from sqlalchemy import text

        async def _query():
            from src.engines.crewai.tools.tool_session_provider import ToolSessionProvider
            async with ToolSessionProvider.session() as session:
                # Strategy 1: Look in execution_trace for the latest UCMV Generator run span
                result = await session.execute(text(
                    "SELECT et.output::text "
                    "FROM execution_trace et "
                    "WHERE et.span_name LIKE 'UC Metric View Generator%run' "
                    "ORDER BY et.created_at DESC LIMIT 1"
                ))
                row = result.fetchone()
                if row and row[0]:
                    try:
                        data = json.loads(row[0])
                        content = data.get('content', '')
                        inner = json.loads(content) if isinstance(content, str) else content
                        if isinstance(inner, dict) and 'yaml' in inner:
                            logger.info("[Validator] Found UCMV output in execution_trace")
                            return inner
                    except Exception:
                        pass

                # Strategy 2: Look in executionhistory.result for the latest UCMV result
                # (populated by the safety-net status update after queue-drain fix)
                result2 = await session.execute(text(
                    "SELECT result::text FROM executionhistory "
                    "WHERE result::text LIKE '%\"yaml\"%' "
                    "  AND result::text LIKE '%\"sql\"%' "
                    "  AND result::text LIKE '%\"stats\"%' "
                    "ORDER BY created_at DESC LIMIT 1"
                ))
                row2 = result2.fetchone()
                if row2 and row2[0]:
                    try:
                        inner2 = json.loads(row2[0])
                        if isinstance(inner2, dict) and 'yaml' in inner2:
                            logger.info("[Validator] Found UCMV output in executionhistory.result")
                            return inner2
                    except Exception:
                        pass

                return {}

        # Use create_and_run_loop — the only reliable pattern in subprocess context.
        # asyncio.run() fails in subprocesses because it inherits the parent's event
        # loop policy. create_and_run_loop creates a fully isolated fresh loop, the
        # same pattern used successfully by OTel trace writes in subprocess mode.
        from src.utils.asyncio_utils import create_and_run_loop
        import time

        for attempt in range(5):
            try:
                result = create_and_run_loop(_query())
                if result and isinstance(result, dict) and 'yaml' in result:
                    logger.info(f"[Validator] Found UCMV YAML in DB (attempt {attempt + 1})")
                    return result
                if attempt < 4:
                    logger.info(f"[Validator] UCMV not in DB yet (attempt {attempt+1}/5), retrying in 3s...")
                    time.sleep(3)
            except Exception as e:
                logger.warning(f"[Validator] DB query attempt {attempt+1} failed: {e}")
                if attempt < 4:
                    time.sleep(2)
        return {}
