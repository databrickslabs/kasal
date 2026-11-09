"""Output-side steps: source audit, LLM-failure warning, catch-all view, validation.

These run after the pipeline has emitted its views and shape what the tool returns
alongside them. Each one is fail-open: a failure here is logged and the rest of
the result is still returned.
"""

from __future__ import annotations

import json
import logging
import os
import tempfile
from typing import TYPE_CHECKING

from .shared_types import as_dict, as_list

if TYPE_CHECKING:
    from src.services.tools.metric_view_utils.pipeline import MetricViewPipeline

logger = logging.getLogger(__name__)


def audit_source_mquery(
    pipeline: MetricViewPipeline,
    raw_source_by_name: dict[str, str],
) -> tuple[dict[str, str], dict[str, list[str]]]:
    """Full raw Power Query M per view, plus an audit of what the M carries.

    The audit lists transformation steps (filters, group-by, joins, …) that may NOT
    be reflected in the derived Source SQL, so the loss is visible, not silent.
    Prefers the pre-recovery original M; falls back to the parsed source expression.
    """
    source_mquery: dict[str, str] = {}
    source_transform_todos: dict[str, list[str]] = {}
    try:
        from src.services.tools.metric_view_utils.mquery_transform_audit import (
            audit_mquery_transformations,
        )

        for table_key, table_info in (pipeline.mquery_tables or {}).items():
            raw = (
                raw_source_by_name.get(getattr(table_info, "table_name", ""))
                or getattr(table_info, "raw_transpiled_sql", "")
                or getattr(table_info, "full_sql", "")
            )
            if raw:
                source_mquery[table_key] = raw
                todos = audit_mquery_transformations(raw)
                if todos:
                    source_transform_todos[table_key] = todos
    except Exception as sm_err:  # display aid, never fatal
        logger.warning(
            "[UCMV] source_mquery/audit build failed: %s", sm_err, exc_info=True
        )
    return source_mquery, source_transform_todos


def llm_translation_warning_for(
    *,
    use_llm: bool,
    view_count: int,
    raw_m_count: int,
    raw_m_recovered: int,
) -> str | None:
    """A loud, specific reason when LLM translation was on but produced nothing.

    A total 0-view yield, or raw-M tables that recovered NONE of their sources, is
    the exact shape of the LLM failure (provider routing / FMAPI rate-limit /
    timeout) that otherwise reads as "the tool just produced nothing".
    """
    if not use_llm:
        return None
    warning: str | None = None
    if raw_m_count and not raw_m_recovered:
        warning = (
            f"LLM source recovery produced nothing for all {raw_m_count} raw "
            "Power Query table(s), so they yielded no views. This is almost "
            "always the DAX/M LLM calls failing — provider routing, an FMAPI "
            "rate-limit/throttle, or the subprocess timing out — NOT an empty "
            "model. Check the run log for 'LLM Provider NOT provided' or "
            "throttling and re-run; if it timed out, lower the DAX LLM batch "
            "size/concurrency or raise the subprocess limit."
        )
    elif view_count == 0:
        warning = (
            "0 metric views were generated with LLM translation enabled. If "
            "measures were present, the DAX→SQL LLM calls likely failed "
            "(provider routing / FMAPI throttle / timeout) rather than the "
            "model being empty — check the run log and re-run."
        )
    if warning:
        logger.warning("[UCMV] %s", warning)
    return warning


def build_none_allocated(
    *,
    measures: list[dict],
    pipeline: MetricViewPipeline,
    yaml_output: dict[str, str],
    visual_usage_obj: dict | None,
) -> str | None:
    """Catch-all view for every measure that never landed on a fact view.

    Gathers each such measure into a synthetic ``none_allocated_measures`` view:
    best-effort translated where possible, the rest documented as comments with DAX
    and reason. Visual, formatting, and slicer artifacts are labelled as such.
    Reconciles against the FULL extracted measure set, so nothing can slip through
    the gap between "extracted" and "allocated". Merged after validation, so it is
    not validated as a deployable view (it spans several facts by design).
    """
    try:
        from src.services.tools.metric_view_utils.none_allocated_emitter import (
            build_none_allocated_yaml,
        )

        return build_none_allocated_yaml(
            measures if isinstance(measures, list) else pipeline.mapping,
            pipeline.all_specs,
            yaml_output,  # rendered real views: covered == actually in the export
            pipeline.translator,
            pipeline._PBI_ARTIFACT_PATTERNS,
            visual_usage_index=visual_usage_obj,
        )
    except Exception as na_err:
        logger.warning(
            "[UCMVGenerator] none_allocated_measures emit skipped: %s",
            na_err,
            exc_info=True,
        )
        return None


def run_expression_validation(
    yaml_output: dict[str, str],
    measures_raw: object,
) -> dict[str, dict[str, int]]:
    """Compare each view's DAX structure with its generated SQL (optional).

    Writes each view and its measure mapping to temporary files for the validator,
    which needs paths. The files are removed in every case.
    """
    validation_results: dict[str, dict[str, int]] = {}
    try:
        from src.services.tools.metric_view_validation_utils.pipeline import (
            MetricExpressionValidatorPipeline,
        )

        if measures_raw and measures_raw != "[]":
            mapping_for_val = (
                json.loads(measures_raw)
                if isinstance(measures_raw, str)
                else measures_raw
            )
            for table_key, yml in yaml_output.items():
                with tempfile.NamedTemporaryFile(
                    mode="w", suffix=".yml", delete=False
                ) as yf:
                    yf.write(yml)
                    yf_path = yf.name
                with tempfile.NamedTemporaryFile(
                    mode="w", suffix=".json", delete=False
                ) as mf:
                    json.dump(mapping_for_val, mf)
                    mf_path = mf.name
                try:
                    validator = MetricExpressionValidatorPipeline(
                        table_mappings={table_key: "source"}
                    )
                    vr = validator.run(
                        metrics_view_yaml_path=yf_path,
                        table_mapping_json_path=mf_path,
                    )
                    evaluated = vr.get("evaluated", [])
                    if evaluated:
                        n_valid = sum(
                            1
                            for m in evaluated
                            if m.get("measure_eval_result", {}).get("status") == "VALID"
                        )
                        validation_results[table_key] = {
                            "evaluated": len(evaluated),
                            "valid": n_valid,
                        }
                finally:
                    os.unlink(yf_path)
                    os.unlink(mf_path)
    except ImportError:
        pass  # Validation package not available
    except Exception as e:
        logger.warning("Validation failed: %s", e, exc_info=True)
    return validation_results


def build_resolved_measures_by_table(
    results: object,
) -> dict[str, list[dict[str, object]]]:
    """Resolved measure → DAX map, keyed by FACT TABLE (the YAML table key).

    This is what the Quality Validator needs: each translated measure paired with
    its ORIGINAL DAX, allocated to the correct fact table. It uses the pipeline's own
    allocation, so the validator can pair YAML measures with DAX.
    """
    resolved: dict[str, list[dict[str, object]]] = {}
    specs = as_dict(as_dict(results).get("specs"))
    for table_key, spec_value in specs.items():
        rows: list[dict[str, object]] = []
        for m_value in as_list(as_dict(spec_value).get("measures")):
            m = as_dict(m_value)
            rows.append(
                {
                    "measure_name": m.get("name", ""),
                    "original_name": m.get("original_name", ""),
                    "sql_expr": m.get("sql_expr", ""),
                    "dax_expression": m.get("dax_expression", ""),
                    "proposed_allocation": table_key,  # fact-table key, matches YAML
                    "table_name": table_key,
                }
            )
        if rows:
            resolved[table_key] = rows
    return resolved
