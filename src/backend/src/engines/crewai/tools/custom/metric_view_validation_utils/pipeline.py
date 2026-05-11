"""MetricExpressionValidatorPipeline — orchestrator that ties all validator modules together.

Ported from the monolith's MetricExpressionValidatorTool._run() logic with
exact output parity. Unlike the tool (which is a CrewAI BaseTool wrapper),
this version accepts plain Python arguments and returns structured dicts,
making it usable directly from scripts, tests, and notebooks without a
CrewAI context.
"""
from __future__ import annotations

import json
import logging
import yaml
from typing import Any, Dict, Optional

from .data_input_handler import DataInputHandler
from .expression_validator import ExpressionValidator

logger = logging.getLogger(__name__)


class MetricExpressionValidatorPipeline:
    """Orchestrate metric expression validation between Databricks and DAX expressions.

    Supports two modes:
    1. **Direct validation** – pass ``databricks_expr`` + ``dax_expr`` to compare
       a single pair of expressions.
    2. **File-based validation** – pass ``metrics_view_yaml_path`` +
       ``table_mapping_json_path`` to validate every measure in a UC Metric View
       YAML against the corresponding DAX expressions in the mapping JSON.

    Table and column name mappings can be supplied in both modes to account for
    naming differences between the DAX model and the Databricks schema.

    Example (direct validation)::

        pipeline = MetricExpressionValidatorPipeline(
            table_mappings={"fact_pe002": "source"},
        )
        result = pipeline.run(
            databricks_expr="SUM(source.paid_hours)",
            dax_expr="SUM(fact_pe002[paid_hours])",
        )

    Example (file-based validation)::

        pipeline = MetricExpressionValidatorPipeline()
        result = pipeline.run(
            metrics_view_yaml_path="/path/to/mv.yaml",
            table_mapping_json_path="/path/to/mapping.json",
        )
    """

    def __init__(
        self,
        table_mappings: Optional[Dict[str, str]] = None,
        column_mappings: Optional[Dict[str, str]] = None,
    ) -> None:
        """
        Args:
            table_mappings: Mapping of DAX table names to Databricks table names,
                e.g. ``{"fact_pe002": "source", "Dim_wkctr": "dim_wkctr"}``.
            column_mappings: Mapping of DAX column names to Databricks column names,
                e.g. ``{"opl": "opl", "paid_hours": "paid_hours"}``.
        """
        self.table_mappings: Dict[str, str] = table_mappings or {}
        self.column_mappings: Dict[str, str] = column_mappings or {}

    # ── public API ────────────────────────────────────────────────────────

    def run(
        self,
        *,
        databricks_expr: Optional[str] = None,
        dax_expr: Optional[str] = None,
        measure_name: Optional[str] = None,
        strict_mode: bool = False,
        metrics_view_yaml_path: Optional[str] = None,
        table_mapping_json_path: Optional[str] = None,
        table_mappings: Optional[Dict[str, str]] = None,
        column_mappings: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        """Run the validation pipeline and return a structured result dict.

        Call-level ``table_mappings`` / ``column_mappings`` are merged on top of
        the instance-level ones, with call-level values taking precedence.

        Args:
            databricks_expr: Databricks metric view expression (Spark SQL).
                Required for direct-validation mode.
            dax_expr: DAX expression to validate against.
                Required for direct-validation mode.
            measure_name: Optional label attached to the result in direct mode.
            strict_mode: If ``True``, require exact structural match instead of
                semantic equivalence.
            metrics_view_yaml_path: Path to a UC Metric View YAML file.
                Required for file-based mode.
            table_mapping_json_path: Path to a measure/table mapping JSON file.
                Required for file-based mode.
            table_mappings: Per-call override for table name mappings.
            column_mappings: Per-call override for column name mappings.

        Returns:
            A dict whose shape depends on the mode:

            *Direct mode* – mirrors ``ExpressionValidator.validate()`` output plus
            ``measure_name`` and ``status`` keys.

            *File-based mode* – mirrors ``ExpressionValidator.validate_ucmv()``
            output (``skipped`` + ``evaluated`` lists).

            *Error* – ``{"error": "<message>"}`` if required parameters are absent
            or an unexpected exception occurs.
        """
        # Merge mappings: instance defaults ← call overrides
        effective_table_mappings = {**self.table_mappings, **(table_mappings or {})}
        effective_column_mappings = {**self.column_mappings, **(column_mappings or {})}

        if metrics_view_yaml_path and table_mapping_json_path:
            return self._run_file_based(
                metrics_view_yaml_path=metrics_view_yaml_path,
                table_mapping_json_path=table_mapping_json_path,
                table_mappings=effective_table_mappings,
                column_mappings=effective_column_mappings,
            )

        if databricks_expr and dax_expr:
            return self._run_direct(
                databricks_expr=databricks_expr,
                dax_expr=dax_expr,
                measure_name=measure_name,
                strict_mode=strict_mode,
                table_mappings=effective_table_mappings,
                column_mappings=effective_column_mappings,
            )

        return {
            "error": (
                "Missing required parameters. Provide either: "
                "(1) databricks_expr + dax_expr for direct validation, or "
                "(2) metrics_view_yaml_path + table_mapping_json_path for file-based validation"
            )
        }

    # ── internal helpers ──────────────────────────────────────────────────

    def _run_file_based(
        self,
        metrics_view_yaml_path: str,
        table_mapping_json_path: str,
        table_mappings: Dict[str, str],
        column_mappings: Dict[str, str],
    ) -> Dict[str, Any]:
        """Execute file-based validation mode."""
        logger.info(
            "[MetricExpressionValidatorPipeline] File-based mode: yaml=%s, mapping=%s",
            metrics_view_yaml_path,
            table_mapping_json_path,
        )
        try:
            data_handler = DataInputHandler(
                metrics_view_path=metrics_view_yaml_path,
                table_mapping_path=table_mapping_json_path,
                table_mappings=table_mappings,
            )
            validator = ExpressionValidator(
                data_handler=data_handler,
                table_mappings=table_mappings,
                column_mappings=column_mappings,
            )
            return validator.validate_ucmv()
        except (OSError, ValueError, yaml.YAMLError, json.JSONDecodeError, RuntimeError) as exc:
            logger.error(
                "[MetricExpressionValidatorPipeline] File-based validation failed "
                "for yaml=%s, mapping=%s: %s",
                metrics_view_yaml_path,
                table_mapping_json_path,
                exc,
            )
            return {
                "error": (
                    f"File-based validation failed for yaml={metrics_view_yaml_path}, "
                    f"mapping={table_mapping_json_path}: {exc}"
                )
            }

    def _run_direct(
        self,
        databricks_expr: str,
        dax_expr: str,
        measure_name: Optional[str],
        strict_mode: bool,
        table_mappings: Dict[str, str],
        column_mappings: Dict[str, str],
    ) -> Dict[str, Any]:
        """Execute direct (single-pair) validation mode."""
        logger.info(
            "[MetricExpressionValidatorPipeline] Direct mode: measure=%s, strict=%s",
            measure_name,
            strict_mode,
        )
        try:
            validator = ExpressionValidator(
                data_handler=None,
                table_mappings=table_mappings,
                column_mappings=column_mappings,
            )
            result = validator.validate(databricks_expr, dax_expr, strict=strict_mode)
            result["measure_name"] = measure_name
            result["status"] = "VALID" if result["is_valid"] else "INVALID"
            return result
        except (ValueError, RuntimeError) as exc:
            logger.error(
                "[MetricExpressionValidatorPipeline] Direct validation failed: %s", exc
            )
            return {"error": f"Direct validation failed: {exc}"}

    # ── convenience serializer ────────────────────────────────────────────

    def run_as_json(self, **kwargs: Any) -> str:
        """Like :meth:`run`, but returns the result serialised as a JSON string.

        Useful when the pipeline is called from a CrewAI tool that expects a
        string response.
        """
        return json.dumps(self.run(**kwargs), indent=2, default=str)
