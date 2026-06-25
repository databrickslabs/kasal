"""Parent-process MLflow tracing setup.

The dispatcher (intent) and the crew/agent/task generation services all run in
the FastAPI **parent** process — not the crew-execution subprocess. This module
configures MLflow so their traces land in the **same** Unity Catalog experiment
as crew-execution traces (``<base>-uc``), written to the ``<prefix>_otel_*``
Delta tables via ``DatabricksUCTableSpanExporter`` (``log_spans`` -> SQL
warehouse, never the localhost OTLP sidecar).

Correctness mirrors the subprocess fix in ``mlflow_setup.py``:

1. Bind the dedicated ``-uc`` experiment (never the base name a legacy/managed
   trace may have poisoned — MLflow permanently refuses to link a UC Trace
   Destination to an experiment that already contains traces).
2. Do **NOT** pin the destination to ``Databricks(experiment_id)`` when UC is
   active: that resolves to an ``MlflowExperimentLocation`` and makes MLflow skip
   UC auto-resolution, routing spans to managed (artifact) storage.
3. Enable autolog so spans flow through MLflow's own tracer to the UC exporter.

Auth uses the Databricks Apps SPN (``DATABRICKS_CLIENT_ID`` /
``DATABRICKS_CLIENT_SECRET`` / ``DATABRICKS_HOST``) — never a PAT in production.
"""

import asyncio
import logging
import os
from typing import Any, Optional

logger = logging.getLogger(__name__)

try:  # mirror dispatcher's guarded import
    import mlflow as _mlflow  # noqa: F401
    _HAS_MLFLOW = True
except Exception:  # pragma: no cover - mlflow always present in prod
    _mlflow = None
    _HAS_MLFLOW = False


def set_mlflow_tracing(enabled: bool) -> None:
    """Hard-toggle MLflow tracing to match the setup outcome.

    Importing mlflow (litellm/crewai integrations) can leave a trace exporter
    armed even when setup decides tracing is off — every parent LLM call then
    attempts a doomed export and logs 'experiment_id is missing'. Disable
    explicitly when setup is skipped/fails, re-enable on success.
    """
    if not _HAS_MLFLOW:
        return
    try:
        if enabled:
            _mlflow.tracing.enable()
        else:
            _mlflow.tracing.disable()
    except Exception:
        pass


def set_root_span_outputs(span: Any, result: Any) -> None:
    """Best-effort set outputs on a root trace span so the trace's Response is
    populated (otherwise it shows ``null``). Normalizes Pydantic models /
    objects with ``model_dump``/``dict`` to plain dicts so MLflow can render them.
    """
    if span is None or not hasattr(span, "set_outputs"):
        return
    try:
        out = result
        if hasattr(result, "model_dump") and callable(result.model_dump):
            out = result.model_dump()
        elif hasattr(result, "dict") and callable(getattr(result, "dict")):
            try:
                out = result.dict()
            except Exception:
                out = result
        span.set_outputs(out)
    except Exception:
        pass


def _setup_sync(
    exp_name: str,
    uc_catalog: Optional[str],
    uc_schema: Optional[str],
    warehouse_id: Optional[str],
    label: str,
) -> bool:
    """Blocking MLflow configuration — run via ``asyncio.to_thread``."""
    import mlflow

    host = os.environ.get("DATABRICKS_HOST", "")
    client_id = os.environ.get("DATABRICKS_CLIENT_ID", "")
    client_secret = os.environ.get("DATABRICKS_CLIENT_SECRET", "")

    # Neutral wording: the Apps proxy redacts lines containing SECRET/TOKEN.
    logger.info(
        "[%s] MLflow auth env — host=%s, spn_id=%s, spn_cred=%s",
        label,
        "yes" if host else "no",
        "yes" if client_id else "no",
        "yes" if client_secret else "no",
    )

    # SPN credentials are required — never fall back to a PAT in production.
    if not (host and client_id and client_secret):
        logger.warning("[%s] SPN credentials required for MLflow — skipping", label)
        return False

    workspace_url = host.rstrip("/")
    auth_method = None
    try:
        from databricks.sdk import WorkspaceClient as _WC

        # Temporarily strip PAT/API-KEY so the SDK does not raise on dual-auth
        # (oauth + pat). This runs in the main server process, so restore them.
        _pat_backup = {}
        for _k in ("DATABRICKS_TOKEN", "DATABRICKS_API_KEY"):
            if _k in os.environ:
                _pat_backup[_k] = os.environ.pop(_k)
        try:
            w = _WC(host=host, client_id=client_id, client_secret=client_secret)
            headers = w.config.authenticate()
        finally:
            os.environ.update(_pat_backup)

        auth_header = headers.get("Authorization", "")
        if auth_header.startswith("Bearer "):
            os.environ["DATABRICKS_TOKEN"] = auth_header[len("Bearer "):]
            if not workspace_url.startswith("http"):
                workspace_url = f"https://{workspace_url}"
            os.environ["DATABRICKS_HOST"] = workspace_url
            # Env now holds oauth AND a token; pin SPN oauth to disambiguate.
            os.environ.setdefault("DATABRICKS_AUTH_TYPE", "oauth-m2m")
            auth_method = "service_principal"
            logger.info("[%s] SPN credential set", label)
        else:
            logger.warning("[%s] Unexpected auth header format", label)
    except Exception as spn_err:
        logger.warning(
            "[%s] SPN extraction failed: %s: %s",
            label, type(spn_err).__name__, spn_err,
        )

    if not auth_method:
        logger.warning("[%s] SPN credential extraction failed — MLflow disabled", label)
        return False

    mlflow.set_tracking_uri("databricks")

    from src.services.otel_tracing.mlflow_setup import (
        _build_uc_trace_location,
        uc_experiment_name,
        KASAL_TRACE_TABLE_PREFIX,
    )

    if warehouse_id:
        os.environ["MLFLOW_TRACING_SQL_WAREHOUSE_ID"] = str(warehouse_id)

    trace_location = _build_uc_trace_location(uc_catalog, uc_schema, warehouse_id, logger)
    uc_active = trace_location is not None

    if uc_active:
        # Dedicated UC-only experiment, shared with crew-execution traces.
        exp_name = uc_experiment_name(exp_name)
        exp = mlflow.set_experiment(exp_name, trace_location=trace_location)
        logger.info(
            "[%s] MLflow trace storage: Unity Catalog %s.%s.%s_otel_* (warehouse=%s), experiment=%s",
            label, uc_catalog, uc_schema, KASAL_TRACE_TABLE_PREFIX, warehouse_id, exp_name,
        )
    else:
        exp = mlflow.set_experiment(exp_name)
        logger.info("[%s] MLflow experiment set (managed): %s", label, exp_name)

    # Ensure the OTel SDK is enabled or MLflow traces won't record.
    if os.environ.get("OTEL_SDK_DISABLED", "").lower() in ("", "true", "1"):
        os.environ["OTEL_SDK_DISABLED"] = "false"

    try:
        if uc_active:
            # Do NOT pin Databricks(experiment_id): MLflow auto-resolves the
            # experiment's UnityCatalog location and installs the UC span
            # processor. Pinning it would route spans to managed storage.
            logger.info(
                "[%s] UC trace storage: destination auto-resolves to experiment %s "
                "(Databricks experiment-id destination intentionally NOT set)",
                label, getattr(exp, "experiment_id", "?"),
            )
        else:
            from mlflow.tracing.destination import Databricks as _Dest
            mlflow.tracing.set_destination(
                _Dest(experiment_id=str(getattr(exp, "experiment_id", "")))
            )
        mlflow.tracing.enable()
    except Exception as te:
        logger.info("[%s] MLflow tracing destination not set: %s", label, te)

    # LiteLLM autolog: child spans nest under the parent root span (the
    # generation/dispatcher start_root_trace), flowing through MLflow's tracer.
    try:
        mlflow.litellm.autolog(log_traces=False)
        logger.info("[%s] MLflow LiteLLM autolog enabled (spans only)", label)
    except Exception as ae:
        logger.info("[%s] MLflow LiteLLM autolog not available: %s", label, ae)

    return True


async def configure_parent_mlflow_tracing(
    session: Any,
    group_context: Any = None,
    *,
    label: str = "Parent",
) -> bool:
    """Configure MLflow tracing for a parent-process operation.

    Makes the caller's traces land in the same UC experiment as crew execution.
    Returns True when tracing is ready (caller should then wrap its work in
    ``start_root_trace``); False when MLflow is disabled or setup failed.
    """
    try:
        from src.services.mlflow_service import MLflowService

        group_id = (
            getattr(group_context, "primary_group_id", None) if group_context else None
        )
        svc = MLflowService(session, group_id=group_id)
        if not await svc.is_enabled():
            logger.info("[%s] MLflow disabled for this workspace; skipping tracing", label)
            set_mlflow_tracing(False)
            return False

        # Resolve experiment + UC config in the async context (avoids nesting an
        # event loop inside the to_thread setup).
        exp_name = "/Shared/kasal-crew-execution-traces"
        uc_catalog = uc_schema = warehouse_id = None
        try:
            from src.services.databricks_service import DatabricksService

            db_config = await DatabricksService(
                session, group_id=group_id
            ).get_databricks_config()
            if db_config:
                if getattr(db_config, "mlflow_experiment_name", None):
                    name = db_config.mlflow_experiment_name
                    exp_name = name if name.startswith("/") else f"/Shared/{name}"
                uc_catalog = getattr(db_config, "catalog", None)
                # schema field is `db_schema` (aliased "schema"); reading
                # "schema" returns BaseModel.schema (a method) -> MLflow error.
                uc_schema = getattr(db_config, "db_schema", None)
                warehouse_id = getattr(db_config, "warehouse_id", None)
        except Exception as cfg_err:
            logger.info("[%s] Could not fetch MLflow config: %s; using default", label, cfg_err)

        setup_ok = await asyncio.to_thread(
            _setup_sync, exp_name, uc_catalog, uc_schema, warehouse_id, label
        )
        if not setup_ok:
            set_mlflow_tracing(False)
            return False

        logger.info("[%s] MLflow tracing configured (experiment + autolog)", label)
        set_mlflow_tracing(True)
        return True
    except Exception as e:
        logger.warning("[%s] MLflow setup skipped: %s", label, e)
        set_mlflow_tracing(False)
        return False
