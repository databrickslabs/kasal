"""Create or resolve the team's MLflow experiment with its UC trace storage.

Hosted installations derive the provisioning namespace from their assigned
volume. No experiment resource is required. Existing experiments keep their
saved storage location; new ones need the app's schema/table permissions.

The blocking auth and MLflow calls here run in the setup caller's worker thread.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, Optional

from src.core.databricks_app import is_databricks_app
from src.services.mlflow.trace_storage import select_experiment

logger = logging.getLogger(__name__)


def create_databricks_experiment(
    auth_context: Any,
    experiment_path: str,
    *,
    uc_catalog: Optional[str] = None,
    uc_schema: Optional[str] = None,
    warehouse_id: Optional[str] = None,
) -> Dict[str, str]:
    """Create (or resolve, if it already exists) a Databricks MLflow experiment.

    Blocking — run under ``asyncio.to_thread``. ``auth_context`` is the resolved
    SPN/PAT context from ``get_auth_context`` (``.workspace_url`` + ``.token``).
    Returns ``{experiment_id, experiment_name}``.

    When ``uc_catalog``/``uc_schema``/``warehouse_id`` are supplied, the
    experiment is created WITH a Unity Catalog trace_location — the SAME storage
    the runtime tracer uses. This matters twice over: it unlocks the UC-only
    trace charts (latency percentiles, token/cost, tool metrics), and MLflow
    permanently REFUSES to attach a UC trace destination to an experiment that
    was created without one, so an eagerly-created plain experiment would poison
    the name for tracing. Missing any of the three → plain experiment (dev/local).

    Auth: presents the SP/PAT token as the SINGLE method via ``single_auth_env``
    (removes OAuth env vars, pins ``auth_type=pat``) so MLflow's own SDK client
    creation doesn't hit "more than one authorization method configured".
    """
    import mlflow

    from src.services.mlflow.sp_auth import single_auth_env

    with single_auth_env(host=auth_context.workspace_url, token=auth_context.token):
        mlflow.set_tracking_uri("databricks")

        trace_location = None
        if uc_catalog and uc_schema and warehouse_id:
            try:
                from src.services.otel_tracing.mlflow_setup import (
                    _build_uc_trace_location,
                )

                trace_location = _build_uc_trace_location(
                    uc_catalog,
                    uc_schema,
                    warehouse_id,
                    logger,
                    **(
                        {"experiment_name": experiment_path}
                        if is_databricks_app()
                        else {}
                    ),
                )
            except Exception as exc:  # noqa: BLE001 — fall back to plain experiment
                if is_databricks_app():
                    raise
                logger.warning(
                    f"[experiment_setup] Could not build UC trace_location: {exc}"
                )

        if is_databricks_app() and trace_location is None:
            raise RuntimeError("Databricks Apps tracing resources are not ready")

        # set_experiment creates the experiment if it does not exist, and returns
        # the existing one otherwise — safe to call on every save. With a UC
        # trace_location it links UC trace storage at creation.
        if trace_location is not None:
            exp = select_experiment(mlflow, experiment_path, trace_location)
        else:
            exp = mlflow.set_experiment(experiment_path)
        return {
            "experiment_id": str(getattr(exp, "experiment_id", "")),
            "experiment_name": experiment_path,
        }
