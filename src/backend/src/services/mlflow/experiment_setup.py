"""Create the MLflow experiment up front, so it can be attached as an app resource.

On Databricks Apps, MLflow calls go through the app's service principal, which
only gains MLflow access once an **MLflow experiment resource** is attached to
the app — and that attachment needs the experiment to already EXIST. Kasal
otherwise creates its experiment lazily (on the first traced run), which is too
late: prompt registration and the first traces run before that, and the app
admin cannot attach a resource that isn't there yet.

So when the user saves the MLflow settings, we create the experiment eagerly
here. The auth + env-swap mirror ``MLflowService.get_experiment_info`` exactly
(SPN/PAT via ``get_auth_context``); this is the create-if-missing half factored
out so it can take an explicit path and be called from the settings save.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, Optional

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

    with single_auth_env(
        host=auth_context.workspace_url, token=auth_context.token
    ):
        mlflow.set_tracking_uri("databricks")

        trace_location = None
        if uc_catalog and uc_schema and warehouse_id:
            try:
                from src.services.otel_tracing.mlflow_setup import (
                    _build_uc_trace_location,
                )

                trace_location = _build_uc_trace_location(
                    uc_catalog, uc_schema, warehouse_id, logger
                )
            except Exception as exc:  # noqa: BLE001 — fall back to plain experiment
                logger.warning(
                    f"[experiment_setup] Could not build UC trace_location: {exc}"
                )

        # set_experiment creates the experiment if it does not exist, and returns
        # the existing one otherwise — safe to call on every save. With a UC
        # trace_location it links UC trace storage at creation.
        if trace_location is not None:
            exp = mlflow.set_experiment(
                experiment_path, trace_location=trace_location
            )
        else:
            exp = mlflow.set_experiment(experiment_path)
        return {
            "experiment_id": str(getattr(exp, "experiment_id", "")),
            "experiment_name": experiment_path,
        }
