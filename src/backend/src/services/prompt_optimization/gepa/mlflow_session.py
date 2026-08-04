"""Resolve and enter the ACTIVE MLflow backend for judge/scorer operations.

Judges (MLflow scorers) used to be local-server-only. This lets them work on
whichever backend is actually configured:

* **Local MLflow server** (dev): ``MCP_SERVER_ENABLED=true`` + a non-databricks
  ``MLFLOW_TRACKING_URI``. Judges register on that server.
* **Databricks managed MLflow** (deployed): a workspace is configured. Judges
  register through ``databricks`` auth (the app SP), the same env-swap the
  tracing + prompt-registry paths use, so they show up in the Databricks MLflow
  UI under the crew-traces experiment.

Split in two so the async DB/auth work happens on the event loop and only the
blocking MLflow calls run in a worker thread:

* :func:`resolve_mlflow_backend` (async) — decide local vs databricks, resolving
  auth + experiment name. Returns ``None`` when neither backend is available
  (callers then no-op / raise, exactly as the old ``_local_mlflow_uri`` gate did).
* :func:`mlflow_session` (sync context manager) — set the tracking URI and pin
  the experiment for the chosen backend, restoring both afterwards. Runs inside
  the ``asyncio.to_thread`` body.
"""

from __future__ import annotations

import logging
import os
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Iterator, Optional

logger = logging.getLogger(__name__)


@dataclass
class MLflowBackend:
    """The resolved MLflow backend for a judge operation.

    ``kind`` is ``"local"`` or ``"databricks"``. For local, ``uri`` is the
    server URL. For databricks, ``auth`` is the resolved auth context
    (``.workspace_url`` + ``.token``). ``experiment`` is the experiment to pin
    so scorers land where everything else looks for them.
    """

    kind: str
    experiment: str
    uri: Optional[str] = None
    auth: Optional[Any] = None


async def resolve_mlflow_backend(
    session: Any, group_context: Optional[Any]
) -> Optional[MLflowBackend]:
    """Pick the active MLflow backend, or ``None`` if neither is available.

    Local wins when a local server is configured (dev convenience); otherwise a
    configured Databricks workspace is used. Auth + experiment resolution for
    the Databricks path reuse :class:`MLflowService`.
    """
    # 1. Local server (dev): MCP_SERVER_ENABLED + a non-databricks tracking URI.
    if os.getenv("MCP_SERVER_ENABLED", "").lower() == "true":
        uri = os.getenv("KASAL_LAUNCH_MLFLOW_TRACKING_URI") or os.getenv(
            "MLFLOW_TRACKING_URI"
        )
        if uri and not uri.startswith("databricks"):
            exp = os.environ.get("MLFLOW_EXPERIMENT_NAME") or "kasal"
            return MLflowBackend(kind="local", experiment=exp, uri=uri)

    # 2. Databricks managed MLflow: a workspace is configured for this group.
    group_id = (
        getattr(group_context, "primary_group_id", None) if group_context else None
    )
    if not group_id:
        return None
    try:
        from src.services.mlflow.service import MLflowService

        svc = MLflowService(session, group_id=group_id)
        workspace_url = await svc._configured_workspace_url()
        if not workspace_url:
            return None
        auth = await svc._setup_mlflow_auth()
        if not auth:
            logger.warning(
                "[judges] Databricks configured but MLflow auth unavailable; "
                "judge operation skipped."
            )
            return None
        # Pin the SAME experiment tracing uses, so judges are visible alongside
        # the crew traces in the Databricks MLflow UI.
        exp_path = os.getenv(
            "MLFLOW_CREW_TRACES_EXPERIMENT", "/Shared/kasal-crew-execution-traces"
        )
        return MLflowBackend(kind="databricks", experiment=exp_path, auth=auth)
    except Exception as exc:  # noqa: BLE001 — absence/auth failure is a no-op
        logger.debug(f"[judges] Could not resolve Databricks MLflow backend: {exc}")
        return None


@contextmanager
def mlflow_session(backend: MLflowBackend) -> Iterator[None]:
    """Set tracking URI + experiment for ``backend`` for one op, then restore.

    Blocking — run inside ``asyncio.to_thread``. For databricks it swaps
    ``DATABRICKS_HOST``/``DATABRICKS_TOKEN`` (MLflow reads them from the env),
    mirroring the tracing + prompt-registry auth.
    """
    import mlflow

    prev_uri = mlflow.get_tracking_uri()
    old_host = os.environ.get("DATABRICKS_HOST")
    old_token = os.environ.get("DATABRICKS_TOKEN")
    try:
        if backend.kind == "databricks":
            os.environ["DATABRICKS_HOST"] = backend.auth.workspace_url
            os.environ["DATABRICKS_TOKEN"] = backend.auth.token
            mlflow.set_tracking_uri("databricks")
        else:
            mlflow.set_tracking_uri(backend.uri)
        try:
            mlflow.set_experiment(backend.experiment)
        except Exception as exp_err:  # noqa: BLE001
            logger.warning(
                f"[judges] Could not pin experiment '{backend.experiment}': {exp_err}"
            )
        yield
    finally:
        mlflow.set_tracking_uri(prev_uri)
        if backend.kind == "databricks":
            if old_host is not None:
                os.environ["DATABRICKS_HOST"] = old_host
            elif "DATABRICKS_HOST" in os.environ:
                del os.environ["DATABRICKS_HOST"]
            if old_token is not None:
                os.environ["DATABRICKS_TOKEN"] = old_token
            elif "DATABRICKS_TOKEN" in os.environ:
                del os.environ["DATABRICKS_TOKEN"]
