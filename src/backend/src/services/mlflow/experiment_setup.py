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
import os
from typing import Any, Dict

logger = logging.getLogger(__name__)


def create_databricks_experiment(
    auth_context: Any, experiment_path: str
) -> Dict[str, str]:
    """Create (or resolve, if it already exists) a Databricks MLflow experiment.

    Blocking — run under ``asyncio.to_thread``. ``auth_context`` is the resolved
    SPN/PAT context from ``get_auth_context`` (``.workspace_url`` + ``.token``).
    Returns ``{experiment_id, experiment_name}``.

    Mirrors the credential handling in ``MLflowService.get_experiment_info``:
    MLflow reads ``DATABRICKS_HOST``/``DATABRICKS_TOKEN`` from the environment, so
    they are set for the duration of the call and always restored.
    """
    import mlflow

    old_host = os.environ.get("DATABRICKS_HOST")
    old_token = os.environ.get("DATABRICKS_TOKEN")
    try:
        os.environ["DATABRICKS_HOST"] = auth_context.workspace_url
        os.environ["DATABRICKS_TOKEN"] = auth_context.token
        mlflow.set_tracking_uri("databricks")
        # set_experiment creates the experiment if it does not exist, and returns
        # the existing one otherwise — safe to call on every save.
        exp = mlflow.set_experiment(experiment_path)
        return {
            "experiment_id": str(getattr(exp, "experiment_id", "")),
            "experiment_name": experiment_path,
        }
    finally:
        if old_host is not None:
            os.environ["DATABRICKS_HOST"] = old_host
        elif "DATABRICKS_HOST" in os.environ:
            del os.environ["DATABRICKS_HOST"]
        if old_token is not None:
            os.environ["DATABRICKS_TOKEN"] = old_token
        elif "DATABRICKS_TOKEN" in os.environ:
            del os.environ["DATABRICKS_TOKEN"]
