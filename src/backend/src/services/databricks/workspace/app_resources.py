"""Resolve installation resources using the hosting app's identity.

The selected experiment supplies the approved UC namespace. Each personal space
or teamspace gets its own experiment and tables; never write user traces into
the installation's resource experiment. Resource access does not grant CREATE
TABLE: that privilege belongs to the installer's namespace setup.
"""

import asyncio
import logging
import os
import threading
import time
from dataclasses import dataclass

from src.core.databricks_app import DatabricksAppInstallation
from src.utils.telemetry import KASAL_BASE, VERSION, KasalProduct

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class TraceNamespace:
    catalog: str = ""
    schema: str = ""
    error: str = ""


_cache: dict[tuple[str, str, str], tuple[float, TraceNamespace]] = {}
_lock = threading.Lock()


def _read_namespace(installation: DatabricksAppInstallation) -> TraceNamespace:
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.useragent import with_product

    key = (installation.host, installation.app_name, installation.experiment_id)
    with _lock:
        cached = _cache.get(key)
        if cached and cached[0] > time.monotonic():
            return cached[1]
        try:
            with_product(f"{KASAL_BASE}_{KasalProduct.MLFLOW}", VERSION)
            client = WorkspaceClient(
                host=installation.host,
                client_id=os.environ.get("DATABRICKS_CLIENT_ID"),
                client_secret=os.environ.get("DATABRICKS_CLIENT_SECRET"),
                auth_type="oauth-m2m",
            )
            experiment = client.experiments.get_experiment(
                installation.experiment_id
            ).experiment
            tags = {tag.key: tag.value for tag in (experiment.tags or [])}
            from mlflow.utils.mlflow_tags import (
                MLFLOW_EXPERIMENT_DATABRICKS_TRACE_DESTINATION_PATH,
            )

            destination = tags.get(
                MLFLOW_EXPERIMENT_DATABRICKS_TRACE_DESTINATION_PATH, ""
            )
            parts = destination.split(".")
            if len(parts) != 3 or not all(parts):
                raise ValueError(
                    "The installation experiment has no Unity Catalog trace storage"
                )
            namespace = TraceNamespace(catalog=parts[0], schema=parts[1])
        except Exception as exc:
            logger.warning(
                "Could not resolve the installed MLflow resource: %s",
                type(exc).__name__,
            )
            namespace = TraceNamespace(
                error=(
                    "The experiment app resource must reference an accessible "
                    "experiment with Unity Catalog trace storage. An administrator "
                    "must also grant the app permission to create private trace tables "
                    "in that schema."
                )
            )
        # Bound the cache across resource changes; failures retry quickly.
        if len(_cache) >= 8:
            _cache.clear()
        _cache[key] = (time.monotonic() + (15 if namespace.error else 300), namespace)
        return namespace


async def trace_namespace(installation: DatabricksAppInstallation) -> TraceNamespace:
    if not installation.hosted:
        return TraceNamespace()
    if not installation.experiment_id:
        return TraceNamespace(
            error="Attach the experiment resource in the Databricks App installation."
        )
    return await asyncio.to_thread(_read_namespace, installation)


async def connection_status(installation: DatabricksAppInstallation) -> dict:
    """Test the assigned warehouse without requiring a saved team configuration."""
    if not installation.warehouse_id:
        return {
            "status": "error",
            "connected": False,
            "message": "Attach the sql-warehouse app resource.",
        }
    try:
        from src.utils.databricks_app_auth import get_app_client

        client = await asyncio.to_thread(get_app_client)
        await asyncio.to_thread(client.warehouses.get, installation.warehouse_id)
        return {
            "status": "success",
            "connected": True,
            "message": "Connected through the Databricks App resources.",
        }
    except Exception as exc:
        logger.warning("Installed warehouse connection failed: %s", type(exc).__name__)
        return {
            "status": "error",
            "connected": False,
            "message": "The app cannot access its assigned SQL warehouse. Check the sql-warehouse resource grant.",
        }
