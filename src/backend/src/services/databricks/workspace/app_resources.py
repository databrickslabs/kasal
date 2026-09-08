"""Resolve installation resources using the hosting app's identity.

The assigned volume supplies the UC namespace for new trace destinations. Each
personal space or teamspace gets its own experiment and tables. Volume access
does not grant CREATE TABLE: that belongs to the installer's namespace setup.
"""

import asyncio
import logging
from dataclasses import dataclass

from src.core.databricks_app import DatabricksAppInstallation

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class TraceNamespace:
    catalog: str = ""
    schema: str = ""
    error: str = ""


async def trace_namespace(installation: DatabricksAppInstallation) -> TraceNamespace:
    """Derive a provisioning namespace from the assigned UC volume, without I/O.

    Existing experiments retain their saved destination in trace_storage.py.
    A volume resource identifies its parent schema but does not grant permission
    to create or read/write tables there; the installer grants those separately.
    """
    if not installation.hosted:
        return TraceNamespace()
    parts = installation.output_volume.rstrip("/").split("/")
    if (
        len(parts) != 5
        or parts[:2] != ["", "Volumes"]
        or any(not part or "." in part for part in parts[2:])
    ):
        return TraceNamespace(
            error=(
                "Attach the volume app resource with a path of "
                "/Volumes/<catalog>/<schema>/<volume> to configure MLflow trace storage."
            )
        )
    return TraceNamespace(catalog=parts[2], schema=parts[3])


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
