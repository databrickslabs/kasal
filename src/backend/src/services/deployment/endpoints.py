"""Reading and deleting a crew's Model Serving endpoint (the SDK side).

The Databricks SDK is synchronous (client auth resolution included), so these
run in a worker thread; ``CrewDeploymentService`` calls them only after
``ServingEndpointOwnershipService`` has proven the endpoint is the caller's.
"""

from typing import Any, Dict

__all__ = ["endpoint_status", "delete_endpoint_blocking"]


def endpoint_status(endpoint_name: str, endpoint: Any) -> Dict[str, Any]:
    """The status fields the UI shows for a serving endpoint."""
    state = getattr(endpoint, "state", None)
    config = getattr(endpoint, "config", None)
    return {
        "endpoint_name": endpoint_name,
        "state": state.ready.value if state and state.ready else "UNKNOWN",
        "config_update": (
            state.config_update.value if state and state.config_update else None
        ),
        "pending_config": getattr(endpoint, "pending_config", None) is not None,
        "ready_replicas": getattr(state, "ready_replicas", 0) if state else 0,
        "target_replicas": getattr(config, "target_replicas", 0) if config else 0,
        "creator": getattr(endpoint, "creator", None),
        "creation_timestamp": getattr(endpoint, "creation_timestamp", None),
        "last_updated_timestamp": getattr(endpoint, "last_updated_timestamp", None),
    }


def delete_endpoint_blocking(endpoint_name: str) -> None:
    """Blocking: delete the serving endpoint with the app's credential."""
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.useragent import with_product

    from src.utils.telemetry import KASAL_BASE, VERSION, KasalProduct

    with_product(f"{KASAL_BASE}_{KasalProduct.DEPLOYMENT}", VERSION)
    WorkspaceClient().serving_endpoints.delete(endpoint_name)
