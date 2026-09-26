"""Proving a Model Serving endpoint belongs to a crew before acting on it.

``DELETE /crews/{crew_id}/deployment/{endpoint_name}`` and
``GET /crews/{crew_id}/deployment/status`` act with the app's own Databricks
credential, so without a check any workspace admin could delete, and any editor
could read (state, creator email, timestamps), ANY serving endpoint in the
workspace by name — including ones Kasal never created or that another tenant's
crew serves.

Kasal keeps no deployment table, so ownership is read from the endpoint itself:
every crew deployment (``CrewDeploymentService``) registers an MLflow model whose
run carries ``config/crew_config.json`` with the crew's ``id``. An endpoint
belongs to a crew when one of its served entities is a model version whose run
carries that crew's config. Combined with the crew being in the caller's group,
that proves the endpoint is this tenant's deployment of this crew.

Anything that cannot be proven — the endpoint is missing, serves a foreign
model, or the provenance cannot be read — is refused as "not found", so the
check fails closed and does not reveal which endpoints exist.
"""

import asyncio
import json
import logging
import tempfile
from typing import Any, Set, Tuple
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from src.core.exceptions import NotFoundError
from src.services.catalog.crews import CrewService
from src.utils.user_context import GroupContext

logger = logging.getLogger(__name__)

__all__ = ["ServingEndpointOwnershipService", "CREW_CONFIG_ARTIFACT"]

# Where ``CrewDeploymentService._create_mlflow_model`` logs the crew config.
CREW_CONFIG_ARTIFACT = "config/crew_config.json"


def _registry_uri_for(model_name: str) -> str:
    """UC models are three-part names; the rest live in the workspace registry."""
    return "databricks-uc" if model_name.count(".") == 2 else "databricks"


def _read_endpoint(endpoint_name: str) -> Tuple[Any, Set[str]]:
    """Blocking: the endpoint, and the crew ids whose deployed models it serves."""
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.useragent import with_product

    from src.utils.telemetry import KASAL_BASE, VERSION, KasalProduct

    with_product(f"{KASAL_BASE}_{KasalProduct.DEPLOYMENT}", VERSION)
    endpoint = WorkspaceClient().serving_endpoints.get(endpoint_name)
    return endpoint, _served_crew_ids(endpoint, endpoint_name)


def _served_crew_ids(endpoint: Any, endpoint_name: str) -> Set[str]:
    """Blocking: the crew ids whose deployed models ``endpoint`` serves."""
    from mlflow.tracking import MlflowClient

    config = getattr(endpoint, "config", None)
    entities = list(getattr(config, "served_entities", None) or [])
    entities += list(getattr(config, "served_models", None) or [])

    crew_ids: Set[str] = set()
    for entity in entities:
        name = getattr(entity, "entity_name", None) or getattr(
            entity, "model_name", None
        )
        version = getattr(entity, "entity_version", None) or getattr(
            entity, "model_version", None
        )
        if not name or not version:
            continue
        try:
            client = MlflowClient(
                tracking_uri="databricks", registry_uri=_registry_uri_for(name)
            )
            run_id = client.get_model_version(name, str(version)).run_id
            if not run_id:
                continue
            with tempfile.TemporaryDirectory() as tmp:
                path = client.download_artifacts(run_id, CREW_CONFIG_ARTIFACT, tmp)
                with open(path) as f:
                    crew_id = json.load(f).get("id")
            if crew_id:
                crew_ids.add(str(crew_id))
        except Exception as exc:  # provenance unreadable -> not proven
            logger.warning(
                "Could not read crew provenance of %s v%s on endpoint %s: %s",
                name,
                version,
                endpoint_name,
                exc,
            )
    return crew_ids


class ServingEndpointOwnershipService:
    """Checks a serving endpoint is the caller's group's deployment of a crew."""

    def __init__(self, session: AsyncSession):
        # The crew is read through its owning service (audit N5), which scopes
        # it to the caller's current workspace.
        self.crew_service = CrewService(session)

    async def assert_endpoint_belongs_to_crew(
        self, crew_id: str, endpoint_name: str, group_context: GroupContext
    ) -> Any:
        """Return the endpoint, or raise ``NotFoundError`` unless it serves this
        group's crew."""
        not_found = NotFoundError(
            detail=f"No deployment {endpoint_name} found for crew {crew_id}"
        )
        try:
            crew_uuid = UUID(str(crew_id))
        except ValueError:
            raise not_found
        crew = await self.crew_service.get_by_group(crew_uuid, group_context)
        if not crew:
            raise not_found

        try:
            endpoint, served = await asyncio.to_thread(_read_endpoint, endpoint_name)
        except Exception as exc:
            logger.warning("Could not read serving endpoint %s: %s", endpoint_name, exc)
            raise not_found
        if str(crew.id) not in served:
            logger.warning(
                "Refused to act on endpoint %s: it does not serve crew %s",
                endpoint_name,
                crew_id,
            )
            raise not_found
        return endpoint
