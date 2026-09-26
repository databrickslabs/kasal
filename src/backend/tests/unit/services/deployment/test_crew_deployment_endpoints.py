"""Reading and deleting a crew's serving endpoint go through the ownership check.

``GET /crews/{crew_id}/deployment/status`` had no ownership check, so any editor
could read any serving endpoint (state, creator email, timestamps) by name with
the app's credential (audit N3 / SoC N6). Both SDK calls now live in
``CrewDeploymentService``, run off the event loop, and only after
``ServingEndpointOwnershipService`` proves the endpoint is this group's
deployment of this crew.
"""

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.core.exceptions import NotFoundError
from src.services.deployment import crew as crew_module
from src.services.deployment.crew import CrewDeploymentService
from src.services.deployment.endpoint_ownership import (
    ServingEndpointOwnershipService,
)
from src.services.deployment.endpoints import endpoint_status

GC = SimpleNamespace(primary_group_id="g1", group_ids=["g1"])


def _endpoint(state=True):
    return SimpleNamespace(
        state=(
            SimpleNamespace(
                ready=SimpleNamespace(value="READY"),
                config_update=None,
                ready_replicas=1,
            )
            if state
            else None
        ),
        config=SimpleNamespace(target_replicas=2) if state else None,
        pending_config=None,
        creator="someone@example.com",
        creation_timestamp=1,
        last_updated_timestamp=2,
    )


def _service(check):
    service = CrewDeploymentService(session=MagicMock())
    service.endpoint_ownership = MagicMock()
    service.endpoint_ownership.assert_endpoint_belongs_to_crew = check
    return service


def test_service_builds_the_ownership_check():
    service = CrewDeploymentService(session=MagicMock())
    assert isinstance(service.endpoint_ownership, ServingEndpointOwnershipService)


@pytest.mark.asyncio
async def test_status_of_an_owned_endpoint():
    check = AsyncMock(return_value=_endpoint())
    result = await _service(check).get_endpoint_status("crew-1", "ep", GC)
    check.assert_awaited_once_with("crew-1", "ep", GC)
    assert result["state"] == "READY"
    assert result["ready_replicas"] == 1 and result["target_replicas"] == 2


@pytest.mark.asyncio
async def test_status_of_a_foreign_endpoint_is_not_found_and_not_read():
    check = AsyncMock(side_effect=NotFoundError("No deployment"))
    with pytest.raises(NotFoundError):
        await _service(check).get_endpoint_status("crew-1", "someone-elses", GC)


@pytest.mark.asyncio
async def test_delete_of_an_owned_endpoint_runs_off_the_loop():
    check = AsyncMock(return_value=_endpoint())
    with patch.object(crew_module.asyncio, "to_thread", AsyncMock()) as to_thread:
        result = await _service(check).delete_endpoint("crew-1", "ep", GC)
    to_thread.assert_awaited_once_with(crew_module.delete_endpoint_blocking, "ep")
    assert result["endpoint_name"] == "ep"


@pytest.mark.asyncio
async def test_delete_of_a_foreign_endpoint_never_calls_the_sdk():
    check = AsyncMock(side_effect=NotFoundError("No deployment"))
    with patch("databricks.sdk.WorkspaceClient") as ws:
        with pytest.raises(NotFoundError):
            await _service(check).delete_endpoint("crew-1", "someone-elses", GC)
    ws.return_value.serving_endpoints.delete.assert_not_called()


def test_status_with_no_state_is_unknown():
    result = endpoint_status("ep", _endpoint(state=False))
    assert result["state"] == "UNKNOWN"
    assert result["ready_replicas"] == 0 and result["target_replicas"] == 0
