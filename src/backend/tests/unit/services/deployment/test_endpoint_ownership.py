"""Serving-endpoint ownership check before a deployment is read or deleted.

Both run with the app's Databricks credential, so the endpoint must be proven
to serve this crew, and the crew must be in the caller's workspace — read
through ``CrewService`` (audit N5), not the crew repository.
"""

import json
import uuid
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import requests
from databricks.sdk.errors import (
    InternalError,
    NotFound,
    PermissionDenied,
    ResourceDoesNotExist,
    TemporarilyUnavailable,
    TooManyRequests,
    Unauthenticated,
)

from src.core.exceptions import KasalError, NotFoundError
from src.services.catalog.crews import CrewService
from src.services.deployment import endpoint_ownership
from src.services.deployment.endpoint_ownership import (
    ServingEndpointOwnershipService,
    _read_endpoint,
)

CREW_ID = uuid.uuid4()
GC = SimpleNamespace(primary_group_id="g1", group_ids=["g1"])
ENDPOINT = SimpleNamespace(name="ep")


def _service(crew=None):
    svc = ServingEndpointOwnershipService(session=MagicMock())
    svc.crew_service = MagicMock()
    svc.crew_service.get_by_group = AsyncMock(return_value=crew)
    return svc


def _served(crew_ids):
    return patch.object(
        endpoint_ownership, "_read_endpoint", return_value=(ENDPOINT, crew_ids)
    )


def _crew():
    return SimpleNamespace(id=CREW_ID, group_id="g1")


def test_crew_is_read_through_the_crew_service():
    assert isinstance(
        ServingEndpointOwnershipService(session=MagicMock()).crew_service, CrewService
    )


@pytest.mark.asyncio
async def test_owned_endpoint_passes_and_is_returned():
    svc = _service(_crew())
    with _served({str(CREW_ID)}):
        endpoint = await svc.assert_endpoint_belongs_to_crew(str(CREW_ID), "ep", GC)
    assert endpoint is ENDPOINT
    svc.crew_service.get_by_group.assert_awaited_once_with(CREW_ID, GC)


@pytest.mark.asyncio
async def test_crew_of_another_group_is_refused_before_touching_databricks():
    svc = _service(None)  # get_by_group filters by group -> not visible
    with patch.object(endpoint_ownership, "_read_endpoint") as read:
        with pytest.raises(NotFoundError):
            await svc.assert_endpoint_belongs_to_crew(str(CREW_ID), "ep", GC)
    read.assert_not_called()


@pytest.mark.asyncio
async def test_endpoint_serving_another_crew_is_refused():
    svc = _service(_crew())
    with _served({str(uuid.uuid4())}):
        with pytest.raises(NotFoundError):
            await svc.assert_endpoint_belongs_to_crew(str(CREW_ID), "ep", GC)


@pytest.mark.asyncio
async def test_endpoint_without_provenance_is_refused():
    svc = _service(_crew())
    with _served(set()):
        with pytest.raises(NotFoundError):
            await svc.assert_endpoint_belongs_to_crew(str(CREW_ID), "ep", GC)


@pytest.mark.parametrize(
    "error, status",
    [
        (NotFound("Endpoint ep does not exist"), 404),
        (ResourceDoesNotExist("Endpoint ep does not exist"), 404),
        (Unauthenticated("token expired for sp@example.com"), 401),
        (PermissionDenied("sp@example.com cannot view ep"), 403),
        (TemporarilyUnavailable("try later"), 503),
        (TooManyRequests("slow down"), 503),
        (requests.exceptions.ConnectionError("https://example.com refused"), 503),
        (requests.exceptions.ReadTimeout("read timed out"), 503),
        (InternalError("upstream 500 at https://example.com"), 502),
        (RuntimeError("unexpected"), 502),
    ],
)
@pytest.mark.asyncio
async def test_a_failed_read_maps_to_the_right_status(error, status, caplog):
    """Only a real "does not exist" is 404: an expired credential or an outage
    must not tell the caller the deployment is gone."""
    svc = _service(_crew())
    with patch.object(endpoint_ownership, "_read_endpoint", side_effect=error):
        with caplog.at_level("INFO", logger=endpoint_ownership.__name__):
            with pytest.raises(KasalError) as raised:
                await svc.assert_endpoint_belongs_to_crew(str(CREW_ID), "ep", GC)

    assert raised.value.status_code == status
    assert raised.value.__cause__ is error
    # The SDK's text (hosts, principals) is logged, never returned.
    assert "example.com" not in raised.value.detail
    assert "ep" in caplog.text
    if status == 404:
        assert isinstance(raised.value, NotFoundError)
    else:
        assert any(r.levelname == "ERROR" for r in caplog.records)


@pytest.mark.asyncio
async def test_malformed_crew_id_is_refused():
    svc = _service(_crew())
    with pytest.raises(NotFoundError):
        await svc.assert_endpoint_belongs_to_crew("not-a-uuid", "ep", GC)
    svc.crew_service.get_by_group.assert_not_called()


@pytest.mark.asyncio
async def test_no_workspace_is_refused():
    svc = ServingEndpointOwnershipService(session=MagicMock())
    with pytest.raises(NotFoundError):
        await svc.assert_endpoint_belongs_to_crew(
            str(CREW_ID), "ep", SimpleNamespace(primary_group_id=None, group_ids=[])
        )


def test_read_endpoint_reads_crew_config_of_each_served_model(tmp_path):
    entity = SimpleNamespace(entity_name="main.kasal.crew_model", entity_version="3")
    endpoint = SimpleNamespace(
        config=SimpleNamespace(served_entities=[entity], served_models=None)
    )
    ws = MagicMock()
    ws.serving_endpoints.get.return_value = endpoint

    config_file = tmp_path / "crew_config.json"
    config_file.write_text(json.dumps({"id": str(CREW_ID), "name": "c"}))
    client = MagicMock()
    client.get_model_version.return_value = SimpleNamespace(run_id="run-1")
    client.download_artifacts.return_value = str(config_file)

    with (
        patch("databricks.sdk.WorkspaceClient", return_value=ws),
        patch("mlflow.tracking.MlflowClient", return_value=client) as client_cls,
    ):
        assert _read_endpoint("ep") == (endpoint, {str(CREW_ID)})

    client_cls.assert_called_once_with(
        tracking_uri="databricks", registry_uri="databricks-uc"
    )
    client.get_model_version.assert_called_once_with("main.kasal.crew_model", "3")
    assert client.download_artifacts.call_args.args[:2] == (
        "run-1",
        "config/crew_config.json",
    )


def test_read_endpoint_ignores_models_without_crew_config():
    entity = SimpleNamespace(entity_name="some_model", entity_version="1")
    endpoint = SimpleNamespace(
        config=SimpleNamespace(served_entities=[entity], served_models=None)
    )
    ws = MagicMock()
    ws.serving_endpoints.get.return_value = endpoint
    client = MagicMock()
    client.get_model_version.return_value = SimpleNamespace(run_id="run-1")
    client.download_artifacts.side_effect = OSError("no such artifact")

    with (
        patch("databricks.sdk.WorkspaceClient", return_value=ws),
        patch("mlflow.tracking.MlflowClient", return_value=client) as client_cls,
    ):
        assert _read_endpoint("ep") == (endpoint, set())
    client_cls.assert_called_once_with(
        tracking_uri="databricks", registry_uri="databricks"
    )
