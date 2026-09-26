"""Serving-endpoint ownership check before a deployment is deleted.

The delete runs with the app's Databricks credential, so the endpoint must be
proven to serve this crew, and the crew must be in the caller's group.
"""

import json
import uuid
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.core.exceptions import NotFoundError
from src.services.deployment import endpoint_ownership
from src.services.deployment.endpoint_ownership import (
    ServingEndpointOwnershipService,
    _served_crew_ids,
)

CREW_ID = uuid.uuid4()


def _service(crew=None):
    svc = ServingEndpointOwnershipService(session=MagicMock())
    svc.crew_repository = MagicMock()
    svc.crew_repository.get_by_group = AsyncMock(return_value=crew)
    return svc


def _crew():
    return SimpleNamespace(id=CREW_ID, group_id="g1")


@pytest.mark.asyncio
async def test_owned_endpoint_passes():
    svc = _service(_crew())
    with patch.object(
        endpoint_ownership, "_served_crew_ids", return_value={str(CREW_ID)}
    ):
        await svc.assert_endpoint_belongs_to_crew(str(CREW_ID), "ep", ["g1"])
    svc.crew_repository.get_by_group.assert_awaited_once_with(CREW_ID, ["g1"])


@pytest.mark.asyncio
async def test_crew_of_another_group_is_refused_before_touching_databricks():
    svc = _service(None)  # get_by_group filters by group -> not visible
    with patch.object(endpoint_ownership, "_served_crew_ids") as served:
        with pytest.raises(NotFoundError):
            await svc.assert_endpoint_belongs_to_crew(str(CREW_ID), "ep", ["g2"])
    served.assert_not_called()


@pytest.mark.asyncio
async def test_endpoint_serving_another_crew_is_refused():
    svc = _service(_crew())
    with patch.object(
        endpoint_ownership, "_served_crew_ids", return_value={str(uuid.uuid4())}
    ):
        with pytest.raises(NotFoundError):
            await svc.assert_endpoint_belongs_to_crew(str(CREW_ID), "ep", ["g1"])


@pytest.mark.asyncio
async def test_endpoint_without_provenance_is_refused():
    svc = _service(_crew())
    with patch.object(endpoint_ownership, "_served_crew_ids", return_value=set()):
        with pytest.raises(NotFoundError):
            await svc.assert_endpoint_belongs_to_crew(str(CREW_ID), "ep", ["g1"])


@pytest.mark.asyncio
async def test_unreadable_endpoint_is_refused():
    svc = _service(_crew())
    with patch.object(
        endpoint_ownership, "_served_crew_ids", side_effect=RuntimeError("404")
    ):
        with pytest.raises(NotFoundError):
            await svc.assert_endpoint_belongs_to_crew(str(CREW_ID), "ep", ["g1"])


@pytest.mark.asyncio
async def test_malformed_crew_id_is_refused():
    svc = _service(_crew())
    with pytest.raises(NotFoundError):
        await svc.assert_endpoint_belongs_to_crew("not-a-uuid", "ep", ["g1"])
    svc.crew_repository.get_by_group.assert_not_called()


@pytest.mark.asyncio
async def test_no_groups_is_refused():
    svc = _service(None)
    with pytest.raises(NotFoundError):
        await svc.assert_endpoint_belongs_to_crew(str(CREW_ID), "ep", [])


def test_served_crew_ids_reads_crew_config_of_each_served_model(tmp_path):
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
        assert _served_crew_ids("ep") == {str(CREW_ID)}

    client_cls.assert_called_once_with(
        tracking_uri="databricks", registry_uri="databricks-uc"
    )
    client.get_model_version.assert_called_once_with("main.kasal.crew_model", "3")
    assert client.download_artifacts.call_args.args[:2] == (
        "run-1",
        "config/crew_config.json",
    )


def test_served_crew_ids_ignores_models_without_crew_config():
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
        assert _served_crew_ids("ep") == set()
    client_cls.assert_called_once_with(
        tracking_uri="databricks", registry_uri="databricks"
    )
