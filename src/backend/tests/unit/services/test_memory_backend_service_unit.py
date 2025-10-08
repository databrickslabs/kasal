import pytest
from types import SimpleNamespace
from unittest.mock import AsyncMock

from src.services.memory_backend_service import MemoryBackendService


@pytest.mark.asyncio
async def test_delegations_and_error_paths(monkeypatch):
    svc = MemoryBackendService(session=SimpleNamespace())

    # Patch sub-services
    svc._base_service = SimpleNamespace(
        create_memory_backend=AsyncMock(return_value="created"),
        get_memory_backends=AsyncMock(return_value=[1]),
        get_memory_backend=AsyncMock(return_value={"id": "b"}),
        get_default_memory_backend=AsyncMock(return_value={"id": "d"}),
        update_memory_backend=AsyncMock(return_value={"ok": True}),
        delete_memory_backend=AsyncMock(return_value=True),
        set_default_backend=AsyncMock(return_value=True),
        get_memory_stats=AsyncMock(return_value={"count": 0}),
        delete_all_and_create_disabled=AsyncMock(return_value={"done": True}),
        delete_disabled_configurations=AsyncMock(return_value=2),
    )
    svc._config_service = SimpleNamespace(
        get_active_config=AsyncMock(return_value={"active": True})
    )
    svc._connection_service = SimpleNamespace(
        test_databricks_connection=AsyncMock(return_value={"ok": 1}),
        get_databricks_endpoint_status=AsyncMock(return_value={"status": "RUNNING"}),
        get_databricks_auth_token=AsyncMock(return_value=("tok", "obo")),
    )
    svc._index_service = SimpleNamespace(
        create_databricks_index=AsyncMock(return_value={"created": True}),
        get_databricks_indexes=AsyncMock(return_value=["i1"]),
        delete_databricks_index=AsyncMock(return_value=True),
        delete_databricks_endpoint=AsyncMock(return_value=True),
        get_index_info=AsyncMock(return_value={"docs": 0}),
        empty_index=AsyncMock(return_value={"emptied": True}),
        get_index_documents=AsyncMock(return_value=[{"id": 1}]),
        search_vectors=AsyncMock(return_value=[{"s": 1}]),
    )
    svc._setup_service = SimpleNamespace(
        one_click_databricks_setup=AsyncMock(return_value={"ok": True})
    )
    svc._verification_service = SimpleNamespace(
        verify_databricks_resources=AsyncMock(return_value={"exists": True})
    )

    # Base CRUD
    assert await svc.create_memory_backend("g", SimpleNamespace()) == "created"
    assert await svc.get_memory_backends("g") == [1]
    assert (await svc.get_memory_backend("g", "b"))["id"] == "b"
    assert (await svc.get_default_memory_backend("g"))["id"] == "d"
    assert (await svc.update_memory_backend("g", "b", SimpleNamespace()))["ok"]
    assert await svc.delete_memory_backend("g", "b") is True
    assert await svc.set_default_backend("g", "b") is True
    assert (await svc.get_memory_stats("g", "c"))["count"] == 0
    assert (await svc.delete_all_and_create_disabled("g"))["done"] is True
    assert await svc.delete_disabled_configurations("g") == 2

    # get_all patches repository class at import location
    # Patch where it's imported (inside the repository module)
    from src.repositories import memory_backend_repository as repo_mod
    class FakeRepo:
        def __init__(self, session):
            self.session = session
        async def get_all(self):
            return ["A", "B"]
    repo_mod.MemoryBackendRepository = FakeRepo
    assert await svc.get_all() == ["A", "B"]

    # Config & connection & index
    assert (await svc.get_active_config("g"))["active"] is True
    assert (await svc.test_databricks_connection(SimpleNamespace()))["ok"] == 1
    assert (await svc.get_databricks_endpoint_status("u", "e"))["status"] == "RUNNING"
    tok, method = await svc._get_databricks_auth_token("u")
    assert tok == "tok" and method == "obo"

    assert (await svc.create_databricks_index(SimpleNamespace(), "vs", "c", "s", "t"))["created"]
    assert (await svc.get_databricks_indexes(SimpleNamespace())) == ["i1"]
    assert await svc.delete_databricks_index("u", "i", "e") is True
    assert await svc.delete_databricks_endpoint("u", "e") is True
    assert (await svc.get_index_info("u", "i", "e"))["docs"] == 0
    assert (await svc.empty_index("u", "i", "e", "delta", 1024))["emptied"]
    assert (await svc.get_index_documents("u", "e", "i"))[0]["id"] == 1

    # search_vectors error fallback -> []
    svc._index_service.search_vectors = AsyncMock(side_effect=Exception("boom"))
    assert await svc.search_vectors("u", "i", "e", [0.1], "short_term") == []

    # verification and workspace url
    out = await svc.verify_databricks_resources("u")
    assert out["exists"] is True

    # workspace url from unified auth
    async def fake_auth():
        return SimpleNamespace(workspace_url="https://ws", auth_method="obo")
    from src.utils import databricks_auth as auth_mod
    auth_mod.get_auth_context = fake_auth
    info = await svc.get_workspace_url()
    assert info["workspace_url"] == "https://ws" and info["detected"] is True

