"""The memory browser's ontology read: what a teamspace's graph holds, without
a run, an embedder or an LLM."""

from unittest.mock import AsyncMock

import pytest

from src.api.memory_backend.records_router import (
    forget_memory_fact,
    get_memory_graph,
)
from src.schemas.memory_backend import (
    MemoryBackendConfig,
    MemoryBackendType,
    MemoryTuningConfig,
)
from src.services.memory.engine.extract import parse_facts
from src.services.memory.storage import graph_browse
from src.services.memory.storage.local_graph import LocalGraphStore

from .graph_fakes import embed, fact


def _config(graph=True):
    return MemoryBackendConfig(
        backend_type=MemoryBackendType.DEFAULT,
        cognitive_config=MemoryTuningConfig(graph_memory=graph),
    )


@pytest.fixture
def store_dir(tmp_path, monkeypatch):
    monkeypatch.setattr(graph_browse, "local_memory_store_dir", lambda group: tmp_path)
    return tmp_path


def _write(store_dir, scope="/grp"):
    store = LocalGraphStore(store_dir / "memory.db", embedder=embed, scope=scope)
    for item in (
        fact(
            "Atlas", "deadline_is", "15 June", "Atlas is due 15 June.", kind="project"
        ),
        fact(
            "Atlas", "deadline_is", "30 July", "Atlas is due 30 July.", kind="project"
        ),
        fact(
            "Atlas",
            "owned_by",
            "Dana",
            "Dana owns Atlas.",
            kind="project",
            object_type="person",
        ),
    ):
        (parsed,) = parse_facts({"facts": [item]})
        store.add_fact(
            parsed, store.embed(parsed.statement), metadata={"execution_id": "run-1"}
        )
    store.close()


def test_snapshot_lists_entities_and_facts_including_replaced_ones(store_dir):
    _write(store_dir)
    snapshot = graph_browse.graph_snapshot(_config(), "grp")
    assert snapshot.enabled and snapshot.backend == "default"
    assert {(e.name, e.type) for e in snapshot.entities} == {
        ("Atlas", "project"),
        ("Dana", "person"),
    }
    by_object = {f.object: f for f in snapshot.facts}
    assert by_object["15 June"].valid_to is not None
    assert by_object["15 June"].superseded_by == by_object["30 July"].id
    assert by_object["30 July"].valid_to is None
    assert by_object["Dana"].object_id is not None  # an edge between two entities
    assert by_object["Dana"].execution_id == "run-1"


def test_snapshot_is_scoped_to_the_teamspace(store_dir):
    _write(store_dir, scope="/other")
    snapshot = graph_browse.graph_snapshot(_config(), "grp")
    assert snapshot.enabled and snapshot.entities == [] and snapshot.facts == []


def test_switched_off_reads_nothing(store_dir):
    _write(store_dir)
    for config in (_config(graph=False), None):
        snapshot = graph_browse.graph_snapshot(config, "grp")
        assert not snapshot.enabled and snapshot.facts == []


def test_looking_does_not_create_a_store(store_dir):
    assert graph_browse.graph_snapshot(_config(), "grp").facts == []
    assert not (store_dir / "memory.db").exists()


@pytest.mark.asyncio
async def test_endpoint_reads_the_callers_teamspace(store_dir):
    _write(store_dir)
    service = AsyncMock()
    service.get_active_config.return_value = _config()
    group_context = type("Ctx", (), {"primary_group_id": "grp", "group_ids": ["grp"]})()
    response = await get_memory_graph(group_context, service, limit=10)
    service.get_active_config.assert_awaited_once_with("grp")
    assert len(response.facts) == 3


@pytest.mark.asyncio
async def test_forgetting_a_fact_retires_it_without_deleting_it(store_dir):
    _write(store_dir)
    service = AsyncMock()
    service.get_active_config.return_value = _config()
    group_context = type("Ctx", (), {"primary_group_id": "grp", "group_ids": ["grp"]})()
    owner = next(
        f
        for f in graph_browse.graph_snapshot(_config(), "grp").facts
        if f.object == "Dana"
    )

    response = await forget_memory_fact(owner.id, group_context, service)
    assert response.forgotten is True
    after = {f.id: f for f in graph_browse.graph_snapshot(_config(), "grp").facts}
    assert after[owner.id].valid_to is not None  # still listed, as history
    again = await forget_memory_fact(owner.id, group_context, service)
    assert again.forgotten is False


def test_a_fact_of_another_teamspace_cannot_be_forgotten(store_dir):
    _write(store_dir, scope="/other")
    store = LocalGraphStore(store_dir / "memory.db", embedder=embed, scope="/other")
    theirs, *_rest = store.list_facts()
    store.close()
    assert graph_browse.forget_fact(_config(), "grp", theirs.id) is False
