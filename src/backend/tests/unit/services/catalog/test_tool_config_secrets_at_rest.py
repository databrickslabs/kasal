"""Secrets in agent, crew and task tool configs reach the database as ciphertext.

Every request session commits, GETs included, so these drive the real catalog
services on a real SQLite engine and read the stored row back with raw SQL
after each commit. The ORM side must hold plaintext (tools are built from it);
the row must only ever hold ciphertext for a sensitive value.
"""

import json
import uuid

import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

import src.db.all_models  # noqa: F401  registers every table on Base
from src.db.base import Base
from src.schemas.agent import AgentCreate, AgentUpdate
from src.schemas.crew import CrewCreate
from src.schemas.task import TaskCreate, TaskUpdate
from src.services.catalog.agents import AgentService
from src.services.catalog.crews import CrewService
from src.services.catalog.tasks import TaskService
from src.utils.user_context import GroupContext

SECRET = "sp-secret-value-123"
PAT = "dapi-pat-value-456"
WS = "ws-at-rest"

GROUP = GroupContext(
    group_ids=[WS], group_email="a@example.com", email_domain="example.com"
)
PBI = {
    "Power BI Relationships Tool": {
        "tenant_id": "tenant-1",
        "client_id": "client-1",
        "client_secret": SECRET,
    }
}


@pytest_asyncio.fixture
async def factory(isolated_encryption_keys):
    engine = create_async_engine(
        "sqlite+aiosqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    # The flags the request sessionmaker uses (db/session.py).
    yield sessionmaker(
        engine, class_=AsyncSession, expire_on_commit=False, autoflush=False
    )
    await engine.dispose()


async def _raw(factory, table, column, row_id):
    async with factory() as session:
        value = (
            await session.execute(
                text(f"SELECT {column} FROM {table} WHERE id = :id"), {"id": row_id}
            )
        ).scalar_one()
    return json.loads(value) if isinstance(value, str) else value


def _crew_id(crew_id):
    # crews.id is a UUID column, stored on SQLite as 32 hex chars.
    return uuid.UUID(str(crew_id)).hex


async def _seed_ciphertext(factory, table, row_id, column="tool_configs"):
    """Store PBI with its secret encrypted, bypassing the ORM entirely."""
    from src.utils.sensitive_data_utils import encrypt_value

    leaf = dict(PBI["Power BI Relationships Tool"])
    leaf["client_secret"] = encrypt_value(SECRET)
    sealed = {"Power BI Relationships Tool": leaf}
    async with factory() as session:
        await session.execute(
            text(f"UPDATE {table} SET {column} = :v WHERE id = :id"),
            {"v": json.dumps(sealed), "id": row_id},
        )
        await session.commit()
    return sealed


def _agent_in(**extra):
    return AgentCreate(
        name="analyst", role="r", goal="g", backstory="b", tool_configs=PBI, **extra
    )


async def _create_agent(factory):
    async with factory() as session:
        agent = await AgentService(session).create_with_group(_agent_in(), GROUP)
        await session.commit()
        return agent.id


class TestAgents:
    async def test_a_create_stores_only_ciphertext(self, factory):
        agent_id = await _create_agent(factory)
        stored = await _raw(factory, "agents", "tool_configs", agent_id)
        leaf = stored["Power BI Relationships Tool"]
        assert leaf["client_secret"].startswith("ENC:")
        assert leaf["tenant_id"] == "tenant-1"
        assert SECRET not in json.dumps(stored)

    async def test_the_service_still_hands_back_plaintext(self, factory):
        agent_id = await _create_agent(factory)
        async with factory() as session:
            agent = await AgentService(session).get_with_group_check(agent_id, GROUP)
            assert agent.tool_configs == PBI

    @pytest.mark.parametrize("read", ["list", "get", "get_by_id"])
    async def test_a_read_then_commit_writes_nothing(self, factory, read):
        agent_id = await _create_agent(factory)
        before = await _seed_ciphertext(factory, "agents", agent_id)
        async with factory() as session:
            service = AgentService(session)
            if read == "list":
                (agent,) = await service.find_by_group(GROUP)
            elif read == "get":
                agent = await service.get_with_group_check(agent_id, GROUP)
            else:
                agent = await service.get(agent_id)
            assert agent.tool_configs == PBI
            await session.commit()
        assert await _raw(factory, "agents", "tool_configs", agent_id) == before

    async def test_an_update_stores_only_ciphertext(self, factory):
        agent_id = await _create_agent(factory)
        changed = {"Power BI Relationships Tool": {"client_secret": "rotated-789"}}
        async with factory() as session:
            await AgentService(session).update_with_group_check(
                agent_id, AgentUpdate(tool_configs=changed), GROUP
            )
            await session.commit()
        stored = await _raw(factory, "agents", "tool_configs", agent_id)
        assert stored["Power BI Relationships Tool"]["client_secret"].startswith("ENC:")
        assert "rotated-789" not in json.dumps(stored)

    async def test_an_undecryptable_secret_is_kept_not_blanked(self, factory):
        agent_id = await _create_agent(factory)
        unreadable = {"Power BI Relationships Tool": {"client_secret": "ENC:Zm9v"}}
        async with factory() as session:
            await session.execute(
                text("UPDATE agents SET tool_configs = :v WHERE id = :id"),
                {"v": json.dumps(unreadable), "id": agent_id},
            )
            await session.commit()
        async with factory() as session:
            (agent,) = await AgentService(session).find_by_group(GROUP)
            leaf = agent.tool_configs["Power BI Relationships Tool"]
            assert leaf["client_secret"] == "ENC:Zm9v"
            await session.commit()
        assert await _raw(factory, "agents", "tool_configs", agent_id) == unreadable

    async def test_a_legacy_plaintext_row_still_reads(self, factory):
        agent_id = await _create_agent(factory)
        async with factory() as session:
            await session.execute(
                text("UPDATE agents SET tool_configs = :v WHERE id = :id"),
                {"v": json.dumps(PBI), "id": agent_id},
            )
            await session.commit()
        async with factory() as session:
            agent = await AgentService(session).get(agent_id)
            assert agent.tool_configs == PBI


def _crew_in():
    agent_node = {
        "id": "agent-a1",
        "type": "agentNode",
        "position": {"x": 0, "y": 0},
        "data": {
            "label": "analyst",
            "tool_configs": PBI,
            "embedder_config": {"provider": "openai", "config": {"api_key": SECRET}},
        },
    }
    task_node = {
        "id": "task-t1",
        "type": "taskNode",
        "position": {"x": 1, "y": 1},
        "data": {
            "label": "extract",
            "tool_configs": {
                "MCP_SERVERS": {"servers": [{"name": "s1", "auth_token": PAT}]}
            },
            "knowledge_sources": [{"type": "volume", "access_token": PAT}],
        },
    }
    return CrewCreate(
        name="pbi crew",
        agent_ids=["a1"],
        task_ids=["t1"],
        nodes=[agent_node, task_node],
        edges=[],
        tool_configs={"PowerBI": {"password": SECRET}},
    )


async def _create_crew(factory):
    async with factory() as session:
        crew = await CrewService(session).create_with_group(_crew_in(), GROUP)
        await session.commit()
        return crew.id


class TestCrews:
    async def test_every_leaf_in_nodes_is_ciphertext(self, factory):
        crew_id = await _create_crew(factory)
        nodes = await _raw(factory, "crews", "nodes", _crew_id(crew_id))
        agent_data, task_data = nodes[0]["data"], nodes[1]["data"]
        pbi = agent_data["tool_configs"]["Power BI Relationships Tool"]
        assert pbi["client_secret"].startswith("ENC:")
        assert agent_data["embedder_config"]["config"]["api_key"].startswith("ENC:")
        server = task_data["tool_configs"]["MCP_SERVERS"]["servers"][0]
        assert server["auth_token"].startswith("ENC:")
        assert server["name"] == "s1"
        assert task_data["knowledge_sources"][0]["access_token"].startswith("ENC:")
        assert SECRET not in json.dumps(nodes) and PAT not in json.dumps(nodes)

    async def test_crew_tool_configs_are_ciphertext(self, factory):
        crew_id = await _create_crew(factory)
        stored = await _raw(factory, "crews", "tool_configs", _crew_id(crew_id))
        assert stored["PowerBI"]["password"].startswith("ENC:")

    @pytest.mark.parametrize("read", ["list", "get"])
    async def test_a_read_then_commit_writes_nothing(self, factory, read):
        crew_id = await _create_crew(factory)
        nodes = await _raw(factory, "crews", "nodes", _crew_id(crew_id))
        configs = await _seed_ciphertext(factory, "crews", _crew_id(crew_id))
        async with factory() as session:
            service = CrewService(session)
            if read == "list":
                (crew,) = await service.find_by_group(GROUP)
            else:
                crew = await service.get_by_group(crew_id, GROUP)
            assert crew.nodes[0]["data"]["tool_configs"] == PBI
            assert crew.tool_configs == PBI
            await session.commit()
        assert await _raw(factory, "crews", "nodes", _crew_id(crew_id)) == nodes
        assert await _raw(factory, "crews", "tool_configs", _crew_id(crew_id)) == (
            configs
        )


async def _create_task(factory):
    async with factory() as session:
        task = await TaskService(session).create_with_group(
            TaskCreate(
                name="extract",
                description="d",
                expected_output="o",
                tool_configs=PBI,
                callback_config={"volume_path": "/Volumes/a/b/c", "token": PAT},
                # Where the task form saves it: config.callback_config.
                config={"callback_config": {"token": PAT}, "timeout": 30},
            ),
            GROUP,
        )
        await session.commit()
        return task.id


class TestTasks:
    async def test_tool_configs_and_callback_config_are_ciphertext(self, factory):
        task_id = await _create_task(factory)
        configs = await _raw(factory, "tasks", "tool_configs", task_id)
        callback = await _raw(factory, "tasks", "callback_config", task_id)
        pbi = configs["Power BI Relationships Tool"]
        assert pbi["client_secret"].startswith("ENC:")
        assert callback["token"].startswith("ENC:")
        assert callback["volume_path"] == "/Volumes/a/b/c"
        assert SECRET not in json.dumps(configs) and PAT not in json.dumps(callback)

    async def test_the_callback_token_in_config_is_ciphertext(self, factory):
        task_id = await _create_task(factory)
        config = await _raw(factory, "tasks", "config", task_id)
        assert config["callback_config"]["token"].startswith("ENC:")
        assert config["timeout"] == 30
        assert PAT not in json.dumps(config)
        async with factory() as session:
            task = await TaskService(session).get(task_id)
            assert task.config["callback_config"]["token"] == PAT

    async def test_a_read_then_commit_writes_nothing(self, factory):
        task_id = await _create_task(factory)
        before = await _seed_ciphertext(factory, "tasks", task_id)
        async with factory() as session:
            (task,) = await TaskService(session).find_by_group(GROUP)
            assert task.tool_configs == PBI
            assert task.callback_config["token"] == PAT
            await session.commit()
        assert await _raw(factory, "tasks", "tool_configs", task_id) == before

    @pytest.mark.parametrize("path", ["partial", "full"])
    async def test_both_update_paths_store_ciphertext(self, factory, path):
        task_id = await _create_task(factory)
        changed = {"Power BI Relationships Tool": {"client_secret": "rotated-789"}}
        async with factory() as session:
            service = TaskService(session)
            if path == "partial":
                await service.update_with_group_check(
                    task_id, TaskUpdate(tool_configs=changed), GROUP
                )
            else:
                await service.update_full_with_group_check(
                    task_id, {"tool_configs": changed}, GROUP
                )
            await session.commit()
        stored = await _raw(factory, "tasks", "tool_configs", task_id)
        assert stored["Power BI Relationships Tool"]["client_secret"].startswith("ENC:")
        assert "rotated-789" not in json.dumps(stored)
