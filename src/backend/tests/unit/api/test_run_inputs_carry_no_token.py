"""A run's stored inputs never carry the caller's access token, and the run
APIs never return one.

The flow subprocess persists its launch config, which carries the caller's
token so the run can act as them. Everything here runs against a real SQLite
database and the real routers: the row is read back the way another member of
the workspace reads it (GET /executions, /executions/{id}, /executions/history).

Covered: a flow run written by the real flow-runner path (pre-created row and
new row), a crew run written by the real create path (which stores a fixed set
of keys, so this one guards rather than reproduces), and rows that were stored
WITH a token before the write side was fixed.
"""

from contextlib import asynccontextmanager
from datetime import datetime
from unittest.mock import AsyncMock, patch

import httpx
import pytest
import pytest_asyncio
from fastapi import FastAPI
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.pool import StaticPool

from src.api.execution_history_router import router as history_router
from src.api.executions_router import router as executions_router
from src.db.base import Base
from src.dependencies.providers import get_group_context, get_smart_db_session
from src.models.execution_history import ExecutionHistory
from src.schemas.execution import CrewConfig
from src.services.execution.service import ExecutionService
from src.services.flow_builder.flow_runner_service import FlowRunnerService
from src.utils.user_context import GroupContext

STORED_TOKEN = "stored-obo-token-value"
FRESH_TOKEN = "fresh-obo-token-value"
WS = "ws-a"
CREDENTIAL_KEYS = ("user_token", "access_token", "group_context")
AGENTS = {"writer": {"role": "Writer", "max_tokens": 4096, "llm": "some-model"}}
TASKS = {"draft": {"description": "Draft it", "agent": "writer"}}


def _ctx(token=FRESH_TOKEN):
    return GroupContext(group_ids=[WS], group_email="a@example.com", access_token=token)


@pytest_asyncio.fixture
async def maker():
    engine = create_async_engine(
        "sqlite+aiosqlite:///:memory:",
        poolclass=StaticPool,
        connect_args={"check_same_thread": False},
    )
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield async_sessionmaker(engine, expire_on_commit=False)
    await engine.dispose()


@pytest_asyncio.fixture
async def legacy(maker):
    async with maker() as session:
        session.add_all(
            [
                # Rows written before the fix: the flow child stored its whole
                # config, token included.
                ExecutionHistory(
                    job_id="flow-legacy",
                    status="COMPLETED",
                    group_id=WS,
                    execution_type="flow",
                    created_at=datetime(2026, 9, 1, 12, 0, 0),
                    inputs={
                        "nodes": [{"id": "n1"}],
                        "edges": [],
                        "inputs": {"topic": "t"},
                        "group_id": WS,
                        "user_token": STORED_TOKEN,
                        "access_token": STORED_TOKEN,
                    },
                ),
                ExecutionHistory(
                    job_id="crew-legacy",
                    status="COMPLETED",
                    group_id=WS,
                    execution_type="crew",
                    created_at=datetime(2026, 9, 1, 12, 1, 0),
                    inputs={
                        "agents_yaml": AGENTS,
                        "tasks_yaml": TASKS,
                        "inputs": {},
                        "model": "some-model",
                        "user_token": STORED_TOKEN,
                    },
                ),
            ]
        )
        await session.commit()


@pytest_asyncio.fixture
async def client(maker):
    app = FastAPI()
    # History first: its /executions/history routes would otherwise be read
    # as /executions/{execution_id}.
    app.include_router(history_router)
    app.include_router(executions_router)

    async def _session():
        async with maker() as session:
            yield session

    app.dependency_overrides[get_smart_db_session] = _session
    app.dependency_overrides[get_group_context] = _ctx
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="https://example.com"
    ) as c:
        yield c


@asynccontextmanager
async def _isolated(maker):
    async with maker() as session:
        yield session


async def _stored_inputs(maker, job_id):
    async with maker() as session:
        row = (
            await session.execute(
                select(ExecutionHistory).where(ExecutionHistory.job_id == job_id)
            )
        ).scalar_one()
        return row.id, row.inputs


def _assert_no_credentials(inputs):
    assert inputs, "expected stored inputs"
    for key in CREDENTIAL_KEYS:
        assert key not in inputs, f"{key} was stored"
    for token in (STORED_TOKEN, FRESH_TOKEN):
        assert token not in str(inputs)


async def _assert_apis_return_no_token(client, maker, job_id):
    row_id, _ = await _stored_inputs(maker, job_id)
    responses = [
        await client.get(f"/executions/{job_id}"),
        await client.get(
            "/executions", params={"include_payload": "true"}, headers={"group_id": WS}
        ),
        await client.get("/executions/history", params={"include_payload": "true"}),
        await client.get(f"/executions/history/{row_id}"),
    ]
    for response in responses:
        assert response.status_code == 200, response.text
        for token in (STORED_TOKEN, FRESH_TOKEN):
            assert token not in response.text, response.request.url
        for key in CREDENTIAL_KEYS:
            assert f'"{key}"' not in response.text, (key, response.request.url)
    return responses


def _flow_child_config(job_id):
    """What the flow subprocess hands FlowRunnerService.run_flow."""
    return {
        "flow_id": None,
        "run_name": "Flow run",
        "nodes": [{"id": "n1", "type": "crewNode", "data": {"label": "c"}}],
        "edges": [],
        "flow_config": {},
        "inputs": {"topic": "t"},
        "model": None,
        "group_context": _ctx(),
        "group_id": WS,
        "user_token": FRESH_TOKEN,
        "execution_id": job_id,
    }


async def _run_flow_child(maker, job_id):
    body = AsyncMock(return_value={"success": True, "result": {"content": "ok"}})
    with (
        patch.object(FlowRunnerService, "_run_dynamic_flow", body),
        patch(
            "src.db.session.get_isolated_db_session",
            side_effect=lambda: _isolated(maker),
        ),
    ):
        async with maker() as session:
            await FlowRunnerService(session).run_flow(
                flow_id=None,
                job_id=job_id,
                run_name="Flow run",
                config=_flow_child_config(job_id),
            )
    return body


class TestRowsStoredBeforeTheFix:
    @pytest.mark.asyncio
    async def test_the_run_apis_return_no_token(self, client, maker, legacy):
        await _assert_apis_return_no_token(client, maker, "flow-legacy")
        responses = await _assert_apis_return_no_token(client, maker, "crew-legacy")
        crew = responses[0].json()
        # Only the named keys go: a numeric *_tokens setting is not a secret.
        assert crew["inputs"]["agents_yaml"]["writer"]["max_tokens"] == 4096
        assert crew["inputs"]["model"] == "some-model"


class TestAFlowRunStoresNoToken:
    @pytest.mark.asyncio
    async def test_on_the_row_the_api_precreated(self, client, maker):
        async with maker() as session:
            session.add(
                ExecutionHistory(
                    job_id="flow-new",
                    status="RUNNING",
                    group_id=WS,
                    execution_type="flow",
                    inputs={"nodes": [{"id": "n1"}], "execution_type": "flow"},
                )
            )
            await session.commit()

        body = await _run_flow_child(maker, "flow-new")

        _, inputs = await _stored_inputs(maker, "flow-new")
        _assert_no_credentials(inputs)
        assert inputs["inputs"] == {"topic": "t"}
        # The flow body still runs as the caller.
        assert body.await_args.args[2]["user_token"] == FRESH_TOKEN
        await _assert_apis_return_no_token(client, maker, "flow-new")

    @pytest.mark.asyncio
    async def test_on_a_row_it_creates(self, client, maker):
        body = await _run_flow_child(maker, "flow-adhoc")

        _, inputs = await _stored_inputs(maker, "flow-adhoc")
        _assert_no_credentials(inputs)
        assert body.await_args.args[2]["user_token"] == FRESH_TOKEN
        await _assert_apis_return_no_token(client, maker, "flow-adhoc")


class TestACrewRunStoresNoToken:
    @pytest.mark.asyncio
    async def test_through_the_create_path(self, client, maker):
        config = CrewConfig(
            agents_yaml=AGENTS,
            tasks_yaml=TASKS,
            inputs={"topic": "t"},
            model="some-model",
            execution_type="crew",
        )
        with (
            patch(
                "src.db.session.get_isolated_db_session",
                side_effect=lambda: _isolated(maker),
            ),
            patch.object(
                ExecutionService, "_run_in_background", new_callable=AsyncMock
            ) as runner,
            patch("src.services.execution.service.ExecutionNameService"),
            patch.object(ExecutionService, "_generate_run_name_async", AsyncMock()),
        ):
            async with maker() as session:
                created = await ExecutionService(session=session).create_execution(
                    config, group_context=_ctx()
                )
        job_id = created["execution_id"]

        _, inputs = await _stored_inputs(maker, job_id)
        _assert_no_credentials(inputs)
        assert inputs["agents_yaml"]["writer"]["max_tokens"] == 4096
        # The run itself still gets the caller's token, from the request.
        assert runner.call_args.kwargs["group_context"].access_token == FRESH_TOKEN
        await _assert_apis_return_no_token(client, maker, job_id)
        ExecutionService.executions.pop(job_id, None)
