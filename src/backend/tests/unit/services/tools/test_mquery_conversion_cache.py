"""The M-Query conversion cache stays inside one group and one caller's access.

A cached conversion is stored under the run's own group and a key that covers
the settings that shaped it, and it is served only after the caller's Power BI
credentials have been checked against the workspace (and dataset).

The tool-level tests run the real cache service and repository on SQLite with
two groups; only the Power BI conversion itself and the access check are faked.
"""

from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool

from src.models.powerbi_semantic_model_cache import PowerBISemanticModelCache
from src.services.tools import mquery_conversion_cache as cache
from src.services.tools.mquery_conversion_pipeline_tool import (
    MqueryConversionPipelineTool,
)
from src.utils.user_context import GroupContext, UserContext

A, B = "group_a", "group_b"
WORKSPACE = "11111111-2222-3333-4444-555555555555"
DATASET = "66666666-7777-8888-9999-000000000000"


@pytest.fixture
def db_url(tmp_path):
    path = tmp_path / "mquery_cache.db"
    sync = create_engine(f"sqlite:///{path}")
    PowerBISemanticModelCache.__table__.create(sync)
    sync.dispose()
    return f"sqlite+aiosqlite:///{path}"


@asynccontextmanager
async def _session(db_url):
    engine = create_async_engine(db_url, poolclass=NullPool)
    try:
        async with async_sessionmaker(engine, expire_on_commit=False)() as s:
            yield s
    finally:
        await engine.dispose()


@pytest.fixture
def routed_to(db_url, monkeypatch):
    import src.db.session as db_session

    monkeypatch.setattr(db_session, "routed_scoped_session", lambda: _session(db_url))
    yield db_url
    UserContext.clear_context()


def _as_group(group_id):
    UserContext.set_group_context(GroupContext(group_ids=[group_id]))


def _convert(*, can_read=True, conversion_ok=True, **settings):
    """Run the tool once as the current group; return (output, conversions run)."""
    tool = MqueryConversionPipelineTool(
        workspace_id=WORKSPACE,
        dataset_id=DATASET,
        access_token="caller-token",
        **settings,
    )
    group = getattr(UserContext.get_group_context(), "primary_group_id", None)
    result = (
        {"success": True, "models": {}, "summary": {}, "model_count": 1}
        if conversion_ok
        else {"success": False, "error": "401 Unauthorized"}
    )
    conversion = AsyncMock(return_value=result)
    with (
        patch.object(MqueryConversionPipelineTool, "_execute_conversion", conversion),
        patch.object(
            MqueryConversionPipelineTool,
            "_format_output",
            MagicMock(return_value=f"converted for {group}"),
        ),
        patch.object(
            MqueryConversionPipelineTool, "_save_to_conversion_history", AsyncMock()
        ),
        patch(
            "src.services.tools.async_bridge.workspace_llm_credentials",
            return_value=(None, None),
        ),
        patch.object(cache, "caller_can_read", AsyncMock(return_value=can_read)),
    ):
        return tool._run(), conversion.await_count


async def _rows(db_url):
    from sqlalchemy import select

    async with _session(db_url) as s:
        rows = (await s.execute(select(PowerBISemanticModelCache))).scalars().all()
    return [(r.group_id, r.dataset_id) for r in rows]


# ── Tool level ──────────────────────────────────────────────────────────────


def test_group_a_result_is_not_served_to_group_b(routed_to):
    _as_group(A)
    out_a, _ = _convert()
    assert out_a == f"converted for {A}"

    _as_group(B)
    out_b, conversions = _convert()

    assert out_b == f"converted for {B}"
    assert conversions == 1


def test_same_group_rerun_is_served_from_cache(routed_to):
    _as_group(A)
    _convert()

    out, conversions = _convert()

    assert out == f"converted for {A}"
    assert conversions == 0


def test_cached_result_needs_credentials_that_can_read_the_dataset(routed_to):
    _as_group(A)
    _convert()

    out, conversions = _convert(can_read=False, conversion_ok=False)

    assert out.startswith("Error")
    assert "converted for" not in out
    assert conversions == 1


def test_changed_settings_miss_the_cache(routed_to):
    _as_group(A)
    _convert(target_catalog="main")

    _, conversions = _convert(target_catalog="other_catalog")

    assert conversions == 1


@pytest.mark.asyncio
async def test_rows_are_stored_under_the_run_group(routed_to):
    _as_group(A)
    _convert()

    rows = await _rows(routed_to)
    assert [g for g, _ in rows] == [A]
    assert all(key.startswith(f"{WORKSPACE}__{DATASET}__") for _, key in rows)


@pytest.mark.asyncio
async def test_without_a_group_nothing_is_cached_or_served(routed_to):
    UserContext.clear_context()

    _convert()
    _, conversions = _convert()

    assert conversions == 1
    assert await _rows(routed_to) == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "output",
    ["Error during validation: boom", "No semantic models found in workspace."],
)
async def test_failure_output_is_not_cached(routed_to, output):
    _as_group(A)
    await cache.save_cached_output("key", WORKSPACE, {"formatted_output": output})
    assert await _rows(routed_to) == []


# ── Key ─────────────────────────────────────────────────────────────────────


def test_key_ignores_secrets_and_tracks_settings():
    base = {"target_catalog": "main", "use_llm": True, "access_token": "t1"}
    same = cache.cache_key(WORKSPACE, DATASET, {**base, "access_token": "t2"})
    assert cache.cache_key(WORKSPACE, DATASET, base) == same
    assert cache.cache_key(WORKSPACE, DATASET, {**base, "use_llm": False}) != same
    assert cache.cache_key(WORKSPACE, None, base).startswith(f"{WORKSPACE}__all__")
    assert "t1" not in cache.cache_key(WORKSPACE, DATASET, base)


# ── Access check ────────────────────────────────────────────────────────────


def _http(status, datasets=()):
    response = MagicMock(status_code=status)
    response.json.return_value = {"value": [{"id": d} for d in datasets]}
    client = MagicMock()
    client.get = AsyncMock(return_value=response)
    client.__aenter__ = AsyncMock(return_value=client)
    client.__aexit__ = AsyncMock(return_value=None)
    return client


async def _can_read(client, dataset_id=DATASET, token="tok", workspace=WORKSPACE):
    token_fn = (
        AsyncMock(side_effect=token)
        if isinstance(token, Exception)
        else AsyncMock(return_value=token)
    )
    with (
        patch(
            "src.services.tools.powerbi_auth_utils.get_powerbi_access_token_from_config",
            token_fn,
        ),
        patch("httpx.AsyncClient", return_value=client),
    ):
        return await cache.caller_can_read(
            {"access_token": "tok"}, workspace, dataset_id
        )


@pytest.mark.asyncio
async def test_access_check_passes_when_the_dataset_is_listed():
    client = _http(200, [DATASET])
    assert await _can_read(client) is True
    url = client.get.await_args.args[0]
    assert url.endswith(f"/admin/groups/{WORKSPACE}/datasets")
    assert client.get.await_args.kwargs["headers"]["Authorization"] == "Bearer tok"


@pytest.mark.asyncio
async def test_access_check_without_a_dataset_needs_only_the_workspace():
    assert await _can_read(_http(200, []), dataset_id=None) is True


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "client",
    [_http(200, ["some-other-dataset"]), _http(401), _http(403), _http(429)],
)
async def test_access_check_fails_closed(client):
    assert await _can_read(client) is False


@pytest.mark.asyncio
async def test_access_check_fails_when_no_token_can_be_obtained():
    assert await _can_read(_http(200, [DATASET]), token=ValueError("bad")) is False


@pytest.mark.asyncio
async def test_access_check_escapes_the_workspace_id():
    client = _http(404)
    await _can_read(client, workspace="../../admin/x?y=1")
    url = client.get.await_args.args[0]
    assert "/admin/groups/..%2F..%2Fadmin%2Fx%3Fy%3D1/datasets" in url
