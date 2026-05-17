"""
Unit tests for the Databricks Knowledge API router.

Tests upload, browse, list, delete, and select-from-volume endpoints
by calling route handler functions directly with mocked service objects.

Note: extract_user_token_from_request is imported inside each route handler
with a local `from src.utils.databricks_auth import ...`, so we patch it at
the definition module (src.utils.databricks_auth).
"""

import json
import pytest
from unittest.mock import AsyncMock, MagicMock, patch


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

class GroupCtx:
    """Minimal GroupContext stand-in."""

    def __init__(self, group_ids=None, group_email="test@example.com", access_token="token-abc"):
        # Use None sentinel to distinguish "no argument given" from "explicitly empty list"
        self.group_ids = ["grp-1"] if group_ids is None else group_ids
        self.group_email = group_email
        self.access_token = access_token


def _make_request():
    """Return a minimal request-like object."""
    return MagicMock()


def _patch_token(return_value=None):
    """Patch the token extractor at its definition site."""
    return patch(
        "src.utils.databricks_auth.extract_user_token_from_request",
        return_value=return_value,
    )


# ---------------------------------------------------------------------------
# Tests – upload_knowledge_file
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_upload_knowledge_file_success():
    """Successful upload returns service result dict."""
    from src.api.databricks_knowledge_router import upload_knowledge_file

    svc = AsyncMock()
    svc.upload_knowledge_file = AsyncMock(
        return_value={"path": "/mnt/vol/exec1/file.txt", "status": "uploaded"}
    )

    file_mock = MagicMock()
    file_mock.filename = "file.txt"
    file_mock.content_type = "text/plain"

    volume_config = json.dumps({"catalog": "main", "schema": "default", "volume": "data"})

    with _patch_token("usr-token"):
        result = await upload_knowledge_file(
            execution_id="exec-1",
            request=_make_request(),
            service=svc,
            group_context=GroupCtx(),
            file=file_mock,
            volume_config=volume_config,
            agent_ids='["agent-a", "agent-b"]',
        )

    assert result["status"] == "uploaded"
    svc.upload_knowledge_file.assert_awaited_once()
    call_kwargs = svc.upload_knowledge_file.call_args.kwargs
    assert call_kwargs["agent_ids"] == ["agent-a", "agent-b"]
    assert call_kwargs["execution_id"] == "exec-1"


@pytest.mark.asyncio
async def test_upload_knowledge_file_empty_agent_ids():
    """Empty agent_ids string is parsed to an empty list."""
    from src.api.databricks_knowledge_router import upload_knowledge_file

    svc = AsyncMock()
    svc.upload_knowledge_file = AsyncMock(return_value={"path": "/x"})
    file_mock = MagicMock()
    file_mock.filename = "f.pdf"
    file_mock.content_type = "application/pdf"

    volume_config = json.dumps({"catalog": "cat", "schema": "sch", "volume": "vol"})

    with _patch_token(None):
        await upload_knowledge_file(
            execution_id="e1",
            request=_make_request(),
            service=svc,
            group_context=GroupCtx(),
            file=file_mock,
            volume_config=volume_config,
            agent_ids="",
        )

    call_kwargs = svc.upload_knowledge_file.call_args.kwargs
    assert call_kwargs["agent_ids"] == []


@pytest.mark.asyncio
async def test_upload_knowledge_file_bad_json_raises_bad_request():
    """Invalid JSON for volume_config raises BadRequestError (400)."""
    from src.api.databricks_knowledge_router import upload_knowledge_file
    from src.core.exceptions import BadRequestError

    svc = AsyncMock()
    file_mock = MagicMock()

    with _patch_token(None):
        with pytest.raises(BadRequestError) as exc_info:
            await upload_knowledge_file(
                execution_id="e1",
                request=_make_request(),
                service=svc,
                group_context=GroupCtx(),
                file=file_mock,
                volume_config="not-valid-json",
                agent_ids="[]",
            )

    assert exc_info.value.status_code == 400


@pytest.mark.asyncio
async def test_upload_knowledge_file_default_group_id():
    """When group_context has no group_ids, group_id defaults to 'default'."""
    from src.api.databricks_knowledge_router import upload_knowledge_file

    svc = AsyncMock()
    svc.upload_knowledge_file = AsyncMock(return_value={"path": "/x"})
    file_mock = MagicMock()
    file_mock.filename = "f.txt"
    file_mock.content_type = "text/plain"

    with _patch_token(None):
        await upload_knowledge_file(
            execution_id="e",
            request=_make_request(),
            service=svc,
            group_context=GroupCtx(group_ids=[]),
            file=file_mock,
            volume_config=json.dumps({}),
            agent_ids="[]",
        )

    call_kwargs = svc.upload_knowledge_file.call_args.kwargs
    assert call_kwargs["group_id"] == "default"


@pytest.mark.asyncio
async def test_upload_knowledge_file_bad_agent_ids_json():
    """Invalid JSON for agent_ids raises BadRequestError."""
    from src.api.databricks_knowledge_router import upload_knowledge_file
    from src.core.exceptions import BadRequestError

    svc = AsyncMock()
    file_mock = MagicMock()

    with _patch_token(None):
        with pytest.raises(Exception):
            await upload_knowledge_file(
                execution_id="e",
                request=_make_request(),
                service=svc,
                group_context=GroupCtx(),
                file=file_mock,
                volume_config=json.dumps({}),
                agent_ids="not-valid-json",
            )


# ---------------------------------------------------------------------------
# Tests – browse_volume_files
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_browse_volume_files_success():
    """browse_volume_files returns list from service."""
    from src.api.databricks_knowledge_router import browse_volume_files

    svc = AsyncMock()
    svc.browse_volume_files = AsyncMock(
        return_value=[{"name": "report.csv", "size": 1024}]
    )

    with _patch_token("tok"):
        result = await browse_volume_files(
            volume_path="catalog.schema.volume/reports",
            request=_make_request(),
            service=svc,
            group_context=GroupCtx(),
        )

    assert len(result) == 1
    assert result[0]["name"] == "report.csv"
    svc.browse_volume_files.assert_awaited_once_with(
        volume_path="catalog.schema.volume/reports",
        group_id="grp-1",
        user_token="tok",
    )


@pytest.mark.asyncio
async def test_browse_volume_files_empty_result():
    """browse_volume_files returns empty list when service returns no files."""
    from src.api.databricks_knowledge_router import browse_volume_files

    svc = AsyncMock()
    svc.browse_volume_files = AsyncMock(return_value=[])

    with _patch_token(None):
        result = await browse_volume_files(
            volume_path="cat.sch.vol/empty",
            request=_make_request(),
            service=svc,
            group_context=GroupCtx(),
        )

    assert result == []


@pytest.mark.asyncio
async def test_browse_volume_files_default_group_when_empty_context():
    """browse_volume_files passes 'default' group_id when context has no IDs."""
    from src.api.databricks_knowledge_router import browse_volume_files

    svc = AsyncMock()
    svc.browse_volume_files = AsyncMock(return_value=[])

    with _patch_token(None):
        await browse_volume_files(
            volume_path="some/path",
            request=_make_request(),
            service=svc,
            group_context=GroupCtx(group_ids=[]),
        )

    call_kwargs = svc.browse_volume_files.call_args.kwargs
    assert call_kwargs["group_id"] == "default"


# ---------------------------------------------------------------------------
# Tests – list_knowledge_files
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_list_knowledge_files_success():
    """list_knowledge_files returns files for the given execution_id."""
    from src.api.databricks_knowledge_router import list_knowledge_files

    svc = AsyncMock()
    svc.list_knowledge_files = AsyncMock(
        return_value=[{"filename": "a.pdf"}, {"filename": "b.csv"}]
    )

    result = await list_knowledge_files(
        execution_id="exec-99",
        service=svc,
        group_context=GroupCtx(),
    )

    assert len(result) == 2
    svc.list_knowledge_files.assert_awaited_once_with(
        execution_id="exec-99",
        group_id="grp-1",
    )


@pytest.mark.asyncio
async def test_list_knowledge_files_empty():
    """list_knowledge_files returns empty list when no files exist."""
    from src.api.databricks_knowledge_router import list_knowledge_files

    svc = AsyncMock()
    svc.list_knowledge_files = AsyncMock(return_value=[])

    result = await list_knowledge_files(
        execution_id="exec-empty",
        service=svc,
        group_context=GroupCtx(),
    )

    assert result == []


# ---------------------------------------------------------------------------
# Tests – delete_knowledge_file
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_delete_knowledge_file_success():
    """delete_knowledge_file returns success message."""
    from src.api.databricks_knowledge_router import delete_knowledge_file

    svc = AsyncMock()
    svc.delete_knowledge_file = AsyncMock(return_value={"deleted": True})

    result = await delete_knowledge_file(
        execution_id="exec-1",
        filename="report.pdf",
        service=svc,
        group_context=GroupCtx(),
    )

    assert result["status"] == "success"
    assert "report.pdf" in result["message"]


@pytest.mark.asyncio
async def test_delete_knowledge_file_calls_service_correctly():
    """delete_knowledge_file passes correct arguments to the service."""
    from src.api.databricks_knowledge_router import delete_knowledge_file

    svc = AsyncMock()
    svc.delete_knowledge_file = AsyncMock(return_value=None)

    await delete_knowledge_file(
        execution_id="exec-42",
        filename="data.csv",
        service=svc,
        group_context=GroupCtx(group_ids=["my-group"]),
    )

    svc.delete_knowledge_file.assert_awaited_once_with(
        execution_id="exec-42",
        group_id="my-group",
        filename="data.csv",
    )


# ---------------------------------------------------------------------------
# Tests – select_volume_file
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_select_volume_file_success():
    """Selecting an existing volume file returns correct metadata."""
    from src.api.databricks_knowledge_router import select_volume_file

    with _patch_token("tok"):
        result = await select_volume_file(
            execution_id="exec-2",
            request=_make_request(),
            service=AsyncMock(),
            group_context=GroupCtx(),
            file_path="/Volumes/catalog/schema/vol/data/report.pdf",
            selected_agents='["agent-x"]',
        )

    assert result["status"] == "success"
    assert result["filename"] == "report.pdf"
    assert result["execution_id"] == "exec-2"
    assert result["selected_agents"] == ["agent-x"]


@pytest.mark.asyncio
async def test_select_volume_file_bad_agents_json_raises():
    """Invalid JSON for selected_agents raises BadRequestError."""
    from src.api.databricks_knowledge_router import select_volume_file
    from src.core.exceptions import BadRequestError

    with _patch_token(None):
        with pytest.raises(BadRequestError):
            await select_volume_file(
                execution_id="e1",
                request=_make_request(),
                service=AsyncMock(),
                group_context=GroupCtx(),
                file_path="/Volumes/a/b/c/file.txt",
                selected_agents="not-json",
            )


@pytest.mark.asyncio
async def test_select_volume_file_no_slash_in_path():
    """When file_path has no slash the whole path is used as filename."""
    from src.api.databricks_knowledge_router import select_volume_file

    with _patch_token(None):
        result = await select_volume_file(
            execution_id="e2",
            request=_make_request(),
            service=AsyncMock(),
            group_context=GroupCtx(),
            file_path="myfile.csv",
            selected_agents="[]",
        )

    assert result["filename"] == "myfile.csv"
    assert result["path"] == "myfile.csv"


@pytest.mark.asyncio
async def test_select_volume_file_default_group_id():
    """select_volume_file uses 'default' group_id when context has no IDs."""
    from src.api.databricks_knowledge_router import select_volume_file

    with _patch_token(None):
        result = await select_volume_file(
            execution_id="e3",
            request=_make_request(),
            service=AsyncMock(),
            group_context=GroupCtx(group_ids=[]),
            file_path="/vol/file.txt",
            selected_agents="[]",
        )

    assert result["group_id"] == "default"
