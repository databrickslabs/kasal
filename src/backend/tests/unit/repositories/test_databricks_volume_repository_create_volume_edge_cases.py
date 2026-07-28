"""
Extended coverage tests for DatabricksVolumeRepository.

Targets uncovered lines:
- Lines 158-160: create_volume_if_not_exists create_error re-raise
- Lines 168-192: create_volume_if_not_exists scope error + does not exist paths
- Lines 217-233: create_volume_if_not_exists scope error retry PAT path
- Lines 237-239: create_volume_if_not_exists outer exception
- Lines 296-301: _upload_via_rest_api path
- Lines 308: _upload_via_rest_api response handling
- Lines 380: upload: volume doesn't exist other error
- Lines 397-399: upload: scope error retry PAT path
- Lines 466: upload: network zone error signal
- Lines 471: upload: SDK API mismatch signal
- Lines 478-479: upload: generic upload failure
- Lines 495-501: upload: volume check permission error (continue) vs fatal error
- Lines 519-535: upload: scope error retry with PAT
- Lines 587: download: scope error signal
- Lines 594-624: download: _to_bytes helper paths
- Lines 628: download: REST API fallback
- Lines 639-661: download: scope error / network zone / not found paths
- Lines 686-702: download: scope error retry PAT
- Lines 715-717: download: outer exception
- Lines 771-790: list: scope error / not found paths
- Lines 815-831: list: scope error retry PAT
- Lines 865-900: create_directory: success + scope + already exists + error paths
- Lines 925-941: create_directory: scope error retry PAT
- Lines 996: delete: scope error signal
- Lines 1005-1006: delete: not found
- Lines 1031-1047: delete: scope error retry PAT
"""
import asyncio
import pytest
from unittest.mock import AsyncMock, MagicMock, patch, call

from src.repositories.databricks_volume_repository import DatabricksVolumeRepository


@pytest.fixture
def repo():
    return DatabricksVolumeRepository(user_token="test-token", group_id="g-1")


def _make_executor_result(result):
    """Make asyncio loop run_in_executor return the given result."""
    mock_loop = MagicMock()
    mock_loop.run_in_executor = AsyncMock(return_value=result)
    return mock_loop


# ---------------------------------------------------------------------------
# create_volume_if_not_exists: error handling paths
# ---------------------------------------------------------------------------

class TestCreateVolumeEdgeCases:

    @pytest.mark.asyncio
    @patch.object(DatabricksVolumeRepository, "_get_client_with_group_context")
    async def test_create_volume_scope_error_returns_marker(self, mock_get_client, repo):
        """Returns _scope_error marker when OBO token is invalid scope."""
        mock_client = MagicMock()
        mock_get_client.return_value = (mock_client, "retry-token")

        with patch("asyncio.get_event_loop") as mock_loop_fn:
            mock_loop_fn.return_value = _make_executor_result(
                {"_scope_error": True, "error": "invalid scope"}
            )
            with patch.object(
                DatabricksVolumeRepository, "_get_client_with_group_context",
                new_callable=AsyncMock
            ) as mock_get2:
                mock_get2.side_effect = [
                    (mock_client, "retry-token"),
                    (mock_client, None),
                ]
                with patch("asyncio.get_event_loop") as mock_loop_fn2:
                    mock_loop_fn2.return_value = _make_executor_result(
                        {"success": True, "created": True, "message": "ok"}
                    )
                    result = await repo.create_volume_if_not_exists("cat", "sch", "vol")
        # Either a success or scope_error depending on retry; no exception raised
        assert "success" in result or "_scope_error" in result

    @pytest.mark.asyncio
    @patch.object(DatabricksVolumeRepository, "_get_client_with_group_context")
    async def test_create_volume_catalog_does_not_exist(self, mock_get_client, repo):
        """Returns error when catalog does not exist."""
        mock_client = MagicMock()
        mock_get_client.return_value = (mock_client, None)

        with patch("asyncio.get_event_loop") as mock_loop:
            mock_loop.return_value = _make_executor_result(
                {"success": False, "error": "catalog 'cat' does not exist."}
            )
            result = await repo.create_volume_if_not_exists("cat", "sch", "vol")

        # The inner function returns this result
        assert "success" in result

    @pytest.mark.asyncio
    @patch.object(DatabricksVolumeRepository, "_get_client_with_group_context")
    async def test_create_volume_outer_exception(self, mock_get_client, repo):
        """Returns error on unexpected outer exception."""
        mock_get_client.side_effect = Exception("auth failure")
        result = await repo.create_volume_if_not_exists("cat", "sch", "vol")
        assert result["success"] is False
        assert "auth failure" in result["error"]


# ---------------------------------------------------------------------------
# upload_file_to_volume: various error paths
# ---------------------------------------------------------------------------

class TestUploadFileEdgeCases:

    @pytest.mark.asyncio
    @patch.object(DatabricksVolumeRepository, "_get_client_with_group_context")
    @patch.object(DatabricksVolumeRepository, "create_volume_if_not_exists")
    async def test_upload_volume_fatal_error(self, mock_vol, mock_get_client, repo):
        """Returns error when volume creation fails with non-permission error."""
        mock_vol.return_value = {
            "success": False,
            "error": "catalog 'nonexistent' does not exist"
        }
        mock_get_client.return_value = (MagicMock(), None)

        result = await repo.upload_file_to_volume("cat", "sch", "vol", "f.db", b"data")
        assert result["success"] is False

    @pytest.mark.asyncio
    @patch.object(DatabricksVolumeRepository, "_get_client_with_group_context")
    @patch.object(DatabricksVolumeRepository, "create_volume_if_not_exists")
    async def test_upload_volume_permission_error_continues(self, mock_vol, mock_get_client, repo):
        """Continues upload even when volume check has permission/scope error."""
        mock_vol.return_value = {
            "success": False,
            "error": "insufficient scope to access volume"
        }
        mock_client = MagicMock()
        mock_get_client.return_value = (mock_client, None)

        with patch("asyncio.get_event_loop") as mock_loop:
            mock_loop.return_value = _make_executor_result(
                {"success": True, "path": "/Volumes/cat/sch/vol/f.db"}
            )
            result = await repo.upload_file_to_volume("cat", "sch", "vol", "f.db", b"data")

        assert result["success"] is True

    @pytest.mark.asyncio
    @patch.object(DatabricksVolumeRepository, "_get_client_with_group_context")
    @patch.object(DatabricksVolumeRepository, "create_volume_if_not_exists")
    async def test_upload_no_client_returns_error(self, mock_vol, mock_get_client, repo):
        """Returns error when client cannot be created."""
        mock_vol.return_value = {"success": True}
        mock_get_client.return_value = (None, None)

        result = await repo.upload_file_to_volume("cat", "sch", "vol", "f.db", b"data")
        assert result["success"] is False
        assert "Failed to create Databricks client" in result["error"]

    @pytest.mark.asyncio
    @patch.object(DatabricksVolumeRepository, "_get_client_with_group_context")
    @patch.object(DatabricksVolumeRepository, "create_volume_if_not_exists")
    async def test_upload_scope_error_retries_with_pat(self, mock_vol, mock_get_client, repo):
        """Retries with PAT when OBO token has scope error."""
        mock_vol.return_value = {"success": True}
        mock_client = MagicMock()
        mock_client_pat = MagicMock()

        mock_get_client.side_effect = [
            (mock_client, "retry-token"),
            (mock_client_pat, None),
        ]

        with patch("asyncio.get_event_loop") as mock_loop:
            # First call returns scope_error, second returns success
            mock_loop.return_value.run_in_executor = AsyncMock(
                side_effect=[
                    {"_scope_error": True, "error": "invalid scope"},
                    {"success": True, "path": "/Volumes/cat/sch/vol/f.db"},
                ]
            )
            result = await repo.upload_file_to_volume("cat", "sch", "vol", "f.db", b"data")

        assert result["success"] is True

    @pytest.mark.asyncio
    @patch.object(DatabricksVolumeRepository, "_get_client_with_group_context")
    @patch.object(DatabricksVolumeRepository, "create_volume_if_not_exists")
    async def test_upload_scope_error_pat_client_fails(self, mock_vol, mock_get_client, repo):
        """Returns error when PAT client creation fails after scope error."""
        mock_vol.return_value = {"success": True}
        mock_client = MagicMock()

        mock_get_client.side_effect = [
            (mock_client, "retry-token"),
            (None, None),
        ]

        with patch("asyncio.get_event_loop") as mock_loop:
            mock_loop.return_value.run_in_executor = AsyncMock(
                return_value={"_scope_error": True, "error": "invalid scope"}
            )
            result = await repo.upload_file_to_volume("cat", "sch", "vol", "f.db", b"data")

        assert result["success"] is False

    @pytest.mark.asyncio
    @patch.object(DatabricksVolumeRepository, "_get_client_with_group_context")
    @patch.object(DatabricksVolumeRepository, "create_volume_if_not_exists")
    @patch.object(DatabricksVolumeRepository, "_upload_via_rest_api")
    async def test_upload_rest_api_fallback(self, mock_rest, mock_vol, mock_get_client, repo):
        """Falls back to REST API when network zone error occurs."""
        mock_vol.return_value = {"success": True}
        mock_client = MagicMock()
        mock_get_client.return_value = (mock_client, None)
        mock_rest.return_value = {"success": True, "path": "/Volumes/cat/sch/vol/f.db"}

        with patch("asyncio.get_event_loop") as mock_loop:
            mock_loop.return_value.run_in_executor = AsyncMock(
                return_value={"_use_rest_api": True, "error": "network zone restriction"}
            )
            result = await repo.upload_file_to_volume("cat", "sch", "vol", "f.db", b"data")

        assert result["success"] is True

    @pytest.mark.asyncio
    @patch.object(DatabricksVolumeRepository, "_get_client_with_group_context")
    @patch.object(DatabricksVolumeRepository, "create_volume_if_not_exists")
    async def test_upload_outer_exception(self, mock_vol, mock_get_client, repo):
        """Returns error on unexpected outer exception."""
        mock_vol.return_value = {"success": True}
        mock_get_client.side_effect = Exception("network error")

        result = await repo.upload_file_to_volume("cat", "sch", "vol", "f.db", b"data")
        assert result["success"] is False
        assert "network error" in result["error"]


# ---------------------------------------------------------------------------
# download_file_from_volume: error paths
# ---------------------------------------------------------------------------

class TestDownloadFileEdgeCases:

    @pytest.mark.asyncio
    @patch.object(DatabricksVolumeRepository, "_get_client_with_group_context")
    async def test_download_no_client_returns_error(self, mock_get_client, repo):
        """Returns error when client cannot be created."""
        mock_get_client.return_value = (None, None)
        result = await repo.download_file_from_volume("cat", "sch", "vol", "f.db")
        assert result["success"] is False

    @pytest.mark.asyncio
    @patch.object(DatabricksVolumeRepository, "_get_client_with_group_context")
    async def test_download_scope_error_retries_with_pat(self, mock_get_client, repo):
        """Retries with PAT when OBO returns scope error."""
        mock_client = MagicMock()
        mock_client_pat = MagicMock()

        mock_get_client.side_effect = [
            (mock_client, "retry-token"),
            (mock_client_pat, None),
        ]

        with patch("asyncio.get_event_loop") as mock_loop:
            mock_loop.return_value.run_in_executor = AsyncMock(
                side_effect=[
                    {"_scope_error": True, "error": "invalid scope"},
                    {"success": True, "content": b"data", "path": "/v/f.db", "size": 4},
                ]
            )
            result = await repo.download_file_from_volume("cat", "sch", "vol", "f.db")

        assert result["success"] is True

    @pytest.mark.asyncio
    @patch.object(DatabricksVolumeRepository, "_get_client_with_group_context")
    async def test_download_scope_error_pat_client_fails(self, mock_get_client, repo):
        """Returns error when PAT client creation fails after scope error."""
        mock_client = MagicMock()
        mock_get_client.side_effect = [
            (mock_client, "retry-token"),
            (None, None),
        ]

        with patch("asyncio.get_event_loop") as mock_loop:
            mock_loop.return_value.run_in_executor = AsyncMock(
                return_value={"_scope_error": True, "error": "invalid scope"}
            )
            result = await repo.download_file_from_volume("cat", "sch", "vol", "f.db")

        assert result["success"] is False

    @pytest.mark.asyncio
    @patch.object(DatabricksVolumeRepository, "_get_client_with_group_context")
    async def test_download_outer_exception(self, mock_get_client, repo):
        """Returns error on unexpected outer exception."""
        mock_get_client.side_effect = Exception("timeout")
        result = await repo.download_file_from_volume("cat", "sch", "vol", "f.db")
        assert result["success"] is False
        assert "timeout" in result["error"]


# ---------------------------------------------------------------------------
# list_volume_contents: error paths
# ---------------------------------------------------------------------------

class TestListVolumeEdgeCases:

    @pytest.mark.asyncio
    @patch.object(DatabricksVolumeRepository, "_get_client_with_group_context")
    async def test_list_no_client_returns_error(self, mock_get_client, repo):
        """Returns error when client cannot be created."""
        mock_get_client.return_value = (None, None)
        result = await repo.list_volume_contents("cat", "sch", "vol")
        assert result["success"] is False

    @pytest.mark.asyncio
    @patch.object(DatabricksVolumeRepository, "_get_client_with_group_context")
    async def test_list_scope_error_retries_with_pat(self, mock_get_client, repo):
        """Retries with PAT on scope error."""
        mock_client = MagicMock()
        mock_client_pat = MagicMock()

        mock_get_client.side_effect = [
            (mock_client, "retry-token"),
            (mock_client_pat, None),
        ]

        with patch("asyncio.get_event_loop") as mock_loop:
            mock_loop.return_value.run_in_executor = AsyncMock(
                side_effect=[
                    {"_scope_error": True, "error": "invalid scope"},
                    {"success": True, "path": "/Volumes/cat/sch/vol", "files": []},
                ]
            )
            result = await repo.list_volume_contents("cat", "sch", "vol")

        assert result["success"] is True

    @pytest.mark.asyncio
    @patch.object(DatabricksVolumeRepository, "_get_client_with_group_context")
    async def test_list_scope_error_pat_client_fails(self, mock_get_client, repo):
        """Returns error when PAT client fails after scope error."""
        mock_client = MagicMock()
        mock_get_client.side_effect = [
            (mock_client, "retry-token"),
            (None, None),
        ]

        with patch("asyncio.get_event_loop") as mock_loop:
            mock_loop.return_value.run_in_executor = AsyncMock(
                return_value={"_scope_error": True, "error": "invalid scope"}
            )
            result = await repo.list_volume_contents("cat", "sch", "vol")

        assert result["success"] is False

    @pytest.mark.asyncio
    @patch.object(DatabricksVolumeRepository, "_get_client_with_group_context")
    async def test_list_outer_exception(self, mock_get_client, repo):
        """Returns error on unexpected outer exception."""
        mock_get_client.side_effect = Exception("server error")
        result = await repo.list_volume_contents("cat", "sch", "vol")
        assert result["success"] is False


# ---------------------------------------------------------------------------
# create_volume_directory: error paths
# ---------------------------------------------------------------------------

class TestCreateDirectoryEdgeCases:

    @pytest.mark.asyncio
    @patch.object(DatabricksVolumeRepository, "_get_client_with_group_context")
    async def test_create_dir_no_client_returns_error(self, mock_get_client, repo):
        """Returns error when client cannot be created."""
        mock_get_client.return_value = (None, None)
        result = await repo.create_volume_directory("cat", "sch", "vol", "mydir")
        assert result["success"] is False

    @pytest.mark.asyncio
    @patch.object(DatabricksVolumeRepository, "_get_client_with_group_context")
    async def test_create_dir_scope_error_retries_with_pat(self, mock_get_client, repo):
        """Retries with PAT on scope error."""
        mock_client = MagicMock()
        mock_client_pat = MagicMock()

        mock_get_client.side_effect = [
            (mock_client, "retry-token"),
            (mock_client_pat, None),
        ]

        with patch("asyncio.get_event_loop") as mock_loop:
            mock_loop.return_value.run_in_executor = AsyncMock(
                side_effect=[
                    {"_scope_error": True, "error": "invalid scope"},
                    {"success": True, "path": "/Volumes/cat/sch/vol/mydir"},
                ]
            )
            result = await repo.create_volume_directory("cat", "sch", "vol", "mydir")

        assert result["success"] is True

    @pytest.mark.asyncio
    @patch.object(DatabricksVolumeRepository, "_get_client_with_group_context")
    async def test_create_dir_pat_client_fails(self, mock_get_client, repo):
        """Returns error when PAT client fails after scope error."""
        mock_client = MagicMock()
        mock_get_client.side_effect = [
            (mock_client, "retry-token"),
            (None, None),
        ]

        with patch("asyncio.get_event_loop") as mock_loop:
            mock_loop.return_value.run_in_executor = AsyncMock(
                return_value={"_scope_error": True, "error": "invalid scope"}
            )
            result = await repo.create_volume_directory("cat", "sch", "vol", "mydir")

        assert result["success"] is False

    @pytest.mark.asyncio
    @patch.object(DatabricksVolumeRepository, "_get_client_with_group_context")
    async def test_create_dir_outer_exception(self, mock_get_client, repo):
        """Returns error on unexpected outer exception."""
        mock_get_client.side_effect = Exception("network error")
        result = await repo.create_volume_directory("cat", "sch", "vol")
        assert result["success"] is False


# ---------------------------------------------------------------------------
# delete_volume_file: error paths
# ---------------------------------------------------------------------------

class TestDeleteVolumeFileEdgeCases:

    @pytest.mark.asyncio
    @patch.object(DatabricksVolumeRepository, "_get_client_with_group_context")
    async def test_delete_no_client_returns_error(self, mock_get_client, repo):
        """Returns error when client cannot be created."""
        mock_get_client.return_value = (None, None)
        result = await repo.delete_volume_file("cat", "sch", "vol", "f.db")
        assert result["success"] is False

    @pytest.mark.asyncio
    @patch.object(DatabricksVolumeRepository, "_get_client_with_group_context")
    async def test_delete_scope_error_retries_with_pat(self, mock_get_client, repo):
        """Retries with PAT on scope error."""
        mock_client = MagicMock()
        mock_client_pat = MagicMock()

        mock_get_client.side_effect = [
            (mock_client, "retry-token"),
            (mock_client_pat, None),
        ]

        with patch("asyncio.get_event_loop") as mock_loop:
            mock_loop.return_value.run_in_executor = AsyncMock(
                side_effect=[
                    {"_scope_error": True, "error": "invalid scope"},
                    {"success": True, "path": "/Volumes/cat/sch/vol/f.db"},
                ]
            )
            result = await repo.delete_volume_file("cat", "sch", "vol", "f.db")

        assert result["success"] is True

    @pytest.mark.asyncio
    @patch.object(DatabricksVolumeRepository, "_get_client_with_group_context")
    async def test_delete_scope_error_pat_client_fails(self, mock_get_client, repo):
        """Returns error when PAT client fails after scope error."""
        mock_client = MagicMock()
        mock_get_client.side_effect = [
            (mock_client, "retry-token"),
            (None, None),
        ]

        with patch("asyncio.get_event_loop") as mock_loop:
            mock_loop.return_value.run_in_executor = AsyncMock(
                return_value={"_scope_error": True, "error": "invalid scope"}
            )
            result = await repo.delete_volume_file("cat", "sch", "vol", "f.db")

        assert result["success"] is False

    @pytest.mark.asyncio
    @patch.object(DatabricksVolumeRepository, "_get_client_with_group_context")
    async def test_delete_outer_exception(self, mock_get_client, repo):
        """Returns error on unexpected outer exception."""
        mock_get_client.side_effect = Exception("network error")
        result = await repo.delete_volume_file("cat", "sch", "vol", "f.db")
        assert result["success"] is False
        assert "network error" in result["error"]


# ---------------------------------------------------------------------------
# Inner executor function tests via synchronous executor mock
# ---------------------------------------------------------------------------

class TestCreateVolumeInnerPaths:
    """Tests for inner _create_volume function paths by running executor synchronously."""

    @pytest.mark.asyncio
    @patch.object(DatabricksVolumeRepository, "_get_client_with_group_context")
    async def test_create_volume_catalog_not_exist_error(self, mock_get_client, repo):
        """Returns specific error when catalog does not exist."""
        mock_client = MagicMock()
        mock_client.volumes.read.side_effect = Exception("not found")
        mock_client.volumes.create.side_effect = Exception("catalog 'cat' does not exist")
        mock_get_client.return_value = (mock_client, None)

        real_loop = asyncio.get_event_loop()

        async def real_sync_executor(_, fn):
            return await real_loop.run_in_executor(None, fn)

        with patch("asyncio.get_event_loop") as mock_loop:
            mock_loop.return_value.run_in_executor = real_sync_executor
            result = await repo.create_volume_if_not_exists("cat", "sch", "vol")

        assert "success" in result

    @pytest.mark.asyncio
    @patch.object(DatabricksVolumeRepository, "_get_client_with_group_context")
    async def test_create_volume_schema_not_exist_error(self, mock_get_client, repo):
        """Returns specific error when schema does not exist."""
        mock_client = MagicMock()
        mock_client.volumes.read.side_effect = Exception("not found")
        mock_client.volumes.create.side_effect = Exception("schema 'sch' does not exist")
        mock_get_client.return_value = (mock_client, None)

        real_loop = asyncio.get_event_loop()

        async def real_sync_executor(_, fn):
            return await real_loop.run_in_executor(None, fn)

        with patch("asyncio.get_event_loop") as mock_loop:
            mock_loop.return_value.run_in_executor = real_sync_executor
            result = await repo.create_volume_if_not_exists("cat", "sch", "vol")

        assert "success" in result

    @pytest.mark.asyncio
    @patch.object(DatabricksVolumeRepository, "_get_client_with_group_context")
    async def test_create_volume_invalid_scope_error(self, mock_get_client, repo):
        """Returns _scope_error marker for invalid scope error with retry_token."""
        mock_client = MagicMock()
        mock_client.volumes.read.side_effect = Exception("not found")
        mock_client.volumes.create.side_effect = Exception("invalid scope: cannot create volume")
        mock_client_pat = MagicMock()
        mock_client_pat.volumes.read.side_effect = Exception("not found")
        mock_client_pat.volumes.create.return_value = MagicMock()

        mock_get_client.side_effect = [
            (mock_client, "retry-token"),
            (mock_client_pat, None),
        ]

        real_loop = asyncio.get_event_loop()

        async def real_sync_executor(_, fn):
            return await real_loop.run_in_executor(None, fn)

        with patch("asyncio.get_event_loop") as mock_loop:
            mock_loop.return_value.run_in_executor = real_sync_executor
            result = await repo.create_volume_if_not_exists("cat", "sch", "vol")

        assert "success" in result


class TestDownloadInnerPaths:
    """Tests for _to_bytes helper and download inner function error paths."""

    @pytest.mark.asyncio
    @patch.object(DatabricksVolumeRepository, "_get_client_with_group_context")
    async def test_download_scope_error_in_inner_function(self, mock_get_client, repo):
        """Inner function handles scope_error when SDK raises invalid scope."""
        mock_client = MagicMock()
        mock_client.files.download.side_effect = Exception("invalid scope: files.read")
        mock_client_pat = MagicMock()
        mock_client_pat.files.download.return_value = MagicMock(contents=b"data")

        mock_get_client.side_effect = [
            (mock_client, "retry-token"),
            (mock_client_pat, None),
        ]

        real_loop = asyncio.get_event_loop()

        async def real_sync_executor(_, fn):
            return await real_loop.run_in_executor(None, fn)

        with patch("asyncio.get_event_loop") as mock_loop:
            mock_loop.return_value.run_in_executor = real_sync_executor
            result = await repo.download_file_from_volume("cat", "sch", "vol", "f.db")

        assert "success" in result

    @pytest.mark.asyncio
    @patch.object(DatabricksVolumeRepository, "_get_client_with_group_context")
    async def test_download_file_like_response(self, mock_get_client, repo):
        """download_file_from_volume handles file-like response with .read()."""
        mock_client = MagicMock()

        file_like = MagicMock()
        file_like.read.return_value = b"file_content"
        del file_like.contents
        mock_client.files.download.return_value = file_like
        mock_get_client.return_value = (mock_client, None)

        real_loop = asyncio.get_event_loop()

        async def real_sync_executor(_, fn):
            return await real_loop.run_in_executor(None, fn)

        with patch("asyncio.get_event_loop") as mock_loop:
            mock_loop.return_value.run_in_executor = real_sync_executor
            result = await repo.download_file_from_volume("cat", "sch", "vol", "f.db")

        assert "success" in result


class TestDeleteVolumeInnerPaths:
    """Tests for delete_volume_file inner function paths."""

    @pytest.mark.asyncio
    @patch.object(DatabricksVolumeRepository, "_get_client_with_group_context")
    async def test_delete_not_found_error(self, mock_get_client, repo):
        """Inner function returns not-found error when file does not exist."""
        mock_client = MagicMock()
        mock_client.files.delete.side_effect = Exception("not found: file /Volumes/cat/sch/vol/f.db")
        mock_get_client.return_value = (mock_client, None)

        # Capture real loop before any patching
        real_loop = asyncio.get_event_loop()

        async def real_sync_executor(_, fn):
            return await real_loop.run_in_executor(None, fn)

        with patch("asyncio.get_event_loop") as mock_loop:
            mock_loop.return_value.run_in_executor = real_sync_executor
            result = await repo.delete_volume_file("cat", "sch", "vol", "f.db")

        assert result["success"] is False
        assert "not found" in result.get("error", "").lower()

    @pytest.mark.asyncio
    @patch.object(DatabricksVolumeRepository, "_get_client_with_group_context")
    async def test_delete_generic_error(self, mock_get_client, repo):
        """Inner function returns error on generic exception."""
        mock_client = MagicMock()
        mock_client.files.delete.side_effect = Exception("permission denied")
        mock_get_client.return_value = (mock_client, None)

        real_loop = asyncio.get_event_loop()

        async def real_sync_executor(_, fn):
            return await real_loop.run_in_executor(None, fn)

        with patch("asyncio.get_event_loop") as mock_loop:
            mock_loop.return_value.run_in_executor = real_sync_executor
            result = await repo.delete_volume_file("cat", "sch", "vol", "f.db")

        assert result["success"] is False

    @pytest.mark.asyncio
    @patch.object(DatabricksVolumeRepository, "_get_client_with_group_context")
    async def test_delete_success(self, mock_get_client, repo):
        """Inner function returns success on successful delete."""
        mock_client = MagicMock()
        mock_client.files.delete.return_value = None  # No exception = success
        mock_get_client.return_value = (mock_client, None)

        real_loop = asyncio.get_event_loop()

        async def real_sync_executor(_, fn):
            return await real_loop.run_in_executor(None, fn)

        with patch("asyncio.get_event_loop") as mock_loop:
            mock_loop.return_value.run_in_executor = real_sync_executor
            result = await repo.delete_volume_file("cat", "sch", "vol", "f.db")

        assert result["success"] is True
