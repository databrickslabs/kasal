"""Unit tests for DatabricksVolumeRepository."""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from src.repositories.databricks_volume_repository import DatabricksVolumeRepository


@pytest.fixture
def repo():
    return DatabricksVolumeRepository(user_token="test-token", group_id="g-1")


class TestInit:

    def test_initialization_with_token(self):
        repo = DatabricksVolumeRepository(user_token="tok", group_id="g-1")
        assert repo._user_token == "tok"
        assert repo._group_id == "g-1"

    def test_initialization_without_token(self):
        repo = DatabricksVolumeRepository()
        assert repo._user_token is None
        assert repo._group_id is None
        assert repo._workspace_client is None


class TestEnsureClient:

    @pytest.mark.asyncio
    async def test_returns_true_when_client_exists(self, repo):
        repo._workspace_client = MagicMock()
        result = await repo._ensure_client()
        assert result is True

    @pytest.mark.asyncio
    @patch("src.repositories.databricks_volume_repository.get_workspace_client")
    async def test_creates_client_successfully(self, mock_get_client, repo):
        mock_get_client.return_value = MagicMock()
        result = await repo._ensure_client()
        assert result is True

    @pytest.mark.asyncio
    @patch("src.repositories.databricks_volume_repository.get_workspace_client")
    async def test_returns_false_when_client_creation_fails(self, mock_get_client, repo):
        mock_get_client.return_value = None
        result = await repo._ensure_client()
        assert result is False

    @pytest.mark.asyncio
    @patch("src.repositories.databricks_volume_repository.get_workspace_client")
    async def test_returns_false_on_exception(self, mock_get_client, repo):
        mock_get_client.side_effect = Exception("Auth error")
        result = await repo._ensure_client()
        assert result is False


class TestCreateVolumeIfNotExists:

    @pytest.mark.asyncio
    @patch.object(DatabricksVolumeRepository, "_get_client_with_group_context")
    async def test_returns_error_when_no_client(self, mock_get_client, repo):
        mock_get_client.return_value = (None, None)

        result = await repo.create_volume_if_not_exists("cat", "sch", "vol")

        assert result["success"] is False
        assert "Failed to create" in result["error"]

    @pytest.mark.asyncio
    @patch.object(DatabricksVolumeRepository, "_get_client_with_group_context")
    async def test_returns_success_when_volume_created(self, mock_get_client, repo):
        mock_client = MagicMock()
        mock_client.volumes.read.side_effect = Exception("not found")
        mock_client.volumes.create.return_value = MagicMock()
        mock_get_client.return_value = (mock_client, None)

        # We need to mock run_in_executor to run the function directly
        with patch("asyncio.get_event_loop") as mock_loop:
            mock_loop.return_value.run_in_executor = AsyncMock(
                return_value={"success": True, "created": True, "message": "created"}
            )
            result = await repo.create_volume_if_not_exists("cat", "sch", "vol")

        assert result["success"] is True

    @pytest.mark.asyncio
    @patch.object(DatabricksVolumeRepository, "_get_client_with_group_context")
    async def test_returns_exists_when_volume_exists(self, mock_get_client, repo):
        mock_client = MagicMock()
        mock_get_client.return_value = (mock_client, None)

        with patch("asyncio.get_event_loop") as mock_loop:
            mock_loop.return_value.run_in_executor = AsyncMock(
                return_value={"success": True, "exists": True, "message": "Volume already exists"}
            )
            result = await repo.create_volume_if_not_exists("cat", "sch", "vol")

        assert result["success"] is True
        assert result.get("exists") is True


class TestUploadFileToVolume:

    @pytest.mark.asyncio
    @patch.object(DatabricksVolumeRepository, "create_volume_if_not_exists")
    @patch.object(DatabricksVolumeRepository, "_get_client_with_group_context")
    async def test_returns_error_when_no_client(self, mock_get_client, mock_create_vol, repo):
        mock_create_vol.return_value = {"success": True}
        mock_get_client.return_value = (None, None)

        result = await repo.upload_file_to_volume("cat", "sch", "vol", "file.db", b"content")

        assert result["success"] is False

    @pytest.mark.asyncio
    @patch.object(DatabricksVolumeRepository, "create_volume_if_not_exists")
    @patch.object(DatabricksVolumeRepository, "_get_client_with_group_context")
    async def test_upload_success(self, mock_get_client, mock_create_vol, repo):
        mock_create_vol.return_value = {"success": True}
        mock_client = MagicMock()
        mock_get_client.return_value = (mock_client, None)

        with patch("asyncio.get_event_loop") as mock_loop:
            mock_loop.return_value.run_in_executor = AsyncMock(
                return_value={"success": True, "path": "/Volumes/cat/sch/vol/file.db", "size": 7}
            )
            result = await repo.upload_file_to_volume("cat", "sch", "vol", "file.db", b"content")

        assert result["success"] is True

    @pytest.mark.asyncio
    @patch.object(DatabricksVolumeRepository, "create_volume_if_not_exists")
    @patch.object(DatabricksVolumeRepository, "_get_client_with_group_context")
    async def test_falls_back_to_rest_api_on_network_error(self, mock_get_client, mock_create_vol, repo):
        mock_create_vol.return_value = {"success": True}
        mock_client = MagicMock()
        mock_get_client.return_value = (mock_client, None)

        with patch("asyncio.get_event_loop") as mock_loop:
            mock_loop.return_value.run_in_executor = AsyncMock(
                return_value={"_use_rest_api": True, "error": "network zone error"}
            )
            with patch.object(repo, "_upload_via_rest_api", return_value={"success": True, "path": "/p", "size": 1}) as mock_rest:
                result = await repo.upload_file_to_volume("cat", "sch", "vol", "f.db", b"x")

        assert result["success"] is True


class TestDownloadFileFromVolume:

    @pytest.mark.asyncio
    @patch.object(DatabricksVolumeRepository, "_get_client_with_group_context")
    async def test_returns_error_when_no_client(self, mock_get_client, repo):
        mock_get_client.return_value = (None, None)

        result = await repo.download_file_from_volume("cat", "sch", "vol", "file.db")

        assert result["success"] is False

    @pytest.mark.asyncio
    @patch.object(DatabricksVolumeRepository, "_get_client_with_group_context")
    async def test_download_success(self, mock_get_client, repo):
        mock_client = MagicMock()
        mock_get_client.return_value = (mock_client, None)

        with patch("asyncio.get_event_loop") as mock_loop:
            mock_loop.return_value.run_in_executor = AsyncMock(
                return_value={"success": True, "path": "/Volumes/cat/sch/vol/file.db", "content": b"data", "size": 4}
            )
            result = await repo.download_file_from_volume("cat", "sch", "vol", "file.db")

        assert result["success"] is True
        assert result["content"] == b"data"

    @pytest.mark.asyncio
    @patch.object(DatabricksVolumeRepository, "_get_client_with_group_context")
    async def test_falls_back_to_rest_on_network_error(self, mock_get_client, repo):
        mock_client = MagicMock()
        mock_get_client.return_value = (mock_client, None)

        with patch("asyncio.get_event_loop") as mock_loop:
            mock_loop.return_value.run_in_executor = AsyncMock(
                return_value={"_use_rest_api": True, "error": "network zone"}
            )
            with patch.object(repo, "_download_via_rest_api", return_value={"success": True, "content": b"data", "size": 4}):
                result = await repo.download_file_from_volume("cat", "sch", "vol", "f.db")

        assert result["success"] is True


class TestListVolumeContents:

    @pytest.mark.asyncio
    @patch.object(DatabricksVolumeRepository, "_get_client_with_group_context")
    async def test_returns_error_when_no_client(self, mock_get_client, repo):
        mock_get_client.return_value = (None, None)

        result = await repo.list_volume_contents("cat", "sch", "vol")

        assert result["success"] is False

    @pytest.mark.asyncio
    @patch.object(DatabricksVolumeRepository, "_get_client_with_group_context")
    async def test_list_success(self, mock_get_client, repo):
        mock_client = MagicMock()
        mock_get_client.return_value = (mock_client, None)

        with patch("asyncio.get_event_loop") as mock_loop:
            mock_loop.return_value.run_in_executor = AsyncMock(
                return_value={"success": True, "path": "/Volumes/cat/sch/vol", "files": []}
            )
            result = await repo.list_volume_contents("cat", "sch", "vol")

        assert result["success"] is True


class TestDeleteVolumeFile:

    @pytest.mark.asyncio
    @patch.object(DatabricksVolumeRepository, "_get_client_with_group_context")
    async def test_returns_error_when_no_client(self, mock_get_client, repo):
        mock_get_client.return_value = (None, None)

        result = await repo.delete_volume_file("cat", "sch", "vol", "file.db")

        assert result["success"] is False

    @pytest.mark.asyncio
    @patch.object(DatabricksVolumeRepository, "_get_client_with_group_context")
    async def test_delete_success(self, mock_get_client, repo):
        mock_client = MagicMock()
        mock_get_client.return_value = (mock_client, None)

        with patch("asyncio.get_event_loop") as mock_loop:
            mock_loop.return_value.run_in_executor = AsyncMock(
                return_value={"success": True, "path": "/Volumes/cat/sch/vol/file.db"}
            )
            result = await repo.delete_volume_file("cat", "sch", "vol", "file.db")

        assert result["success"] is True


class TestGetDatabricksUrl:

    @pytest.mark.asyncio
    @patch("src.utils.databricks_auth._databricks_auth")
    async def test_generates_volume_url(self, mock_auth):
        mock_auth.get_workspace_url = AsyncMock(return_value="https://workspace.databricks.com")
        repo = DatabricksVolumeRepository()

        result = await repo.get_databricks_url("cat", "sch", "vol")

        assert "cat/sch/vol" in result
        assert result.startswith("https://")

    @pytest.mark.asyncio
    @patch("src.utils.databricks_auth._databricks_auth")
    async def test_generates_file_url(self, mock_auth):
        mock_auth.get_workspace_url = AsyncMock(return_value="https://workspace.databricks.com")
        repo = DatabricksVolumeRepository()

        result = await repo.get_databricks_url("cat", "sch", "vol", "file.db")

        assert "file.db" in result

    @pytest.mark.asyncio
    @patch("src.utils.databricks_auth.get_auth_context")
    @patch("src.utils.databricks_auth._databricks_auth")
    async def test_fallback_when_no_workspace_url(self, mock_auth, mock_ctx):
        mock_auth.get_workspace_url = AsyncMock(return_value=None)
        mock_ctx.return_value = MagicMock(workspace_url="https://fallback.databricks.com")

        repo = DatabricksVolumeRepository()
        result = await repo.get_databricks_url("cat", "sch", "vol")

        assert "cat/sch/vol" in result
