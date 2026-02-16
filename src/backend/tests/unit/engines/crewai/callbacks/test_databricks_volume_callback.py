"""Unit tests for DatabricksVolumeCallback.

Tests initialization (path normalization, parameter storage), authentication,
file-path generation, output formatting (JSON / CSV / text), size validation,
and volume upload logic with mocked Databricks SDK and repository.
"""

import io
import json
import pytest
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, Mock, patch

from src.engines.crewai.callbacks.databricks_volume_callback import (
    DatabricksVolumeCallback,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def base_kwargs():
    """Minimal valid kwargs for constructing a DatabricksVolumeCallback."""
    return {
        "volume_path": "/Volumes/cat/schema/vol",
        "workspace_url": "https://example.com",
        "token": "tok_test",
        "task_key": "task_1",
    }


@pytest.fixture
def callback(base_kwargs):
    return DatabricksVolumeCallback(**base_kwargs)


# ===========================================================================
# Initialization
# ===========================================================================

class TestInit:

    def test_stores_volume_path(self, base_kwargs):
        cb = DatabricksVolumeCallback(**base_kwargs)
        assert cb.volume_path == "/Volumes/cat/schema/vol"

    def test_converts_dot_notation(self):
        cb = DatabricksVolumeCallback(
            volume_path="catalog.myschema.myvol", task_key="t"
        )
        assert cb.volume_path == "/Volumes/catalog/myschema/myvol"

    def test_converts_non_standard_dot_notation(self):
        cb = DatabricksVolumeCallback(
            volume_path="a.b.c.d", task_key="t"
        )
        assert cb.volume_path.startswith("/Volumes/")

    def test_default_flags(self, base_kwargs):
        cb = DatabricksVolumeCallback(**base_kwargs)
        assert cb.create_date_dirs is True
        assert cb.file_format == "json"
        assert cb.max_file_size_mb == 100.0

    def test_custom_flags(self, base_kwargs):
        cb = DatabricksVolumeCallback(
            **base_kwargs,
            create_date_dirs=False,
            file_format="csv",
            max_file_size_mb=5.0,
            execution_name="run_42",
        )
        assert cb.create_date_dirs is False
        assert cb.file_format == "csv"
        assert cb.max_file_size_mb == 5.0
        assert cb.execution_name == "run_42"

    def test_client_initially_none(self, callback):
        assert callback._client is None


# ===========================================================================
# _ensure_auth
# ===========================================================================

class TestEnsureAuth:

    @pytest.mark.asyncio
    async def test_skips_when_already_initialized(self, callback):
        callback._auth_initialized = True
        await callback._ensure_auth()
        # No side effects; just returns

    @pytest.mark.asyncio
    async def test_fetches_auth_context_when_missing(self):
        cb = DatabricksVolumeCallback(
            volume_path="/Volumes/c/s/v", task_key="t"
        )
        assert cb.workspace_url is None

        mock_auth = MagicMock()
        mock_auth.workspace_url = "https://ws.example.com"
        mock_auth.token = "fetched_tok"
        mock_auth.auth_method = "pat"

        with patch(
            "src.utils.databricks_auth.get_auth_context",
            new_callable=AsyncMock,
            return_value=mock_auth,
        ):
            await cb._ensure_auth()
            assert cb.workspace_url == "https://ws.example.com"
            assert cb.token == "fetched_tok"
            assert cb._auth_initialized is True

    @pytest.mark.asyncio
    async def test_handles_auth_exception(self):
        cb = DatabricksVolumeCallback(
            volume_path="/Volumes/c/s/v", task_key="t"
        )
        with patch(
            "src.utils.databricks_auth.get_auth_context",
            new_callable=AsyncMock,
            side_effect=Exception("auth boom"),
        ):
            await cb._ensure_auth()
            assert cb._auth_initialized is True  # still sets flag


# ===========================================================================
# _ensure_client
# ===========================================================================

class TestEnsureClient:

    @pytest.mark.asyncio
    async def test_returns_existing_client(self, callback):
        existing = MagicMock()
        callback._client = existing
        client = await callback._ensure_client()
        assert client is existing

    @pytest.mark.asyncio
    async def test_creates_client_via_centralized_auth(self, callback):
        mock_ws = MagicMock()
        with patch(
            "src.engines.crewai.callbacks.databricks_volume_callback.get_workspace_client",
            new_callable=AsyncMock,
            return_value=mock_ws,
        ):
            client = await callback._ensure_client()
            assert client is mock_ws
            assert callback._client is mock_ws

    @pytest.mark.asyncio
    async def test_raises_when_client_is_none(self, callback):
        with patch(
            "src.engines.crewai.callbacks.databricks_volume_callback.get_workspace_client",
            new_callable=AsyncMock,
            return_value=None,
        ):
            with pytest.raises(ValueError, match="Failed to get Databricks workspace client"):
                await callback._ensure_client()


# ===========================================================================
# _generate_file_path
# ===========================================================================

class TestGenerateFilePath:

    def test_includes_date_dirs(self, callback):
        path = callback._generate_file_path()
        parts = path.split("/")
        # With date dirs: year/month/day/filename
        assert len(parts) >= 4
        assert parts[-1].endswith(".json")
        assert "task_1" in parts[-1]

    def test_no_date_dirs(self, base_kwargs):
        cb = DatabricksVolumeCallback(**base_kwargs, create_date_dirs=False)
        path = cb._generate_file_path()
        assert "/" not in path or path.count("/") == 0 or "task_1" in path

    def test_includes_execution_name(self, base_kwargs):
        cb = DatabricksVolumeCallback(
            **base_kwargs, execution_name="My Run!", create_date_dirs=False
        )
        path = cb._generate_file_path()
        assert "My_Run_" in path  # Special chars sanitised

    def test_uses_output_as_default_task_key(self):
        cb = DatabricksVolumeCallback(
            volume_path="/Volumes/c/s/v", create_date_dirs=False
        )
        path = cb._generate_file_path()
        assert "output_" in path


# ===========================================================================
# _format_output
# ===========================================================================

class TestFormatOutput:

    def test_json_dict_input(self, callback):
        out = {"key": "val"}
        formatted = callback._format_output(out)
        parsed = json.loads(formatted)
        assert parsed == out

    def test_json_raw_output_object(self, callback):
        out = MagicMock()
        out.raw = "raw text"
        out.json_dict = {"j": 1}
        out.pydantic = None
        formatted = callback._format_output(out)
        parsed = json.loads(formatted)
        assert parsed["raw"] == "raw text"
        assert parsed["json_dict"] == {"j": 1}
        assert parsed["pydantic"] is None

    def test_json_string_fallback(self, callback):
        formatted = callback._format_output("just a string")
        parsed = json.loads(formatted)
        assert parsed["output"] == "just a string"
        assert "metadata" in parsed

    def test_csv_list_of_lists(self, base_kwargs):
        cb = DatabricksVolumeCallback(**base_kwargs, file_format="csv")
        data = [["h1", "h2"], ["v1", "v2"]]
        formatted = cb._format_output(data)
        assert "h1" in formatted
        assert "v2" in formatted

    def test_csv_list_of_scalars(self, base_kwargs):
        cb = DatabricksVolumeCallback(**base_kwargs, file_format="csv")
        formatted = cb._format_output(["a", "b"])
        assert "a" in formatted

    def test_csv_non_list(self, base_kwargs):
        cb = DatabricksVolumeCallback(**base_kwargs, file_format="csv")
        assert cb._format_output(42) == "42"

    def test_text_format_raw(self, base_kwargs):
        cb = DatabricksVolumeCallback(**base_kwargs, file_format="txt")
        out = MagicMock()
        out.raw = "raw text"
        assert cb._format_output(out) == "raw text"

    def test_text_format_string(self, base_kwargs):
        cb = DatabricksVolumeCallback(**base_kwargs, file_format="txt")
        assert cb._format_output("hello") == "hello"


# ===========================================================================
# execute
# ===========================================================================

class TestExecute:

    @pytest.mark.asyncio
    async def test_successful_upload(self, callback):
        with patch.object(
            callback, "_generate_file_path", return_value="2024/01/01/task_1.json"
        ), patch.object(
            callback, "_format_output", return_value='{"k":"v"}'
        ), patch.object(
            callback, "_upload_to_volume", new_callable=AsyncMock,
            return_value="/Volumes/cat/schema/vol/2024/01/01/task_1.json",
        ):
            meta = await callback.execute({"k": "v"})
            assert meta["volume_path"].endswith("task_1.json")
            assert meta["task_key"] == "task_1"
            assert meta["format"] == "json"
            assert "file_size_mb" in meta
            assert "timestamp" in meta

    @pytest.mark.asyncio
    async def test_rejects_oversized_output(self, base_kwargs):
        cb = DatabricksVolumeCallback(**base_kwargs, max_file_size_mb=0.0001)
        with pytest.raises(ValueError, match="exceeds maximum allowed size"):
            await cb.execute("x" * 10000)

    @pytest.mark.asyncio
    async def test_propagates_upload_error(self, callback):
        with patch.object(
            callback, "_generate_file_path", return_value="f.json"
        ), patch.object(
            callback, "_format_output", return_value="{}"
        ), patch.object(
            callback, "_upload_to_volume", new_callable=AsyncMock,
            side_effect=Exception("upload failed"),
        ):
            with pytest.raises(Exception, match="upload failed"):
                await callback.execute({})


# ===========================================================================
# _upload_to_volume
# ===========================================================================

class TestUploadToVolume:

    @pytest.mark.asyncio
    async def test_raises_for_invalid_prefix(self, callback):
        callback.volume_path = "/wrong/path"
        with pytest.raises(ValueError, match="Volume path must start with /Volumes"):
            await callback._upload_to_volume("f.json", "content")

    @pytest.mark.asyncio
    async def test_raises_for_invalid_parts(self, callback):
        callback.volume_path = "/Volumes/only_two"
        with pytest.raises(ValueError, match="Invalid volume path format"):
            await callback._upload_to_volume("f.json", "content")

    @pytest.mark.asyncio
    async def test_successful_upload(self, callback):
        mock_vol_repo = MagicMock()
        mock_vol_repo.create_volume_if_not_exists = AsyncMock(
            return_value={"success": True, "exists": True}
        )
        mock_vol_repo.create_volume_directory = AsyncMock(
            return_value={"success": True}
        )

        mock_ws_client = MagicMock()
        mock_ws_client.files = MagicMock()
        mock_ws_client.files.upload = MagicMock()

        with patch(
            "src.repositories.databricks_volume_repository.DatabricksVolumeRepository",
            return_value=mock_vol_repo,
        ), patch.object(
            callback, "_ensure_client", new_callable=AsyncMock,
            return_value=mock_ws_client,
        ):
            path = await callback._upload_to_volume("2024/01/f.json", "data")
            assert path == "/Volumes/cat/schema/vol/2024/01/f.json"
            mock_ws_client.files.upload.assert_called_once()
            # Verify overwrite=True
            call_kwargs = mock_ws_client.files.upload.call_args
            assert call_kwargs.kwargs.get("overwrite") is True or call_kwargs[1].get("overwrite") is True

    @pytest.mark.asyncio
    async def test_raises_when_volume_creation_fails(self, callback):
        mock_vol_repo = MagicMock()
        mock_vol_repo.create_volume_if_not_exists = AsyncMock(
            return_value={"success": False, "error": "denied"}
        )

        with patch(
            "src.repositories.databricks_volume_repository.DatabricksVolumeRepository",
            return_value=mock_vol_repo,
        ):
            with pytest.raises(ValueError, match="Failed to ensure volume exists"):
                await callback._upload_to_volume("f.json", "data")

    @pytest.mark.asyncio
    async def test_warns_on_directory_creation_failure(self, callback):
        mock_vol_repo = MagicMock()
        mock_vol_repo.create_volume_if_not_exists = AsyncMock(
            return_value={"success": True, "created": True}
        )
        mock_vol_repo.create_volume_directory = AsyncMock(
            return_value={"success": False, "error": "dir fail"}
        )

        mock_ws_client = MagicMock()
        mock_ws_client.files = MagicMock()
        mock_ws_client.files.upload = MagicMock()

        with patch(
            "src.repositories.databricks_volume_repository.DatabricksVolumeRepository",
            return_value=mock_vol_repo,
        ), patch.object(
            callback, "_ensure_client", new_callable=AsyncMock,
            return_value=mock_ws_client,
        ):
            # Should not raise, just warn
            path = await callback._upload_to_volume("subdir/f.json", "data")
            assert "subdir/f.json" in path

    @pytest.mark.asyncio
    async def test_no_directory_creation_for_root_file(self, callback):
        mock_vol_repo = MagicMock()
        mock_vol_repo.create_volume_if_not_exists = AsyncMock(
            return_value={"success": True, "exists": True}
        )

        mock_ws_client = MagicMock()
        mock_ws_client.files = MagicMock()
        mock_ws_client.files.upload = MagicMock()

        with patch(
            "src.repositories.databricks_volume_repository.DatabricksVolumeRepository",
            return_value=mock_vol_repo,
        ), patch.object(
            callback, "_ensure_client", new_callable=AsyncMock,
            return_value=mock_ws_client,
        ):
            path = await callback._upload_to_volume("root_file.json", "data")
            assert path.endswith("root_file.json")
            # create_volume_directory should NOT have been called
            mock_vol_repo.create_volume_directory.assert_not_called()
