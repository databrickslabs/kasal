"""The SDK upload must match the installed SDK's signature.

It passed `content=`, but `FilesExt.upload` takes `contents=`: every SDK upload
raised TypeError and only worked via the REST fallback that error triggers.
"""

from unittest.mock import AsyncMock, MagicMock, create_autospec, patch

import pytest
from databricks.sdk.mixins.files import FilesExt

from src.repositories.databricks_volume_repository import DatabricksVolumeRepository


@pytest.mark.asyncio
async def test_sdk_upload_uses_the_real_signature() -> None:
    repo = DatabricksVolumeRepository(user_token="test-token", group_id="g-1")
    client = MagicMock()
    client.files = create_autospec(FilesExt, instance=True)
    rest = AsyncMock(return_value={"success": True})
    with (
        patch.object(
            repo,
            "_get_client_with_group_context",
            AsyncMock(return_value=(client, None)),
        ),
        patch.object(
            repo,
            "create_volume_if_not_exists",
            AsyncMock(return_value={"success": True}),
        ),
        patch.object(repo, "_upload_via_rest_api", rest),
    ):
        result = await repo.upload_file_to_volume("cat", "sch", "vol", "f.db", b"data")

    assert result["success"] is True
    client.files.upload.assert_called_once()
    assert (
        client.files.upload.call_args.kwargs["file_path"] == "/Volumes/cat/sch/vol/f.db"
    )
    rest.assert_not_awaited()
