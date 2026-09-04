"""Images attached in the chat: upload, serve, delete — tenant-scoped.

An attached document is indexed and searched; an attached IMAGE is shown.
The model never receives the bytes (that is the vision slice, separately):
it receives the image's name, id and dimensions, and refers to it in the
HTML it writes as ``<img src="asset:<id>">``. The frontend resolves that
reference to the bytes served here when it renders — see ``attachment_hint``
for the prompt side and ``utils/assetRefs`` on the frontend for the other.
"""

import logging
from typing import Any, Dict, List, Optional

from sqlalchemy.ext.asyncio import AsyncSession

from src.db.session import get_isolated_db_session
from src.repositories.chat_asset_repository import ChatAssetRepository

logger = logging.getLogger(__name__)

#: What may be attached as an image. GIF for the occasional animation; SVG is
#: NOT accepted — it is a script vector, and the renderer's sandbox is the
#: only thing standing between it and the app.
ALLOWED_MIME = {"image/png", "image/jpeg", "image/webp", "image/gif"}
#: A screenshot is well under a megabyte; a phone photo a few. Ten is generous
#: without letting one upload dominate a database row.
MAX_BYTES = 10 * 1024 * 1024


class AssetValidationError(ValueError):
    """The upload is not an image this store accepts."""


def asset_ref(asset_id: str) -> str:
    """How the model (and the HTML it writes) refers to a stored image."""
    return f"asset:{asset_id}"


class ChatAssetService:
    def __init__(
        self,
        session: AsyncSession,
        repository_class=ChatAssetRepository,
    ):
        self.session = session
        self.repository_class = repository_class
        self.repository = repository_class(session)

    async def upload(
        self,
        *,
        data: bytes,
        name: str,
        mime: str,
        width: Optional[int],
        height: Optional[int],
        session_id: Optional[str],
        group_context: Any,
    ) -> Dict[str, Any]:
        mime = (mime or "").lower().strip()
        if mime not in ALLOWED_MIME:
            raise AssetValidationError(
                f"'{mime or 'unknown'}' is not an accepted image type "
                f"({', '.join(sorted(m.split('/')[1] for m in ALLOWED_MIME))})."
            )
        if not data:
            raise AssetValidationError("The image is empty.")
        if len(data) > MAX_BYTES:
            raise AssetValidationError(
                f"The image is {len(data) / (1024 * 1024):.1f} MB; the limit is "
                f"{MAX_BYTES // (1024 * 1024)} MB."
            )
        row = {
            "group_id": getattr(group_context, "primary_group_id", None),
            "created_by_email": getattr(group_context, "group_email", None),
            "session_id": session_id or None,
            "name": (name or "image")[:255],
            "mime": mime,
            "size": len(data),
            "width": width or None,
            "height": height or None,
            "data": data,
        }
        # Written on a PRIVATE connection. On SQLite the request session shares
        # one connection with every other request; between this INSERT and its
        # COMMIT another request's rollback discards the row — the upload then
        # answers with an id that nothing can fetch ("Asset not found" on every
        # render). Observed live on a 113 KB image; the window grows with the
        # bytes. On PG/Lakebase a checkout is already private (no-op detour).
        async with get_isolated_db_session() as iso:
            asset = await self.repository_class(iso).create(row)
            described = self.describe(asset)
            await iso.commit()
        return described

    async def get(self, asset_id: str, group_context: Any):
        """The stored asset with its bytes, or None (missing / another tenant)."""
        return await self.repository.get(asset_id, _group_ids(group_context))

    async def list_for_session(
        self, session_id: str, group_context: Any
    ) -> List[Dict[str, Any]]:
        rows = await self.repository.list_for_session(
            session_id, _group_ids(group_context)
        )
        return [self.describe(r) for r in rows]

    async def delete(self, asset_id: str, group_context: Any) -> bool:
        # Same private connection as the upload, for the same reason.
        async with get_isolated_db_session() as iso:
            deleted = await self.repository_class(iso).delete(
                asset_id, _group_ids(group_context)
            )
            if deleted:
                await iso.commit()
        return deleted

    @staticmethod
    def describe(asset: Any) -> Dict[str, Any]:
        """The metadata the chat keeps and the prompt sees — never the bytes."""
        return {
            "id": asset.id,
            "name": asset.name,
            "mime": asset.mime,
            "size": asset.size,
            "width": asset.width,
            "height": asset.height,
            "session_id": asset.session_id,
            "ref": asset_ref(asset.id),
        }


def _group_ids(group_context: Any) -> List[str]:
    """The caller's teamspaces — the only rows they may see. An EMPTY list
    matches nothing: a context with no groups must not read as "no filter"."""
    ids = getattr(group_context, "group_ids", None) or []
    return [str(g) for g in ids]
