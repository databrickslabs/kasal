"""Chat assets — images attached in the chat, uploaded, served and deleted."""

import logging

from fastapi import APIRouter, File, Form, Response, UploadFile

from src.core.dependencies import GroupContextDep, SessionDep
from src.core.exceptions import BadRequestError, NotFoundError
from src.schemas.chat_asset import ChatAssetOut
from src.services.assets.service import (
    MAX_BYTES,
    AssetValidationError,
    ChatAssetService,
)

router = APIRouter(
    prefix="/chat/assets",
    tags=["chat-assets"],
    responses={404: {"description": "Not found"}},
)

logger = logging.getLogger(__name__)


@router.post("", response_model=ChatAssetOut)
async def upload_asset(
    session: SessionDep,
    group_context: GroupContextDep,
    file: UploadFile = File(...),
    session_id: str = Form(""),
    width: int = Form(0),
    height: int = Form(0),
):
    """Store an image attached in the chat. The browser reports the pixel
    size (it has already decoded the image to show a thumbnail)."""
    data = await file.read(MAX_BYTES + 1)
    try:
        return await ChatAssetService(session).upload(
            data=data,
            name=file.filename or "image",
            mime=file.content_type or "",
            width=width or None,
            height=height or None,
            session_id=session_id or None,
            group_context=group_context,
        )
    except AssetValidationError as exc:
        raise BadRequestError(str(exc)) from exc


@router.get("/{asset_id}")
async def get_asset(asset_id: str, session: SessionDep, group_context: GroupContextDep):
    """The image bytes. Immutable by id, so the browser may cache them."""
    asset = await ChatAssetService(session).get(asset_id, group_context)
    if asset is None:
        raise NotFoundError("Asset not found")
    return Response(
        content=bytes(asset.data),
        media_type=asset.mime,
        headers={"Cache-Control": "private, max-age=31536000, immutable"},
    )


@router.delete("/{asset_id}", status_code=204)
async def delete_asset(
    asset_id: str, session: SessionDep, group_context: GroupContextDep
):
    if not await ChatAssetService(session).delete(asset_id, group_context):
        raise NotFoundError("Asset not found")
    return Response(status_code=204)
