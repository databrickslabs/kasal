"""Schemas for chat assets — images attached in the chat."""

from typing import Optional

from pydantic import BaseModel, Field


class ChatAssetOut(BaseModel):
    """An attached image's metadata; the bytes are served separately."""

    id: str
    name: str
    mime: str
    size: int
    width: Optional[int] = None
    height: Optional[int] = None
    session_id: Optional[str] = None
    ref: str = Field(description='How HTML refers to it: <img src="asset:<id>">')


class ImageAssetRef(BaseModel):
    """An image attached to a chat turn, as the run receives it — enough for
    the model to place it in a layout, and nothing more."""

    id: str = Field(..., max_length=64)
    name: str = Field("", max_length=255)
    width: Optional[int] = None
    height: Optional[int] = None
