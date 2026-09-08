"""Builder state attached to a shared session; metadata stays cheap to list."""

import json
from typing import Any, Literal

from pydantic import BaseModel, Field, field_validator


class BuilderCanvasRequest(BaseModel):
    title: str = Field(min_length=1, max_length=255)
    mode: Literal["crew", "flow"]
    revision: int = Field(default=0, ge=0)
    state: dict[str, Any]

    @field_validator("state")
    @classmethod
    def bounded_canvas(cls, value):
        if len(json.dumps(value).encode()) > 4 * 1024 * 1024:
            raise ValueError("Canvas exceeds the 4 MB session limit")
        for key in ("nodes", "edges", "flowNodes", "flowEdges"):
            items = value.get(key, [])
            if not isinstance(items, list) or len(items) > 2000:
                raise ValueError(f"Invalid {key} in canvas")
        return value


class BuilderCanvasResponse(BaseModel):
    state: dict[str, Any] | None = None
    revision: int = 0
