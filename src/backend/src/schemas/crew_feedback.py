"""Schemas for crew thumbs feedback (chat votes shown in the catalog)."""

from datetime import datetime
from typing import Optional

from pydantic import BaseModel, ConfigDict, Field, model_validator


class CrewFeedbackCreateRequest(BaseModel):
    rating: str = Field(..., pattern="^(up|down)$", description="Thumbs up or down")
    comment: Optional[str] = Field(None, max_length=4000, description="What went wrong (required for down)")

    @model_validator(mode="after")
    def _down_requires_comment(self):
        if self.rating == "down" and not (self.comment or "").strip():
            raise ValueError("a comment explaining what went wrong is required for thumbs-down")
        return self


class CrewFeedbackResponse(BaseModel):
    id: str
    crew_id: str
    rating: str
    comment: Optional[str] = None
    created_at: datetime
    group_email: Optional[str] = Field(None, description="Who left the feedback")

    model_config = ConfigDict(from_attributes=True)


class CrewFeedbackSummaryEntry(BaseModel):
    crew_id: str
    up: int = 0
    down: int = 0
