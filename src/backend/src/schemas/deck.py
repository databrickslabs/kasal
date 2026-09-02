"""Schemas for deck editing — one slide at a time."""

from typing import Literal, Optional

from pydantic import BaseModel, Field

#: A slide is a self-contained <section>; anything near this size is not one.
MAX_SLIDE_CHARS = 60_000


class SlideRefineRequest(BaseModel):
    """Revise one slide (``refine``) or write a new one between two (``add``)."""

    mode: Literal["refine", "add"] = "refine"
    instruction: str = Field(
        "",
        max_length=4000,
        description="What should change / what the new slide covers",
    )
    slide: Optional[str] = Field(
        None, max_length=MAX_SLIDE_CHARS, description="The slide to revise (refine)"
    )
    reference: Optional[str] = Field(
        None, max_length=MAX_SLIDE_CHARS, description="A slide whose design to match"
    )
    before: Optional[str] = Field(
        None,
        max_length=MAX_SLIDE_CHARS,
        description="The slide before the new one (add)",
    )
    after: Optional[str] = Field(
        None,
        max_length=MAX_SLIDE_CHARS,
        description="The slide after the new one (add)",
    )
    position: str = Field("", max_length=40, description='Where it sits, e.g. "3 of 8"')
    model: Optional[str] = Field(None, description="Model key from the chat picker")


class SlideRefineResponse(BaseModel):
    """The revised (or new) slide as one <section class="slide"> element."""

    section: Optional[str] = None
    error: Optional[str] = Field(None, description="Set when no slide came back")
    model: Optional[str] = Field(None, description="The model that served the call")
    attempts: int = Field(
        1, description="LLM calls made (2 when the first reply held no slide)"
    )
    job_id: Optional[str] = Field(
        None, description="The run recording the call; its trace is the run activity"
    )
    duration_ms: float = 0
