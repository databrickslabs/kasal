"""Schemas for workflow recipes — executed crews kept for reuse."""

from typing import List, Optional

from pydantic import BaseModel, ConfigDict, Field


class RecipeSuggestRequest(BaseModel):
    """Ask whether this workspace has already built something like a prompt."""

    prompt: str = Field(..., description="The crew-generation prompt to match against")
    limit: int = Field(3, ge=1, le=10, description="Maximum suggestions to return")
    min_similarity: Optional[float] = Field(
        None,
        ge=0.0,
        le=1.0,
        description=(
            "Override the similarity floor. Defaults to "
            "WORKFLOW_RECIPE_MIN_SIMILARITY."
        ),
    )


class RecipeCurateRequest(BaseModel):
    """Record (or clear) a human judgement on a recipe.

    ``None`` clears the judgement. 'bad' and 'hidden' both remove the recipe
    from suggestions; they differ only in what they mean to the person setting
    them ("this crew is wrong" vs "stop offering me this").
    """

    curation: Optional[str] = Field(
        None,
        description="'good' | 'bad' | 'hidden', or null to clear",
        json_schema_extra={"enum": ["good", "bad", "hidden", None]},
    )


class RecipeSummary(BaseModel):
    """A recipe as offered for reuse.

    Carries the SHAPE of the crew (counts, tools, servers) rather than the full
    agents/tasks payload: this is a "have you built this before?" answer, and the
    caller that actually materialises a crew fetches the graph deliberately.
    """

    recipe_id: int
    intent_text: str
    run_count: int = Field(..., description="How many runs of this intent folded in")
    agent_count: int
    task_count: int
    tool_names: List[str] = Field(default_factory=list)
    mcp_servers: List[str] = Field(default_factory=list)
    source_job_id: Optional[str] = None
    # Present on suggestions, absent when simply listing the library.
    similarity: Optional[float] = Field(
        None, description="Cosine similarity to the prompt, when this was a match"
    )
    curation: Optional[str] = None
    times_reused: Optional[int] = None
    updated_at: Optional[str] = None

    model_config = ConfigDict(from_attributes=True)
