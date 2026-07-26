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


class RecipeJobEntry(BaseModel):
    """The recipe a given run was mined into, as shown on a run row."""

    recipe_id: int
    curation: Optional[str] = None
    intent_text: str
    run_count: int
    times_reused: Optional[int] = None


class RecipeArmStats(BaseModel):
    """Outcomes for one arm of the reuse experiment.

    Rates are over LINKED runs, not over generations: a generated crew that was
    never run is not a failed crew, and counting it as one would punish whichever
    arm produced crews people chose not to run. ``linked_runs`` is reported so a
    completion rate computed on three runs is visibly that.
    """

    generations: int
    linked_runs: int
    completed: int
    completion_rate: Optional[float] = None
    median_duration_ms: Optional[float] = None
    median_error_spans: Optional[float] = None
    median_agents: Optional[float] = None
    median_tasks: Optional[float] = None


class RecipeEffectiveness(BaseModel):
    """Whether reusing recipes measurably helps.

    ``comparable`` is the field to read first: it is True only when both the
    exemplar and control arms have data, which is the only pair that differs by
    TREATMENT rather than by how familiar the request was. When it is False the
    coverage numbers are still real, but no causal claim is available.
    """

    window_days: int
    generations: int
    with_candidates: int
    with_blessed_candidates: int
    coverage_rate: Optional[float] = None
    injection_rate: Optional[float] = None
    holdout_fraction: float
    min_similarity: float
    arms: dict[str, RecipeArmStats]
    comparable: bool
    note: str


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
