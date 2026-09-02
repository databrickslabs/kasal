"""Schemas for Agent Skills.

Field CONSTRAINTS are not restated here. The spec's rules — the name pattern,
the length caps, the required fields — are enforced by the reference validator
in ``services/skills/parser.py``, and duplicating them in Pydantic would create
a second source of truth that drifts from it. These types describe the API's
shape; conformance is decided in one place.
"""

from datetime import datetime
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field

#: Where a skill came from. Drives trust: an uploaded skill is untrusted text
#: headed for a system prompt.
SKILL_SOURCES = ("builtin", "uploaded", "authored")


class SkillBase(BaseModel):
    name: str
    description: str = Field(
        ...,
        description=(
            "What it does AND when to use it. This is all the model sees when "
            "deciding whether to activate the skill."
        ),
    )
    body: str = ""
    license: Optional[str] = None
    compatibility: Optional[str] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)
    enabled: bool = True
    global_enabled: bool = False


class SkillFileInput(BaseModel):
    """A bundled reference file, as the editor sends it."""

    path: str
    content: str = ""


class SkillCreate(SkillBase):
    #: Bundled files, replacing any existing set. Omit to leave files alone.
    files: Optional[List[SkillFileInput]] = None


class SkillUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    body: Optional[str] = None
    license: Optional[str] = None
    compatibility: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None
    enabled: Optional[bool] = None
    global_enabled: Optional[bool] = None
    files: Optional[List["SkillFileInput"]] = None


class SkillFileResponse(BaseModel):
    path: str
    size_bytes: Optional[int] = None
    sha256: Optional[str] = None

    class Config:
        from_attributes = True


class SkillResponse(SkillBase):
    id: int
    source: str = "authored"
    #: NULL for a globally-available skill, set for one a workspace owns.
    group_id: Optional[str] = None
    #: True when this workspace row replaces a skill Kasal ships. It is what the
    #: UI needs to offer "reset to default" only where that means something.
    overrides_builtin: bool = False
    files: List[SkillFileResponse] = Field(default_factory=list)
    created_by_email: Optional[str] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    class Config:
        from_attributes = True


class SkillListResponse(BaseModel):
    skills: List[SkillResponse] = Field(default_factory=list)
    count: int = 0


class SkillValidationResult(BaseModel):
    """The reference validator's verdict, verbatim.

    ``errors`` are its own messages rather than Kasal's paraphrase: an author
    fixing a skill needs the wording the rest of the ecosystem uses, so that
    searching for it finds the spec rather than this codebase.
    """

    valid: bool
    errors: List[str] = Field(default_factory=list)
    warnings: List[str] = Field(default_factory=list)


class UcSyncTarget(BaseModel):
    """Where to publish a skill in Unity Catalog.

    The wire key is ``schema``, but the Python field is ``schema_name`` — a
    Pydantic field literally named ``schema`` shadows ``BaseModel.schema`` and
    warns on every import. The alias keeps the API contract (``catalog`` +
    ``schema``) while sidestepping that.
    """

    model_config = ConfigDict(populate_by_name=True)

    catalog: str = Field(..., description="Target Unity Catalog catalog")
    schema_name: str = Field(
        ..., alias="schema", description="Target schema within the catalog"
    )
