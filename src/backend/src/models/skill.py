from datetime import datetime

from sqlalchemy import (
    JSON,
    Boolean,
    Column,
    DateTime,
    ForeignKey,
    Integer,
    String,
    Text,
    UniqueConstraint,
)

from sqlalchemy.orm import relationship

from src.db.base import Base


class Skill(Base):
    """An Agent Skill — packaged procedural know-how.

    The gap it fills: Kasal has TOOLS (code an agent calls), KNOWLEDGE
    (documents an agent searches) and PROMPT TEMPLATES (how Kasal talks to the
    model). None of them holds "how we do a quarterly review here". Today that
    goes in an agent's backstory, where it is always in context, cannot be
    shared between agents, and grows until it crowds out the task.

    Stored as rows rather than as a directory on disk because Kasal is
    multi-tenant and runs on Databricks Apps: a skill is group-scoped content
    that has to survive a stateless container. The FORMAT is still the
    standard's — the columns are its frontmatter fields, and
    ``services/skills/packaging.py`` round-trips a row to the folder layout
    every other Agent Skills client reads.
    """

    __tablename__ = "skills"
    __table_args__ = (UniqueConstraint("name", "group_id", name="uq_skill_name_group"),)

    id = Column(Integer, primary_key=True)

    #: Spec-constrained: 1-64 chars, lowercase, digits and single hyphens, and
    #: it MUST match the directory name on export. Validated by the reference
    #: library rather than by a regex written here.
    name = Column(String(64), nullable=False)

    #: What it does AND when to use it. This is the field that decides whether a
    #: skill is ever activated — it is all the model sees at discovery time —
    #: which is why the spec caps it at 1024 and the authoring UI treats it as
    #: the important field rather than an afterthought.
    description = Column(String(1024), nullable=False)

    #: The SKILL.md body: Markdown, no format restrictions, recommended under
    #: ~5000 tokens with the detail pushed into reference files.
    body = Column(Text, nullable=False, default="")

    license = Column(String(255), nullable=True)
    compatibility = Column(String(500), nullable=True)
    #: The spec's arbitrary string map. Kasal-specific fields go HERE and never
    #: as new top-level frontmatter — forking the format would forfeit the
    #: portability that is the whole reason to adopt it.
    skill_metadata = Column(JSON, nullable=True, default=dict)

    #: builtin (seeded) | uploaded | authored. Drives whether ingest treats the
    #: content as untrusted: an uploaded skill is text that will be placed in a
    #: system prompt, which is prompt injection with a friendly name.
    source = Column(String(32), nullable=False, default="authored")

    #: NULL for a globally-available skill; set for one a workspace owns.
    group_id = Column(String(100), nullable=True)
    created_by_email = Column(String(255), nullable=True)

    enabled = Column(Boolean, default=True)
    #: Attached to every agent without being selected, matching what the same
    #: flag means for a tool or an MCP server.
    global_enabled = Column(Boolean, default=False)

    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    #: Eager-loadable and cascade-deleted: a skill is one unit, and a bundled
    #: file outliving its skill is a row nothing can ever reach.
    files = relationship(
        "SkillFile",
        back_populates="skill",
        cascade="all, delete-orphan",
        lazy="selectin",
    )


class SkillFile(Base):
    """One bundled file — ``references/``, ``assets/``.

    Tier 3 of progressive disclosure: never loaded with the skill, only when the
    instructions tell the model to read it. Kept as rows beside the skill so a
    skill is one transactional unit and there is no volume to keep in sync with
    the database.

    ``scripts/`` is deliberately NOT accepted at ingest. Executing bundled code
    needs a sandbox, an approval model and a threat review that assumes the
    skill is hostile; storing the files first and deciding later is how that
    ends up shipping by accident.
    """

    __tablename__ = "skill_files"
    __table_args__ = (
        UniqueConstraint("skill_id", "path", name="uq_skillfile_skill_path"),
    )

    id = Column(Integer, primary_key=True)
    skill_id = Column(
        Integer, ForeignKey("skills.id", ondelete="CASCADE"), nullable=False
    )
    #: Relative to the skill root, one level deep, forward slashes. The loader
    #: re-validates on every read rather than trusting what ingest stored.
    path = Column(String(500), nullable=False)
    content = Column(Text, nullable=False, default="")
    sha256 = Column(String(64), nullable=True)
    size_bytes = Column(Integer, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)

    skill = relationship("Skill", back_populates="files")
