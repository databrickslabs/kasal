"""SKILL.md parsing and validation, against the reference implementation.

The rules are NOT reimplemented here. ``skills-ref`` is Anthropic's own
reference library (github.com/anthropics/agentskills) and it is what decides
whether a skill is valid, because the entire reason to adopt this format is that
a skill authored in Kasal runs unchanged in Claude Code, Cursor, Codex and
Gemini CLI. A validator that drifts from the reference is how "compatible"
quietly stops being true, and nobody finds out until a skill fails somewhere
else.

The reference works on DIRECTORIES; Kasal stores skills as rows. So a row is
materialised into a temporary directory and handed to the real validator. That
is a round-trip per validation, which is fine: validation happens on ingest and
on save, never on the run path.
"""

import logging
import pathlib
import tempfile
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

#: The reference's own recommendation, not a hard limit — a long body is a
#: warning that detail belongs in ``references/``, where it loads only when the
#: instructions call for it.
RECOMMENDED_BODY_LINES = 500


class SkillValidationError(ValueError):
    """A skill does not conform. Carries the reference validator's messages."""

    def __init__(self, errors: List[str]):
        self.errors = errors
        super().__init__("; ".join(errors))


@dataclass
class ParsedSkill:
    """One SKILL.md, in Kasal's own shape."""

    name: str
    description: str
    body: str
    license: Optional[str] = None
    compatibility: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    warnings: List[str] = field(default_factory=list)


def to_skill_md(
    name: str,
    description: str,
    body: str,
    license_: Optional[str] = None,
    compatibility: Optional[str] = None,
    metadata: Optional[Dict[str, Any]] = None,
) -> str:
    """Kasal's columns -> a SKILL.md file.

    The inverse of parsing, and the thing that makes a stored skill portable:
    export and validation both go through here, so a row that validates is a
    row that exports to something another client will accept.
    """
    lines = ["---", f"name: {name}", f"description: {_yaml_scalar(description)}"]
    if license_:
        lines.append(f"license: {_yaml_scalar(license_)}")
    if compatibility:
        lines.append(f"compatibility: {_yaml_scalar(compatibility)}")
    if metadata:
        lines.append("metadata:")
        for key, value in metadata.items():
            lines.append(f"  {key}: {_yaml_scalar(str(value))}")
    lines.append("---")
    lines.append("")
    return "\n".join(lines) + (body or "")


def _yaml_scalar(value: str) -> str:
    """Quote a value when plain YAML would misread it.

    A description is free text written by a person; one containing a colon or
    starting with a quote is normal and must not silently become a mapping or a
    parse error.
    """
    value = (value or "").replace("\n", " ").strip()
    if not value:
        return '""'
    if value[0] in "\"'{}[]&*#?|-<>=!%@`" or ": " in value or value.endswith(":"):
        escaped = value.replace('"', '\\"')
        return f'"{escaped}"'
    return value


def parse(skill_md: str, name_hint: Optional[str] = None) -> ParsedSkill:
    """Parse and VALIDATE a SKILL.md, using the reference library.

    ``name_hint`` is the directory the file came from. The spec requires the two
    to match, and the reference checks it — which only works if the temporary
    directory is named after the hint rather than after the frontmatter, or the
    check would validate a name against itself.
    """
    with tempfile.TemporaryDirectory() as tmp:
        root = pathlib.Path(tmp) / (name_hint or _peek_name(skill_md) or "skill")
        root.mkdir(parents=True, exist_ok=True)
        (root / "SKILL.md").write_text(skill_md, encoding="utf-8")
        return _parse_dir(root)


def validate_row(
    name: str,
    description: str,
    body: str,
    license_: Optional[str] = None,
    compatibility: Optional[str] = None,
    metadata: Optional[Dict[str, Any]] = None,
) -> ParsedSkill:
    """Validate what is about to be stored, before it is stored.

    Rendering to SKILL.md and validating that is deliberate: it checks the
    artefact users will actually receive on export, not an in-memory
    approximation of it.
    """
    return parse(
        to_skill_md(name, description, body, license_, compatibility, metadata),
        name_hint=name,
    )


def _parse_dir(root: pathlib.Path) -> ParsedSkill:
    import skills_ref

    errors = skills_ref.validate(root)
    if errors:
        raise SkillValidationError(list(errors))

    props = skills_ref.read_properties(root)
    body = _body_of((root / "SKILL.md").read_text(encoding="utf-8"))

    warnings: List[str] = []
    if body.count("\n") > RECOMMENDED_BODY_LINES:
        warnings.append(
            f"The body is over {RECOMMENDED_BODY_LINES} lines. The spec suggests "
            "moving detail into references/, which loads only when the "
            "instructions ask for it."
        )
    if getattr(props, "allowed_tools", None):
        warnings.append(
            "allowed-tools is marked experimental in the spec and Kasal does not "
            "act on it — tool access stays governed by the agent's own tools."
        )

    return ParsedSkill(
        name=props.name,
        description=props.description,
        body=body,
        license=getattr(props, "license", None),
        compatibility=getattr(props, "compatibility", None),
        metadata=dict(getattr(props, "metadata", None) or {}),
        warnings=warnings,
    )


def parse_directory(root: pathlib.Path) -> ParsedSkill:
    """Validate a skill folder on disk, as uploaded or seeded."""
    return _parse_dir(root)


def _body_of(skill_md: str) -> str:
    """Everything after the frontmatter block."""
    text = skill_md.lstrip("﻿")
    if not text.startswith("---"):
        return text
    end = text.find("\n---", 3)
    if end == -1:
        return ""
    rest = text[end + 4 :]
    return rest[1:] if rest.startswith("\n") else rest


def _peek_name(skill_md: str) -> Optional[str]:
    """The declared name, for choosing a directory before validation runs.

    Deliberately naive — it exists only so the temp directory can be named
    plausibly. Anything wrong with the name is the validator's to report, in the
    validator's own words.
    """
    for line in _frontmatter_lines(skill_md):
        if line.startswith("name:"):
            return line.split(":", 1)[1].strip().strip("\"'") or None
    return None


def _frontmatter_lines(skill_md: str) -> List[str]:
    text = skill_md.lstrip("﻿")
    if not text.startswith("---"):
        return []
    end = text.find("\n---", 3)
    return text[3:end].splitlines() if end != -1 else []
