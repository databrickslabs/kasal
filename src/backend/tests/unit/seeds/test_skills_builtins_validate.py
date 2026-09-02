"""Every builtin skill must pass the SKILL.md reference validator.

The seeder validates at startup and SKIPS a malformed builtin with an error
log — which reads as "the skill is missing" in the UI. This catches it at
test time instead, and keeps descriptions within the size the discovery
prompt can afford.
"""

import pytest

from src.seeds.skills_data import BUILTIN_SKILLS
from src.services.skills import parser


@pytest.mark.parametrize("skill", BUILTIN_SKILLS, ids=lambda s: s["name"])
def test_builtin_skill_validates(skill):
    parsed = parser.validate_row(
        skill["name"],
        skill["description"],
        skill["body"],
        skill.get("license"),
        skill.get("compatibility"),
        skill.get("metadata"),
    )
    assert parsed.name == skill["name"]
    assert not getattr(parsed, "warnings", None), parsed.warnings
    # The description is the only text an agent sees at discovery time and is
    # pre-loaded for EVERY builtin on EVERY agent — keep it a paragraph.
    assert 120 <= len(skill["description"]) <= 700


def test_builtin_names_are_unique():
    names = [s["name"] for s in BUILTIN_SKILLS]
    assert len(names) == len(set(names))
