"""The skill name chooses a temp directory; it must not choose any other
directory (audit F05).

``parse`` joined the name onto the temp root as-is and wrote ``SKILL.md``
there before validation ran: an absolute name replaced the root, ``..``
walked out of it, and the write landed wherever that pointed.
"""

import pytest

from src.services.skills import parser


@pytest.mark.parametrize(
    "name",
    ["/tmp/elsewhere", "../escape", "a/b", "..", ".", "C:\\\\evil", "bad\\\\name"],
)
def test_a_name_that_is_not_one_directory_is_refused(name):
    with pytest.raises(parser.SkillValidationError, match="single directory name"):
        parser._directory_name(name)


def test_a_plain_name_passes_and_a_missing_one_gets_a_default():
    assert parser._directory_name("quarterly-review") == "quarterly-review"
    assert parser._directory_name(None) == "skill"


def test_nothing_is_written_outside_the_temp_directory(tmp_path):
    target = tmp_path / "skill-write-target"
    md = parser.to_skill_md("x", "A description.", "body")
    with pytest.raises(parser.SkillValidationError):
        parser.parse(md, name_hint=str(target))
    assert not target.exists()
    with pytest.raises(parser.SkillValidationError):
        parser.validate_row(str(target), "A description.", "body")
    assert not target.exists()


def test_the_frontmatter_name_is_held_to_the_same_rule(tmp_path):
    target = tmp_path / "from-frontmatter"
    md = parser.to_skill_md(str(target), "A description.", "body")
    with pytest.raises(parser.SkillValidationError):
        parser.parse(md)  # no hint: the declared name picks the directory
    assert not target.exists()
