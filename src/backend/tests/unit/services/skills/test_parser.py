"""Conformance, delegated to the reference validator.

These do not re-test the spec's rules — ``skills-ref`` owns those. They test the
BRIDGE: that Kasal's rows render to a SKILL.md the reference accepts, that its
refusals reach the caller intact, and that the round trip does not corrupt
ordinary English.
"""

import pytest

from src.services.skills import parser


class TestValidation:
    def test_a_conforming_skill_parses(self):
        parsed = parser.validate_row(
            "quarterly-review",
            "How we run a QBR. Use when preparing or critiquing one.",
            "# Steps\n1. Pull the metrics\n",
        )
        assert parsed.name == "quarterly-review"
        assert parsed.body.startswith("# Steps")

    def test_the_references_own_message_reaches_the_caller(self):
        """An author needs the wording the rest of the ecosystem uses, so that
        searching for it finds the spec rather than this codebase."""
        with pytest.raises(parser.SkillValidationError) as exc:
            parser.validate_row("Not Kebab Case", "d", "b")
        assert any("lowercase" in e for e in exc.value.errors)

    def test_a_missing_description_is_refused(self):
        with pytest.raises(parser.SkillValidationError):
            parser.validate_row("fine-name", "", "body")

    def test_the_name_is_checked_against_the_directory_not_itself(self):
        """The spec requires the two to match. Validating a name against a
        directory named after that same name would pass anything."""
        skill_md = parser.to_skill_md("declared-name", "A description.", "body")
        with pytest.raises(parser.SkillValidationError):
            parser.parse(skill_md, name_hint="different-directory")


class TestRendering:
    def test_a_description_containing_a_colon_survives(self):
        """Plain YAML would read it as a mapping. Descriptions are free text
        written by people, and colons are normal English."""
        text = "Formats output: tables, charts, and prose. Use for reports."
        assert parser.validate_row("x-y", text, "b").description == text

    def test_a_description_starting_with_a_quote_survives(self):
        text = '"Definition of done" checks before a release.'
        assert parser.validate_row("x-y", text, "b").description == text

    def test_metadata_round_trips(self):
        """Kasal-specific fields belong under metadata — the spec provides it
        for exactly this — and never as new top-level frontmatter."""
        parsed = parser.validate_row(
            "x-y", "A description.", "b", metadata={"author": "kasal", "version": "1"}
        )
        assert parsed.metadata["author"] == "kasal"

    def test_the_body_is_separated_from_the_frontmatter(self):
        parsed = parser.validate_row("x-y", "A description.", "# Title\n\ntext\n")
        assert not parsed.body.startswith("---")
        assert parsed.body.startswith("# Title")

    def test_an_empty_body_is_allowed(self):
        """A skill whose whole content is its description is unusual but valid —
        rejecting it would be Kasal inventing a rule the spec does not have."""
        assert parser.validate_row("x-y", "A description.", "").body == ""


class TestWarnings:
    def test_a_long_body_is_warned_about_not_rejected(self):
        """The spec RECOMMENDS moving detail into references/; it does not
        require it, and refusing would be a divergent rule."""
        parsed = parser.validate_row(
            "x-y", "A description.", "\n".join(f"line {i}" for i in range(600))
        )
        assert any("references/" in w for w in parsed.warnings)

    def test_allowed_tools_is_flagged_as_not_acted_on(self):
        """It is experimental in the spec and Kasal does not enforce it. Silence
        would imply the skill's tool list is being honoured."""
        skill_md = (
            "---\nname: x-y\ndescription: A description.\n"
            "allowed-tools: Read Write\n---\n\nbody\n"
        )
        parsed = parser.parse(skill_md, name_hint="x-y")
        assert any("experimental" in w for w in parsed.warnings)
