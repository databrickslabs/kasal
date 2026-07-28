"""Tier 1 — the block that decides whether a skill is ever activated.

The shape is pinned against the reference library's own ``to_prompt`` output.
Anthropic's models were trained against that XML, so a prettier format here
would trade activation quality for nothing — and the only way to know it has not
drifted is to compare the two.
"""

import pathlib
import tempfile
from types import SimpleNamespace

from src.services.skills import injection
from src.services.skills.parser import to_skill_md


def _skill(name, description):
    return SimpleNamespace(name=name, description=description)


class TestBlockShape:
    def test_it_matches_the_reference_librarys_own_output(self):
        """Pinned, not paraphrased. If skills-ref changes its block, this fails
        and someone decides deliberately rather than drifting."""
        import skills_ref

        skills = [
            _skill("pricing", "How we price a deal."),
            _skill("qbr", "How we run a quarterly review."),
        ]

        with tempfile.TemporaryDirectory() as tmp:
            dirs = []
            for s in skills:
                root = pathlib.Path(tmp) / s.name
                root.mkdir()
                (root / "SKILL.md").write_text(
                    to_skill_md(s.name, s.description, "body"), encoding="utf-8"
                )
                dirs.append(root)
            reference = skills_ref.to_prompt(dirs)

        ours = injection.build_skill_block(skills)

        # The reference includes a <location> per skill — a filesystem path, which
        # a database-backed skill does not have and a model cannot use. Everything
        # else must match line for line.
        expected, skipping = [], False
        for line in reference.strip().splitlines():
            if line.strip() == "<location>":
                skipping = True
            elif line.strip() == "</location>":
                skipping = False
            elif not skipping:
                expected.append(line)
        assert ours.splitlines() == expected

    def test_no_skills_means_no_block_at_all(self):
        """A "no skills available" sentence is pure cost in every prompt of every
        agent that has none — which is most of them."""
        assert injection.build_skill_block([]) == ""
        assert injection.build_prompt_section([]) == ""

    def test_only_the_name_and_description_are_advertised(self):
        """The whole mechanism is that bodies stay out of context until needed."""
        skill = SimpleNamespace(
            name="pricing", description="How we price.", body="SECRET MARGIN TABLE"
        )
        assert "SECRET MARGIN TABLE" not in injection.build_skill_block([skill])


class TestRobustness:
    def test_an_ampersand_in_a_description_does_not_break_the_block(self):
        """Unescaped, one author's ampersand silently hides every skill listed
        after theirs."""
        block = injection.build_skill_block(
            [_skill("a-b", "Handles R&D <and> more"), _skill("c-d", "Second skill")]
        )
        assert "&amp;" in block and "&lt;" in block
        assert block.count("<skill>") == 2

    def test_dicts_work_as_well_as_rows(self):
        """The kernel builds agents from both — rows in a crew run, dicts in
        generation — and the untested path is the one that breaks."""
        block = injection.build_skill_block([{"name": "a-b", "description": "d"}])
        assert "<name>\na-b\n</name>" in block

    def test_the_prompt_section_tells_the_model_what_to_do_with_the_list(self):
        """Without the instruction the model has a list and no idea it can act
        on it. It names the tool because activation is tool-side."""
        section = injection.build_prompt_section([_skill("a-b", "d")])
        assert "load_skill" in section

    def test_advertising_is_capped_and_the_truncation_is_logged(self, caplog):
        """Two hundred skills is 20k tokens before the task starts. A skill that
        never appears looks exactly like one the model ignored, so silence here
        would be the worst failure mode."""
        many = [
            _skill(f"skill-{i}", "d") for i in range(injection.MAX_SKILLS_IN_PROMPT + 5)
        ]
        with caplog.at_level("WARNING"):
            block = injection.build_skill_block(many)

        assert block.count("<skill>") == injection.MAX_SKILLS_IN_PROMPT
        assert "invisible to the model" in caplog.text
