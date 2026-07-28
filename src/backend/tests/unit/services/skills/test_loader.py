"""Tiers 2 and 3 — activation and file reads.

A skill file path arrives from a MODEL, which may have been steered by the
skill's own text. Most of this is about that.
"""

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.services.skills import loader


def _file(path, content="x"):
    return SimpleNamespace(path=path, content=content)


def _skill(name="pricing", enabled=True, files=None, body="# How we price"):
    return SimpleNamespace(
        name=name,
        description="How we price a deal.",
        body=body,
        enabled=enabled,
        global_enabled=False,
        files=files or [],
    )


def _repo(skill=None, enabled_list=None):
    repository = MagicMock()
    repository.find_by_name = AsyncMock(return_value=skill)
    repository.list_enabled = AsyncMock(return_value=enabled_list or [])
    return patch(
        "src.repositories.skill_repository.SkillRepository", return_value=repository
    )


class TestPathSafety:
    """There is no directory to escape from — paths resolve against the stored
    set — but the spec's shape is still enforced, because a path that looks like
    traversal should be refused at the boundary rather than silently missing."""

    @pytest.mark.parametrize(
        "path",
        [
            "../../../etc/passwd",
            "references/../../secrets.md",
            "/etc/passwd",
            "C:/windows/system32",
            "..\\..\\windows",
        ],
    )
    def test_traversal_is_refused(self, path):
        with pytest.raises(loader.SkillFileNotFound):
            loader.normalise_path(path)

    def test_a_file_outside_references_and_assets_is_refused(self):
        with pytest.raises(loader.SkillFileNotFound):
            loader.normalise_path("SKILL.md")

    def test_scripts_are_not_readable(self):
        """Being able to READ a bundled script is the first half of being able
        to run one, and execution needs a sandbox that does not exist yet."""
        with pytest.raises(loader.SkillFileNotFound):
            loader.normalise_path("scripts/run.py")

    def test_nesting_beyond_one_level_is_refused(self):
        with pytest.raises(loader.SkillFileNotFound):
            loader.normalise_path("references/deep/deeper/file.md")

    def test_a_valid_path_is_normalised(self):
        assert loader.normalise_path("references/./pricing.md") == (
            "references/pricing.md"
        )
        assert loader.normalise_path("assets\\template.md") == "assets/template.md"

    def test_an_empty_path_is_refused(self):
        with pytest.raises(loader.SkillFileNotFound):
            loader.normalise_path("")


class TestLoadSkill:
    @pytest.mark.asyncio
    async def test_it_returns_the_body_and_lists_the_files(self):
        """A body saying "see references/pricing.md" is useless if the model has
        to guess what exists."""
        skill = _skill(files=[_file("references/pricing.md")])
        with _repo(skill):
            result = await loader.load_skill("pricing", ["acme"], MagicMock())

        assert result["body"] == "# How we price"
        assert result["files"] == ["references/pricing.md"]

    @pytest.mark.asyncio
    async def test_a_disabled_skill_cannot_be_loaded(self):
        """A skill an admin turned off must not reach a prompt through a stale
        agent config."""
        with _repo(_skill(enabled=False)):
            with pytest.raises(loader.SkillNotFound):
                await loader.load_skill("pricing", ["acme"], MagicMock())

    @pytest.mark.asyncio
    async def test_an_unknown_skill_and_a_forbidden_one_are_the_same_error(self):
        """Otherwise a name is an oracle for what other workspaces authored."""
        with _repo(None):
            with pytest.raises(loader.SkillNotFound):
                await loader.load_skill("someone-elses", ["acme"], MagicMock())


class TestReadFile:
    @pytest.mark.asyncio
    async def test_it_returns_the_stored_content(self):
        skill = _skill(files=[_file("references/pricing.md", "margins")])
        with _repo(skill):
            result = await loader.read_file(
                "pricing", "references/pricing.md", ["acme"], MagicMock()
            )
        assert result["content"] == "margins"

    @pytest.mark.asyncio
    async def test_a_missing_file_says_what_the_skill_actually_has(self):
        """The model is going to try again; telling it the real paths turns a
        dead end into one more round."""
        skill = _skill(files=[_file("references/pricing.md")])
        with _repo(skill):
            with pytest.raises(loader.SkillFileNotFound) as exc:
                await loader.read_file(
                    "pricing", "references/nope.md", ["acme"], MagicMock()
                )
        assert "references/pricing.md" in str(exc.value)

    @pytest.mark.asyncio
    async def test_the_path_is_checked_before_the_skill_is_looked_up(self):
        """A refusal should not depend on the skill existing, or the shape of
        the error leaks whether it does."""
        repository = MagicMock()
        repository.find_by_name = AsyncMock(return_value=None)
        with patch(
            "src.repositories.skill_repository.SkillRepository",
            return_value=repository,
        ):
            with pytest.raises(loader.SkillFileNotFound):
                await loader.read_file("x", "../etc/passwd", ["acme"], MagicMock())
        repository.find_by_name.assert_not_awaited()


class TestResolveForAgent:
    @pytest.mark.asyncio
    async def test_named_skills_are_resolved(self):
        with _repo(enabled_list=[_skill("pricing"), _skill("qbr")]):
            chosen = await loader.resolve_for_agent(["qbr"], ["acme"], MagicMock())
        assert [s.name for s in chosen] == ["qbr"]

    @pytest.mark.asyncio
    async def test_globally_enabled_skills_come_along_unasked(self):
        """The same meaning global_enabled carries for a tool or an MCP server:
        an admin who turned it on for everyone should not have to also attach it
        everywhere."""
        everywhere = _skill("house-style")
        everywhere.global_enabled = True
        with _repo(enabled_list=[_skill("pricing"), everywhere]):
            chosen = await loader.resolve_for_agent([], ["acme"], MagicMock())
        assert [s.name for s in chosen] == ["house-style"]

    @pytest.mark.asyncio
    async def test_a_deleted_skill_does_not_break_the_agent(self):
        """It should run, minus that skill — not fail to build."""
        with _repo(enabled_list=[_skill("pricing")]):
            chosen = await loader.resolve_for_agent(
                ["pricing", "gone"], ["acme"], MagicMock()
            )
        assert [s.name for s in chosen] == ["pricing"]
