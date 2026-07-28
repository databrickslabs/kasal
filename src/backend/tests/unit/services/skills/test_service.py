"""Ownership and scoping for skills.

The interesting decision here is that skills are owned DIFFERENTLY from remote
A2A agents. An agent is a URL and a credential — system administration. A skill
is domain knowledge, so a workspace authors its own, and Kasal's builtins are
read-only from a workspace's point of view.
"""

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.schemas.skill import SkillCreate, SkillUpdate
from src.services.skills.service import SkillService


class _Ctx:
    group_ids = ["acme"]
    primary_group_id = "acme"
    group_email = "editor@example.com"


def _service():
    session = MagicMock()
    session.add = MagicMock()
    session.flush = AsyncMock()
    session.delete = AsyncMock()
    service = SkillService(session)
    service.repository = MagicMock()
    service.repository.find_by_name = AsyncMock(return_value=None)
    service.repository.find_visible = AsyncMock(return_value=None)
    service.repository.list_visible = AsyncMock(return_value=[])
    service.repository.replace_files = AsyncMock()
    return service


def _row(**overrides):
    row = SimpleNamespace(
        id=1,
        name="pricing",
        description="How we price a deal.",
        body="# Steps",
        license=None,
        compatibility=None,
        skill_metadata={},
        source="authored",
        group_id="acme",
        enabled=True,
        global_enabled=False,
        files=[],
        created_by_email=None,
    )
    for k, v in overrides.items():
        setattr(row, k, v)
    return row


def _create(name="pricing"):
    return SkillCreate(name=name, description="How we price a deal.", body="# Steps")


class TestAuthoring:
    @pytest.mark.asyncio
    async def test_a_workspace_authors_its_own_skill(self):
        """Unlike a remote agent, which only a Kasal admin registers. Routing
        every team's own procedure through a platform admin would make the
        feature unusable."""
        service = _service()
        service.repository.find_visible = AsyncMock(return_value=_row())
        await service.create_skill(_create(), _Ctx())

        created = service.session.add.call_args[0][0]
        assert created.group_id == "acme"
        assert created.source == "authored"

    @pytest.mark.asyncio
    async def test_an_invalid_skill_never_reaches_the_database(self):
        from src.services.skills import parser

        service = _service()
        with pytest.raises(parser.SkillValidationError):
            await service.create_skill(
                SkillCreate(name="Not Kebab", description="d", body=""), _Ctx()
            )
        service.session.add.assert_not_called()

    @pytest.mark.asyncio
    async def test_a_duplicate_name_in_the_same_workspace_is_refused(self):
        service = _service()
        service.repository.find_by_name = AsyncMock(return_value=_row())
        with pytest.raises(ValueError, match="already has a skill"):
            await service.create_skill(_create(), _Ctx())

    @pytest.mark.asyncio
    async def test_a_caller_with_no_workspace_cannot_author(self):
        service = _service()

        class _NoGroup:
            group_ids = []
            primary_group_id = None
            group_email = "x@example.com"

        with pytest.raises(ValueError, match="workspace is required"):
            await service.create_skill(_create(), _NoGroup())


class TestBuiltins:
    @pytest.mark.asyncio
    async def test_a_builtin_cannot_be_edited_in_place(self):
        """The row is shared across tenants. Editing it would either change
        Kasal's content for everyone or be silently undone by the next seed."""
        service = _service()
        service.repository.find_visible = AsyncMock(
            return_value=_row(group_id=None, source="builtin")
        )
        assert await service.update_skill(1, SkillUpdate(body="mine"), _Ctx()) is None

    @pytest.mark.asyncio
    async def test_a_builtin_cannot_be_deleted(self):
        service = _service()
        service.repository.find_visible = AsyncMock(return_value=_row(group_id=None))
        assert await service.delete_skill(1, _Ctx()) is False

    @pytest.mark.asyncio
    async def test_disabling_a_builtin_clones_it_into_the_workspace(self):
        """One tenant turning a builtin off must not turn it off for everyone."""
        service = _service()
        builtin = _row(group_id=None, source="builtin")
        service.repository.find_visible = AsyncMock(return_value=builtin)

        result = await service.set_enabled(1, False, _Ctx())

        assert result.group_id == "acme"
        assert result.enabled is False
        assert builtin.enabled is True

    @pytest.mark.asyncio
    async def test_toggling_the_workspaces_own_skill_flips_it_in_place(self):
        service = _service()
        own = _row(enabled=True)
        service.repository.find_visible = AsyncMock(return_value=own)

        await service.set_enabled(1, False, _Ctx())

        assert own.enabled is False
        service.session.add.assert_not_called()


class TestUpload:
    @pytest.mark.asyncio
    async def test_an_uploaded_skill_is_marked_as_uploaded(self):
        """Not bookkeeping: uploaded content is untrusted text headed for a
        system prompt, and the marker is what lets a guardrail treat it so."""
        service = _service()
        service.repository.find_visible = AsyncMock(return_value=_row())
        parsed = SimpleNamespace(
            name="pricing",
            description="How we price a deal.",
            body="b",
            license=None,
            compatibility=None,
            metadata={},
        )
        with patch("src.services.skills.packaging.read_zip", return_value=(parsed, [])):
            await service.import_zip(b"zip", _Ctx())

        assert service.session.add.call_args[0][0].source == "uploaded"

    @pytest.mark.asyncio
    async def test_re_uploading_without_replace_is_refused(self):
        """Silently overwriting someone's edits because a filename matched is
        not something to do by default."""
        service = _service()
        service.repository.find_by_name = AsyncMock(return_value=_row())
        parsed = SimpleNamespace(
            name="pricing",
            description="d",
            body="b",
            license=None,
            compatibility=None,
            metadata={},
        )
        with patch("src.services.skills.packaging.read_zip", return_value=(parsed, [])):
            with pytest.raises(ValueError, match="replace=true"):
                await service.import_zip(b"zip", _Ctx())


class TestValidateEndpointBehaviour:
    def test_validation_returns_the_errors_rather_than_raising(self):
        """The editor asked whether a draft is valid; an invalid draft is the
        answer, not a failure."""
        result = SkillService.validate(
            SkillCreate(name="Not Kebab", description="d", body="")
        )
        assert result.valid is False
        assert result.errors

    def test_a_valid_skill_can_still_carry_warnings(self):
        result = SkillService.validate(
            SkillCreate(
                name="x-y",
                description="A description.",
                body="\n".join(f"l{i}" for i in range(600)),
            )
        )
        assert result.valid is True
        assert result.warnings
