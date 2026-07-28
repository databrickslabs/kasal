"""Resolving an external caller to a tenant.

Every one of these is a refusal test. The permissive failure — resolving to
*something* when the caller could not be identified — is what produces a context
that sails past `.in_(group_ids)` filters.
"""

from unittest.mock import AsyncMock, patch

import pytest

from src.services.external.identity import (
    ExternalAuthError,
    ExternalCaller,
    resolve_caller,
)


class _Ctx:
    def __init__(self, group_ids, email="alice@acme-corp.com"):
        self.group_ids = list(group_ids)
        self.group_email = email

    @property
    def primary_group_id(self):
        return self.group_ids[0] if self.group_ids else None


def _patch_from_email(context):
    return patch(
        "src.services.external.identity.GroupContext.from_email",
        new=AsyncMock(return_value=context),
    )


class TestRefusals:
    @pytest.mark.asyncio
    async def test_no_email_is_refused(self):
        with pytest.raises(ExternalAuthError) as exc:
            await resolve_caller(protocol="mcp", email=None)
        assert "identity" in str(exc.value).lower()

    @pytest.mark.asyncio
    async def test_caller_with_no_group_is_refused(self):
        """Not an empty capability list — a refusal. An empty GroupContext would
        pass through every group filter written as `.in_(group_ids)`, and a query
        that forgot the emptiness guard would return every tenant's rows."""
        with _patch_from_email(_Ctx([])):
            with pytest.raises(ExternalAuthError) as exc:
                await resolve_caller(protocol="mcp", email="nobody@example.com")
        assert "workspace" in str(exc.value).lower()

    @pytest.mark.asyncio
    async def test_requesting_a_workspace_you_are_not_in_is_refused(self):
        """A caller-supplied group id taken on trust is a one-header
        cross-tenant read."""
        with _patch_from_email(_Ctx(["acme_corp"])):
            with pytest.raises(ExternalAuthError) as exc:
                await resolve_caller(
                    protocol="a2a",
                    email="alice@acme-corp.com",
                    group_id="globex_inc",
                )
        assert "not a member" in str(exc.value).lower()

    @pytest.mark.asyncio
    async def test_refusal_does_not_silently_fall_back_to_the_primary_group(self):
        """Returning data from a DIFFERENT workspace than the one asked for,
        without saying so, is worse than an error."""
        with _patch_from_email(_Ctx(["acme_corp"])):
            with pytest.raises(ExternalAuthError):
                await resolve_caller(
                    protocol="mcp", email="alice@acme-corp.com", group_id="globex_inc"
                )

    @pytest.mark.asyncio
    async def test_error_carries_a_scheme_so_the_caller_knows_what_to_present(self):
        err = ExternalAuthError("nope", scheme="oauth2")
        assert err.scheme == "oauth2"


class TestSuccessfulResolution:
    @pytest.mark.asyncio
    async def test_resolves_to_the_callers_groups(self):
        with _patch_from_email(_Ctx(["acme_corp"])):
            caller = await resolve_caller(protocol="mcp", email="alice@acme-corp.com")
        assert caller.group_ids == ["acme_corp"]
        assert caller.protocol == "mcp"

    @pytest.mark.asyncio
    async def test_requested_workspace_is_honoured_when_a_member(self):
        with _patch_from_email(_Ctx(["acme_corp", "globex_inc"])):
            caller = await resolve_caller(
                protocol="a2a", email="alice@acme-corp.com", group_id="globex_inc"
            )
        assert "globex_inc" in caller.group_ids

    @pytest.mark.asyncio
    async def test_origin_records_protocol_and_caller(self):
        """Stamped onto the execution so "who called this crew, over which
        protocol, and what did it cost" is answerable from the first run."""
        with _patch_from_email(_Ctx(["acme_corp"])):
            caller = await resolve_caller(protocol="mcp", email="alice@acme-corp.com")
        assert caller.origin == "mcp:alice@acme-corp.com"

    @pytest.mark.asyncio
    async def test_caller_is_immutable(self):
        """The context is resolved once and threaded down; a mutable caller is a
        way for a later layer to widen its own scope."""
        caller = ExternalCaller(
            group_context=_Ctx(["acme_corp"]), protocol="mcp", identifier="a@b.c"
        )
        with pytest.raises(Exception):
            caller.protocol = "a2a"
