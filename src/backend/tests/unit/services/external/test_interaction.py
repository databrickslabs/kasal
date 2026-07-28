"""The input_required round-trip, and the OBO rules around it.

The behaviour under test is the one Kasal has and almost nothing an external
agent can call does: a run that pauses for a human and continues once answered.
"""

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.services.external.identity import ExternalAuthError, ExternalCaller
from src.services.external.interaction import (
    PendingInteraction,
    pending_for_run,
    respond,
)


class _Ctx:
    def __init__(self, token="tok-123", group_ids=("acme_corp",)):
        self.group_ids = list(group_ids)
        self.group_email = "caller@example.com"
        self.user_role = "admin"
        self.highest_role = "admin"
        self.current_user = None
        self.access_token = token

    @property
    def primary_group_id(self):
        return self.group_ids[0] if self.group_ids else None


def _caller(token="tok-123"):
    return ExternalCaller(
        group_context=_Ctx(token=token),
        protocol="mcp",
        identifier="caller@example.com",
    )


def _hitl_with(pending):
    service = MagicMock()
    service.get_execution_hitl_status = AsyncMock(
        return_value=SimpleNamespace(pending_approvals=pending)
    )
    service.approve = AsyncMock(return_value=True)
    service.reject = AsyncMock(return_value=True)
    return service


def _approval(approval_id=1, prompt="Approve the draft?", output="the draft"):
    return SimpleNamespace(
        id=approval_id,
        prompt=prompt,
        gate_config={},
        previous_crew_output=output,
    )


class TestPending:
    @pytest.mark.asyncio
    async def test_reports_what_the_run_is_waiting_for(self):
        with patch(
            "src.services.hitl.service.HITLService",
            return_value=_hitl_with([_approval()]),
        ):
            pending = await pending_for_run(_caller(), "run-1")

        assert pending == [
            PendingInteraction(
                approval_id=1, prompt="Approve the draft?", context="the draft"
            )
        ]

    @pytest.mark.asyncio
    async def test_a_run_with_no_hitl_is_not_an_error(self):
        """The common case. A status poll must not break because a run has no
        approval gates configured."""
        service = MagicMock()
        service.get_execution_hitl_status = AsyncMock(side_effect=RuntimeError("no"))
        with patch("src.services.hitl.service.HITLService", return_value=service):
            assert await pending_for_run(_caller(), "run-1") == []

    @pytest.mark.asyncio
    async def test_falls_back_to_the_gate_prompt(self):
        approval = SimpleNamespace(
            id=7,
            prompt=None,
            gate_config={"prompt": "Ship it?"},
            previous_crew_output=None,
        )
        with patch(
            "src.services.hitl.service.HITLService",
            return_value=_hitl_with([approval]),
        ):
            pending = await pending_for_run(_caller(), "run-1")
        assert pending[0].prompt == "Ship it?"


class TestRespond:
    @pytest.mark.asyncio
    async def test_free_text_approves_and_is_kept_as_the_comment(self):
        service = _hitl_with([_approval()])
        with patch("src.services.hitl.service.HITLService", return_value=service):
            assert await respond(_caller(), "run-1", "Looks good, proceed.") is True

        service.approve.assert_awaited_once()
        assert service.approve.await_args.kwargs["comment"] == "Looks good, proceed."
        service.reject.assert_not_awaited()

    @pytest.mark.asyncio
    @pytest.mark.parametrize("word", ["no", "No", "reject", "DENY", " declined "])
    async def test_explicit_refusals_reject(self, word):
        service = _hitl_with([_approval()])
        with patch("src.services.hitl.service.HITLService", return_value=service):
            await respond(_caller(), "run-1", word)
        service.reject.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_ambiguous_text_does_not_reject(self):
        """Rejection usually fails the run, so guessing toward it is expensive.
        Only the explicit words reject."""
        service = _hitl_with([_approval()])
        with patch("src.services.hitl.service.HITLService", return_value=service):
            await respond(_caller(), "run-1", "I am not sure, but go ahead")
        service.approve.assert_awaited_once()
        service.reject.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_resumes_on_the_callers_own_token(self):
        """OBO: the resumed work continues under the identity that answered."""
        service = _hitl_with([_approval()])
        with patch("src.services.hitl.service.HITLService", return_value=service):
            await respond(_caller(token="tok-abc"), "run-1", "yes")
        assert service.approve.await_args.kwargs["user_token"] == "tok-abc"

    @pytest.mark.asyncio
    async def test_without_a_token_the_auth_chain_takes_over(self):
        """Not a refusal. The approval still lands; the Databricks auth chain
        resolves credentials exactly as it does for an approval given in the
        UI. Refusing here would make the external surface the only part of
        Kasal that demands OBO."""
        service = _hitl_with([_approval()])
        with patch("src.services.hitl.service.HITLService", return_value=service):
            assert await respond(_caller(token=None), "run-1", "yes") is True
        assert service.approve.await_args.kwargs["user_token"] is None

    @pytest.mark.asyncio
    async def test_single_pending_gate_needs_no_approval_id(self):
        """Forcing an id lookup first turns a one-call answer into three."""
        service = _hitl_with([_approval(approval_id=42)])
        with patch("src.services.hitl.service.HITLService", return_value=service):
            await respond(_caller(), "run-1", "yes")
        assert service.approve.await_args.kwargs["approval_id"] == 42

    @pytest.mark.asyncio
    async def test_several_pending_gates_require_an_explicit_id(self):
        """Guessing which gate was meant would approve the wrong thing."""
        service = _hitl_with([_approval(1), _approval(2)])
        with patch("src.services.hitl.service.HITLService", return_value=service):
            with pytest.raises(ValueError, match="approval_id"):
                await respond(_caller(), "run-1", "yes")

    @pytest.mark.asyncio
    async def test_responding_to_a_run_that_is_not_waiting_is_false(self):
        service = _hitl_with([])
        with patch("src.services.hitl.service.HITLService", return_value=service):
            assert await respond(_caller(), "run-1", "yes") is False
