"""Fail-soft gateway. A disabled workspace never calls the provider."""

import asyncio
import json
import logging
from time import monotonic
from typing import Any

from src.services.decisions import provider
from src.services.decisions.contracts import Choice, choices_from_response

logger = logging.getLogger(__name__)

#: The provider's payload budget, per ``state`` and per ``questions``, in bytes
#: of ``json.dumps`` output (ASCII-escaped, so a non-ASCII character costs 6-12).
MAX_PAYLOAD_BYTES = 20000

#: Why a decision was not made. Short codes; callers that show them map them to
#: words (``model_selection.FALLBACK_REASONS``, the chat's i18n keys).
NO_WORKSPACE = "no_workspace"
TOO_LARGE = "too_large"
NOT_CONFIGURED = "not_configured"
NO_KEY = "no_key"
TIMEOUT = "timeout"
UNREACHABLE = "unreachable"
PROVIDER_ERROR = "provider_error"
ABSTAINED = "abstained"


def encoded_size(value: object) -> int:
    """``value``'s size as the payload budget counts it."""
    return len(json.dumps(value).encode())


def workspace_id(group_id: Any = None) -> str | None:
    if isinstance(group_id, str) and group_id:
        return group_id
    from src.utils.user_context import UserContext

    context = UserContext.get_group_context()
    candidate = context.primary_group_id if context else None
    return candidate if isinstance(candidate, str) and candidate else None


async def decide(
    policy: str, state: dict, questions: dict, *, group_id: str | None = None
) -> dict[str, Choice] | None:
    answers, _ = await decide_with_reason(policy, state, questions, group_id=group_id)
    return answers


async def decide_with_reason(
    policy: str, state: dict, questions: dict, *, group_id: str | None = None
) -> tuple[dict[str, Choice] | None, str | None]:
    """``decide``, plus why it abstained: ``(answers, None)`` or ``(None, reason)``."""
    if not questions:
        return None, ABSTAINED
    started = monotonic()
    status = "fallback"
    model = provider.model_for()
    key = None
    try:
        group_id = workspace_id(group_id)
        if not group_id:
            return None, NO_WORKSPACE
        # Abstain instead of truncating evidence to fit the provider budget.
        if (
            encoded_size(state) > MAX_PAYLOAD_BYTES
            or encoded_size(questions) > MAX_PAYLOAD_BYTES
        ):
            return None, TOO_LARGE
        if len(questions) > 64 or any(
            not 2 <= len(q["criteria"]) <= 255 for q in questions.values()
        ):
            return None, TOO_LARGE
        # No endpoint configured for this deployment: off, silently.
        if not provider.is_configured(policy):
            return None, NOT_CONFIGURED
        async with asyncio.timeout(6):
            from src.services.decisions.credentials import decision_credential

            key = await decision_credential(group_id)
            if not key:
                return None, NO_KEY
            payload = await provider.evaluate(key, state, questions, policy)
            answers = choices_from_response(payload, questions)
            status = (
                "accepted" if all(a.accepted for a in answers.values()) else "uncertain"
            )
            if status == "accepted":
                return answers, None
            return None, ABSTAINED
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        # Do not expose headers, credentials, request bodies or provider echoes.
        name = type(exc).__name__
        logger.warning("Jev %s fell back (%s)", policy, name)
        # By name: the budget's TimeoutError, httpx's *Timeout and *ConnectError.
        if isinstance(exc, TimeoutError) or "Timeout" in name:
            return None, TIMEOUT
        return None, UNREACHABLE if "Connect" in name else PROVIDER_ERROR
    finally:
        # Off is silent: only actual provider attempts produce a decision event.
        if key:
            from src.services.decisions.telemetry import record_decision

            record_decision(policy, model, status, (monotonic() - started) * 1000)


def decide_sync(
    policy: str, state: dict, questions: dict, *, group_id: str | None = None
) -> dict[str, Choice] | None:
    """Worker-thread callers only; never block an active asyncio event loop."""
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(decide(policy, state, questions, group_id=group_id))
    return None
