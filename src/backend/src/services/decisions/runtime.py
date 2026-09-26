"""Fail-soft gateway. A disabled workspace never calls the provider."""

import asyncio
import json
import logging
from time import monotonic

from src.services.decisions import provider
from src.services.decisions.contracts import Choice, choices_from_response

logger = logging.getLogger(__name__)


def workspace_id(group_id=None):
    if isinstance(group_id, str) and group_id:
        return group_id
    from src.utils.user_context import UserContext

    context = UserContext.get_group_context()
    candidate = context.primary_group_id if context else None
    return candidate if isinstance(candidate, str) and candidate else None


async def decide(
    policy: str, state: dict, questions: dict, *, group_id: str | None = None
) -> dict[str, Choice] | None:
    if not questions:
        return None
    started = monotonic()
    status = "fallback"
    model = provider.MODEL
    key = None
    try:
        group_id = workspace_id(group_id)
        if not group_id:
            return None
        # Abstain instead of truncating evidence to fit the provider budget.
        if (
            len(json.dumps(state).encode()) > 20000
            or len(json.dumps(questions).encode()) > 20000
        ):
            return None
        if len(questions) > 64 or any(
            not 2 <= len(q["criteria"]) <= 255 for q in questions.values()
        ):
            return None
        # No endpoint configured for this deployment: off, silently.
        if not provider.is_configured():
            return None
        async with asyncio.timeout(6):
            from src.services.decisions.credentials import decision_credential

            key = await decision_credential(group_id)
            if not key:
                return None
            payload = await provider.evaluate(key, state, questions)
            answers = choices_from_response(payload, questions)
            status = (
                "accepted" if all(a.accepted for a in answers.values()) else "uncertain"
            )
            return answers if status == "accepted" else None
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        # Do not expose headers, credentials, request bodies or provider echoes.
        logger.warning("Jev %s fell back (%s)", policy, type(exc).__name__)
        return None
    finally:
        # Off is silent: only actual provider attempts produce a decision event.
        if key:
            from src.services.decisions.telemetry import record_decision

            record_decision(policy, model, status, (monotonic() - started) * 1000)


def decide_sync(policy, state, questions, *, group_id=None):
    """Worker-thread callers only; never block an active asyncio event loop."""
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(decide(policy, state, questions, group_id=group_id))
    return None
