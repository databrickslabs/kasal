"""Bounded Jev HTTP transport, independent of storage and decision policy.

Both connections speak TypeSafe's System One shapes (``model``, ``state``,
``questions`` in; ``answers`` out):

- Jev API: ``POST {jev_api_base}/v1/systemone`` with model ``jev-1.13.0``.
- OpenRouter: ``POST {openrouter_api_base}/systemone`` with model
  ``typesafe/jev-1.13``. OpenRouter documents this route as compatible with
  TypeSafe's SDKs (see "Under the OpenRouter connection" in
  ``src/docs/DECISION_MODEL.md``). Jev returns a typed choice there too, never
  text, so nothing but the decision comes back.

Under OpenRouter only Auto (``model_selection``) asks: the other policies stay
off there, exactly as before, until someone decides to send their state to
OpenRouter as well.
"""

from typing import Any, Optional

import httpx

from src.services.decisions.connection import JEV
from src.services.decisions.connection import current as current_connection

MODEL = "jev-1.13.0"
#: Jev as OpenRouter names it (``~typesafe/jev-latest`` tracks the newest).
OPENROUTER_MODEL = "typesafe/jev-1.13"
#: The policies the OpenRouter connection serves.
OPENROUTER_POLICIES = frozenset({"model_selection"})


def api_base(policy: Optional[str] = None) -> str | None:
    """The URL decisions for ``policy`` go to, or None when they do not go anywhere.

    Jev API: the URL a system admin set in System administration → Models.
    Deployment-owned configuration, never supplied by a prompt or tool result.
    There is deliberately no built-in default: a deployment that has not
    configured an endpoint does not call one.

    OpenRouter: its URL (the public API by default), for Auto only. Every other
    policy abstains there exactly as when the feature is off.
    """
    connection = current_connection()
    if connection.kind == JEV:
        return connection.jev_api_base
    return connection.openrouter_api_base if policy in OPENROUTER_POLICIES else None


def is_configured(policy: Optional[str] = None) -> bool:
    return api_base(policy) is not None


def model_for() -> str:
    """The Jev model id the current connection expects."""
    return MODEL if current_connection().kind == JEV else OPENROUTER_MODEL


def endpoint(base: str) -> str:
    """The System One route under ``base`` for the current connection."""
    return (
        f"{base}/v1/systemone"
        if current_connection().kind == JEV
        else f"{base}/systemone"
    )


async def evaluate(
    api_key: str, state: dict, questions: dict, policy: Optional[str] = None
) -> dict:
    base = api_base(policy)
    if base is None:
        raise ValueError(
            "The Jev API URL is not configured (System administration → Models)"
        )
    if not base.startswith(("https://", "http://")):
        raise ValueError("The Jev API URL must be an http:// or https:// address")
    async with httpx.AsyncClient(timeout=5.0, follow_redirects=False) as client:
        response = await client.post(
            endpoint(base),
            headers={"Authorization": f"Bearer {api_key}"},
            json={"model": model_for(), "state": state, "questions": questions},
        )
        response.raise_for_status()
        result: dict[Any, Any] = response.json()
        return result
