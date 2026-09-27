"""Bounded Jev HTTP transport, independent of storage and decision policy."""

from typing import Any

import httpx

from src.services.decisions.connection import JEV
from src.services.decisions.connection import current as current_connection

MODEL = "jev-1.13.0"


def api_base() -> str | None:
    """The Jev API URL a system admin set in System administration → Models, or None.

    Deployment-owned configuration, never supplied by a prompt or tool result
    (it replaced the JEV_API_BASE env var). There is deliberately no built-in
    default: a deployment that has not configured an endpoint does not call one.

    None under the OpenRouter connection: OpenRouter has no native decision
    endpoint, so every policy abstains there exactly as when the feature is off.
    """
    connection = current_connection()
    return connection.jev_api_base if connection.kind == JEV else None


def is_configured() -> bool:
    return api_base() is not None


async def evaluate(api_key: str, state: dict, questions: dict) -> dict:
    base = api_base()
    if base is None:
        raise ValueError(
            "The Jev API URL is not configured (System administration → Models)"
        )
    if not base.startswith(("https://", "http://")):
        raise ValueError("The Jev API URL must be an http:// or https:// address")
    async with httpx.AsyncClient(timeout=5.0, follow_redirects=False) as client:
        response = await client.post(
            f"{base}/v1/systemone",
            headers={"Authorization": f"Bearer {api_key}"},
            json={"model": MODEL, "state": state, "questions": questions},
        )
        response.raise_for_status()
        result: dict[Any, Any] = response.json()
        return result
