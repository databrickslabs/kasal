"""Bounded Jev HTTP transport, independent of storage and decision policy."""

from typing import Any

import httpx

from src.config.settings import settings

MODEL = "jev-1.13.0"


def api_base() -> str | None:
    """The configured Jev base URL (``JEV_API_BASE``), or None when unset.

    Deployment-owned configuration, never supplied by a prompt or tool result.
    There is deliberately no built-in default: a deployment that has not
    configured an endpoint does not call one.
    """
    base = (settings.JEV_API_BASE or "").strip().rstrip("/")
    return base or None


def is_configured() -> bool:
    return api_base() is not None


async def evaluate(api_key: str, state: dict, questions: dict) -> dict:
    base = api_base()
    if base is None:
        raise ValueError("JEV_API_BASE is not configured")
    if not base.startswith("https://"):
        raise ValueError("JEV_API_BASE must use HTTPS")
    async with httpx.AsyncClient(timeout=5.0, follow_redirects=False) as client:
        response = await client.post(
            f"{base}/v1/systemone",
            headers={"Authorization": f"Bearer {api_key}"},
            json={"model": MODEL, "state": state, "questions": questions},
        )
        response.raise_for_status()
        result: dict[Any, Any] = response.json()
        return result
