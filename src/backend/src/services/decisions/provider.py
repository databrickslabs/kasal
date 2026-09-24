"""Bounded Jev HTTP transport, independent of storage and decision policy."""

import os

import httpx

MODEL = "jev-1.13.0"


async def evaluate(api_key: str, state: dict, questions: dict) -> dict:
    # Deployment-owned endpoint, never supplied by a prompt or tool result.
    base = os.environ.get("JEV_API_BASE", "https://api.typesafe.ai").rstrip("/")
    if not base.startswith("https://"):
        raise ValueError("JEV_API_BASE must use HTTPS")
    async with httpx.AsyncClient(timeout=5.0, follow_redirects=False) as client:
        response = await client.post(
            f"{base}/v1/systemone",
            headers={"Authorization": f"Bearer {api_key}"},
            json={"model": MODEL, "state": state, "questions": questions},
        )
        response.raise_for_status()
        return response.json()
