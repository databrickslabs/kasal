"""The A2A client — Kasal calling someone else's agent.

The mirror image of the rest of this package. Everything else here answers A2A
requests; this issues them, against a URL a tenant supplied, so it is held to
the same rules as ``push.py``: SSRF-checked on every request, redirects not
followed, remote bodies treated as untrusted input.

Two things are deliberately NOT abstracted away:

- **The remote's vocabulary is translated at the boundary**, into the same
  ``ExternalTaskState`` the inbound surface uses. A remote's ``TASK_STATE_*``
  never reaches Kasal's logic, so a remote that invents a state cannot make
  anything downstream branch on a string it has never seen.
- **Nothing polls in here.** A remote task is polled by the caller that cares
  (the tool), because the right timeout for a crew delegating work is a
  property of the crew, not of the protocol.
"""

import logging
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin, urlparse

import httpx

from src.services.external.state import ExternalTaskState
from src.utils.url_security import UnsafeUrlError, assert_safe_outbound_url

logger = logging.getLogger(__name__)

#: A card fetch should be fast; a slow one is a misconfiguration, and blocking
#: the UI on it teaches operators that the page is broken.
CARD_TIMEOUT_SECONDS = 15.0

#: Sending a message returns a task handle immediately in a well-behaved
#: implementation. This is not the run's budget — that belongs to the poller.
REQUEST_TIMEOUT_SECONDS = 60.0

WELL_KNOWN_PATH = "/.well-known/agent.json"

#: The wire states, back into Kasal's canonical vocabulary. Unknown maps to
#: WORKING rather than FAILED for the same reason it does inbound: a state this
#: version has not heard of is far more likely to be a newer spec than a dead
#: task, and calling it failed would abandon work that is still running.
_WIRE_TO_STATE = {
    "TASK_STATE_SUBMITTED": ExternalTaskState.SUBMITTED,
    "TASK_STATE_WORKING": ExternalTaskState.WORKING,
    "TASK_STATE_INPUT_REQUIRED": ExternalTaskState.INPUT_REQUIRED,
    "TASK_STATE_AUTH_REQUIRED": ExternalTaskState.AUTH_REQUIRED,
    "TASK_STATE_COMPLETED": ExternalTaskState.COMPLETED,
    "TASK_STATE_FAILED": ExternalTaskState.FAILED,
    "TASK_STATE_CANCELED": ExternalTaskState.CANCELED,
    "TASK_STATE_REJECTED": ExternalTaskState.REJECTED,
}


class RemoteAgentError(RuntimeError):
    """A remote agent could not be reached, or answered unusably.

    One exception type on purpose: to a calling crew, "the URL is blocked",
    "the card is malformed" and "the remote returned 500" are the same event —
    the delegation did not happen — and each needs the same handling.
    """


def from_wire_state(value: Any) -> ExternalTaskState:
    return _WIRE_TO_STATE.get(str(value or ""), ExternalTaskState.WORKING)


def card_url_for(url: str) -> str:
    """Resolve an operator-supplied URL to a card URL.

    Accepts either the card itself or the agent's base URL, because "paste the
    agent's address" is what an operator will actually do, and making them
    remember a well-known path is a support ticket per remote.
    """
    url = (url or "").strip()
    if not url:
        raise RemoteAgentError("No agent URL configured.")
    if url.rstrip("/").endswith(WELL_KNOWN_PATH.rstrip("/")):
        return url
    if url.endswith(".json"):
        return url
    return urljoin(url if url.endswith("/") else url + "/", WELL_KNOWN_PATH.lstrip("/"))


def interface_url_of(card: Dict[str, Any], fallback: str) -> str:
    """Where to send messages, per the card.

    A card that names no interface falls back to the URL the card was fetched
    from with the well-known suffix removed — which is what a card served at
    the root of its own agent means.
    """
    for interface in card.get("interfaces") or []:
        url = (interface or {}).get("url")
        if url:
            return url.rstrip("/")

    parsed = urlparse(fallback)
    base = f"{parsed.scheme}://{parsed.netloc}"
    return f"{base}/a2a/v1"


def _headers(api_key: Optional[str], token: Optional[str]) -> Dict[str, str]:
    """Auth for one outbound call.

    A configured API key wins over the caller's token: it was set deliberately
    for THIS remote, whereas the OBO token is ambient. Sending a user's
    Databricks token to a remote that expects its own key would leak the token
    to a third party for no benefit.
    """
    headers = {"Content-Type": "application/json", "Accept": "application/json"}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
    elif token:
        headers["Authorization"] = f"Bearer {token}"
        # Databricks Apps' own header, so a Kasal-to-Kasal hop identifies the
        # end user rather than the calling workspace.
        headers["X-Forwarded-Access-Token"] = token
    return headers


async def _request(
    method: str,
    url: str,
    *,
    api_key: Optional[str] = None,
    token: Optional[str] = None,
    json_body: Optional[Dict[str, Any]] = None,
    timeout: float = REQUEST_TIMEOUT_SECONDS,
) -> Dict[str, Any]:
    """One SSRF-checked request to a remote agent.

    The check runs per request, not once at configuration time: DNS can change
    between the two, which is exactly what rebinding is.
    """
    try:
        await assert_safe_outbound_url(url)
    except UnsafeUrlError as exc:
        raise RemoteAgentError(f"Refusing to call {url}: {exc}") from exc

    try:
        async with httpx.AsyncClient(timeout=timeout, follow_redirects=False) as client:
            response = await client.request(
                method, url, headers=_headers(api_key, token), json=json_body
            )
    except Exception as exc:  # noqa: BLE001 — httpx raises a family, all the same here
        raise RemoteAgentError(f"Could not reach {url}: {exc}") from exc

    if not response.is_success:
        # The remote's body is not echoed: it is an untrusted response to a
        # server-side request, and it lands in logs and LLM context.
        raise RemoteAgentError(f"{url} answered HTTP {response.status_code}")

    try:
        payload = response.json()
    except Exception as exc:  # noqa: BLE001
        raise RemoteAgentError(f"{url} did not answer JSON") from exc

    if not isinstance(payload, dict):
        raise RemoteAgentError(
            f"{url} answered {type(payload).__name__}, not an object"
        )
    return payload


async def fetch_card(
    url: str, *, api_key: Optional[str] = None, token: Optional[str] = None
) -> Dict[str, Any]:
    """The remote's Agent Card."""
    return await _request(
        "GET",
        card_url_for(url),
        api_key=api_key,
        token=token,
        timeout=CARD_TIMEOUT_SECONDS,
    )


def skills_of(card: Dict[str, Any]) -> List[Dict[str, Any]]:
    """The card's skills, defensively.

    A card is remote input. Anything malformed is dropped rather than raised on:
    one bad skill entry must not make an otherwise usable agent unusable.
    """
    out = []
    for skill in card.get("skills") or []:
        if not isinstance(skill, dict):
            continue
        skill_id = skill.get("id") or skill.get("name")
        if not skill_id:
            continue
        out.append(
            {
                "id": str(skill_id),
                "name": str(skill.get("name") or skill_id),
                "description": str(skill.get("description") or ""),
            }
        )
    return out


async def send_message(
    interface_url: str,
    text: str,
    *,
    skill_id: Optional[str] = None,
    task_id: Optional[str] = None,
    api_key: Optional[str] = None,
    token: Optional[str] = None,
    timeout: float = REQUEST_TIMEOUT_SECONDS,
) -> Dict[str, Any]:
    """Start a remote task, or answer one that is waiting on input."""
    body: Dict[str, Any] = {
        "message": {"role": "user", "parts": [{"kind": "text", "text": text}]}
    }
    if skill_id:
        body["skillId"] = skill_id
    if task_id:
        body["taskId"] = task_id
    return await _request(
        "POST",
        f"{interface_url.rstrip('/')}/message:send",
        api_key=api_key,
        token=token,
        json_body=body,
        timeout=timeout,
    )


async def get_task(
    interface_url: str,
    task_id: str,
    *,
    api_key: Optional[str] = None,
    token: Optional[str] = None,
) -> Dict[str, Any]:
    return await _request(
        "GET",
        f"{interface_url.rstrip('/')}/tasks/{task_id}",
        api_key=api_key,
        token=token,
    )


async def cancel_task(
    interface_url: str,
    task_id: str,
    *,
    api_key: Optional[str] = None,
    token: Optional[str] = None,
) -> Dict[str, Any]:
    return await _request(
        "POST",
        f"{interface_url.rstrip('/')}/tasks/{task_id}:cancel",
        api_key=api_key,
        token=token,
    )


def text_of_task(task: Dict[str, Any]) -> str:
    """Everything readable a remote task carries.

    Artifacts first, then the status message. A caller wants the answer; the
    status message is what a paused or failed task has instead of one.
    """
    chunks: List[str] = []
    for artifact in task.get("artifacts") or []:
        for part in (artifact or {}).get("parts") or []:
            chunks.extend(_text_of_part(part))

    message = (task.get("status") or {}).get("message") or {}
    for part in message.get("parts") or []:
        chunks.extend(_text_of_part(part))

    return "\n".join(c for c in chunks if c).strip()


def _text_of_part(part: Any) -> List[str]:
    if not isinstance(part, dict):
        return []
    if part.get("kind") == "text" and part.get("text"):
        return [str(part["text"])]
    if part.get("kind") == "data" and part.get("data") is not None:
        import json

        return [json.dumps(part["data"], default=str)]
    if part.get("kind") == "url" and part.get("url"):
        return [str(part["url"])]
    return []
