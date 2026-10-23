"""Bounded Jev HTTP transport, independent of storage and decision policy.

Every connection speaks TypeSafe's System One shapes (``model``, ``state``,
``questions`` in; ``answers`` out):

- Jev API: ``POST {jev_api_base}/v1/systemone`` with model ``jev-1.13.0``.
- OpenRouter: ``POST {openrouter_api_base}/systemone`` with model
  ``typesafe/jev-1.13``. OpenRouter documents this route as compatible with
  TypeSafe's SDKs (see "Under the OpenRouter connection" in
  ``src/docs/DECISION_MODEL.md``). Jev returns a typed choice there too, never
  text, so nothing but the decision comes back.
- Databricks: ``POST {workspace_host}/ai-gateway/typesafe/v1/systemone`` with
  the ``databricks_decision_model`` setting (``system.ai.openjev-qwen35-4b`` by
  default). The host and bearer arrive together as a :class:`WorkspaceCredential`
  (``credentials.workspace_credential``), never from a saved URL.

Under OpenRouter only Auto (``model_selection``) asks: the other policies stay
off there, exactly as before, until someone decides to send their state to
OpenRouter as well. Databricks serves every policy, as the Jev API does.
"""

from dataclasses import dataclass
from typing import Any, Optional, Union

import httpx

from src.services.decisions.connection import DATABRICKS, JEV, OPENROUTER
from src.services.decisions.connection import current as current_connection

MODEL = "jev-1.13.0"
#: Jev as OpenRouter names it (``~typesafe/jev-latest`` tracks the newest).
OPENROUTER_MODEL = "typesafe/jev-1.13"
#: The policies the OpenRouter connection serves.
OPENROUTER_POLICIES = frozenset({"model_selection"})
#: The System One route on a Databricks workspace's AI Gateway.
DATABRICKS_PATH = "/ai-gateway/typesafe/v1/systemone"


@dataclass(frozen=True)
class WorkspaceCredential:
    """The Databricks connection's bearer and the workspace host it is for.

    They are resolved together (OBO, PAT or service principal) so a token is
    never sent to a host it was not issued for. ``repr`` hides the token.
    """

    token: str
    host: str

    def __repr__(self) -> str:
        return f"WorkspaceCredential(host={self.host!r})"


#: What ``evaluate`` accepts: a workspace API key (Jev API, OpenRouter) or the
#: Databricks connection's credential.
Credential = Union[str, WorkspaceCredential]


def api_base(policy: Optional[str] = None) -> str | None:
    """The saved URL decisions for ``policy`` go to, or None when there is none.

    Jev API: the URL a system admin set in System administration → Models.
    Deployment-owned configuration, never supplied by a prompt or tool result.
    There is deliberately no built-in default: a deployment that has not
    configured an endpoint does not call one.

    OpenRouter: its URL (the public API by default), for Auto only. Every other
    policy abstains there exactly as when the feature is off.

    Databricks: None. Its host is the workspace's own and comes with the
    credential (:class:`WorkspaceCredential`), so nothing is saved for it.
    """
    connection = current_connection()
    if connection.kind == JEV:
        return connection.jev_api_base
    if connection.kind == OPENROUTER and policy in OPENROUTER_POLICIES:
        return connection.openrouter_api_base
    return None


def is_configured(policy: Optional[str] = None) -> bool:
    """Whether the deployment's connection serves ``policy`` at all.

    Databricks serves every policy; whether a workspace's host and credential
    resolve is decided per decision, by ``credentials.decision_credential``.
    """
    if current_connection().kind == DATABRICKS:
        return True
    return api_base(policy) is not None


def model_for() -> str:
    """The Jev model id the current connection expects."""
    connection = current_connection()
    if connection.kind == JEV:
        return MODEL
    if connection.kind == DATABRICKS:
        return connection.databricks_model
    return OPENROUTER_MODEL


def endpoint(base: str) -> str:
    """The System One route under ``base`` for the current connection."""
    kind = current_connection().kind
    if kind == JEV:
        return f"{base}/v1/systemone"
    if kind == DATABRICKS:
        return f"{base}{DATABRICKS_PATH}"
    return f"{base}/systemone"


def _target(credential: Credential, policy: Optional[str]) -> tuple[str, str, dict]:
    """``(url base, bearer, extra headers)`` for this connection and credential."""
    if current_connection().kind == DATABRICKS:
        if not isinstance(credential, WorkspaceCredential):
            raise ValueError("The Databricks connection needs a workspace credential")
        from src.utils.telemetry import KasalProduct, get_user_agent_header

        base = credential.host.rstrip("/")
        return base, credential.token, get_user_agent_header(KasalProduct.DECISIONS)
    if isinstance(credential, WorkspaceCredential):
        raise ValueError("A workspace credential is only valid for Databricks")
    base_url = api_base(policy)
    if base_url is None:
        raise ValueError(
            "The Jev API URL is not configured (System administration → Models)"
        )
    return base_url, credential, {}


async def evaluate(
    credential: Credential, state: dict, questions: dict, policy: Optional[str] = None
) -> dict:
    base, token, headers = _target(credential, policy)
    if not base.startswith(("https://", "http://")):
        raise ValueError("The Jev API URL must be an http:// or https:// address")
    async with httpx.AsyncClient(timeout=5.0, follow_redirects=False) as client:
        response = await client.post(
            endpoint(base),
            headers={**headers, "Authorization": f"Bearer {token}"},
            json={"model": model_for(), "state": state, "questions": questions},
        )
        response.raise_for_status()
        result: dict[Any, Any] = response.json()
        return result
