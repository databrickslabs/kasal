"""How this deployment reaches its decision model: the Jev API, OpenRouter or Databricks.

One system setting (``decision_connection``) picks the connection. The two
third-party connections each have their own URL and their own per-workspace
API key; Databricks has neither:

- ``jev``: TypeSafe's native API. Decisions POST to ``{jev_api_base}/v1/systemone``
  and Auto asks Jev to choose among the workspace's enabled models. Key:
  ``JEV_API_KEY``.
- ``openrouter``: Jev through OpenRouter's System One route
  (``{openrouter_api_base}/systemone``, see ``provider.py``). Auto asks it to
  choose among the workspace's enabled models, exactly as under ``jev``, and
  the chosen model then answers through its own provider. The other policies
  abstain there. Key: ``OPENROUTER_API_KEY``.
- ``databricks``: Jev served by the workspace's own AI Gateway
  (``{workspace_host}/ai-gateway/typesafe/v1/systemone``). No URL and no key:
  the host and bearer come from the workspace's Databricks auth (OBO, then
  PAT, then service principal), resolved per decision in ``credentials.py``.
  The model is the ``databricks_decision_model`` setting, by default
  ``system.ai.openjev-qwen35-4b`` (Databricks-hosted). Every policy is served,
  as under ``jev``: the state stays on Databricks.

Before the setting existed, the only URL was ``jev_api_base``, and a deployment
pointed it at OpenRouter. With no explicit connection, a ``jev_api_base`` on
``openrouter.ai`` therefore reads as the OpenRouter connection with that URL.

Pure reads over the engine-settings snapshot (no DB, no I/O), so the LLM
manager and the crew/flow subprocesses can call it too.
"""

from dataclasses import dataclass
from typing import Mapping, Optional
from urllib.parse import urlparse

from src.services.settings import engine_settings as es

JEV = "jev"
OPENROUTER = "openrouter"
DATABRICKS = "databricks"
CONNECTIONS = (JEV, OPENROUTER, DATABRICKS)

#: The per-workspace API key each keyed connection spends (Configuration → API
#: Keys). Databricks has none: it uses the workspace's Databricks auth.
KEY_NAMES = {JEV: "JEV_API_KEY", OPENROUTER: "OPENROUTER_API_KEY"}

#: OpenRouter's public API, used when no OpenRouter URL is saved.
OPENROUTER_DEFAULT_BASE = "https://openrouter.ai/api/v1"

#: The Databricks-hosted Jev model (``query-systemone-models`` in the Databricks
#: docs), used when no ``databricks_decision_model`` is saved.
DATABRICKS_DEFAULT_MODEL = "system.ai.openjev-qwen35-4b"

#: The catalogue key of the Jev Router model (served as ``typesafe/jev-router``).
#: Never an Auto answer: see ``model_selection.is_router_model``.
JEV_ROUTER_KEY = "jev-router"


@dataclass(frozen=True)
class Connection:
    """The effective connection. ``jev_api_base`` is None when not set."""

    kind: str
    jev_api_base: Optional[str]
    openrouter_api_base: str
    databricks_model: str = DATABRICKS_DEFAULT_MODEL

    @property
    def key_name(self) -> Optional[str]:
        """The workspace API key this connection spends; None for Databricks."""
        return KEY_NAMES.get(self.kind)

    @property
    def configured(self) -> bool:
        """Whether the deployment has what this connection needs.

        Jev needs its URL; OpenRouter always has one (the public default);
        Databricks needs no saved setting, since its host comes with each
        workspace's credential (``credentials.workspace_credential``).
        """
        return self.kind != JEV or self.jev_api_base is not None


def _clean(url: Optional[str]) -> Optional[str]:
    return (url or "").strip().rstrip("/") or None


def _is_openrouter(url: Optional[str]) -> bool:
    host = (urlparse(url or "").hostname or "").lower()
    return host == "openrouter.ai" or host.endswith(".openrouter.ai")


def resolve(stored: Mapping[str, Optional[str]]) -> Connection:
    """The effective connection from stored ``engine_config`` values."""
    kind = (stored.get(es.DECISION_CONNECTION) or "").strip().lower()
    jev = _clean(stored.get(es.JEV_API_BASE))
    openrouter = _clean(stored.get(es.OPENROUTER_API_BASE))
    model = (stored.get(es.DATABRICKS_DECISION_MODEL) or "").strip()
    databricks_model = model or DATABRICKS_DEFAULT_MODEL
    if kind not in CONNECTIONS:
        # Migration: the old single URL pointed at OpenRouter.
        if _is_openrouter(jev):
            return Connection(
                OPENROUTER, None, openrouter or str(jev), databricks_model
            )
        kind = JEV
    return Connection(
        kind, jev, openrouter or OPENROUTER_DEFAULT_BASE, databricks_model
    )


def current() -> Connection:
    """The connection this process has in its engine-settings snapshot."""
    return resolve(
        {
            key: es.value(key)
            for key in (
                es.DECISION_CONNECTION,
                es.JEV_API_BASE,
                es.OPENROUTER_API_BASE,
                es.DATABRICKS_DECISION_MODEL,
            )
        }
    )
