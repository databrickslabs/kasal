"""Local (OSS) MLflow — the dev-mode backend for tracing.

Kasal's MLflow integration was Databricks-only end to end: the enable flag lives
on the Databricks config row, ``mlflow_setup`` demands SPN/PAT credentials before
it will configure anything, and both the tracking URI and the deep link are
hardcoded to the workspace. With no Databricks configured — the normal dev
state — MLflow could not be switched on at all, by construction rather than by
choice.

This module is the other backend. Which one a run uses is DERIVED, never
configured by hand:

    Databricks configured  -> tracking_uri "databricks", /Shared/<experiment>
    else local server      -> tracking_uri <this>,       <experiment>
    else                   -> tracing off

Three rules this deliberately keeps:

* **An http(s) SERVER, never a file path.** ``main.py`` force-overwrites
  ``MLFLOW_TRACKING_URI`` to ``"databricks"`` at startup precisely so nothing
  scatters ``mlruns/`` directories through the tree. A server URI creates none;
  a ``file://`` store does. Accepting only http(s) keeps that intent instead of
  quietly undoing it.
* **The launch value is the source of truth.** Because of that same override,
  the live ``MLFLOW_TRACKING_URI`` always reads "databricks" and cannot answer
  "is this a local run?" — only ``KASAL_LAUNCH_MLFLOW_TRACKING_URI``, which
  ``main.py`` stashes before overwriting, can.
* **Fail soft.** A tracing backend that is unreachable must disable tracing, not
  fail the run — the same rule the A2UI and guardrail paths already follow.
  ``is_reachable`` exists so a dev machine with no server running degrades to
  "no tracing" instead of paying a connect timeout on every crew execution.
"""

import os
import re
from typing import Optional
from urllib.parse import urlparse

from src.core.logger import LoggerManager

logger = LoggerManager.get_instance().system

#: Where a dev machine's MLflow server is expected. Used when nothing else says
#: otherwise AND no Databricks workspace is configured, so a deployed app cannot
#: be redirected here by accident.
DEFAULT_LOCAL_URI = "http://127.0.0.1:5555"

#: Seconds to wait when checking the server is actually there. Short on purpose:
#: this runs before every traced execution, and a dev server either answers
#: immediately or is not running.
REACHABILITY_TIMEOUT = 2.0


def local_tracking_uri() -> Optional[str]:
    """The OSS MLflow server to trace to, or None if this is not a local setup.

    Reads the value the process was LAUNCHED with (see the module docstring for
    why the live env var cannot be trusted), falling back to the dev default.
    Returns None for a Databricks-schemed value or anything that is not an
    http(s) URL.
    """
    raw = (
        os.getenv("KASAL_LAUNCH_MLFLOW_TRACKING_URI")
        or os.getenv("MLFLOW_TRACKING_URI")
        or ""
    ).strip()

    if raw.startswith("databricks"):
        # An explicit Databricks tracking URI is not a local setup.
        return None
    if raw.startswith(("http://", "https://")):
        return raw.rstrip("/")
    if raw:
        # A file store or some other scheme: not something with a UI to link to,
        # and not something we want creating directories.
        logger.debug("[mlflow-local] ignoring non-http tracking URI %r", raw)
        return None
    return DEFAULT_LOCAL_URI


def experiment_slug(teamspace: Optional[str]) -> str:
    """``kasal-<teamspace>-traces``, the default experiment for a workspace.

    One experiment per teamspace rather than one global
    ``kasal-crew-execution-traces``: an MLflow server is commonly shared, and a
    single experiment collecting every teamspace's traces makes the one you care
    about impossible to find. The name is also what a person reads in the MLflow
    UI, so it carries the teamspace rather than an internal id.

    Falls back to ``kasal-traces`` when there is no teamspace to name — a
    slugless default beats a name with an empty segment in it.
    """
    slug = re.sub(r"[^a-z0-9]+", "-", (teamspace or "").strip().lower()).strip("-")
    return f"kasal-{slug}-traces" if slug else "kasal-traces"


def local_experiment_name(
    configured: Optional[str] = None, teamspace: Optional[str] = None
) -> str:
    """The experiment name to use on an OSS server.

    An explicitly configured name always wins — it is a decision. Otherwise the
    per-teamspace default applies.

    ``/Shared/kasal-…`` is a Databricks WORKSPACE PATH. On an OSS server it would
    become an experiment literally named "/Shared/…", which works but reads as a
    mistake, so the workspace prefix is stripped.
    """
    name = (configured or os.getenv("MLFLOW_CREW_TRACES_EXPERIMENT") or "").strip()
    if not name:
        return experiment_slug(teamspace)
    if name.startswith("/Shared/"):
        name = name[len("/Shared/") :]
    return name.strip("/") or experiment_slug(teamspace)


def is_reachable(uri: str, timeout: float = REACHABILITY_TIMEOUT) -> bool:
    """Whether an MLflow server is actually answering at ``uri``.

    A TCP connect rather than an HTTP request: it is the cheapest thing that
    distinguishes "server running" from "nothing listening", and it cannot be
    confused by an auth redirect or a slow health endpoint.
    """
    import socket

    parsed = urlparse(uri)
    host = parsed.hostname
    if not host:
        return False
    port = parsed.port or (443 if parsed.scheme == "https" else 80)
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError as exc:
        logger.info(
            "[mlflow-local] no MLflow server at %s (%s); tracing stays off",
            uri,
            exc.__class__.__name__,
        )
        return False


def experiment_id(uri: str, name: str, timeout: float = REACHABILITY_TIMEOUT) -> str:
    """The numeric id of ``name`` on the server at ``uri``, or "" if absent.

    A direct REST call rather than the ``mlflow`` client on purpose: ``main.py``
    force-sets ``MLFLOW_TRACKING_URI`` to "databricks" process-wide, so using the
    client here would mean mutating global state and restoring it — the pattern
    that already required careful env save/restore dances elsewhere in this
    service. One GET has no such side effects.
    """
    import json
    import urllib.error
    import urllib.parse
    import urllib.request

    query = urllib.parse.urlencode({"experiment_name": name})
    url = f"{uri.rstrip('/')}/api/2.0/mlflow/experiments/get-by-name?{query}"
    try:
        with urllib.request.urlopen(url, timeout=timeout) as response:  # noqa: S310
            payload = json.loads(response.read().decode("utf-8"))
        return str(payload.get("experiment", {}).get("experiment_id", "") or "")
    except urllib.error.HTTPError as exc:
        if exc.code == 404:
            # Not created yet — the traces tab for the whole server is still a
            # more useful destination than nothing.
            logger.info("[mlflow-local] experiment %r does not exist yet", name)
        else:
            logger.warning("[mlflow-local] experiment lookup failed: %s", exc)
        return ""
    except Exception as exc:  # noqa: BLE001 — a deep link must never raise
        logger.warning("[mlflow-local] experiment lookup failed: %s", exc)
        return ""


def traces_url(
    base_uri: str, experiment_id: str, trace_id: Optional[str] = None
) -> str:
    """A deep link into an OSS MLflow UI.

    The OSS UI is a HASH router — ``/#/experiments/<id>/traces`` — which is the
    only structural difference from the Databricks path
    (``/ml/experiments/<id>/traces``). It is the same MLflow UI underneath, so
    the trace-selection query parameter is identical.
    """
    base = base_uri.rstrip("/")
    if not experiment_id:
        return f"{base}/#/experiments"
    url = f"{base}/#/experiments/{experiment_id}/traces"
    if trace_id:
        url += f"?selectedEvaluationId={trace_id}"
    return url
