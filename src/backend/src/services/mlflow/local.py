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
    else local server set  -> tracking_uri <that URL>,   <experiment>
    else                   -> tracing off

Three rules this deliberately keeps:

* **An http(s) SERVER, never a file path.** ``main.py`` force-overwrites
  ``MLFLOW_TRACKING_URI`` to ``"databricks"`` at startup precisely so nothing
  scatters ``mlruns/`` directories through the tree. A server URI creates none;
  a ``file://`` store does. Accepting only http(s) keeps that intent instead of
  quietly undoing it.
* **Configured, not launched.** The server is the workspace's
  ``mlflowconfig.local_tracking_uri`` (Configuration → MLflow). It used to be
  the MLFLOW_TRACKING_URI the backend was launched with (plus
  MCP_SERVER_ENABLED), which nothing in the UI could set — and which
  ``main.py``'s "databricks" override hid unless it had been stashed first.
* **Fail soft.** A tracing backend that is unreachable must disable tracing, not
  fail the run — the same rule the A2UI and guardrail paths already follow.
  ``is_reachable`` exists so a dev machine with no server running degrades to
  "no tracing" instead of paying a connect timeout on every crew execution.
"""

import re
from typing import Optional
from urllib.parse import urlparse

from src.core.logger import LoggerManager

logger = LoggerManager.get_instance().system

#: What the Configuration → MLflow form suggests for a local server.
SUGGESTED_LOCAL_URI = "http://127.0.0.1:5555"

#: Seconds to wait when checking the server is actually there. Short on purpose:
#: this runs before every traced execution, and a dev server either answers
#: immediately or is not running.
REACHABILITY_TIMEOUT = 2.0


#: Hostnames that name this machine without resolving anything.
_LOOPBACK_NAMES = frozenset({"localhost"})


def _is_loopback(host: str) -> bool:
    import ipaddress

    if host.lower() in _LOOPBACK_NAMES:
        return True
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


def validate_local_tracking_uri(raw: str) -> str:
    """``raw`` as a safe local MLflow server URL, or ValueError saying why not.

    Traces carry prompts, outputs and tool results, so the destination is a
    security decision, not a formatting one:

    * **https anywhere; plain http only to this machine** (localhost,
      127.0.0.0/8, ::1). Plain http to another host would ship every trace in
      the clear, and an arbitrary http URL is exactly how a workspace member
      redirected traces to a host of their choosing.
    * **No credentials in the URL.** ``user:pass@`` would be stored in the
      database and shown back in the Configuration form.
    """
    value = raw.strip().rstrip("/")
    parsed = urlparse(value)
    if parsed.scheme not in ("http", "https"):
        raise ValueError(
            "The local MLflow server must be an http(s) URL, "
            "e.g. http://127.0.0.1:5555"
        )
    if parsed.username is not None or parsed.password is not None:
        raise ValueError("The local MLflow server URL must not contain credentials")
    try:
        host = parsed.hostname
        _ = parsed.port  # raises ValueError on a malformed port
    except ValueError as exc:
        raise ValueError(f"Invalid local MLflow server URL: {exc}") from exc
    if not host:
        raise ValueError("The local MLflow server URL must name a host")
    if parsed.scheme == "http" and not _is_loopback(host):
        raise ValueError(
            "Plain http is only allowed for a server on this machine "
            "(localhost, 127.0.0.1 or ::1); use https for any other host"
        )
    return value


def local_tracking_uri(configured: Optional[str]) -> Optional[str]:
    """The OSS MLflow server to trace to, from Configuration → MLflow, or None.

    Only a URL :func:`validate_local_tracking_uri` accepts counts (see the
    module docstring); anything else — unset, a file store, a Databricks URI, a
    plain-http remote host saved before that rule existed — means "no local
    server". The same rule is applied on read as on write so a value stored
    before it (or written straight to the database) cannot redirect traces.
    """
    raw = (configured or "").strip()
    if not raw:
        return None
    try:
        return validate_local_tracking_uri(raw)
    except ValueError as exc:
        logger.warning("[mlflow-local] ignoring local tracking URI: %s", exc)
        return None


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
    # From the MLflow configuration only (Configuration → MLflow); the old
    # MLFLOW_CREW_TRACES_EXPERIMENT env override is gone.
    name = (configured or "").strip()
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
        with urllib.request.urlopen(url, timeout=timeout) as response:
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


def prompt_url(base_uri: str, prompt_name: str) -> str:
    """A deep link to a prompt in an OSS MLflow UI (``/#/prompts/<name>``).

    Prompts are registry-wide in the OSS UI, so no experiment id is needed;
    on Databricks they are reached through an experiment's Prompts tab instead.
    """
    from urllib.parse import quote

    return f"{base_uri.rstrip('/')}/#/prompts/{quote(prompt_name, safe='')}"


def ensure_experiment(uri: str, name: str) -> str:
    """Make ``name`` usable on the server at ``uri``; return its experiment id.

    Creates it when missing, and RESTORES it when it was deleted: MLflow refuses
    to set a deleted experiment active or to create a new one with its name, so
    an experiment deleted in the MLflow UI otherwise switched tracing off for
    good ("Cannot set a deleted experiment ... as the active experiment"). It is
    Kasal's own trace destination, the same one enabling tracing creates.

    Uses a client bound to ``uri`` rather than the global tracking URI, so it is
    safe to call from the server process. Blocking: run it in a thread.
    """
    from mlflow.tracking import MlflowClient

    client = MlflowClient(tracking_uri=uri)
    experiment = client.get_experiment_by_name(name)
    if experiment is None:
        experiment_id = str(client.create_experiment(name))
        logger.info("[mlflow-local] created experiment %s (id=%s)", name, experiment_id)
        return experiment_id
    if getattr(experiment, "lifecycle_stage", "") == "deleted":
        client.restore_experiment(experiment.experiment_id)
        logger.warning(
            "[mlflow-local] experiment %s (id=%s) had been deleted; restored it so "
            "tracing can resume",
            name,
            experiment.experiment_id,
        )
    return str(experiment.experiment_id)
