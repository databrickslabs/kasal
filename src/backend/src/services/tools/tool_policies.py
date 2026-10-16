"""Per-tool POLICY that rides on a tool's teamspace config.

A policy is something the engine does *around* a tool call — pause it for a
human, serve it from a recording — as opposed to something the tool needs to
run. That distinction is why these live here rather than in the tool's
constructor: ``tool_class(**tool_config)`` gets the API keys and endpoints, and
a policy key reaching it is a ``TypeError`` on some tools and a silently
ignored kwarg on others.

So the config carries them, ``extract_tool_policies`` POPS them on the way past,
and ``stamp_tool_policies`` puts them back on the built instance under a private
attribute. The engine hooks then read one attribute regardless of which of the
factory's many construction branches built the tool.

Two policies today, deliberately the same shape:

``requires_approval: true`` (+ optional ``approval: {...}``)
    Every use pauses until a human approves it. Read by
    ``execution/kernel/tool_approval.py``.

``replayable: true`` (+ optional ``replay: {...}``)
    The cassette. This tool's result may be served from a RECORDING of an
    earlier identical call instead of calling out again — the VCR pattern, for
    the case where re-running a workload to test the steps after it should not
    re-pay for the search that starts it.

    Opt-in per tool because the choice is not the engine's to make: it is
    knowing that this tool is a read whose answer does not have to be fresh.
    Never turn it on for a tool that writes, that is gated on approval, or
    whose answer is scoped to the caller's identity rather than the workspace.

Both are the TEAMSPACE's decision. A per-agent, per-task or run ``tool_configs``
entry is merged over the teamspace tool config by ``merge_tool_config_override``,
which keeps policy, identity, endpoint and naming keys out of it (see
``RESERVED_OVERRIDE_KEYS``) and keeps a teamspace credential from following an
endpoint the override moved. The one policy change an override may make is to
add an approval gate the teamspace does not have.

Extracted from ``tool_factory.py`` (2,700 lines, over the ceiling) rather than
added to it, per the file-size rule in CLAUDE.md.
"""

import logging
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlsplit

from src.utils.coercion import positive_int

logger = logging.getLogger(__name__)

#: How long a recording stays usable, when the policy does not say. An hour is
#: short enough that "latest news" is still roughly latest, and long enough to
#: cover a morning of re-running the same crew against the same question.
DEFAULT_REPLAY_TTL_SECONDS = 3600

#: Whose recordings a call may be served from. ``group`` — any earlier run in
#: the same workspace, which is the point (yesterday's run pays, today's does
#: not). ``run`` — only earlier calls in the SAME execution, for a tool where
#: even a workspace-mate's recording would be wrong.
DEFAULT_REPLAY_SCOPE = "group"

#: The attribute each policy is stamped under. Private by convention: nothing
#: outside the engine hooks should be reading them.
APPROVAL_ATTR = "_approval_policy"
REPLAY_ATTR = "_replay_policy"

#: A tool's config, or one policy's options: JSON-shaped, keyed by name.
ToolConfig = Dict[str, Any]
#: Stamped attribute name (``APPROVAL_ATTR`` / ``REPLAY_ATTR``) -> its options.
ToolPolicies = Dict[str, ToolConfig]

#: Keys a per-agent, per-task or run ``tool_configs`` entry may not set. The
#: factory takes identity from the run, and the teamspace config decides
#: endpoints and policies. A denylist, not an allowlist: the task forms write
#: tool-specific ids, secrets and hosts (Power BI, converters) into these
#: entries, and saved workflows depend on them.
_IDENTITY_KEYS = frozenset(
    {"group_id", "group_ids", "user_email", "user_token", "execution_id"}
)
#: Where a teamspace provider key is sent (Serper, Image Generation).
_ENDPOINT_KEYS = frozenset({"base_url"})
_POLICY_KEYS = frozenset({"requires_approval", "approval", "replayable", "replay"})
#: What the model and the approval prompt know the tool by. An approval
#: decision is remembered per tool name for the run.
_NAMING_KEYS = frozenset({"name", "description"})
RESERVED_OVERRIDE_KEYS = _IDENTITY_KEYS | _ENDPOINT_KEYS | _POLICY_KEYS | _NAMING_KEYS

#: An endpoint an override may set, and the credentials sent to it. When the
#: override moves the endpoint to another host without bringing its own
#: credential, the teamspace's stays behind: the tool then uses the run's own
#: credential for its own workspace (``workspace_llm_credentials``) or none.
#: The scrape tool's ``cookies`` and ``headers`` are set in the same teamspace
#: block as its ``website_url``, so they belong to that site.
_ENDPOINT_CREDENTIALS: Dict[str, Tuple[str, ...]] = {
    "llm_workspace_url": ("llm_token",),
    "databricks_sql_endpoint": ("databricks_pat",),
    "website_url": ("cookies", "headers"),
}


def extract_tool_policies(tool_config: ToolConfig) -> ToolPolicies:
    """Pop every policy key out of ``tool_config`` and return the policies.

    Mutates ``tool_config`` — that is the job. What is left is safe to splat
    into a tool constructor.

    Returns a dict of attribute name -> policy dict, ready for
    ``stamp_tool_policies``. A policy the config does not ask for is absent
    rather than empty, so "off" and "on with all defaults" stay distinguishable.
    """
    policies: ToolPolicies = {}

    approval = _pop_policy(tool_config, flag="requires_approval", options="approval")
    if approval is not None:
        policies[APPROVAL_ATTR] = approval

    replay = _pop_policy(tool_config, flag="replayable", options="replay")
    if replay is not None:
        policies[REPLAY_ATTR] = {
            "ttl_seconds": positive_int(
                replay.get("ttl_seconds"), DEFAULT_REPLAY_TTL_SECONDS
            ),
            "scope": (
                replay.get("scope")
                if replay.get("scope") in ("group", "run")
                else DEFAULT_REPLAY_SCOPE
            ),
        }

    return policies


def merge_tool_config_override(
    tool_name: str, base_config: ToolConfig, override: Optional[ToolConfig]
) -> Tuple[ToolConfig, ToolConfig, ToolPolicies]:
    """Merge a run, agent or task override over the teamspace tool config.

    Returns ``(override, tool_config, policies)``:

    - ``override``: the entry without its reserved keys. Factory branches that
      read the override again must use this one, never the raw entry.
    - ``tool_config``: the teamspace config with that override on top and the
      policy keys popped, ready for a constructor.
    - ``policies``: from the teamspace config. An override may add an approval
      gate the teamspace does not have; it cannot remove or change one, and it
      cannot turn replay on.

    A teamspace credential is not sent to a host the override chose (see
    ``_ENDPOINT_CREDENTIALS``). Neither input is mutated. Dropped keys are
    logged by name, never by value.
    """
    raw = dict(override or {})
    dropped = sorted(key for key in raw if key in RESERVED_OVERRIDE_KEYS)
    requested = extract_tool_policies(raw)
    clean = {k: v for k, v in raw.items() if k not in RESERVED_OVERRIDE_KEYS}

    tool_config = {**base_config, **clean}
    withheld = _withhold_moved_credentials(base_config, clean, tool_config)
    policies = extract_tool_policies(tool_config)
    if APPROVAL_ATTR in requested and APPROVAL_ATTR not in policies:
        policies[APPROVAL_ATTR] = requested[APPROVAL_ATTR]
        dropped = [k for k in dropped if k not in ("requires_approval", "approval")]

    if dropped:
        logger.warning(
            "[ToolFactory] %s: ignored tool_configs keys the run may not set: %s",
            tool_name,
            ", ".join(dropped),
        )
    if withheld:
        logger.warning(
            "[ToolFactory] %s: tool_configs moves an endpoint to another host; "
            "the teamspace's %s is not sent there",
            tool_name,
            ", ".join(withheld),
        )
    return clean, tool_config, policies


def _withhold_moved_credentials(
    base_config: ToolConfig, clean: ToolConfig, tool_config: ToolConfig
) -> List[str]:
    """Pop from ``tool_config`` each teamspace credential whose endpoint the
    override moved to another origin without supplying its own. Returns the
    names popped."""
    withheld: List[str] = []
    for endpoint, credentials in _ENDPOINT_CREDENTIALS.items():
        moved_to = clean.get(endpoint)
        if not str(moved_to or "").strip():
            continue
        if _origin(moved_to) == _origin(base_config.get(endpoint)):
            continue
        for credential in credentials:
            if base_config.get(credential) and not clean.get(credential):
                tool_config.pop(credential, None)
                withheld.append(credential)
    return withheld


def _origin(endpoint: object) -> Tuple[str, str]:
    """``(scheme, host[:port])`` of a configured endpoint, lower-cased; a bare
    host counts as https, the way the tools read it."""
    text = str(endpoint or "").strip().lower()
    if text and "://" not in text:
        text = f"https://{text}"
    try:
        parts = urlsplit(text)
    except ValueError:  # e.g. an unclosed IPv6 bracket: compare it as written
        return "", text
    return parts.scheme, parts.netloc


def stamp_tool_policies(result: object, policies: ToolPolicies) -> None:
    """Attach the extracted policies to the built tool instance(s).

    ``object.__setattr__`` because tools are pydantic models with no field for
    these, and because some factory branches hand back a LIST of instances (one
    MCP server yields every tool it serves) — each gets the same policy.

    A failure to stamp is logged, never raised: the tool itself is fine, and
    losing a policy must not lose the run. The approval hook treats an absent
    attribute as "no gate", which is the safe direction for the cassette too
    (call out for real).
    """
    if result is None or not policies:
        return
    for instance in result if isinstance(result, list) else [result]:
        for attr, policy in policies.items():
            try:
                object.__setattr__(instance, attr, dict(policy))
            except Exception as stamp_err:  # noqa: BLE001
                logger.warning(
                    f"[ToolFactory] could not stamp {attr} on "
                    f"{type(instance).__name__}: {stamp_err}"
                )


def replay_policy(tool: object) -> Optional[ToolConfig]:
    """This tool's cassette policy, or None when it is not replayable."""
    policy = getattr(tool, REPLAY_ATTR, None)
    return dict(policy) if isinstance(policy, dict) else None


def _pop_policy(
    tool_config: ToolConfig, *, flag: str, options: str
) -> Optional[ToolConfig]:
    """Read the ``flag: true`` / ``options: {...}`` pair both policies use.

    Either turns the policy on. The options dict alone counts as on, so
    ``{"replay": {"ttl_seconds": 60}}`` does not silently do nothing — a config
    that bothers to set the knobs plainly wants the feature.
    """
    enabled = bool(tool_config.pop(flag, False))
    options_value = tool_config.pop(options, None)
    if isinstance(options_value, dict):
        return {**options_value}
    return {} if enabled else None
