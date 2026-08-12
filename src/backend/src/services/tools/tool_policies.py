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

Extracted from ``tool_factory.py`` (2,700 lines, over the ceiling) rather than
added to it, per the file-size rule in CLAUDE.md.
"""

import logging
from typing import Any, Dict, Optional

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


def extract_tool_policies(tool_config: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    """Pop every policy key out of ``tool_config`` and return the policies.

    Mutates ``tool_config`` — that is the job. What is left is safe to splat
    into a tool constructor.

    Returns a dict of attribute name -> policy dict, ready for
    ``stamp_tool_policies``. A policy the config does not ask for is absent
    rather than empty, so "off" and "on with all defaults" stay distinguishable.
    """
    policies: Dict[str, Dict[str, Any]] = {}

    approval = _pop_policy(tool_config, flag="requires_approval", options="approval")
    if approval is not None:
        policies[APPROVAL_ATTR] = approval

    replay = _pop_policy(tool_config, flag="replayable", options="replay")
    if replay is not None:
        policies[REPLAY_ATTR] = {
            "ttl_seconds": _positive_int(
                replay.get("ttl_seconds"), DEFAULT_REPLAY_TTL_SECONDS
            ),
            "scope": (
                replay.get("scope")
                if replay.get("scope") in ("group", "run")
                else DEFAULT_REPLAY_SCOPE
            ),
        }

    return policies


def stamp_tool_policies(result: Any, policies: Dict[str, Dict[str, Any]]) -> None:
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


def replay_policy(tool: Any) -> Optional[Dict[str, Any]]:
    """This tool's cassette policy, or None when it is not replayable."""
    policy = getattr(tool, REPLAY_ATTR, None)
    return dict(policy) if isinstance(policy, dict) else None


def _pop_policy(
    tool_config: Dict[str, Any], *, flag: str, options: str
) -> Optional[Dict[str, Any]]:
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


def _positive_int(value: Any, default: int) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError, OverflowError):
        # OverflowError is not paranoia: JSON has no infinity, but Python's
        # decoder accepts `Infinity` and hands back a float that int() refuses.
        return default
    return parsed if parsed > 0 else default
