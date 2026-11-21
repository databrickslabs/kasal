"""Tool-call arguments as they may be stored and shown.

A tool call's arguments are copied into the run log, the stored trace, MLflow spans and
approval records. Some tools take credentials as arguments (``client_secret``, ``password``,
``access_token``): those values are masked on the way out. The tool itself is called with
the real arguments; only the copies observers keep go through here.
"""

from __future__ import annotations

import json

from src.utils.sensitive_data_utils import mask_sensitive_fields


def observable_tool_args(tool_args: object) -> object:
    """``tool_args`` with every value under a credential-named key masked.

    A dict is masked key by key (nested dicts and lists included); a string that is a JSON
    object is parsed, masked and written back; anything else is returned as it is."""
    if isinstance(tool_args, dict):
        return mask_sensitive_fields(dict(tool_args))
    if isinstance(tool_args, str) and tool_args.lstrip().startswith("{"):
        try:
            parsed = json.loads(tool_args)
        except ValueError:
            return tool_args
        if isinstance(parsed, dict):
            return json.dumps(mask_sensitive_fields(parsed), default=str)
    return tool_args


def tool_args_text(tool_args: object) -> str:
    """The masked arguments as one string, for a log line or a trace field."""
    masked = observable_tool_args(tool_args)
    if masked is None:
        return ""
    if isinstance(masked, str):
        return masked
    try:
        return json.dumps(masked, default=str)
    except (TypeError, ValueError):
        return str(masked)
