"""Slash commands: ``/list crews``, ``/run flow my-flow``, ``/help``.

The one branch of intent detection that costs nothing. A message starting with
``/`` is not a natural-language request at all — it is a typed command, and
running it past a model would spend a call to rediscover what the leading slash
already said.

Its own module because it is a PARSER, not dispatch logic, and it was taking up
more of ``dispatcher.py`` than the classifier it sits in front of. Everything
here is pure: message in, intent-result dict out, no session, no LLM, no I/O.

The result shape matches what ``detect_intent`` returns from every other branch,
with ``source: "slash_command"`` — which is also what tells ``detect_intent_logged``
not to write an llmlog row for a call that never happened.
"""

from typing import Any, Dict, Optional


def detect_slash_command(message: str) -> Optional[Dict[str, Any]]:
    """Detect and parse slash commands (e.g., /list, /load my-plan).

    Returns a fully formed intent result dict if the message is a recognized
    slash command, or None otherwise.
    """
    stripped = message.strip()
    if not stripped.startswith("/"):
        return None

    parts = stripped.split(None, 1)  # split into command + rest
    command = parts[0].lower()
    args = parts[1].strip() if len(parts) > 1 else ""

    COMMAND_MAP = {
        "/list": "catalog_list",
        "/plans": "catalog_list",
        "/flows": "flow_list",
        "/load": "catalog_load",
        "/save": "catalog_save",
        "/schedule": "catalog_schedule",
        "/help": "catalog_help",
        "/run": "execute_crew",
        "/exec": "execute_crew",
        "/delete": "catalog_delete",
    }

    intent = COMMAND_MAP.get(command)
    if intent is None:
        if stripped.startswith("/"):
            # Unrecognized slash command -> show help with error
            return {
                "intent": "catalog_help",
                "confidence": 1.0,
                "extracted_info": {
                    "command": command,
                    "args": args,
                    "invalid_command": True,
                },
                "suggested_prompt": stripped,
                "source": "slash_command",
                "suggested_tools": [],
            }
        return None

    # Check for flow qualifier in args (e.g. "/list flows", "/load flow my-flow")
    qualifier_found = False
    FLOW_INTENT_MAP = {
        "catalog_list": "flow_list",
        "catalog_load": "flow_load",
        "catalog_save": "flow_save",
        "execute_crew": "execute_flow",
        "catalog_delete": "flow_delete",
    }
    if args.lower().startswith(("flow", "flows")) and intent in FLOW_INTENT_MAP:
        intent = FLOW_INTENT_MAP[intent]
        qualifier_found = True
        # Strip "flow" or "flows" prefix from args
        remaining = args.split(None, 1)
        args = remaining[1].strip() if len(remaining) > 1 else ""

    # Check for crew/crews qualifier (e.g. "/list crews", "/save crew My Crew")
    CREW_QUALIFIABLE = {
        "catalog_list",
        "catalog_load",
        "catalog_save",
        "catalog_schedule",
        "execute_crew",
        "catalog_delete",
    }
    if (
        not qualifier_found
        and args.lower().startswith(("crew", "crews"))
        and intent in CREW_QUALIFIABLE
    ):
        qualifier_found = True
        remaining = args.split(None, 1)
        args = remaining[1].strip() if len(remaining) > 1 else ""

    # Commands that require a crew/flow qualifier (bare /list, /load etc. show usage help)
    # /plans and /flows are aliases that already imply the qualifier, so they're excluded.
    QUALIFIER_REQUIRED = {
        "/list",
        "/load",
        "/save",
        "/run",
        "/exec",
        "/schedule",
        "/delete",
    }
    if not qualifier_found and command in QUALIFIER_REQUIRED:
        COMMAND_USAGE = {
            "/list": "Usage: `/list crews` or `/list flows`",
            "/load": "Usage: `/load crew <name>` or `/load flow <name>`",
            "/save": "Usage: `/save crew [name]` or `/save flow [name]`",
            "/run": "Usage: `/run crew` or `/run flow`",
            "/exec": "Usage: `/run crew` or `/run flow`",
            "/schedule": "Usage: `/schedule crew`",
            "/delete": "Usage: `/delete crew <name>` or `/delete flow <name>`",
        }
        return {
            "intent": "catalog_help",
            "confidence": 1.0,
            "extracted_info": {
                "command": command,
                "args": args,
                "command_help": COMMAND_USAGE.get(command, ""),
            },
            "suggested_prompt": stripped,
            "source": "slash_command",
            "suggested_tools": [],
        }

    return {
        "intent": intent,
        "confidence": 1.0,
        "extracted_info": {"command": command, "args": args},
        "suggested_prompt": stripped,
        "source": "slash_command",
        "suggested_tools": [],
    }
