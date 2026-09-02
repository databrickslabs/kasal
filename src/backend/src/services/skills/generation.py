"""Draft an Agent Skill from a request or a conversation — a generation call.

Creating a skill is a product action, not agent knowledge: the same shape as
crew generation (a DB-backed prompt template, one focused LLM call, JSON out,
validated before anyone sees it) rather than a meta-skill the agent has to
decide to load mid-turn and then obey. The model only PROPOSES; the draft goes
back to the chat as a card whose Save button is the human commit gate.

Two modes, one call:
- **capture** — the request arrives with the conversation transcript; the
  corrections the user made ARE the skill, so the template mines those first.
- **blank page** — the request alone; thin requests still get a best draft with
  an "Open questions" section rather than an interview (the user refines by
  asking again).
"""

import json
import logging
from typing import Any, Dict, List, Optional

from src.services.catalog.templates import TemplateService
from src.services.llm.manager import LLMManager
from src.services.skills import parser
from src.utils.prompt_utils import robust_json_parser
from src.utils.telemetry import KasalProduct, get_user_agent_header
from src.utils.user_context import GroupContext

logger = logging.getLogger(__name__)

TEMPLATE_NAME = "generate_skill"
#: Transcript turns kept for capture mode (the tail of the conversation).
MAX_TRANSCRIPT_TURNS = 30
MAX_TURN_CHARS = 4000


class SkillGenerationService:
    """One focused LLM call → a validated ``{name, description, body}`` draft."""

    @staticmethod
    async def draft(
        request: str,
        group_context: Optional[GroupContext],
        *,
        transcript: Optional[List[Dict[str, str]]] = None,
        model: Optional[str] = None,
    ) -> Dict[str, Any]:
        system = await _system_prompt(group_context)
        user = _user_message(request, transcript)
        messages = [
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ]
        draft = await _ask(messages, model)
        verdict = _validate(draft)
        if not verdict["valid"]:
            # One retry with the validator's own words: the errors are exact
            # (name shape, missing description) and the model fixes them
            # reliably when told; a second failure is returned as-is so the
            # card can show the same messages on Save.
            logger.info(
                "[skills] draft failed validation, retrying once: %s", verdict["errors"]
            )
            messages.append({"role": "assistant", "content": json.dumps(draft)})
            messages.append(
                {
                    "role": "user",
                    "content": "That draft failed validation:\n- "
                    + "\n- ".join(verdict["errors"])
                    + "\nReturn the corrected JSON object only.",
                }
            )
            draft = await _ask(messages, model)
            verdict = _validate(draft)
        return {**draft, **verdict}


async def _system_prompt(group_context: Optional[GroupContext]) -> str:
    """The DB-backed template (group/user overrides apply), else the seed."""
    try:
        if group_context is not None:
            content = await TemplateService.get_effective_template_content(
                TEMPLATE_NAME, group_context
            )
            if content and content.strip():
                return content
    except Exception as exc:  # noqa: BLE001 — a template read must not block a draft
        logger.warning("[skills] template %r unavailable: %s", TEMPLATE_NAME, exc)
    from src.seeds.prompt_templates import GENERATE_SKILL_TEMPLATE

    return GENERATE_SKILL_TEMPLATE


def _user_message(request: str, transcript: Optional[List[Dict[str, str]]]) -> str:
    turns = [
        t
        for t in (transcript or [])
        if isinstance(t, dict)
        and t.get("role") in ("user", "assistant")
        and t.get("content")
    ][-MAX_TRANSCRIPT_TURNS:]
    if not turns:
        return f"MODE: blank page\n\nREQUEST:\n{request.strip()}"
    lines = [
        f"{t['role'].upper()}: {str(t['content'])[:MAX_TURN_CHARS]}" for t in turns
    ]
    return (
        "MODE: capture — mine this conversation. What the user corrected, "
        "rejected or asked for twice becomes the skill's first rules; what they "
        "accepted without comment is the output shape.\n\n"
        f"REQUEST:\n{request.strip() or 'Save what we learned in this conversation as a skill.'}\n\n"
        "CONVERSATION:\n" + "\n\n".join(lines)
    )


async def _ask(messages: List[Dict[str, str]], model: Optional[str]) -> Dict[str, Any]:
    content = await LLMManager.completion(
        messages=messages,
        model=model,
        temperature=0.4,
        max_tokens=4000,
        extra_headers=get_user_agent_header(KasalProduct.SKILL),
    )
    try:
        data = robust_json_parser(content or "")
    except Exception:  # noqa: BLE001 — unparseable reply -> empty, invalid draft
        data = {}
    if not isinstance(data, dict):
        data = {}
    return {
        "name": str(data.get("name") or "").strip(),
        "description": str(data.get("description") or "").strip(),
        "body": str(data.get("body") or "").strip(),
    }


def _validate(draft: Dict[str, Any]) -> Dict[str, Any]:
    """The reference validator's verdict, in the shape the card understands."""
    try:
        parsed = parser.validate_row(draft["name"], draft["description"], draft["body"])
        return {
            "valid": True,
            "errors": [],
            "warnings": list(getattr(parsed, "warnings", None) or []),
        }
    except Exception as exc:  # noqa: BLE001 — SkillValidationError or a bad field
        errors = getattr(exc, "errors", None)
        return {
            "valid": False,
            "errors": [str(e) for e in errors] if errors else [str(exc)],
            "warnings": [],
        }
