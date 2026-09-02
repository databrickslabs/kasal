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
import time
from typing import Any, Dict, List, Optional, Tuple

from src.services.catalog.templates import TemplateService
from src.services.llm.manager import LLMManager
from src.services.skills import draft_run, parser
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
        session: Any = None,
    ) -> Dict[str, Any]:
        """The validated draft, plus how it was made: the served ``model``,
        the ``attempts`` it took and the ``job_id`` of the run recording its
        LLM calls (see :mod:`draft_run`) — None when no ``session`` was given
        to open one with, or the record could not be written."""
        system = await _system_prompt(group_context)
        user = _user_message(request, transcript)
        messages = [
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ]
        job_id = await draft_run.open_run(
            session,
            request=request,
            transcript_turns=len(transcript or []),
            model=model,
            group_context=group_context,
        )
        try:
            draft, served, call = await _ask(messages, model)
            await draft_run.record_call(
                job_id, attempt=1, model=served, group_context=group_context, **call
            )
            verdict = _validate(draft)
            attempts = 1
            if not verdict["valid"]:
                # One retry with the validator's own words: the errors are
                # exact (name shape, missing description) and the model fixes
                # them reliably when told; a second failure is returned as-is
                # so the card can show the same messages on Save.
                logger.info(
                    "[skills] draft failed validation, retrying once: %s",
                    verdict["errors"],
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
                draft, served, call = await _ask(messages, model)
                await draft_run.record_call(
                    job_id, attempt=2, model=served, group_context=group_context, **call
                )
                verdict = _validate(draft)
                attempts = 2
        except Exception as exc:
            await draft_run.close_run(job_id, error=str(exc) or exc.__class__.__name__)
            raise
        # The model that actually answered and how many calls it took: the
        # chat shows this drafting as run activity, and a step that names the
        # model is what makes the work visible (the call is otherwise silent).
        result = {
            **draft,
            **verdict,
            "model": served,
            "attempts": attempts,
            "job_id": job_id,
        }
        await draft_run.close_run(job_id, result=result)
        return result


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


async def _ask(
    messages: List[Dict[str, str]], model: Optional[str]
) -> Tuple[Dict[str, Any], Optional[str], Dict[str, Any]]:
    """One call: the parsed ``{name, description, body}``, the model that
    served it (the resolved one — a picker key may be substituted), and the
    call itself (``prompt`` / ``response`` / ``duration_ms``) for the trace."""
    began = time.monotonic()
    content, served = await LLMManager.completion(
        messages=messages,
        model=model,
        temperature=0.4,
        max_tokens=4000,
        extra_headers=get_user_agent_header(KasalProduct.SKILL),
        with_served_model=True,
    )
    call = {
        "prompt": _render_messages(messages),
        "response": content or "",
        "duration_ms": (time.monotonic() - began) * 1000,
    }
    try:
        data = robust_json_parser(content or "")
    except Exception:  # noqa: BLE001 — unparseable reply -> empty, invalid draft
        data = {}
    if not isinstance(data, dict):
        data = {}
    return (
        {
            "name": str(data.get("name") or "").strip(),
            "description": str(data.get("description") or "").strip(),
            "body": str(data.get("body") or "").strip(),
        },
        _served_name(served, model),
        call,
    )


def _render_messages(messages: List[Dict[str, str]]) -> str:
    """The request as one text, role by role — what the trace shows as the
    call's input (the retry carries the failed draft and the errors too)."""
    return "\n\n".join(
        f"[{m.get('role', '?')}]\n{m.get('content', '')}" for m in messages
    )


def _served_name(served: Any, requested: Optional[str]) -> Optional[str]:
    """The served model as a plain name. ``completion`` reports a substitution
    as ``"<served> (for '<requested>')"``; with no requested model that
    reads ``(for 'None')``, which is noise, so only the served half stays."""
    if not isinstance(served, str) or not served.strip():
        return requested or None
    if requested:
        return served
    return served.split(" (for ", 1)[0]


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
