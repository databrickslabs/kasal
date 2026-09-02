"""Revise ONE slide of an HTML deck — a generation call, recorded as a run.

A deck is slide-addressable by contract (one ``<section class="slide">`` per
slide, a 1280×720 stage, inline styles), so an edit can name its slide: the
model is shown that slide plus a reference slide for the design, and returns
one revised ``<section>``. The frontend splices it back into the deck the
reader already has; nothing else is regenerated. The same call, in ``add``
mode, writes a new slide between two neighbours.

One focused LLM call (the ``refine_slide`` template), with one retry when the
reply holds no slide. Recorded as a run so the call shows in the run activity
and on the Jobs page like any other LLM call.
"""

import logging
import re
import time
from typing import Any, Dict, List, Optional, Tuple

from src.services.catalog.templates import TemplateService
from src.services.execution import generation_run
from src.services.llm.manager import LLMManager
from src.utils.telemetry import KasalProduct, get_user_agent_header
from src.utils.user_context import GroupContext

logger = logging.getLogger(__name__)

TEMPLATE_NAME = "refine_slide"
#: How the rows are attributed in the timeline, and what the Jobs page calls it.
EVENT_SOURCE = "Decks"
EVENT_CONTEXT = "slide refine"
TRIGGER_TYPE = "slide_refine"
_MAX_RUN_NAME = 80

# A slide section opener: <section class="slide"> possibly with more classes /
# attributes. Case-insensitive, tolerant of attribute order and whitespace —
# the same contract the frontend's htmlDeck.ts parses.
_SLIDE_OPEN = re.compile(
    r"<section\b[^>]*\bclass\s*=\s*[\"'][^\"']*\bslide\b[^\"']*[\"'][^>]*>", re.I
)
_SECTION_TAG = re.compile(r"<\s*(/?)section\b[^>]*>", re.I)
_FENCE = re.compile(r"```[a-zA-Z]*\n(.*?)```", re.S)


def first_slide_section(reply: str) -> Optional[str]:
    """The one COMPLETE slide a reply carries — the first
    ``<section class="slide">…</section>`` inside its ```html fence (or bare,
    when the model skipped the fence) — or None when there is no finished slide.
    Nested <section> elements inside the slide are depth-matched."""
    text = reply or ""
    fenced = _FENCE.search(text)
    code = fenced.group(1) if fenced else text
    opener = _SLIDE_OPEN.search(code)
    if not opener:
        return None
    depth = 1
    for tag in _SECTION_TAG.finditer(code, opener.end()):
        depth += -1 if tag.group(1) else 1
        if depth == 0:
            return code[opener.start() : tag.end()].strip()
    return None  # unclosed: the reply was cut off


def run_name(mode: str, instruction: str, position: str) -> str:
    what = " ".join((instruction or "").split())
    if len(what) > _MAX_RUN_NAME:
        what = what[: _MAX_RUN_NAME - 1].rstrip() + "…"
    head = "New slide" if mode == "add" else "Slide refine"
    where = f" (slide {position})" if position else ""
    return f"{head}{where}: {what}" if what else f"{head}{where}"


class SlideRefineService:
    """One focused LLM call → one revised (or new) ``<section class="slide">``."""

    @staticmethod
    async def refine(
        *,
        mode: str,
        instruction: str,
        group_context: Optional[GroupContext],
        slide: Optional[str] = None,
        reference: Optional[str] = None,
        before: Optional[str] = None,
        after: Optional[str] = None,
        position: str = "",
        model: Optional[str] = None,
        session: Any = None,
    ) -> Dict[str, Any]:
        """The revised slide, plus how it was made: the served ``model``, the
        ``attempts`` it took, the ``job_id`` of the run recording its LLM
        calls (None without a ``session``), and ``error`` when no slide came
        back — the caller shows that where the edit was asked for."""
        if mode not in ("refine", "add"):
            raise ValueError(f"unknown slide edit mode: {mode!r}")
        if mode == "refine" and not (slide or "").strip():
            raise ValueError("a refine needs the slide to revise")
        system = await _system_prompt(group_context)
        user = _user_message(
            mode, instruction, slide, reference, before, after, position
        )
        messages: List[Dict[str, str]] = [
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ]
        job_id = await generation_run.open_run(
            session,
            run_name=run_name(mode, instruction, position),
            inputs={
                "mode": mode,
                "instruction": instruction,
                "position": position,
                "model": model,
            },
            trigger_type=TRIGGER_TYPE,
            group_context=group_context,
        )
        began = time.monotonic()
        try:
            section, served, call = await _ask(messages, model)
            await generation_run.record_call(
                job_id,
                source=EVENT_SOURCE,
                context=EVENT_CONTEXT,
                attempt=1,
                model=served,
                group_context=group_context,
                **call,
            )
            attempts = 1
            if section is None:
                # One retry, told exactly what was missing: models sometimes
                # answer with prose or a whole document instead of the section.
                logger.info("[decks] slide reply held no slide, retrying once")
                messages.append({"role": "assistant", "content": call["response"]})
                messages.append(
                    {
                        "role": "user",
                        "content": "That reply did not contain a slide. Return ONLY one "
                        '<section class="slide">…</section> inside a single ```html fence — '
                        "no other slides, no commentary.",
                    }
                )
                section, served, call = await _ask(messages, model)
                await generation_run.record_call(
                    job_id,
                    source=EVENT_SOURCE,
                    context=EVENT_CONTEXT,
                    attempt=2,
                    model=served,
                    group_context=group_context,
                    **call,
                )
                attempts = 2
        except Exception as exc:
            await generation_run.close_run(
                job_id, error=str(exc) or exc.__class__.__name__
            )
            raise
        result = {
            "section": section,
            "error": None if section else "The model did not return a slide.",
            "model": served,
            "attempts": attempts,
            "job_id": job_id,
            "duration_ms": round((time.monotonic() - began) * 1000, 2),
        }
        await generation_run.close_run(
            job_id,
            message=(
                ("New slide written" if mode == "add" else "Slide refined")
                if section
                else "No slide returned"
            ),
            result={k: v for k, v in result.items() if k != "section"}
            | {"section_chars": len(section or "")},
        )
        return result


async def _system_prompt(group_context: Optional[GroupContext]) -> str:
    """The DB-backed template (group/user overrides apply), else the seed."""
    try:
        if group_context is not None:
            text = await TemplateService.get_effective_template_content(
                TEMPLATE_NAME, group_context
            )
            if text:
                return text
    except Exception as exc:  # noqa: BLE001 — the seed is always a valid fallback
        logger.debug("[decks] %s template lookup failed: %s", TEMPLATE_NAME, exc)
    from src.seeds.prompt_templates import REFINE_SLIDE_TEMPLATE

    return REFINE_SLIDE_TEMPLATE


def _user_message(
    mode: str,
    instruction: str,
    slide: Optional[str],
    reference: Optional[str],
    before: Optional[str],
    after: Optional[str],
    position: str,
) -> str:
    where = f" (slide {position})" if position else ""
    if mode == "add":
        return (
            f"MODE: add a new slide{where}\n\n"
            f"WHAT THE NEW SLIDE SHOULD COVER:\n{instruction or 'a slide that fits naturally between its neighbours'}\n\n"
            f"THE SLIDE BEFORE IT:\n{before or '(none — this will be the first slide)'}\n\n"
            f"THE SLIDE AFTER IT:\n{after or '(none — this will be the last slide)'}\n"
        )
    return (
        f"MODE: revise one slide{where}\n\n"
        f"INSTRUCTION:\n{instruction}\n\n"
        f"SLIDE TO REVISE:\n{slide}\n\n"
        f"REFERENCE SLIDE (design to match):\n{reference or '(none — keep the slide’s own design)'}\n"
    )


async def _ask(
    messages: List[Dict[str, str]], model: Optional[str]
) -> Tuple[Optional[str], Optional[str], Dict[str, Any]]:
    """One call: the slide it returned (or None), the model that served it,
    and the call itself (``prompt`` / ``response`` / ``duration_ms``)."""
    began = time.monotonic()
    content, served = await LLMManager.completion(
        messages=messages,
        model=model,
        temperature=0.3,
        max_tokens=6000,
        extra_headers=get_user_agent_header(KasalProduct.DECK_EDIT),
        with_served_model=True,
    )
    call = {
        "prompt": "\n\n".join(
            f"[{m.get('role', '?')}]\n{m.get('content', '')}" for m in messages
        ),
        "response": content or "",
        "duration_ms": (time.monotonic() - began) * 1000,
    }
    return first_slide_section(content or ""), _served_name(served, model), call


def _served_name(served: Any, requested: Optional[str]) -> Optional[str]:
    """The served model as a plain name (see skills.generation)."""
    if not isinstance(served, str) or not served.strip():
        return requested or None
    if requested:
        return served
    return served.split(" (for ", 1)[0]
