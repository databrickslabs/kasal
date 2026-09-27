"""Auto model selection: the decision model picks one of the workspace's enabled models.

``"auto"`` is a request, never a model. It is resolved here, before anything
that calls a model sees it, into the key of one of the models the workspace has
enabled, which is the same list the model selector offers. The decision model
answers with an opaque index; the key comes from Kasal's own row. When it
abstains (off, no key, slow, unsure, too many candidates), the workspace's
default model is used, which is what the selector would have preselected
before Auto existed.
"""

import logging
from contextvars import ContextVar
from dataclasses import dataclass
from time import monotonic
from typing import Optional, Sequence, TypedDict

from sqlalchemy.ext.asyncio import AsyncSession

from src.core.llm.model_capabilities import allowed_efforts, model_capability
from src.models.model_config import ModelConfig
from src.services.decisions.policies import question
from src.services.decisions.runtime import decide
from src.utils.user_context import GroupContext

logger = logging.getLogger(__name__)

AUTO_MODEL = "auto"
POLICY = "model_selection"
#: The select helper's cap; more enabled models than this abstains.
MAX_CANDIDATES = 64
#: Only this much of the prompt is sent: the start (what is asked) and the end
#: (where a long paste usually states the actual question).
MAX_REQUEST_CHARS = 2000
_HEAD_CHARS = 1500

INSTRUCTIONS = (
    "Choose the enabled model best suited to the request. It must be capable "
    "enough for the request's complexity and for the context and output length it "
    "needs; when several are adequate, prefer the smaller or faster one. Use only "
    "the supplied metadata. Do not invent pricing or benchmark claims. Choose none "
    "if the metadata cannot distinguish suitability."
)

#: The selection made for the current request, so the run it starts can record
#: it in its trace. Copied into tasks the request spawns (asyncio copies
#: contextvars), never shared between requests.
current_selection: ContextVar[Optional["ModelSelection"]] = ContextVar(
    "auto_model_selection", default=None
)


class SelectionResponse(TypedDict):
    """What Auto picked, as API responses and trace metadata carry it."""

    requested: str
    model: Optional[str]
    status: str


class RequestExcerpt(TypedDict):
    """The prompt as the decision model receives it."""

    text: str
    truncated: bool
    length: int


@dataclass(frozen=True)
class ModelSelection:
    """What Auto resolved to.

    ``status`` is ``selected`` when the decision model chose, ``fallback`` when
    the workspace default was used instead. ``model`` is None only when the
    workspace has no enabled model: the caller then sends no model, exactly as
    when nothing was selected.
    """

    model: Optional[str]
    status: str
    duration_ms: float = 0.0

    def to_response(self) -> SelectionResponse:
        return {"requested": AUTO_MODEL, "model": self.model, "status": self.status}


def is_auto(value: object) -> bool:
    return isinstance(value, str) and value.strip().lower() == AUTO_MODEL


def request_excerpt(prompt: Optional[str]) -> RequestExcerpt:
    """The prompt as sent: capped at MAX_REQUEST_CHARS, head and tail kept."""
    text = str(prompt or "").strip()
    if len(text) <= MAX_REQUEST_CHARS:
        return {"text": text, "truncated": False, "length": len(text)}
    tail = MAX_REQUEST_CHARS - _HEAD_CHARS
    return {
        "text": f"{text[:_HEAD_CHARS]}\n[...]\n{text[-tail:]}",
        "truncated": True,
        "length": len(text),
    }


def describe_model(model: ModelConfig) -> dict[str, object]:
    """A candidate as the decision model sees it: capabilities, never the key."""
    key = str(getattr(model, "key", "") or "")
    capability = model_capability(key)
    return {
        "name": getattr(model, "name", None),
        "provider": getattr(model, "provider", None),
        "context_window": getattr(model, "context_window", None),
        "max_output_tokens": getattr(model, "max_output_tokens", None),
        "reasoning": capability.style.value if capability else None,
        "reasoning_efforts": list(allowed_efforts(key)),
        "extended_thinking": bool(getattr(model, "extended_thinking", False)),
    }


def fallback_model(models: Sequence[ModelConfig]) -> Optional[str]:
    """The workspace default: the server default when enabled, else the first model.

    The same rule the chat selector uses to preselect a model, so falling back
    lands where the user would have been without Auto.
    """
    from src.utils.model_config import DEFAULT_ENGINE_MODEL

    keys = [str(m.key) for m in models if getattr(m, "key", None)]
    if DEFAULT_ENGINE_MODEL in keys:
        return DEFAULT_ENGINE_MODEL
    return keys[0] if keys else None


async def choose_model(
    models: Sequence[ModelConfig], prompt: Optional[str], *, group_id: Optional[str]
) -> ModelSelection:
    """Pick one of ``models`` for ``prompt``; fall back to the default on abstain."""
    started = monotonic()
    default = fallback_model(models)
    excerpt = request_excerpt(prompt)
    selected: Optional[str] = None
    if models and len(models) <= MAX_CANDIDATES and excerpt["text"]:
        answers = await decide(
            POLICY,
            {"request": excerpt, "models": [describe_model(m) for m in models]},
            {
                "model": question(
                    INSTRUCTIONS,
                    {
                        **{str(i): f"Model {i}" for i in range(len(models))},
                        "none": "The metadata cannot distinguish a suitable model",
                    },
                )
            },
            group_id=group_id,
        )
        if answers is not None and answers["model"].selected != "none":
            selected = str(models[int(answers["model"].selected)].key)
    elapsed = (monotonic() - started) * 1000
    if selected is not None:
        return ModelSelection(selected, "selected", elapsed)
    return ModelSelection(default, "fallback", elapsed)


async def select_for_workspace(
    session: AsyncSession, group_context: Optional[GroupContext], prompt: Optional[str]
) -> ModelSelection:
    """Resolve Auto for one request, from the workspace's own enabled models only.

    The candidates are exactly what ``GET /models/enabled`` returns for this
    group context: the list the user can pick from by hand. There is no
    cross-workspace fallback: no workspace means no candidates and no decision.
    """
    group_id = group_context.primary_group_id if group_context is not None else None
    models: list[ModelConfig] = []
    if group_id and group_context is not None:
        from src.services.settings.models import ModelConfigService

        models = list(
            await ModelConfigService(session, group_id).find_enabled_models_for_group(
                group_context
            )
        )
    selection = await choose_model(models, prompt, group_id=group_id)
    logger.info(
        "Auto model selection for workspace %s: %s (%s)",
        group_id,
        selection.model,
        selection.status,
    )
    current_selection.set(selection)
    return selection


def trace_row(
    selection: ModelSelection, job_id: str, group_id: Optional[str]
) -> dict[str, object]:
    """The run's trace row for an Auto pick, in the decision rows' shape."""
    verb = "picked" if selection.status == "selected" else "fell back to"
    return {
        "job_id": job_id,
        "event_source": "decision",
        "event_context": POLICY,
        "event_type": "decision_evaluated",
        "span_name": "kasal.decision.evaluate",
        "output": f"Auto {verb} {selection.model or 'the default model'}",
        "trace_metadata": {
            "policy": POLICY,
            **selection.to_response(),
        },
        "duration_ms": int(selection.duration_ms),
        "group_id": group_id,
    }
