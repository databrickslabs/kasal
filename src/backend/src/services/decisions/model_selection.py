"""Auto model selection: the decision model picks one of the workspace's enabled models.

``"auto"`` is a request, never a model. It is resolved here, before anything
that calls a model sees it, into the key of one of the models the workspace has
enabled, which is the same list the model selector offers. The decision model
answers with an opaque index; the key comes from Kasal's own row. When it
abstains (off, no key, slow, unsure, too many candidates), the workspace's
default model is used, which is what the selector would have preselected
before Auto existed.

Both connections decide the same way: Jev through the Jev API, or Jev through
OpenRouter's System One route (``provider.py``). Either way the candidates are
the enabled models and nothing else, and ``choose_model`` checks the answer
against them again before anything is called. A router model (Jev Router,
``openrouter/auto``, ...) is never a candidate: it would hand the request to a
model of its own choosing, outside the enabled list.
"""

import logging
from contextvars import ContextVar
from dataclasses import dataclass, field
from time import monotonic
from typing import Optional, Sequence, Tuple, TypedDict

from opentelemetry.sdk.trace.id_generator import RandomIdGenerator
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.llm.model_capabilities import allowed_efforts, model_capability
from src.models.model_config import ModelConfig
from src.services.decisions import runtime
from src.services.decisions.connection import JEV_ROUTER_KEY, OPENROUTER
from src.services.decisions.connection import current as current_connection
from src.services.decisions.policies import question
from src.services.decisions.runtime import decide_with_reason, encoded_size
from src.utils.user_context import GroupContext

logger = logging.getLogger(__name__)

AUTO_MODEL = "auto"
POLICY = "model_selection"
#: The select helper's cap; more enabled models than this abstains.
MAX_CANDIDATES = 64
#: Only this much of the prompt is sent: the start (what is asked) and the end
#: (where a long paste usually states the actual question). It is also cut to
#: what fits the payload budget in ENCODED bytes, so a non-English prompt is
#: shortened rather than abstaining (see ``request_excerpt``).
MAX_REQUEST_CHARS = 2000

#: Why Auto fell back, beside the runtime's reasons (``runtime.NO_KEY`` ...).
NO_MODELS = "no_models"
TOO_MANY_MODELS = "too_many_models"
EMPTY_PROMPT = "empty_prompt"
#: The answer named a model outside the enabled, non-router candidates.
ROUTER_PICKED_DISABLED = "router_picked_disabled_model"

#: The span the decision gets in the run's trace (``execution_trace`` and OTel).
SPAN_NAME = "kasal.decision.model_selection"

#: The reason as the trace row reads it. The chat has its own i18n keys
#: (``chat.autoModel.reasons.*``) for the same codes.
FALLBACK_REASONS = {
    runtime.NO_WORKSPACE: "no workspace",
    runtime.NOT_CONFIGURED: "decision model not configured",
    runtime.NO_KEY: "no decision model key",
    runtime.TOO_LARGE: "request too large",
    runtime.TIMEOUT: "decision model timed out",
    runtime.UNREACHABLE: "decision model unreachable",
    runtime.PROVIDER_ERROR: "decision model error",
    runtime.ABSTAINED: "decision model abstained",
    NO_MODELS: "no enabled models",
    TOO_MANY_MODELS: "too many enabled models",
    EMPTY_PROMPT: "empty request",
    ROUTER_PICKED_DISABLED: "decision model picked a model that is not enabled",
}

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
    reason: Optional[str]
    connection: Optional[str]


class DecisionTrace(TypedDict):
    """The decision as a run carries it into its own trace (and subprocess).

    Ids are OTel hex strings. Never the prompt, never a key.
    """

    policy: str
    trace_id: str
    span_id: str
    model: Optional[str]
    status: str
    reason: Optional[str]
    connection: Optional[str]
    picked: Optional[str]
    candidates: int
    duration_ms: float
    summary: str


class RequestExcerpt(TypedDict):
    """The prompt as the decision model receives it."""

    text: str
    truncated: bool
    length: int


_IDS = RandomIdGenerator()


def _new_span_id() -> str:
    return f"{_IDS.generate_span_id():016x}"


def _new_trace_id() -> str:
    # The same formatting the span exporter gives the run's own rows.
    return f"{_IDS.generate_trace_id():016x}"


@dataclass(frozen=True)
class ModelSelection:
    """What Auto resolved to.

    ``status`` is ``selected`` when the decision model chose, ``fallback`` when
    the workspace default was used instead, and ``reason`` then says why (a
    key of ``FALLBACK_REASONS``). ``model`` is None only when the workspace has
    no enabled model: the caller then sends no model, exactly as when nothing
    was selected.

    The rest describes the decision for the trace: which connection asked,
    how many enabled models were offered, what came back (``picked``: Jev's
    raw option, or the key the guard refused), and the span ids the decision
    row and the run's spans share.
    """

    model: Optional[str]
    status: str
    duration_ms: float = 0.0
    reason: Optional[str] = None
    connection: Optional[str] = field(default=None, compare=False)
    candidates: int = field(default=0, compare=False)
    picked: Optional[str] = field(default=None, compare=False)
    span_id: str = field(default_factory=_new_span_id, compare=False)
    trace_id: str = field(default_factory=_new_trace_id, compare=False)

    def to_response(self) -> SelectionResponse:
        return {
            "requested": AUTO_MODEL,
            "model": self.model,
            "status": self.status,
            "reason": self.reason,
            "connection": self.connection,
        }

    def summary(self) -> str:
        """``Auto (Jev via OpenRouter) → m``, or ``Auto fell back to m (default: …)``."""
        model = self.model or "the default model"
        if self.status == "selected":
            via = " via OpenRouter" if self.connection == OPENROUTER else ""
            return f"Auto (Jev{via}) → {model}"
        why = FALLBACK_REASONS.get(self.reason or "")
        if why and self.reason == ROUTER_PICKED_DISABLED and self.picked:
            why = f"{why}: {self.picked}"
        return f"Auto fell back to {model}" + (f" (default: {why})" if why else "")

    def trace(self) -> DecisionTrace:
        return {
            "policy": POLICY,
            "trace_id": self.trace_id,
            "span_id": self.span_id,
            "model": self.model,
            "status": self.status,
            "reason": self.reason,
            "connection": self.connection,
            "picked": self.picked,
            "candidates": self.candidates,
            "duration_ms": self.duration_ms,
            "summary": self.summary(),
        }


def is_auto(value: object) -> bool:
    return isinstance(value, str) and value.strip().lower() == AUTO_MODEL


def is_router_model(model: ModelConfig) -> bool:
    """A model that forwards each request to a model of its own choosing.

    Jev Router (``typesafe/jev-router``) and OpenRouter's own routers
    (``openrouter/auto``, ``openrouter/free``, ...) pick from OpenRouter's
    whole catalogue, so Auto never offers them, even when enabled.
    """
    name = str(getattr(model, "name", "") or "").strip().lower()
    return (
        str(getattr(model, "key", "") or "") == JEV_ROUTER_KEY
        or name == "typesafe/jev-router"
        or name.startswith("openrouter/")
    )


def _cut(text: str, limit: int) -> str:
    """At most ``limit`` characters of ``text``: three quarters head, the rest tail."""
    if len(text) <= limit:
        return text
    head = limit * 3 // 4
    return f"{text[:head]}\n[...]\n{text[len(text) - (limit - head):]}"


def request_excerpt(
    prompt: Optional[str], max_bytes: int = runtime.MAX_PAYLOAD_BYTES
) -> RequestExcerpt:
    """The prompt as sent: at most MAX_REQUEST_CHARS and ``max_bytes`` encoded.

    The budget counts ``json.dumps`` bytes, where a non-ASCII character costs
    6 to 12, so a character cap alone let a long non-English prompt overrun it
    and abstain every time. The excerpt shrinks until it fits instead; it is
    empty only when not even a few characters fit.
    """
    text = str(prompt or "").strip()
    limit = min(len(text), MAX_REQUEST_CHARS)
    excerpt = _cut(text, limit)
    while excerpt and encoded_size(excerpt) > max_bytes:
        limit = min(limit - 1, limit * max_bytes // encoded_size(excerpt))
        excerpt = _cut(text, limit) if limit > 0 else ""
    return {"text": excerpt, "truncated": excerpt != text, "length": len(text)}


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


def _request_budget(described: list[dict[str, object]]) -> int:
    """Encoded bytes left for the request text once the models are described."""
    skeleton = {
        "request": {"text": "", "truncated": True, "length": 10**9},
        "models": described,
    }
    # +2: the skeleton already counts the empty text's quotes.
    return runtime.MAX_PAYLOAD_BYTES - encoded_size(skeleton) + 2


async def _ask(
    models: Sequence[ModelConfig], prompt: Optional[str], group_id: Optional[str]
) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """``(key, None, raw)`` when the decision model chose, else ``(None, reason, raw)``.

    ``raw`` is the option the decision model answered, when it answered.
    """
    if not models:
        return None, NO_MODELS, None
    if len(models) > MAX_CANDIDATES:
        return None, TOO_MANY_MODELS, None
    if not str(prompt or "").strip():
        return None, EMPTY_PROMPT, None
    described = [describe_model(m) for m in models]
    excerpt = request_excerpt(prompt, _request_budget(described))
    if not excerpt["text"]:
        return None, runtime.TOO_LARGE, None
    answers, reason = await decide_with_reason(
        POLICY,
        {"request": excerpt, "models": described},
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
    if answers is None:
        return None, reason, None
    choice = answers["model"].selected
    if choice == "none":
        return None, runtime.ABSTAINED, choice
    return str(models[int(choice)].key), None, choice


async def choose_model(
    models: Sequence[ModelConfig], prompt: Optional[str], *, group_id: Optional[str]
) -> ModelSelection:
    """Pick one of ``models`` for ``prompt``; fall back to the default on abstain.

    The guarantee lives here: whatever came back, the result is one of the
    enabled, non-router ``models`` (or None when there is none), never a model
    the workspace did not enable.
    """
    started = monotonic()
    candidates = [m for m in models if not is_router_model(m)]
    keys = {str(m.key) for m in candidates}
    selected, reason, picked = await _ask(candidates, prompt, group_id)
    if selected is not None and selected not in keys:
        logger.warning("Auto: refusing %s, which is not an enabled model", selected)
        selected, reason, picked = None, ROUTER_PICKED_DISABLED, selected
    return ModelSelection(
        selected if selected is not None else fallback_model(candidates),
        "selected" if selected is not None else "fallback",
        (monotonic() - started) * 1000,
        reason,
        connection=current_connection().kind,
        candidates=len(candidates),
        picked=picked,
    )


async def _enabled_models(
    session: AsyncSession, group_context: GroupContext
) -> list[ModelConfig]:
    """What ``GET /models/enabled`` returns for this group context."""
    from src.services.settings.models import ModelConfigService

    service = ModelConfigService(session, group_context.primary_group_id)
    return list(await service.find_enabled_models_for_group(group_context))


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
        models = await _enabled_models(session, group_context)
    selection = await choose_model(models, prompt, group_id=group_id)
    logger.info(
        "Auto model selection for workspace %s: %s (%s%s)",
        group_id,
        selection.model,
        selection.status,
        f": {selection.reason}" if selection.reason else "",
    )
    current_selection.set(selection)
    return selection


async def workspace_default(session: AsyncSession, group_id: str) -> Optional[str]:
    """The model Auto falls back to in ``group_id``, without asking anything."""
    models = await _enabled_models(session, GroupContext(group_ids=[group_id]))
    return fallback_model([m for m in models if not is_router_model(m)])


async def resolve_leaked_auto(
    session: AsyncSession, model_name: str, group_id: str
) -> str:
    """The safety net at the LLM builder: Auto never builds a router or "auto".

    "auto" becomes the workspace default. Every entry point resolves Auto
    before a model is built, so reaching this is a bug in the path that called
    it. The warning carries the stack, which names that path; the run still
    gets a real model instead of failing late or, under the CrewAI harness,
    becoming a native OpenAI client.

    Jev Router is refused the same way when this request's Auto pick names it:
    it would answer with a model of OpenRouter's choosing. Picking it by hand
    is still allowed.
    """
    selection = current_selection.get()
    auto_router = (
        model_name == JEV_ROUTER_KEY
        and selection is not None
        and selection.model == model_name
    )
    if not is_auto(model_name) and not auto_router:
        return model_name
    from src.utils.model_config import DEFAULT_ENGINE_MODEL

    default = await workspace_default(session, group_id) or DEFAULT_ENGINE_MODEL
    logger.warning(
        "%r reached the LLM builder unresolved as Auto's answer; using the "
        "workspace default "
        "%s. Resolve Auto where the request enters (run_freeze); leaked by:",
        model_name,
        default,
        stack_info=True,
    )
    return default


def trace_row(
    selection: ModelSelection, job_id: str, group_id: Optional[str]
) -> dict[str, object]:
    """The run's trace row for an Auto pick, in the decision rows' shape."""
    return {
        "job_id": job_id,
        "event_source": "decision",
        "event_context": POLICY,
        "event_type": "decision_evaluated",
        "span_name": SPAN_NAME,
        "span_id": selection.span_id,
        "trace_id": selection.trace_id,
        "parent_span_id": None,
        "output": selection.summary(),
        "trace_metadata": {
            "policy": POLICY,
            **selection.to_response(),
            "picked": selection.picked,
            "candidates": selection.candidates,
        },
        "duration_ms": int(selection.duration_ms),
        "group_id": group_id,
    }
