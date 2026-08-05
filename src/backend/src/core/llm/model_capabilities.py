"""What each model actually accepts, as data.

One registry for every per-model difference that changes the REQUEST — not
reasoning alone. "It is an OpenAI-compatible endpoint" is true right up until it
is not: providers disagree about parameter names, shapes, allowed enum values,
which ordinary sampling knobs they refuse outright, and whether thinking text
comes back at all. None of it follows model families, so no regex over a model
name can encode it, and every mismatch is a hard 400 on a real run rather than a
degraded response.

Two consumers, and they must never disagree:

* the transport, which builds the request;
* the API that tells the UI which controls to render — because offering a
  control the endpoint refuses produces a failed run, not a warning. The
  catalogue currently declares NOTHING for 63 seeded models, which is why the
  Edit Model dialog offers `temperature` on claude-opus-5, a model that rejects
  it (a 400 seen in production earlier the same day this was written).

Scope is deliberately open: anything model-specific that a caller must respect
belongs here, added as a field. It began as reasoning-only and was renamed when
sampling refusals landed, because the narrow name was already misleading.

Lives in ``core/llm/`` and not ``services/llm/`` deliberately: the transport
(``core/llm/transport/completion.py``) is the consumer, and ``core`` must never
import ``services`` (see ``src/services/CLAUDE.md`` — that contract is currently
clean for ``core`` and import-linter enforces it). This module is pure data plus
pure lookups: no I/O, no DB, no session.

HOW THE VALUES HERE WERE ESTABLISHED
====================================
Each entry carries a source. Two kinds, and the distinction matters:

* ``measured`` — the endpoint was asked for an invalid value and ENUMERATED its
  own accepted set in the error ("Supported values are: ...", "expected one of
  ...").  That is the strongest evidence available and it is what most entries
  use.
* ``documented`` — a provider doc states it, but the served endpoint was not
  observed confirming it.

The distinction is not pedantry. Provider docs describe the DIRECT API; Kasal
mostly talks to Databricks model serving, which lags and diverges. Both were
checked, and they disagreed:

  * Anthropic's docs give one adaptive effort scale, and the endpoint confirmed
    it exactly: low, medium, high, xhigh, max.
  * OpenAI's docs give ONE reasoning_effort scale. The endpoints report FOUR
    different scales, split by model — gpt-5 rejects "none", gpt-5-1 rejects
    "minimal", and only the 5-2/5-4/5-6 line accepts "xhigh". A single list
    would 400 somewhere no matter which one was chosen.

So: prefer ``measured``, and treat an unlisted value as unsupported rather than
assuming a family default.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

#: Verified 2026-08-05 against a live Databricks workspace, and against provider
#: docs of the same date. Kept on every record so a stale entry is legible as
#: stale rather than authoritative.
VERIFIED = "2026-08-05"

ANTHROPIC_THINKING_DOC = (
    "https://platform.claude.com/docs/en/build-with-claude/thinking"
    "#controlling-thinking-display"
)
ANTHROPIC_EFFORT_DOC = "https://platform.claude.com/docs/en/build-with-claude/effort"
ANTHROPIC_EXTENDED_DOC = (
    "https://platform.claude.com/docs/en/build-with-claude/extended-thinking"
)
OPENAI_REASONING_DOC = "https://developers.openai.com/api/docs/guides/reasoning"
DATABRICKS_REASON_DOC = (
    "https://docs.databricks.com/aws/en/machine-learning/model-serving/"
    "query-reason-models"
)


class ReasoningStyle(str, Enum):
    """How this model is ASKED to think."""

    #: `thinking: {"type": "enabled", "budget_tokens": N}` — a token budget.
    TOKEN_BUDGET = "token_budget"
    #: `thinking: {"type": "adaptive"}` + `output_config: {"effort": ...}`. The
    #: model decides depth; effort steers it. Rejects a budget.
    ADAPTIVE_EFFORT = "adaptive_effort"
    #: Top-level `reasoning_effort: "..."`.
    REASONING_EFFORT = "reasoning_effort"
    #: Thinking arrives unprompted in a sibling `reasoning_content` field; there
    #: is nothing to send.
    UNPROMPTED = "unprompted"


#: Sampling parameters a UI might offer, and the transport might send. Named
#: here so "which of these does this model refuse" is answerable as data.
SAMPLING_PARAMS = (
    "temperature",
    "top_p",
    "frequency_penalty",
    "presence_penalty",
    "stop",
)


@dataclass(frozen=True)
class ModelCapability:
    """What one model accepts, and what it gives back.

    Reasoning is the largest part of this today, but not the whole of it: ``refuses``
    carries the ordinary sampling parameters the endpoint rejects, so a UI can
    hide a control instead of offering one that 400s. Measured per model, because
    it does not follow families — claude-sonnet-4-5 accepts temperature and
    top_p while refusing both penalties, and claude-opus-5 refuses all four.
    """

    style: ReasoningStyle
    #: Accepted effort values, in increasing depth. Empty for TOKEN_BUDGET.
    #: PER-MODEL, never per-family: see the module docstring.
    efforts: tuple[str, ...] = ()
    #: Floor the endpoint enforces on a token budget.
    budget_min: int | None = None
    #: True when the budget must stay below `max_tokens` (Anthropic enforces
    #: this: "`max_tokens` must be greater than `thinking.budget_tokens`").
    budget_below_max_tokens: bool = False
    #: Whether the thinking TEXT is retrievable at all. False does not mean the
    #: model does not reason — gpt-5* reasons and bills for it, and simply never
    #: returns the trace over chat completions.
    returns_text: bool = True
    #: What has to be sent for the text to arrive, when it is not automatic.
    text_requires: str | None = None
    #: Sampling parameters this endpoint REJECTS (a 400, not a warning — there is
    #: no drop_params net on this path). Drives both the request builder and
    #: which controls a UI should render.
    refuses: tuple[str, ...] = ()
    #: "measured" (endpoint enumerated its own values) or "documented".
    evidence: str = "measured"
    source: str = ""
    note: str = ""

    def supports_effort(self, value: str | None) -> bool:
        """Whether ``value`` is an effort this model actually accepts."""
        if not value:
            return False
        return str(value).strip().lower() in self.efforts

    def accepts(self, param: str) -> bool:
        """Whether this model accepts sampling parameter ``param``."""
        return param not in self.refuses


# ── Anthropic ───────────────────────────────────────────────────────────────
# Two modes, and which one a model takes does NOT follow the version number:
# opus-4-7 and opus-4-8 are ADAPTIVE despite being "4.x". A regex like
# `claude-(opus|sonnet|haiku)-4-\d` looks right and would send "enabled" to
# those two, which they reject outright.

_ANTHROPIC_ADAPTIVE_EFFORTS = ("low", "medium", "high", "xhigh", "max")

#: Claude 4.1–4.6. Docs call `type: "enabled"` deprecated on 4.6 (requests still
#: succeed) and rejected from 4.7 on.
_MANUAL = ModelCapability(
    style=ReasoningStyle.TOKEN_BUDGET,
    budget_min=1024,
    budget_below_max_tokens=True,
    returns_text=True,
    text_requires='display: "summarized"',
    # Measured: temperature and top_p accepted, both penalties refused.
    refuses=("frequency_penalty", "presence_penalty"),
    evidence="measured",
    source=f"{ANTHROPIC_EXTENDED_DOC} + live endpoint",
    note=(
        "Measured thinking text with budget_tokens=10240, max_tokens=16000: "
        "haiku-4-5 1,867 chars, sonnet-4-5 1,739, opus-4-1 562, opus-4-5 375, "
        "opus-4-6 167, sonnet-4-6 76. Below max_tokens the endpoint 400s with "
        '"`max_tokens` must be greater than `thinking.budget_tokens`".'
    ),
)

#: Claude 4.7+, 5, Fable, Mythos. The endpoint enumerated this exact set when
#: asked for an invalid effort: "expected one of `low`, `medium`, `high`,
#: `xhigh`, `max`" — matching the published scale.
_ADAPTIVE = ModelCapability(
    style=ReasoningStyle.ADAPTIVE_EFFORT,
    efforts=_ANTHROPIC_ADAPTIVE_EFFORTS,
    returns_text=True,
    text_requires='display: "summarized"',
    # Measured: all four sampling knobs refused. This is the family whose
    # `temperature` rejection 400'd real runs before it was gated.
    refuses=("temperature", "top_p", "frequency_penalty", "presence_penalty"),
    evidence="measured",
    source=f"{ANTHROPIC_THINKING_DOC} + {ANTHROPIC_EFFORT_DOC} + live endpoint",
    note=(
        'display defaults to "omitted" on these models, returning thinking '
        "blocks with an EMPTY thinking field plus an encrypted signature — "
        "indistinguishable from a provider that redacts, which is a conclusion "
        "this codebase reached and shipped before opting in. With "
        'display: "summarized": opus-5 1,629 chars, fable-5 255. Effort visibly '
        "changes depth on fable-5: low 48 chars, max 140."
    ),
)

# ── OpenAI GPT-5 line ───────────────────────────────────────────────────────
# FOUR different enums, all measured from the endpoints' own error messages
# ("Supported values are: ..."). Docs give a single superset list; the served
# models do not agree with it or with each other.

_GPT5_NO_TEXT = (
    "Reasons and bills for it — reasoning_tokens scales with effort (320 at "
    "high, 0 at minimal) — but the message carries only "
    "['annotations','content','refusal','role']. Per OpenAI: \"While reasoning "
    "tokens are not visible via the API, they still occupy space in the "
    "model's context window and are billed as output tokens.\" Summaries exist "
    "only on the Responses API; `reasoning: {summary}` is rejected here as an "
    "unknown parameter."
)

#: gpt-5, gpt-5-mini, gpt-5-nano — accept "minimal", reject "none"/"xhigh".
_GPT5_MINIMAL = ModelCapability(
    style=ReasoningStyle.REASONING_EFFORT,
    efforts=("minimal", "low", "medium", "high"),
    returns_text=False,
    text_requires="not retrievable via chat completions",
    # Measured: every sampling knob refused, including `stop`.
    refuses=(
        "temperature",
        "top_p",
        "frequency_penalty",
        "presence_penalty",
        "stop",
    ),
    evidence="measured",
    source=f"{OPENAI_REASONING_DOC} + live endpoint",
    note=_GPT5_NO_TEXT,
)

#: gpt-5-1 — accepts "none", rejects "minimal" and "xhigh".
_GPT5_NONE = ModelCapability(
    style=ReasoningStyle.REASONING_EFFORT,
    efforts=("none", "low", "medium", "high"),
    returns_text=False,
    text_requires="not retrievable via chat completions",
    # Measured: every sampling knob refused, including `stop`.
    refuses=(
        "temperature",
        "top_p",
        "frequency_penalty",
        "presence_penalty",
        "stop",
    ),
    evidence="measured",
    source=f"{OPENAI_REASONING_DOC} + live endpoint",
    note=_GPT5_NO_TEXT,
)

#: gpt-5-2, gpt-5-4*, gpt-5-6* — the widest set, including "xhigh".
_GPT5_XHIGH = ModelCapability(
    style=ReasoningStyle.REASONING_EFFORT,
    efforts=("none", "low", "medium", "high", "xhigh"),
    returns_text=False,
    text_requires="not retrievable via chat completions",
    # Measured: every sampling knob refused, including `stop`.
    refuses=(
        "temperature",
        "top_p",
        "frequency_penalty",
        "presence_penalty",
        "stop",
    ),
    evidence="measured",
    source=f"{OPENAI_REASONING_DOC} + live endpoint",
    note=(
        f"{_GPT5_NO_TEXT} Additionally, gpt-5.6* on OpenAI's own endpoint "
        "rejects reasoning_effort alongside function tools and requires "
        "'none' — see _TOOLS_REJECT_REASONING_EFFORT_RE in transport/completion."
    ),
)

# ── Gemini 3.x on Databricks ────────────────────────────────────────────────

#: Rejects "none", "minimal", "xhigh" and "max" — only the three levels.
#: Crucially it needs the parameter to return anything: WITHOUT reasoning_effort
#: the response is text-only, WITH it a populated reasoning block comes back.
_GEMINI = ModelCapability(
    style=ReasoningStyle.REASONING_EFFORT,
    efforts=("low", "medium", "high"),
    returns_text=True,
    text_requires="reasoning_effort must be set",
    evidence="measured",
    source=f"{DATABRICKS_REASON_DOC} + live endpoint",
    note=(
        "Measured with reasoning_effort set: gemini-3-1-flash-lite 2,226 chars, "
        "3-5-flash 2,104, 3-1-pro 1,648. The native Gemini `thinking` shape is "
        "rejected here (400 Invalid JSON payload), so reasoning_effort is the "
        "only lever. 3-5-flash-lite and 3-6-flash accept it but return no text."
    ),
)

# ── Models that just tell you ───────────────────────────────────────────────

#: inkling, kimi-k2-7-code. Nothing to send; the text arrives in a sibling
#: `reasoning_content` field. Not documented anywhere findable — observed only.
_UNPROMPTED = ModelCapability(
    style=ReasoningStyle.UNPROMPTED,
    returns_text=True,
    evidence="measured",
    source="live endpoint (no provider doc located)",
    note=(
        "Returns reasoning in a sibling `reasoning_content` field with no "
        "request parameter: inkling 309 chars, kimi-k2-7-code 137. UNVERIFIED "
        "against any provider doc — behaviour observed, not promised."
    ),
)


#: Model-name fragment -> capability. Ordered MOST SPECIFIC FIRST and matched in
#: order, because the names nest: "gpt-5-1" contains "gpt-5", and matching the
#: shorter one first would hand gpt-5-1 an enum that rejects its values.
_CAPABILITIES: tuple[tuple[str, ModelCapability], ...] = (
    # Anthropic adaptive (must precede the manual 4-x entries: "claude-opus-4-8"
    # would otherwise never be reached).
    ("claude-opus-4-7", _ADAPTIVE),
    ("claude-opus-4-8", _ADAPTIVE),
    ("claude-opus-5", _ADAPTIVE),
    ("claude-sonnet-5", _ADAPTIVE),
    ("claude-fable-5", _ADAPTIVE),
    ("claude-mythos-5", _ADAPTIVE),
    # Anthropic manual.
    ("claude-opus-4-1", _MANUAL),
    ("claude-opus-4-5", _MANUAL),
    ("claude-opus-4-6", _MANUAL),
    ("claude-sonnet-4-5", _MANUAL),
    ("claude-sonnet-4-6", _MANUAL),
    ("claude-haiku-4-5", _MANUAL),
    # GPT-5 line, longest fragment first.
    ("gpt-5-6", _GPT5_XHIGH),
    ("gpt-5.6", _GPT5_XHIGH),
    ("gpt-5-4", _GPT5_XHIGH),
    ("gpt-5.4", _GPT5_XHIGH),
    ("gpt-5-2", _GPT5_XHIGH),
    ("gpt-5.2", _GPT5_XHIGH),
    ("gpt-5-1", _GPT5_NONE),
    ("gpt-5.1", _GPT5_NONE),
    ("gpt-5-mini", _GPT5_MINIMAL),
    ("gpt-5-nano", _GPT5_MINIMAL),
    ("gpt-5", _GPT5_MINIMAL),
    # Gemini 3.x (2.5 exposes nothing and is deliberately absent).
    ("gemini-3", _GEMINI),
    # Unprompted.
    ("inkling", _UNPROMPTED),
    ("kimi-k2-7", _UNPROMPTED),
)


def model_capability(model_name: str | None) -> ModelCapability | None:
    """What ``model_name`` accepts, or None when it has no reasoning surface.

    Tolerates the shapes a model name arrives in: a Kasal key
    (``databricks-claude-opus-5``), a provider-prefixed name
    (``openai/gpt-5.6-terra``) and a served name
    (``global.anthropic.claude-opus-5``). Matching is on substrings in the fixed
    order above, so the specific entry always wins.

    None is the safe answer: the caller sends nothing, and a model absent from
    this table behaves exactly as it did before reasoning existed.
    """
    if not model_name:
        return None
    haystack = str(model_name).lower()
    for fragment, capability in _CAPABILITIES:
        if fragment in haystack:
            return capability
    return None


def reasoning_style(model_name: str | None) -> ReasoningStyle | None:
    """The style ``model_name`` uses, or None."""
    capability = model_capability(model_name)
    return capability.style if capability else None


def allowed_efforts(model_name: str | None) -> tuple[str, ...]:
    """The effort values ``model_name`` accepts — empty when it takes none.

    This is what a UI should render as its options. Hardcoding a list in the
    frontend cannot work: there are five distinct scales across the catalogue,
    and offering a value the endpoint refuses is a 400 rather than a warning.
    """
    capability = model_capability(model_name)
    return capability.efforts if capability else ()


def refused_params(model_name: str | None) -> tuple[str, ...]:
    """Sampling parameters ``model_name`` rejects.

    The UI should hide a control for each of these, and the request builder must
    not send them: there is no drop_params net on this path, so what is set IS
    sent and a refused parameter is a 400. Unknown models refuse nothing, which
    preserves the behaviour every model had before this registry existed.
    """
    capability = model_capability(model_name)
    return capability.refuses if capability else ()


def accepts_param(model_name: str | None, param: str) -> bool:
    """Whether ``model_name`` accepts sampling parameter ``param``."""
    return param not in refused_params(model_name)
