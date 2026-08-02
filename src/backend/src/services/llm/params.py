"""What gets sent with a request, decided in one place.

Kasal's transport declares ``top_p``, ``frequency_penalty``,
``presence_penalty``, ``stop`` and an ``additional_params`` escape hatch, and
forwards every one of them that is set. Nothing set any of them: the model
catalogue could express exactly two knobs, ``temperature`` and
``max_output_tokens``, so influencing anything else meant editing Python in a
provider handler. That is what made every fix per-model, and it is the gap this
closes — ``ModelConfig.params`` is now the declaration, and this module is the
one place it is resolved.

The shape is the one every mature client converged on (LangChain's typed fields
plus ``model_kwargs``/``extra_body``; CrewAI's ``BaseLLM`` fields plus
``additional_params``; LiteLLM's ``get_supported_openai_params`` allowlist):

1. **one sparse bag** — unset means absent, never a default silently applied;
2. **a capability filter** applied ONCE to the merged bag, so a parameter the
   endpoint refuses cannot reach it;
3. **explicit precedence** — built-in defaults, then the model's declaration,
   then whatever the call site asked for.

Why the filter has to be data
=============================

There is no litellm on this request path, so nothing strips an unsupported
parameter after the fact: what is set IS sent, and OpenAI's reasoning models
answer a stray ``frequency_penalty`` with a 400. Kasal answered that question by
matching on the model NAME in three different files, which disagreed with each
other and needed editing for every new model. ``unsupported_params`` moves the
answer next to the model it describes.

Why there are no seeded penalty defaults
========================================

Measured, on a live endpoint, N=5 per condition. ``frequency_penalty=0.3``
turned a repeating 25-item list into a clean one (17.7% duplicate lines and 5/5
truncations, down to 0% and 0/5) — and the same setting turned a 12-row markdown
table from 681 characters with a clean stop into 9679 characters and a
truncation, because a table legitimately repeats its separator row and column
vocabulary. A count-proportional penalty cannot tell those apart.

So this ships the surface and no opinion. A default that fixes one task shape
while breaking another is worse than no default, and the honest place for the
value is beside the model and workload someone actually measured.
"""

import logging
from typing import Any, Dict, Iterable, Mapping, Optional

logger = logging.getLogger(__name__)

#: Parameters that are the transport's own business, never a model's to declare.
#:
#: ``model`` and ``messages`` are the request itself; the credentials and the
#: endpoint are resolved per tenant. Letting a config row set them would turn a
#: catalogue entry into a way to redirect traffic.
RESERVED = frozenset(
    {
        "model",
        "messages",
        "input",
        "api_key",
        "base_url",
        "api_base",
        "provider",
        "stream",
        "tools",
        "tool_choice",
    }
)

#: Kasal-wide starting point. Empty on purpose — see the module docstring.
DEFAULTS: Dict[str, Any] = {}


def _clean(source: Optional[Mapping[str, Any]]) -> Dict[str, Any]:
    """A mapping with the reserved keys and the unset values removed.

    ``None`` means "not specified" throughout — the same convention the
    transport uses when it decides what to put on the wire — so a config row
    carrying an explicit null does not become a null on the request.
    """
    if not isinstance(source, Mapping):
        return {}
    cleaned: Dict[str, Any] = {}
    for key, value in source.items():
        name = str(key)
        if value is None:
            continue
        if name in RESERVED:
            logger.warning(
                "[llm-params] ignoring %r: the transport owns that parameter", name
            )
            continue
        cleaned[name] = value
    return cleaned


def resolve(
    model_params: Optional[Mapping[str, Any]] = None,
    overrides: Optional[Mapping[str, Any]] = None,
    unsupported: Optional[Iterable[str]] = None,
    defaults: Optional[Mapping[str, Any]] = None,
) -> Dict[str, Any]:
    """The parameters to send, after precedence and the capability filter.

    Precedence, lowest first: ``defaults`` (Kasal-wide), ``model_params`` (the
    catalogue row), ``overrides`` (this call site). Each layer replaces a key
    rather than merging into it, so a caller can always state a value outright.

    ``unsupported`` is applied LAST and to the merged result, because that is
    the only point at which every layer's contribution is visible — filtering
    each layer separately lets a later one reintroduce what an earlier one was
    filtered for. Both the top-level name and the same name nested inside
    ``extra_body`` are removed, since an endpoint that rejects a parameter
    rejects it wherever it is written.
    """
    merged = dict(_clean(defaults if defaults is not None else DEFAULTS))
    merged.update(_clean(model_params))
    merged.update(_clean(overrides))

    refused = {str(name) for name in (unsupported or ())}
    if not refused:
        return merged

    dropped = [name for name in merged if name in refused]
    for name in dropped:
        merged.pop(name, None)

    body = merged.get("extra_body")
    if isinstance(body, Mapping):
        kept = {k: v for k, v in body.items() if str(k) not in refused}
        if len(kept) != len(body):
            dropped += [k for k in body if str(k) in refused]
        if kept:
            merged["extra_body"] = kept
        else:
            merged.pop("extra_body", None)

    if dropped:
        logger.info(
            "[llm-params] dropped %s — this endpoint does not accept %s",
            sorted(set(dropped)),
            "them" if len(set(dropped)) > 1 else "it",
        )
    return merged


def rejects(unsupported: Optional[Iterable[str]], name: str) -> bool:
    """Whether this endpoint refuses ``name``. The question, asked of data."""
    return str(name) in {str(item) for item in (unsupported or ())}
