"""How many tokens an LLM is allowed to produce, whichever field carries it.

Two field names mean the same thing. Most endpoints take ``max_tokens``; GPT-5
and the newer OpenAI reasoning models reject it and take
``max_completion_tokens`` instead, so ``LLMManager`` sets whichever the model
accepts (see the ``is_gpt5`` branches there).

That split is fine for making the request and a trap for reading it back.
Diagnostics that looked at ``max_tokens`` alone reported ``None`` for a model
capped at 128,000 — and a configured limit that reads as "no limit" is worse
than no diagnostic, because it sends you looking for a missing setting that is
not missing. This is the one place that reconciles the two names.

No database, no config lookup: it reads the built client, so it reports what
the request will ACTUALLY carry rather than what the catalogue says it should.
"""

from typing import Any, Union

#: What to report when neither field is set — a genuinely uncapped client.
UNSET = "not set"


def output_cap(llm: Any) -> Union[int, str]:
    """The effective output ceiling on a built LLM, or ``UNSET``.

    ``max_completion_tokens`` wins when both are present, mirroring the
    transport, which prefers it when deciding what to send.
    """
    for field in ("max_completion_tokens", "max_tokens"):
        value = getattr(llm, field, None)
        if isinstance(value, int) and value > 0:
            return value
    return UNSET
