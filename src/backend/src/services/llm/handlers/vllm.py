"""Self-hosted vLLM endpoints.

Lives beside the other endpoint policies rather than inside llm_manager: it is
the same kind of thing as DatabricksRetryLLM — an engine LLM subclass that adds
what one serving setup needs — and it was only in the facade for historical
reasons.
"""

import os

from src.core.llm.transport import LLM


class VLLMFunctionCallingLLM(LLM):
    """LLM subclass for self-hosted vLLM endpoints.

    States ``tool_choice="auto"`` when tools are offered: the model is told it
    MAY call them and decides for itself, every turn.

    It used to pin ``tool_choice="required"`` on the opening turn instead, and
    releasing that is why this file was rewritten rather than deleted. Forcing
    cannot tell one kind of request from another — the endpoint sees the same
    thing whether the turn is "gather swiss news from today" or "hello how are
    you", so a greeting opened with a web search and then invented the query for
    it. Measured against the live endpoint, 3 samples per cell: ``required``
    called a tool 3/3 on a greeting; ``auto`` was 0/3 on a greeting and 3/3 on an
    explicit search request.

    No mainstream framework forces on the model's behalf either — CrewAI sets
    exactly this value and stops (``providers/openai/completion.py``), LangGraph
    never mentions ``tool_choice``, LangChain forwards only what the caller
    asked for, and LiteLLM drops even a caller's value once a tool result exists.

    Four further overrides lived here and are gone — ``kasal_engine``'s
    ``BaseLLM``/``OpenAICompletion`` do natively what crewAI had to be fought for:
      - the ``max_tokens`` clamp (prompt+output must fit the window) is now
        ``OpenAICompletion._clamp_output_budget``, sharing one token estimator
        with the input trim instead of running a second, differently-calibrated
        one here — and it protects every provider, not just vLLM.
      - ``supports_function_calling()`` already returns True for every model, so
        the engine never falls back to the ReAct TEXT protocol that Qwen3-Coder
        follows poorly. No litellm model registry is consulted, so the
        ``litellm.register_model`` call that used to accompany this is gone too.
      - ``__copy__``/``__deepcopy__`` preserve the subclass (pydantic's copy plus
        ``BaseLLM.__deepcopy__``). crewAI's hardcoded ``return LLM(...)`` — the
        only reason we re-stamped ``__class__`` — no longer exists.

    Known limitation, not worked around here
    ----------------------------------------

    Qwen3-Coder under-uses its tools under a large scaffolded prompt: it declines
    ``auto`` and writes an answer from nothing rather than calling the tool it was
    handed, while tool-calling correctly on short prompts. Forcing masked that,
    at the cost of making every short turn call a tool too.

    The fix is to choose WHICH TOOLS ARE ATTACHED from the request rather than
    whether a call is compelled: attach none on a turn that wants none, and no
    model can call anything; attach them when the turn wants one, and a reluctant
    model has a far smaller decision to get wrong. Until that exists, prefer a
    tool-following model for tool-heavy work.

    Set ``VLLM_TOOL_CHOICE`` to override the value sent (e.g. ``required`` to
    restore the old behaviour for one deployment, or ``none`` to suppress tool
    use). Anything falsy or ``default`` sends nothing and lets the server decide.
    """

    def _prepare_completion_params(
        self, messages, tools=None, skip_file_processing=False
    ):
        """Declare the tool policy explicitly rather than inheriting a default.

        ``auto`` is what an OpenAI-compatible server already applies when tools
        are present, so this changes no behaviour by itself — it makes the policy
        visible in the request, in the logs, and in one greppable place, instead
        of being whatever the endpoint happened to default to. The vLLM backend
        must still run with ``--enable-auto-tool-choice --tool-call-parser``, or
        it ignores tools regardless of what is sent here.

        Never overwrites a ``tool_choice`` already in ``params``: structured
        output, guardrail calls and a caller naming one specific tool all pin it
        themselves, and each of those outranks an endpoint-wide default.
        """
        params = super()._prepare_completion_params(
            messages, tools=tools, skip_file_processing=skip_file_processing
        )
        choice = os.getenv("VLLM_TOOL_CHOICE", "auto").strip().lower()
        if not choice or choice == "default":
            return params
        if params.get("tools") and "tool_choice" not in params:
            params["tool_choice"] = choice
        return params
