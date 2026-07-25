"""Self-hosted vLLM endpoints.

Lives beside the other endpoint policies rather than inside llm_manager: it is
the same kind of thing as DatabricksRetryLLM — an engine LLM subclass that adds
what one serving setup needs — and it was only in the facade for historical
reasons.
"""

import os

from kasal_engine.llm import LLM

class VLLMFunctionCallingLLM(LLM):
    """LLM subclass for self-hosted vLLM endpoints.

    Post engine-migration this class exists for ONE behaviour, below: pinning
    ``tool_choice="required"`` on the opening turn.

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

    The vLLM backend runs with ``--enable-auto-tool-choice --tool-call-parser``,
    so native tool calls work. Opt out of the forced opening turn with
    ``VLLM_FORCE_TOOL_FIRST_TURN=false``.
    """

    def _prepare_completion_params(self, messages, tools=None, skip_file_processing=False):
        """Force a tool call on the FIRST turn so Qwen3-Coder cannot skip attached
        tools and fabricate an answer.

        Making native function-calling *available* is not enough: under CrewAI's
        large ReAct-scaffolded prompt, Qwen3-Coder declines ``tool_choice="auto"``
        and emits a fabricated "Final Answer" in a single turn without ever calling
        the tool (verified against the live vLLM endpoint: short prompts tool-call
        under ``auto``, the full crew prompt does not). We pin
        ``tool_choice="required"`` only while no tool result is present in the
        message history (i.e. the opening turn); once any tool has run we revert to
        the model's default so it can produce the final answer instead of looping.
        Opt out with ``VLLM_FORCE_TOOL_FIRST_TURN=false``.
        """
        params = super()._prepare_completion_params(
            messages, tools=tools, skip_file_processing=skip_file_processing
        )
        # Force a tool call on the opening turn (see docstring) — opt out via env.
        if os.getenv("VLLM_FORCE_TOOL_FIRST_TURN", "true").lower() == "true":
            # Only act when tools are offered this turn and nothing else already
            # pinned tool_choice (e.g. structured-output / guardrail calls).
            if params.get("tools") and "tool_choice" not in params:
                history = params.get("messages") or []
                already_used_tool = any(
                    isinstance(m, dict) and (m.get("role") == "tool" or m.get("tool_calls"))
                    for m in history
                )
                if not already_used_tool:
                    params["tool_choice"] = "required"
        return params
