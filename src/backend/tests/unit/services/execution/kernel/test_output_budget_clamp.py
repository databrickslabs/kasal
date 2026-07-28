"""The output clamp and the input trim must share one token estimate.

They are two halves of one budget — the server enforces
``prompt + max_tokens <= window`` — and they used to be computed by two different
estimators in two different modules: chars/4 inside the engine's trim, and
``litellm.token_counter`` inside a vLLM-only subclass in llm_manager. Two answers
to "how big is this prompt" can disagree enough to trim what did not need
trimming while still overflowing the request.
"""

import pytest

from src.core.llm.transport import LLM, LLM_CONTEXT_WINDOW_SIZES


@pytest.fixture
def small_window_model():
    """A model with a known, deliberately small window."""
    model = "test-small-window"
    LLM_CONTEXT_WINDOW_SIZES[model] = 10_000
    yield model
    LLM_CONTEXT_WINDOW_SIZES.pop(model, None)


def _messages(chars):
    return [{"role": "user", "content": "x" * chars}]


class TestClampsWhenTheSumOverflows:
    def test_output_is_reduced_to_fit(self, small_window_model):
        # ~2000 tokens of prompt (8000 chars / 4) against a 10k window, asking
        # for 9000 output: the sum overflows and the server would 400.
        llm = LLM(model=small_window_model, api_key="k", max_tokens=9000)
        params = llm._prepare_completion_params(_messages(8000))

        assert params["max_tokens"] < 9000
        assert params["max_tokens"] + llm._estimate_tokens(_messages(8000)) <= 10_000

    def test_never_grows_a_modest_request(self, small_window_model):
        llm = LLM(model=small_window_model, api_key="k", max_tokens=500)
        params = llm._prepare_completion_params(_messages(400))
        assert params["max_tokens"] == 500

    def test_floors_at_256_rather_than_going_negative(self, small_window_model):
        llm = LLM(model=small_window_model, api_key="k", max_tokens=9000)
        params = llm._prepare_completion_params(_messages(39_000))
        assert params["max_tokens"] == 256

    def test_reasoning_models_clamp_max_completion_tokens(self, small_window_model):
        """GPT-5-family models carry the budget under a different key."""
        llm = LLM(model=small_window_model, api_key="k", max_completion_tokens=9000)
        params = llm._prepare_completion_params(_messages(8000))
        assert params["max_completion_tokens"] < 9000
        assert "max_tokens" not in params


class TestDoesNotClampWhenItShouldNot:
    def test_unknown_window_is_left_alone(self):
        """An unregistered model gets DEFAULT_CONTEXT_WINDOW_SIZE from the table;
        clamping against a guessed 8192 would shred a large real window."""
        llm = LLM(model="model-nobody-registered", api_key="k", max_tokens=9000)
        params = llm._prepare_completion_params(_messages(8000))
        assert params["max_tokens"] == 9000

    def test_no_budget_set_means_nothing_to_clamp(self, small_window_model):
        llm = LLM(model=small_window_model, api_key="k")
        params = llm._prepare_completion_params(_messages(80_000))
        assert "max_tokens" not in params
        assert "max_completion_tokens" not in params

    def test_large_window_model_is_untouched(self):
        model = "test-large-window"
        LLM_CONTEXT_WINDOW_SIZES[model] = 1_000_000
        try:
            llm = LLM(model=model, api_key="k", max_tokens=32_000)
            params = llm._prepare_completion_params(_messages(40_000))
            assert params["max_tokens"] == 32_000
        finally:
            LLM_CONTEXT_WINDOW_SIZES.pop(model, None)


class TestSharedEstimator:
    def test_tool_schemas_count_toward_the_prompt(self):
        """They are sent, so they consume window — token_counter(messages=...)
        omitted them, which is part of why the old clamp needed a fudge factor."""
        llm = LLM(model="x", api_key="k")
        msgs = _messages(400)
        tools = [
            {
                "type": "function",
                "function": {"name": "f", "parameters": {"x": "y" * 400}},
            }
        ]
        assert llm._estimate_tokens(msgs, tools) > llm._estimate_tokens(msgs)

    def test_tool_calls_and_outputs_are_counted(self):
        llm = LLM(model="x", api_key="k")
        conversation = [
            {
                "role": "assistant",
                "content": "",
                "tool_calls": [{"id": "1", "x": "y" * 400}],
            },
            {"role": "tool", "content": "z" * 400},
        ]
        assert llm._estimate_tokens(conversation) > 150

    def test_raw_window_is_not_the_derated_one(self, small_window_model):
        """The clamp needs the model's real limit; the 0.85 derate is for trimming
        decisions, and applying it here would shrink outputs for no reason."""
        llm = LLM(model=small_window_model, api_key="k")
        assert llm._raw_context_window() == 10_000
        assert llm.get_context_window_size() == 8_500
