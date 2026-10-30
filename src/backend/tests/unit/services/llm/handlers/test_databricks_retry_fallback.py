"""DatabricksRetryLLM model-fallback wiring: a model-swappable failure delegates
to another enabled model, and once switched, later calls stick to it.

The candidate cache is pre-seeded so the test never rebuilds a real LLM
(configure_kasal_llm is not exercised here — that path is covered elsewhere).
"""

import asyncio

from src.services.llm.handlers.databricks_retry_llm import (
    _NO_FALLBACK,
    DatabricksRetryLLM,
)
from src.services.llm.handlers.model_fallback import ModelCandidate

FALLBACK_KEY = "databricks-claude-sonnet-4-5"


class _Err(Exception):
    def __init__(self, msg, status=None):
        super().__init__(msg)
        if status is not None:
            self.status_code = status


def _ctx_err():
    return _Err("prompt is too long: 9 tokens > 8 maximum", status=400)


class _FakeLLM:
    def __init__(self):
        self.sync_calls = 0
        self.async_calls = 0

    def call(self, messages=None, **kwargs):
        self.sync_calls += 1
        return "FALLBACK_SYNC"

    async def acall(self, messages=None, **kwargs):
        self.async_calls += 1
        return "FALLBACK_ASYNC"


def _llm_with_fallback(fake):
    llm = DatabricksRetryLLM(model="databricks/databricks-gemini-3-5-flash")
    llm._group_id = "g1"
    # Large window so a context-window fallback always has something bigger to
    # pick, independent of the source model's registered window.
    llm._fallback_candidates = [ModelCandidate(FALLBACK_KEY, context_window=2_000_000)]
    llm._fallback_llm_cache = {FALLBACK_KEY: fake}  # pre-seeded: no real rebuild
    return llm


_CALL_KW = {
    "messages": [{"role": "user", "content": "hi"}],
    "tools": None,
    "callbacks": None,
    "available_functions": None,
    "from_task": None,
    "from_agent": None,
}


def test_maybe_model_fallback_delegates_on_context_window():
    fake = _FakeLLM()
    llm = _llm_with_fallback(fake)
    result = llm._maybe_model_fallback(_ctx_err(), "call", dict(_CALL_KW))
    assert result == "FALLBACK_SYNC"
    assert fake.sync_calls == 1
    assert llm._active_fallback is fake
    assert FALLBACK_KEY in llm._tried_models


def test_maybe_model_fallback_sentinel_for_unclassified_error():
    fake = _FakeLLM()
    llm = _llm_with_fallback(fake)
    result = llm._maybe_model_fallback(
        Exception("connection reset"), "call", dict(_CALL_KW)
    )
    assert result is _NO_FALLBACK
    assert fake.sync_calls == 0
    assert llm._active_fallback is None


def test_maybe_model_fallback_sentinel_when_no_candidates():
    fake = _FakeLLM()
    llm = _llm_with_fallback(fake)
    llm._fallback_candidates = []  # nothing to fall back to
    assert llm._maybe_model_fallback(_ctx_err(), "call", dict(_CALL_KW)) is _NO_FALLBACK


def test_active_fallback_short_circuits_subsequent_call():
    fake = _FakeLLM()
    llm = _llm_with_fallback(fake)
    llm._active_fallback = fake  # already switched on a prior turn
    assert llm.call(**_CALL_KW) == "FALLBACK_SYNC"
    assert fake.sync_calls == 1


def test_acall_delegates_on_fallback():
    fake = _FakeLLM()
    llm = _llm_with_fallback(fake)
    result = asyncio.run(
        llm._amaybe_model_fallback(_ctx_err(), "acall", dict(_CALL_KW))
    )
    assert result == "FALLBACK_ASYNC"
    assert fake.async_calls == 1
    assert llm._active_fallback is fake


def test_acall_active_fallback_short_circuits():
    fake = _FakeLLM()
    llm = _llm_with_fallback(fake)
    llm._active_fallback = fake
    assert asyncio.run(llm.acall(**_CALL_KW)) == "FALLBACK_ASYNC"
    assert fake.async_calls == 1


_ARGON_400 = _Err(
    'BadRequestError: Error code: 400 - Unknown name "reasoning_effort" at '
    "'generation_config': Cannot find field. INVALID_ARGUMENT",
    status=400,
)


def _warned(llm, run):
    """Run a fallback with the crew logger captured; return the warning text."""
    from unittest.mock import MagicMock, patch

    crew_logger = MagicMock()
    with patch.object(DatabricksRetryLLM, "_get_crew_logger", return_value=crew_logger):
        run(llm)
    return " ".join(str(c.args[0]) for c in crew_logger.warning.call_args_list)


def test_fallback_warning_names_the_endpoint_error():
    """A fatal 4xx used to log only "model fallback (fatal_4xx): a -> b", so the
    endpoint's reason (Argon rejecting reasoning_effort) was nowhere in the logs."""
    llm = _llm_with_fallback(_FakeLLM())
    text = _warned(
        llm, lambda m: m._maybe_model_fallback(_ARGON_400, "call", dict(_CALL_KW))
    )
    assert "model fallback (fatal_4xx)" in text
    assert 'Unknown name "reasoning_effort"' in text


def test_async_fallback_warning_names_the_endpoint_error():
    llm = _llm_with_fallback(_FakeLLM())
    text = _warned(
        llm,
        lambda m: asyncio.run(
            m._amaybe_model_fallback(_ARGON_400, "acall", dict(_CALL_KW))
        ),
    )
    assert 'Unknown name "reasoning_effort"' in text


def test_fallback_warning_truncates_a_long_error():
    llm = _llm_with_fallback(_FakeLLM())
    long_err = _Err("prompt is too long: " + "x" * 5000, status=400)
    text = _warned(
        llm, lambda m: m._maybe_model_fallback(long_err, "call", dict(_CALL_KW))
    )
    assert "x" * 300 not in text
    assert len(text) < 600
