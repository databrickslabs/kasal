"""Route MemAlign's model calls through LLMManager.

MemAlign (``mlflow.genai.judges.optimizers.MemAlignOptimizer``) distils
guidelines with a DSPy language model and indexes the graded examples with a
DSPy embedder. mlflow builds both INSIDE the optimizer from LiteLLM model URIs
plus whatever credentials LiteLLM finds in the environment. Kasal does not
route models that way: a judge's model is a Kasal model key picked in the UI,
and LLMManager resolves the provider, endpoint, group-scoped API key and
request quirks. This module makes MemAlign use that path — the same bridge
GEPA's reflection already uses (``gepa/reflection.py``).

Two of mlflow's factory hooks are wrapped once, idempotently. The wrappers
consult a process-wide override at call time and otherwise defer to mlflow:

* ``memalign.utils.construct_dspy_lm`` -> a ``dspy.BaseLM`` whose ``forward``
  submits ``LLMManager.completion`` to the main event loop
* ``memalign.optimizer._build_embedder`` -> a ``dspy.Embedder`` over a
  callable that submits ``LLMManager.get_embedding`` per text

The override is process-wide rather than thread-local because MemAlign distils
its batches on a ``ThreadPoolExecutor`` it owns — state set on the aligning
thread would be invisible there. A lock serialises alignments instead: they
are rare, take seconds, and a GEPA run never enters these hooks.

The model URIs MemAlign records on the aligned judge are inert placeholders,
exactly like GEPA's ``openai:/kasal-llm-manager``: parsed for token-budget
lookups (which fall back to defaults for an unknown model), never called.
"""

from __future__ import annotations

import asyncio
import threading
import time
from contextlib import contextmanager
from typing import Any, Dict, Iterator, List, Optional

from src.services.prompt_optimization.gepa.reflection import _sync_llm_completion
from src.utils.user_context import GroupContext

#: Recorded on the aligned judge; parsed by mlflow, never called.
REFLECTION_PLACEHOLDER = "openai:/kasal-llm-manager"
EMBEDDING_PLACEHOLDER = "openai:/kasal-embedder"

#: Room for a batch of guidelines as JSON; forced-thinking models spend part
#: of it reasoning before the visible answer (same figure as GEPA reflection).
DISTILL_MAX_TOKENS = 6000
#: Texts embedded per bridged round trip.
EMBED_BATCH = 16
#: Seconds to wait for one bridged call (matches _sync_llm_completion).
CALL_TIMEOUT = 300

_LOCK = threading.Lock()
_OVERRIDE: Dict[str, Any] = {}


def _sync_embed(
    loop: asyncio.AbstractEventLoop,
    texts: List[str],
    embedder_config: Optional[Dict[str, Any]],
    group_context: Optional[GroupContext],
    user_token: Optional[str],
) -> List[List[float]]:
    """Embed ``texts`` with the crew's embedder, from a worker thread.

    Same shape as ``_sync_llm_completion``: submitted to the MAIN loop (the
    DB engine is bound to it), with the request's UserContext re-established
    inside the coroutine. A failed embedding is an error here rather than the
    ``None`` LLMManager returns — DSPy would index a hole.
    """
    from src.services.llm.manager import LLMManager
    from src.utils.user_context import UserContext

    async def _with_context() -> List[Optional[List[float]]]:
        if group_context:
            UserContext.set_group_context(group_context)
        if user_token:
            UserContext.set_user_token(user_token)
        return await asyncio.gather(
            *(
                LLMManager.get_embedding(text, embedder_config=embedder_config)
                for text in texts
            )
        )

    future = asyncio.run_coroutine_threadsafe(_with_context(), loop)
    vectors = future.result(timeout=CALL_TIMEOUT)
    if any(v is None for v in vectors):
        provider = (embedder_config or {}).get("provider") or "Kasal default"
        raise ValueError(
            f"Embedding failed with the crew's embedder ({provider}). Check the "
            "embedder configured on the crew's agents, then align again."
        )
    return [list(v) for v in vectors]


def _make_lm(
    loop: asyncio.AbstractEventLoop,
    model: str,
    group_context: Optional[GroupContext],
    user_token: Optional[str],
) -> Any:
    """A ``dspy.BaseLM`` whose provider is LLMManager; ``model`` is a Kasal key."""
    import dspy
    from openai.types.chat import ChatCompletion, ChatCompletionMessage
    from openai.types.chat.chat_completion import Choice
    from openai.types.completion_usage import CompletionUsage

    class LLMManagerLM(dspy.BaseLM):
        forward_contract = "legacy"

        def forward(self, prompt=None, messages=None, **kwargs):
            # response_format and friends are dropped: the distillation prompt
            # spells out the JSON it wants, and mlflow already retries without
            # structured output for models that reject it.
            request = [dict(m) for m in (messages or [])] or [
                {"role": "user", "content": str(prompt or "")}
            ]
            text = _sync_llm_completion(
                loop,
                messages=request,
                model=model,
                max_tokens=DISTILL_MAX_TOKENS,
                group_context=group_context,
                user_token=user_token,
            )
            now = int(time.time())
            return ChatCompletion(
                id=f"kasal-{now}",
                object="chat.completion",
                created=now,
                model=model,
                choices=[
                    Choice(
                        index=0,
                        finish_reason="stop",
                        message=ChatCompletionMessage(role="assistant", content=text),
                    )
                ],
                usage=CompletionUsage(
                    prompt_tokens=0, completion_tokens=0, total_tokens=0
                ),
            )

    return LLMManagerLM(model=REFLECTION_PLACEHOLDER, cache=False)


def _make_embedder(
    loop: asyncio.AbstractEventLoop,
    embedder_config: Optional[Dict[str, Any]],
    group_context: Optional[GroupContext],
    user_token: Optional[str],
) -> Any:
    """A ``dspy.Embedder`` over the crew's embedder, via LLMManager."""
    import dspy

    def embed(texts: List[str], **_: Any) -> List[List[float]]:
        return _sync_embed(
            loop, list(texts), embedder_config, group_context, user_token
        )

    return dspy.Embedder(embed, batch_size=EMBED_BATCH, caching=False)


def _install_memalign_bridge() -> None:
    """Wrap mlflow's two factories so an armed override wins (idempotent)."""
    from mlflow.genai.judges.optimizers.memalign import optimizer as _optimizer
    from mlflow.genai.judges.optimizers.memalign import utils as _utils

    if not getattr(_utils.construct_dspy_lm, "_kasal_bridge", False):
        original_lm = _utils.construct_dspy_lm

        def bridged_lm(*args: Any, **kwargs: Any) -> Any:
            override = _OVERRIDE.get("lm")
            return override if override is not None else original_lm(*args, **kwargs)

        bridged_lm._kasal_bridge = True  # type: ignore[attr-defined]
        bridged_lm._kasal_original = original_lm  # type: ignore[attr-defined]
        _utils.construct_dspy_lm = bridged_lm

    if not getattr(_optimizer._build_embedder, "_kasal_bridge", False):
        original_embedder = _optimizer._build_embedder

        def bridged_embedder(*args: Any, **kwargs: Any) -> Any:
            override = _OVERRIDE.get("embedder")
            if override is not None:
                return override
            return original_embedder(*args, **kwargs)

        bridged_embedder._kasal_bridge = True  # type: ignore[attr-defined]
        bridged_embedder._kasal_original = original_embedder  # type: ignore[attr-defined]
        _optimizer._build_embedder = bridged_embedder


@contextmanager
def memalign_via_llm_manager(
    loop: asyncio.AbstractEventLoop,
    model: str,
    embedder_config: Optional[Dict[str, Any]],
    group_context: Optional[GroupContext] = None,
    user_token: Optional[str] = None,
) -> Iterator[Dict[str, Any]]:
    """Arm MemAlign, for the block, to distil with ``model`` (a Kasal key) and
    to embed with the crew's embedder — both through LLMManager.

    Yields the constructor kwargs for ``MemAlignOptimizer``: the placeholder
    URIs plus the embedding dimension, MEASURED by embedding one probe rather
    than configured — a wrong dimension would only surface as bad retrieval.
    """
    _install_memalign_bridge()
    embedder = _make_embedder(loop, embedder_config, group_context, user_token)
    embedding_dim = int(embedder("kasal").shape[0])
    lm = _make_lm(loop, model, group_context, user_token)
    with _LOCK:
        _OVERRIDE.update(lm=lm, embedder=embedder)
        try:
            yield {
                "reflection_lm": REFLECTION_PLACEHOLDER,
                "embedding_model": EMBEDDING_PLACEHOLDER,
                "embedding_dim": embedding_dim,
            }
        finally:
            _OVERRIDE.clear()
