"""Route MLflow's built-in judges through LLMManager.

A built-in scorer (``Safety``, ``Guidelines``, ...) builds its own prompt,
validates its own inputs and parses its own verdict, then asks mlflow's
adapter factory (``judges.adapters.utils.get_adapter``) for a client to call
the judge model with. Left alone, that client is LiteLLM reading provider
credentials from the environment. Kasal does not route models that way: a
judge model is a Kasal model key, and LLMManager resolves its provider,
endpoint, group-scoped API key and request quirks. That is also how Kasal's
custom judges are called, so both kinds are graded the same way.

The scorer is given an inert placeholder model, ``openai:/kasal-judge--<key>``
(like GEPA's ``openai:/kasal-llm-manager``). The factory is wrapped once,
idempotently, in both places mlflow reads it (``adapters.utils`` and the name
``invocation_utils`` imported from it). The wrapper answers a placeholder with
:class:`KasalJudgeAdapter` and defers everything else to mlflow.

The adapter needs the run's event loop, group and user token. They are carried
in a ContextVar armed around each scoring call by :func:`judge_route`. mlflow
invokes a built-in judge synchronously on the calling thread, so the value is
visible where the adapter runs. A placeholder used outside a route fails loudly
rather than reaching a real provider.

This patches an mlflow internal: ``tests/.../test_bridge_contract.py`` pins the
behaviour against the installed mlflow (3.16).
"""

from __future__ import annotations

import asyncio
import json
import re
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from functools import lru_cache, wraps
from typing import TYPE_CHECKING, Callable, Dict, Iterator, List, Optional, Tuple

from src.services.prompt_optimization.gepa import reflection
from src.utils.user_context import GroupContext

if TYPE_CHECKING:
    from mlflow.genai.judges.adapters.base_adapter import BaseJudgeAdapter
    from mlflow.types.llm import ChatMessage

    Prompt = str | List[ChatMessage]

PLACEHOLDER_PREFIX = "openai:/kasal-judge--"
#: Room for forced-thinking judges (same allowance as Kasal's custom judges).
JUDGE_MAX_TOKENS = 1500
_BRIDGE_FLAG = "_kasal_builtin_judge_bridge"
_VERDICT_LINE = re.compile(
    r"""(?:\**result\**\s*[:=]\s*)?["'*]*(yes|no)["'*.!]*""", re.IGNORECASE
)


@dataclass(frozen=True)
class JudgeRoute:
    """Where a bridged judge call goes: the run's loop, group and user."""

    loop: asyncio.AbstractEventLoop
    group_context: Optional[GroupContext] = None
    user_token: Optional[str] = None


_ROUTE: ContextVar[Optional[JudgeRoute]] = ContextVar(
    "kasal_builtin_judge_route", default=None
)


def placeholder_uri(model_key: str) -> str:
    """The model URI a scorer is given so its calls reach LLMManager."""
    return f"{PLACEHOLDER_PREFIX}{model_key}"


def model_key_of(model_uri: str) -> Optional[str]:
    """The Kasal model key inside a placeholder URI, else None."""
    if not model_uri.startswith(PLACEHOLDER_PREFIX):
        return None
    return model_uri[len(PLACEHOLDER_PREFIX) :] or None


@contextmanager
def judge_route(route: JudgeRoute) -> Iterator[None]:
    """Arm the bridge for the block: placeholder calls go through ``route``."""
    install_bridge()
    token = _ROUTE.set(route)
    try:
        yield
    finally:
        _ROUTE.reset(token)


def _text(content: object) -> str:
    """A chat message's content as text (multimodal parts keep their text)."""
    if isinstance(content, list):
        return "\n".join(
            str(part.get("text", "")) for part in content if isinstance(part, dict)
        )
    return "" if content is None else str(content)


def _as_messages(prompt: Prompt, output_fields: List[str]) -> List[Dict[str, str]]:
    """mlflow's prompt as LLMManager messages, with the JSON contract spelled
    out: LiteLLM would send ``response_format``, LLMManager does not."""
    if isinstance(prompt, str):
        messages = [{"role": "user", "content": prompt}]
    else:
        messages = [{"role": m.role, "content": _text(m.content)} for m in prompt]
    contract = (
        "\n\nReply with ONLY a JSON object with the keys "
        + ", ".join(f'"{name}"' for name in output_fields)
        + '. "result" is your verdict.'
    )
    messages[-1] = {**messages[-1], "content": messages[-1]["content"] + contract}
    return messages


def parse_verdict(reply: str) -> Tuple[str, str]:
    """``(result, rationale)`` from a judge reply.

    JSON first (what the prompt asks for, fenced or not); otherwise a final
    line that is only the verdict ("Yes", "result: no"). A "no" anywhere else
    in prose is not a verdict: read that way, "there is no harmful content"
    would fail the Safety gate. Raises ValueError when there is no verdict.
    """
    from mlflow.genai.judges.utils.parsing_utils import _strip_markdown_code_blocks

    text = (reply or "").strip()
    candidates = [_strip_markdown_code_blocks(text)]
    embedded = re.search(r"\{.*\}", text, re.DOTALL)
    if embedded:
        candidates.append(embedded.group(0))
    for candidate in candidates:
        try:
            parsed = json.loads(candidate, strict=False)
        except json.JSONDecodeError:
            continue
        if isinstance(parsed, dict) and "result" in parsed:
            return str(parsed["result"]), str(parsed.get("rationale", ""))
    last_line = text.splitlines()[-1] if text else ""
    bare = _VERDICT_LINE.fullmatch(last_line.strip())
    if bare:
        return bare.group(1).lower(), text
    raise ValueError(f"Judge reply has no verdict: {text[:200]!r}")


@lru_cache(maxsize=None)
def adapter_class() -> type[BaseJudgeAdapter]:
    """:class:`KasalJudgeAdapter`, defined on first use (mlflow stays lazy)."""
    from mlflow.entities.assessment import Feedback
    from mlflow.entities.assessment_source import (
        AssessmentSource,
        AssessmentSourceType,
    )
    from mlflow.exceptions import MlflowException
    from mlflow.genai.judges.adapters.base_adapter import (
        AdapterInvocationInput,
        AdapterInvocationOutput,
        BaseJudgeAdapter,
    )

    class KasalJudgeAdapter(BaseJudgeAdapter):
        """Answers placeholder judge calls through LLMManager."""

        @classmethod
        def is_applicable(cls, model_uri: str, prompt: Prompt) -> bool:
            return model_key_of(model_uri) is not None

        def _invoke(
            self, input_params: AdapterInvocationInput
        ) -> AdapterInvocationOutput:
            route = _ROUTE.get()
            model = model_key_of(input_params.model_uri)
            if route is None or model is None:
                raise MlflowException(
                    f"Judge model '{input_params.model_uri}' is a Kasal placeholder "
                    "and can only be called inside a Kasal judge route"
                )
            fields = (
                list(input_params.response_format.model_fields)
                if input_params.response_format is not None
                else ["result", "rationale"]
            )
            reply = reflection._sync_llm_completion(
                route.loop,
                messages=_as_messages(input_params.prompt, fields),
                model=model,
                max_tokens=JUDGE_MAX_TOKENS,
                group_context=route.group_context,
                user_token=route.user_token,
            )
            result, rationale = parse_verdict(reply)
            return AdapterInvocationOutput(
                feedback=Feedback(
                    name=input_params.assessment_name,
                    value=result,
                    rationale=rationale,
                    source=AssessmentSource(
                        source_type=AssessmentSourceType.LLM_JUDGE,
                        source_id=input_params.model_uri,
                    ),
                )
            )

    return KasalJudgeAdapter


def _wrap(
    original: Callable[[str, Prompt], BaseJudgeAdapter],
) -> Callable[[str, Prompt], BaseJudgeAdapter]:
    @wraps(original)
    def get_adapter(model_uri: str, prompt: Prompt) -> BaseJudgeAdapter:
        if model_key_of(model_uri) is not None:
            return adapter_class()()
        return original(model_uri, prompt)

    setattr(get_adapter, _BRIDGE_FLAG, True)
    return get_adapter


def install_bridge() -> None:
    """Wrap mlflow's judge adapter factory where it is read (idempotent)."""
    from mlflow.genai.judges.adapters import utils as adapter_utils
    from mlflow.genai.judges.utils import invocation_utils

    for module in (adapter_utils, invocation_utils):
        current = module.get_adapter
        if not getattr(current, _BRIDGE_FLAG, False):
            setattr(module, "get_adapter", _wrap(current))
