"""InternalInstructor — structured output without the instructor library.

Authored module; surface validated against the kasal_engine datamodel.
Kills kasal's crewai_instructor_patch.py by design (native requirements
#3 and #4):

- **Per-call credentials**: pass ``api_key``/``base_url`` and the wrapped
  LLM is copied with them — no client mutation, no global state.
- **Schema post-processing**: pass ``schema_transform`` (a callable over the
  JSON schema) to adapt schemas per provider; ``strip_numeric_bounds`` is
  the transform kasal's patch hardcoded (drops numeric range keywords).

Generation is prompt-based JSON with one validation-feedback retry — it
works against every OpenAI-compatible endpoint kasal drives, with no
instructor-mode juggling.
"""

import json
import logging
from collections.abc import Callable
from typing import Any, Generic, TypeVar

from pydantic import BaseModel

from src.services.execution.runtime.executor import extract_json_dict
from .base import BaseLLM

logger = logging.getLogger(__name__)

T = TypeVar("T", bound=BaseModel)


def strip_numeric_bounds(schema: dict[str, Any]) -> dict[str, Any]:
    """Recursively drop numeric range keywords some providers reject."""
    banned = {"minimum", "maximum", "exclusiveMinimum", "exclusiveMaximum", "multipleOf"}

    def clean(node: Any) -> Any:
        if isinstance(node, dict):
            return {k: clean(v) for k, v in node.items() if k not in banned}
        if isinstance(node, list):
            return [clean(item) for item in node]
        return node

    return clean(schema)


class InternalInstructor(Generic[T]):
    def __init__(
        self,
        content: str,
        model: type[T],
        agent: Any | None = None,
        llm: Any | None = None,
        *,
        api_key: str | None = None,
        base_url: str | None = None,
        instructor_mode: str | None = None,
        schema_transform: Callable[[dict[str, Any]], dict[str, Any]] | None = None,
    ) -> None:
        self.content = content
        self.model = model
        self.agent = agent
        self.llm = llm or (
            (agent.function_calling_llm or agent.llm) if agent else None
        )
        self.instructor_mode = instructor_mode  # accepted for compatibility
        self.schema_transform = schema_transform

        if (api_key or base_url) and isinstance(self.llm, BaseLLM):
            overrides: dict[str, Any] = {}
            if api_key:
                overrides["api_key"] = api_key
            if base_url:
                overrides["base_url"] = base_url
            # per-call credentials: copy, never mutate the shared LLM
            self.llm = self.llm.model_copy(update=overrides)
            if isinstance(self.llm, BaseLLM):
                object.__setattr__(self.llm, "_client", None)

    def _extract_provider(self) -> str:
        if self.llm is not None and getattr(self.llm, "provider", None):
            return self.llm.provider
        if isinstance(self.llm, str):
            return self.llm.partition("/")[0] or "openai"
        if self.llm is not None and hasattr(self.llm, "model"):
            return self.llm.model.partition("/")[0] or "openai"
        return "openai"

    def to_pydantic(self) -> T:
        if self.llm is None or isinstance(self.llm, str):
            raise ValueError(
                "InternalInstructor needs a configured LLM object "
                "(kasal builds these via its LLM manager)."
            )
        schema = self.model.model_json_schema()
        if self.schema_transform is not None:
            schema = self.schema_transform(schema)

        prompt = (
            f"{self.content}\n\n"
            "Return ONLY a valid JSON object matching this schema "
            f"(no prose, no markdown fences):\n{json.dumps(schema, indent=2)}"
        )
        last_error: Exception | None = None
        messages = [{"role": "user", "content": prompt}]
        for _attempt in range(2):
            raw = self.llm.call(messages)
            parsed = extract_json_dict(raw)
            if parsed is not None:
                try:
                    return self.model.model_validate(parsed)
                except Exception as e:
                    last_error = e
            messages = [
                {"role": "user", "content": prompt},
                {"role": "assistant", "content": raw},
                {
                    "role": "user",
                    "content": (
                        "That was not a valid JSON object for the schema"
                        + (f" ({last_error})" if last_error else "")
                        + ". Return ONLY the corrected JSON object."
                    ),
                },
            ]
        raise ValueError(
            f"Could not produce structured output for {self.model.__name__}"
            + (f": {last_error}" if last_error else "")
        )

    def to_json(self) -> str:
        return self.to_pydantic().model_dump_json(indent=2)
