"""Native Claude protocol adapter; reuse Kasal's tool loop, budgets and events."""

import json
from collections import OrderedDict
from collections.abc import Generator
from types import SimpleNamespace as NS
from typing import Any

from .anthropic_messages import message_params
from .response_parsing import REDACTED_REASONING


class AnthropicClient:
    def __init__(
        self, *, api_key: Any, base_url: Any, timeout: Any, max_retries: int
    ) -> None:
        from anthropic import Anthropic

        self._native = Anthropic(
            api_key=api_key,
            base_url=base_url.rstrip("/").removesuffix("/v1"),
            timeout=timeout,
            max_retries=max_retries,
        )
        self._signed_blocks: OrderedDict[tuple[str, ...], list[dict[str, Any]]] = (
            OrderedDict()
        )
        self.chat = NS(completions=NS(create=self.create))

    def close(self) -> None:
        self._native.close()

    def _message(self, message: Any) -> tuple[NS, NS, str]:
        blocks = [b.model_dump(exclude_none=True) for b in message.content]
        calls = [b for b in blocks if b["type"] == "tool_use"]
        if calls:
            ids = tuple(b["id"] for b in calls)
            self._signed_blocks[ids] = blocks
            while len(self._signed_blocks) > 128:
                self._signed_blocks.popitem(last=False)
        text = "".join(b.get("text", "") for b in blocks if b["type"] == "text")
        thinking = "".join(
            b.get("thinking", "") for b in blocks if b["type"] == "thinking"
        )
        if not thinking and any(
            b["type"] in ("thinking", "redacted_thinking") for b in blocks
        ):
            thinking = REDACTED_REASONING
        usage = message.usage
        cached = getattr(usage, "cache_read_input_tokens", 0) or 0
        created = getattr(usage, "cache_creation_input_tokens", 0) or 0
        # Native input_tokens excludes both cache buckets; prompt_tokens is the
        # whole prompt, as on every OpenAI-compatible endpoint.
        prompt = usage.input_tokens + cached + created
        tokens = NS(
            prompt_tokens=prompt,
            completion_tokens=usage.output_tokens,
            total_tokens=prompt + usage.output_tokens,
            prompt_tokens_details=NS(cached_tokens=cached),
            cache_creation_input_tokens=created,
        )
        tool_calls = [
            NS(
                index=i,
                id=b["id"],
                type="function",
                function=NS(name=b["name"], arguments=json.dumps(b["input"])),
            )
            for i, b in enumerate(calls)
        ]
        finish = {"tool_use": "tool_calls", "max_tokens": "length"}.get(
            message.stop_reason, "stop"
        )
        return (
            NS(content=text, reasoning_content=thinking, tool_calls=tool_calls),
            tokens,
            finish,
        )

    def create(self, **params: Any) -> Any:
        native = message_params(params, self._signed_blocks)
        if params.get("stream"):
            return self._stream(native)
        message = self._native.messages.create(**native)
        converted, usage, finish = self._message(message)
        return NS(choices=[NS(message=converted, finish_reason=finish)], usage=usage)

    def _stream(self, params: dict[str, Any]) -> Generator[NS, None, None]:
        with self._native.messages.stream(**params) as stream:
            for event in stream:
                if event.type != "content_block_delta":
                    continue
                delta = event.delta
                if delta.type == "text_delta":
                    yield self._chunk(NS(content=delta.text))
                elif delta.type == "thinking_delta":
                    yield self._chunk(NS(reasoning_content=delta.thinking))
            message, usage, finish = self._message(stream.get_final_message())
            # SDK accumulation preserves partial tool JSON and thinking signatures.
            # Tool calls are dispatched only after the complete native message arrives.
            final = NS(tool_calls=message.tool_calls)
            if message.reasoning_content == REDACTED_REASONING:
                final.reasoning_content = REDACTED_REASONING
            yield self._chunk(final, usage, finish)

    @staticmethod
    def _chunk(delta: NS, usage: Any = None, finish: Any = None) -> NS:
        return NS(choices=[NS(delta=delta, finish_reason=finish)], usage=usage)
