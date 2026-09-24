"""LLM — the class kasal's llm_manager instantiates.

Authored module; surface validated against the kasal_engine datamodel.
An OpenAI-compatible provider (Chat Completions / Responses) with
provider-prefix normalization: kasal builds models like
``databricks/model-name`` or ``openai/gpt-4o`` and points base_url at the
right gateway; the prefix selects nothing here beyond the provider label,
since every kasal endpoint speaks the OpenAI protocol.
"""

from typing import Any

from pydantic import ConfigDict, model_validator

from .completion import OpenAICompletion

_KNOWN_PREFIXES = (
    "openai",
    "databricks",
    "anthropic",
    "gemini",
    "azure",
    "hosted_vllm",
    "custom",
)


class LLM(OpenAICompletion):
    model_config = ConfigDict(hide_input_in_errors=True)

    @property
    def client(self) -> Any:
        if self.provider != "anthropic":
            return super().client
        if self._client is None:
            from .anthropic_client import AnthropicClient

            self._client = AnthropicClient(
                api_key=self.api_key,
                base_url=self.base_url or self.api_base,
                timeout=self.timeout,
                max_retries=self.max_retries,
            )
        return self._client

    @model_validator(mode="before")
    @classmethod
    def _split_provider_prefix(cls, data: Any) -> Any:
        if isinstance(data, dict):
            model = data.get("model")
            if isinstance(model, str) and "/" in model and not data.get("provider"):
                prefix = model.partition("/")[0]
                if prefix in _KNOWN_PREFIXES:
                    data = dict(data)
                    data["provider"] = prefix
                    # Databricks serving endpoints (like OpenAI) take the BARE
                    # endpoint name in the request body — the model field is sent
                    # verbatim over the OpenAI SDK, and litellm (which used to
                    # consume this prefix) is no longer on the path. Leaving
                    # "databricks/" in the name makes Databricks look up an
                    # endpoint literally called "databricks/<model>" → 404
                    # ENDPOINT_NOT_FOUND on /serving-endpoints (400 on the gateway).
                    if prefix in ("openai", "databricks"):
                        data["model"] = model.partition("/")[2]
            if data.get("provider") in ("anthropic", "gemini"):
                data = dict(data)
                provider = data["provider"]
                label = provider.capitalize()
                # Also normalize when the caller supplies an explicit provider.
                data["model"] = str(data["model"]).removeprefix(f"{provider}/")
                if not (data.get("base_url") or data.get("api_base")):
                    raise ValueError(f"{label} requires an explicit API endpoint")
                if not data.get("api_key"):
                    raise ValueError(f"{label} requires its own API key")
        return data
