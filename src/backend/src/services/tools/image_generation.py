"""
ImageGenerationTool — text to image over an OpenAI-compatible images endpoint.

Replaces the old DallETool, which hardcoded ``dall-e-3`` and that model's
parameter vocabulary (``quality: standard|hd``, ``256x256``). Three things
changed:

- **The model is configuration, not a constant.** Default is ``gpt-image-1``;
  set ``model`` in the tool config to use anything else the endpoint serves.
- **The endpoint is configuration too.** ``OPENAI_BASE_URL`` already redirected
  the old tool, but nothing said so. A self-hosted or gateway-fronted
  OpenAI-compatible image endpoint works without touching this file.
- **Both response shapes are handled.** Current image models return
  ``b64_json``; the older ones returned a short-lived ``url``. The old tool read
  ``url`` only, so it would have returned "Failed to generate image" against any
  current model.

The API key is read from the environment because that is how every tool in this
package gets one: ``ToolFactory.initialize`` pre-loads it from ApiKeysService
(group-scoped, encrypted at rest) into ``os.environ`` before any tool runs.
"""

import json
import logging
import os
from typing import Any, Literal, Optional

from pydantic import BaseModel, Field

from .base import BaseTool, EnvVar
from .web_fetch import _http_json

logger = logging.getLogger(__name__)

#: How much of a base64 payload to hand back to the agent. A 1024x1024 PNG is
#: ~1-2 MB base64 — pasting that into the conversation blows the context window
#: and costs a fortune in tokens for something the model cannot read anyway.
_B64_PREVIEW_CHARS = 256


class ImagePromptSchema(BaseModel):
    """Input for the image generation tool."""

    image_description: str = Field(
        description="What the image should show. Be specific: subject, style, "
        "composition, colours, mood."
    )


class ImageGenerationTool(BaseTool):
    name: str = "Image Generation Tool"
    description: str = (
        "Generates an image from a text description. Returns the image as a "
        "base64 payload or a URL, depending on the model. Use for illustrations, "
        "concept art, diagrams and visual mockups."
    )
    args_schema: type[BaseModel] = ImagePromptSchema

    #: Any model the configured endpoint serves. gpt-image-1 is OpenAI's current
    #: image model and the successor to dall-e-3.
    model: str = "gpt-image-1"
    size: Literal["auto", "1024x1024", "1536x1024", "1024x1536"] | None = "1024x1024"
    quality: Literal["auto", "low", "medium", "high"] | None = "auto"
    n: int = 1

    env_vars: list[EnvVar] = Field(
        default_factory=lambda: [
            EnvVar(
                name="OPENAI_API_KEY",
                description="API key for the image generation endpoint",
                required=True,
            ),
        ]
    )

    def _run(self, **kwargs: Any) -> str:
        image_description = kwargs.get("image_description")
        if not image_description:
            return "Image description is required."

        api_key = os.environ.get("OPENAI_API_KEY")
        if not api_key:
            return (
                "No API key configured for image generation. Add OPENAI_API_KEY "
                "in the API Keys settings."
            )

        base_url = os.environ.get("OPENAI_BASE_URL", "https://api.openai.com/v1")
        payload: dict[str, Any] = {
            "model": self.model,
            "prompt": image_description,
            "n": self.n,
        }
        # Only send the optional knobs when set: models disagree about which
        # values they accept, and an unsupported one is a hard 400.
        if self.size:
            payload["size"] = self.size
        if self.quality:
            payload["quality"] = self.quality

        try:
            response = _http_json(
                f"{base_url}/images/generations",
                payload,
                {"Authorization": f"Bearer {api_key}"},
                timeout=120,
            )
        except (RuntimeError, ValueError) as e:
            logger.warning(f"[image_generation] request failed: {e}")
            return f"Failed to generate image: {e}"

        data = response.get("data") or []
        if not data:
            return "The endpoint returned no image."

        first = data[0]
        result: dict[str, Any] = {"model": self.model}

        if first.get("url"):
            result["image_url"] = first["url"]
        elif first.get("b64_json"):
            b64 = first["b64_json"]
            # The agent gets a truncated marker; the full payload would swamp the
            # context. Callers that need the bytes read them off the trace event.
            result["image_b64_preview"] = b64[:_B64_PREVIEW_CHARS]
            result["image_b64_bytes"] = len(b64)
            result["note"] = (
                "Image returned inline as base64 and truncated here. "
                "The full payload is on the tool-result event."
            )
        else:
            return "The endpoint returned an image in an unrecognized shape."

        if first.get("revised_prompt"):
            result["revised_prompt"] = first["revised_prompt"]

        return json.dumps(result)
