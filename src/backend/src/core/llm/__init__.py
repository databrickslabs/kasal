"""LLM infrastructure for kasal.

Layering — who owns what, and why a thing lives where it does:

``core.llm.transport``   The transport. An OpenAI-compatible client plus the
                         protocol-level behaviour every model shares: tool-call
                         loops, streaming, context-window trimming, usage
                         accounting, structured output, LLM events. Model- and
                         tenant-agnostic; knows nothing about kasal's database,
                         auth or catalogue.

``core.llm`` (here)      What hangs off an LLM call without touching the
                         database: usage telemetry, context-limit phrases, JSON
                         extraction, and the subprocess token fallback.

``services.llm``         The CONFIGURATION layer, and it is a service.
                         ``LLMManager`` resolves a kasal model KEY to a
                         configured client — catalogue lookup, per-tenant
                         credentials (OBO → PAT → SPN), endpoint URLs,
                         per-endpoint parameter rules — plus ``embeddings`` and
                         the endpoint ``handlers`` (retry/backoff, fallback,
                         message sanitization, Responses API).

The split is by DATABASE ACCESS, not by subject matter. Anything that reads the
model catalogue or a tenant's API key is per-tenant business logic and belongs
in services; keeping it in ``core`` meant core imported services at module
level, which is the layering inverted. The handlers went with the manager
because it builds them and they call back into it: mutually dependent code
belongs in one layer, and it has to be the layer allowed to touch the database.

Nothing here may re-implement transport behaviour the transport already
provides — that duplication is what this layout exists to prevent.
"""

from src.core.llm.usage_telemetry import register_usage_telemetry

__all__ = ["register_usage_telemetry"]
