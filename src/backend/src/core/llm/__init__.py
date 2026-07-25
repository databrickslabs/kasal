"""LLM infrastructure for kasal.

Layering — who owns what, and why a thing lives where it does:

``kasal_engine.llm``      The transport. An OpenAI-compatible client plus the
                          protocol-level behaviour every model shares: tool-call
                          loops, streaming, context-window trimming, usage
                          accounting, structured output, LLM events. Model- and
                          tenant-agnostic; knows nothing about kasal's database,
                          auth or catalogue.

``src.core.llm`` (here)   The configuration layer. Resolves a kasal model KEY to
                          a configured engine LLM: model catalogue lookup,
                          per-tenant credentials (OBO → PAT → SPN), endpoint URLs,
                          per-endpoint parameter rules. Also the side-channels
                          that hang off LLM calls — usage telemetry, embeddings.

``src.core.llm.handlers`` The endpoint policies. Engine-LLM subclasses that add
                          what one serving endpoint needs (retry/backoff,
                          fallback, message sanitization, Responses API).

``LLMManager``            The public facade over all three, in llm_manager.py.
                          38+ call sites use ``LLMManager.completion``; it stays
                          the stable entry point.

Two directories hold LLM code, and only two: this package for everything
kasal-specific (handlers included, as a subpackage) and ``kasal_engine/llm`` for
the transport. The engine is a separate tree because the dependency runs ONE way
— it imports nothing from ``src``, which is what lets it be tested without a
database and vendored into exported Databricks apps. Folding it in here would
invert that and break both.

Nothing in this package may re-implement transport behaviour the engine already
provides — that duplication is what this layout exists to prevent.
"""

from src.core.llm.usage_telemetry import register_usage_telemetry

__all__ = ["register_usage_telemetry"]
