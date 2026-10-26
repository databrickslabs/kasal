"""A2UI: turning an agent's answer into a renderable surface.

Two halves that must not be confused, now in one place:

- ``compose.py`` — the composer. **Stdlib only.** It is vendored VERBATIM into
  every exported Databricks App, which has no ``src`` package at all, so a
  single ``from src.…`` import here would ship a broken export. The LLM is
  always injected by the caller as an ``llm_call`` callable, never imported.
  ``tests/unit/services/a2ui/test_compose_portability.py`` enforces this — the
  constraint used to be carried by the directory name ``shared/``, which said
  nothing to anyone who had not been told what it meant.
- ``runner.py`` — the live-app wiring: build the LLM through ``LLMManager``,
  resolve the workspace's UIConfigurator, run the composer off the event loop,
  and record what it decided on the run's trace.

This lived in ``src/shared/`` (a package that existed for this one module) and
``engines/kasal/kernel/``. Neither is right: composing a surface is a capability
— the crew path, the chat path, an exported app and, in future, anything else
with an answer to render all want it, and none of them are the engine.
"""

from src.services.a2ui.runner import (
    a2ui_enabled,
    compose_surface,
    crew_intent_text,
    wrap_result_with_surface,
)

__all__ = [
    "a2ui_enabled",
    "compose_surface",
    "crew_intent_text",
    "wrap_result_with_surface",
]
