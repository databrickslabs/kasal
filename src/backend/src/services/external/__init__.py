"""The External Invocation Layer (EIL).

Everything about exposing Kasal to callers outside the workspace that is NOT
wire format. Two thin protocol adapters sit on top — ``services/mcp_server/``
and ``services/a2a/`` — holding transport, wire schemas, discovery documents and
streaming translation, and nothing else.

    MCP clients ──►┌──────────────┐        ┌──────────────┐◄── A2A agents
                   │ MCP adapter  │        │ A2A adapter  │
                   └──────┬───────┘        └──────┬───────┘
                          └────────┬──────────────┘
                        ┌──────────▼───────────┐
                        │  External Invocation │  publication · identity
                        │  Layer               │  state · (invocation, HITL,
                        │                      │   artifacts, limits to come)
                        └──────────┬───────────┘
                        ┌──────────▼───────────┐
                        │ services/execution/  │  unchanged
                        └──────────────────────┘

The reason for the shared core is not tidiness. Publication, identity, async
handles, task state and HITL are the same problems in both protocols, and they
are precisely the problems where a mistake leaks tenant data. Written twice,
they drift the first time one is patched and the other is not — and a fix
applied to one adapter and not the other is a bug nobody can reproduce. Written
once, the cross-tenant isolation suite is written once too.

**If a behaviour needs changing in both adapters, it was in the wrong layer.**

This package is a LAUNCHER, not a capability package: it starts executions, so
per ``services/CLAUDE.md`` it is among the few modules allowed to import the
execution layer.
"""

from src.services.external.identity import (
    ExternalAuthError,
    ExternalCaller,
    resolve_caller,
)
from src.services.external.publication import PublicationService
from src.services.external.state import (
    TERMINAL_STATES,
    ExternalTaskState,
    is_terminal,
    to_external_state,
)

__all__ = [
    "ExternalAuthError",
    "ExternalCaller",
    "ExternalTaskState",
    "PublicationService",
    "TERMINAL_STATES",
    "is_terminal",
    "resolve_caller",
    "to_external_state",
]
