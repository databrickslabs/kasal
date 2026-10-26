"""The publication registry — one record per crew or flow, and who may reach it.

It used to live under ``services/external/``, beside ``identity.py`` and
``invocation.py``, because external agents were the only thing that could reach a
published capability. That stopped being true when ChatMode's "Use existing" mode
started routing over the same rows: ``chat`` is a protocol on the same record,
reaching the same registry through the same group filter, and it exposes nothing
outside the workspace.

Leaving it under ``external/`` would have been a standing invitation to the one
mistake that surface cannot afford — wrapping an internal ``GroupContext`` in an
``ExternalCaller`` so it fits the neighbouring API. ``identity.py`` opens with
"An MCP client or an A2A agent is, by definition, outside the workspace"; a chat
user is not, and the two must not share a caller type.

The registry itself is protocol-NEUTRAL and lives here on its own terms. The
external trust boundary stays in ``services/external/``, where it belongs.
"""

from src.services.publications.publication import PublicationService

__all__ = ["PublicationService"]
