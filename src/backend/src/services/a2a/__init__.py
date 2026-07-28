"""Kasal as an A2A agent — the wire surface over the shared EIL.

A thin adapter: card generation, task operations, and the translation between
Kasal's canonical external shapes and A2A's wire types. Publication, identity,
invocation, task state and the human-in-the-loop round-trip live in
``services/external/`` and are shared with the MCP adapter.

The division of labour is worth stating, because A2A carried most of the
modelling weight: the canonical state vocabulary in ``external/state.py`` IS
A2A's, adopted for both protocols. MCP is the beneficiary — it gets a task
lifecycle, including input_required, that MCP itself does not define.
"""

from src.services.a2a.a2a_server.card import AGENT_VERSION, PROTOCOL_VERSION, build_card
from src.services.a2a.a2a_server.tasks import (
    UnknownSkillError,
    UnknownTaskError,
    cancel_task,
    get_task,
    list_tasks,
    send_message,
)

__all__ = [
    "AGENT_VERSION",
    "PROTOCOL_VERSION",
    "UnknownSkillError",
    "UnknownTaskError",
    "build_card",
    "cancel_task",
    "get_task",
    "list_tasks",
    "send_message",
]
