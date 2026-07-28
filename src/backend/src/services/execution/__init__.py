"""
Running an execution: the machinery every path shares.

The three paths (``services/chat``, ``services/agent_builder``,
``services/flow_builder``) differ in what they BUILD. Everything about running
one is here:

- ``service``        — the API-facing orchestrator (create, run, stop, resume)
- ``kasal_service``  — the layer between it and the engine hub
- ``engine_service`` — the hub: resolve the path and delegate
- ``status`` / ``history`` / ``naming`` / ``cleanup`` / ``broadcast``
- ``event_pipe``     — the child→parent live SSE lane
- ``checkpoint``     — task checkpoints + the resume payload
- ``thread_executor``— the in-process threadpool runner
- ``kernel/`` ``config/`` ``logs/`` ``subprocess_bootstrap``

See CLAUDE.md in this directory before changing anything that a subprocess
imports.
"""
