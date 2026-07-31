"""
Agent Builder — the crew path, named for what the UI calls it.

A crew of agents over a task graph, built here and run in a SUBPROCESS
(``agent_builder/process_executor.py``) so a crashing run cannot take the API
with it. ``crew_preparation`` assembles it; the adapters translate stored
agent/task rows into engine objects via ``services/execution/kernel``.

Subprocess boundary: this code is imported inside a spawned interpreter. After
moving or renaming anything here, verify with a REAL subprocess import — an
in-process test suite cannot see a child's import failure.

Naming: "Agent Builder" in the UI, ``appMode="crew"`` in the frontend, and
``execution_type="crew"`` on the wire and in execution_history rows. The wire
value stays as-is; renaming it needs a data migration.
"""
