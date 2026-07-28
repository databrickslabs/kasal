"""
Flow Builder — sequencing crews into a workflow, named for what the UI calls it.

A graph of crews with routers, state and checkpoints, built here and run in a
SUBPROCESS (``services/process_flow_executor.py``). ``flow_runner_service``
drives a run; ``backend_flow`` wraps the engine Flow; ``modules/`` holds the
build pieces (``flow_builder`` constructs the Flow object, ``flow_methods``
generates the per-node methods, ``flow_processors`` handles results).

Subprocess boundary: same rule as agent_builder — verify with a REAL subprocess
import after moving anything, not just the in-process suite.

Naming: "Flow Builder" in the UI, ``appMode="flow"`` in the frontend,
``execution_type="flow"`` on the wire and in execution_history rows. The wire
value stays; renaming it needs a data migration.
"""
