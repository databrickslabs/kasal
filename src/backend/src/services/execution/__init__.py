"""
Running an execution: the machinery every path shares.

The three paths (``services/agent_builder``, ``services/flow_builder``,
``services/chat``) differ in what they build; everything about *running* one is
here — dispatch, status, logs, checkpoints, the live event pipe.
"""
