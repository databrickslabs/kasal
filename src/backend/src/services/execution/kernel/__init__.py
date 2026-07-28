"""Single-source build logic shared by every execution path.

Agents, tasks, tools, security and the approval gate are built the same way
whether the run is an Agent Builder crew, a Flow Builder workflow, or a chat
turn. Anything path-specific belongs in that path's package, not here — the
point of this module is that there is exactly one implementation.
"""
