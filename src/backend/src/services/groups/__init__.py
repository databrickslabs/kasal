"""
Tenancy: groups, their members, and what each group may use.

A group is the isolation boundary for every other service — ``group_id`` on the
row, ``GroupContext`` on the call. ``group_tools`` is per-group tool enablement.

Any service method that reads or writes tenant data takes a ``GroupContext`` and
passes its ``group_id`` down to the repository filter. One that does not is a
data-leak bug, not a style problem.
"""
