"""Keeping the store small and true — Kasal's "sleep-time compute".

* ``passes`` — the passes and when they run: exact dedupe, the LLM merge of
  near-duplicate clusters, ``run_memory_maintenance`` (all four, cheapest
  first), and the per-scope throttle the chat path goes through.
* ``supersession`` — retiring facts a newer record contradicts (validity
  window closes; nothing is deleted).
* ``forgetting`` — deleting records past a retention rule. Off by default.
* ``sweep`` — the scheduled loop that maintains every teamspace by staleness,
  so coverage does not depend on anyone running anything.
"""

from src.services.memory.maintenance.passes import (
    run_memory_maintenance,
    schedule_maintenance_after_writes,
)

__all__ = ["run_memory_maintenance", "schedule_maintenance_after_writes"]
