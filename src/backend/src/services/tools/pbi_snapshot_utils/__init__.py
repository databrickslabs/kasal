"""Opt-in snapshot of Power BI-only tables into Unity Catalog.

A PBI-only table (Excel, SharePoint, Web, a typed literal) has no warehouse
source. When ``pbi_snapshot_tables`` is on, its rows are copied out of Power BI
(``EVALUATE``, paged) into ``snap_<table>`` in the run's source-layer schema.
The copy is a snapshot: it is not refreshed. See ``limits`` for the settings,
``snapshot_runner`` for the per-table checks, and ``adapters`` for the real
executors.
"""

from src.services.tools.pbi_snapshot_utils.limits import SnapshotSettings
from src.services.tools.pbi_snapshot_utils.snapshot_runner import (
    snapshot_leaf_name,
    snapshot_pbi_only_tables,
)

__all__ = ["SnapshotSettings", "snapshot_leaf_name", "snapshot_pbi_only_tables"]
