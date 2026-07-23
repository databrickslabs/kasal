#!/usr/bin/env python3
"""
Clear the pre-seeded 'bi-specialist' workspace's local crew memory.

WHY: The BI pipeline crews used to run with memory enabled, which auto-saved each
task's output — including the ~174K-char pipeline-config JSON — into the group's
unified local memory store and recalled it workspace-wide into every later agent
prompt. That overflowed the model context window and stalled runs. Memory is now
disabled for these crews (seed + startup self-heal in db/session.py), but the
blobs already written to the local store keep being recalled by any other
memory-enabled crew in the same workspace until purged. This script removes them.

SCOPE: Only the DEFAULT (local, LanceDB/SQLite) memory store for the given group
is deleted — the directory ``~/.kasal/memory/kasal_default_<group>`` (or under
$KASAL_MEMORY_DIR). Databricks Vector Search / Lakebase backends are NOT touched
by this script; for those, drop/recreate the index via the memory-backend admin
API (see memory_backend_router.py) since Vector Search has no bulk delete.

USAGE:
    python -m src.scripts.clear_bi_specialist_memory              # clears 'bi-specialist'
    python -m src.scripts.clear_bi_specialist_memory my-group     # clears a specific group
    python -m src.scripts.clear_bi_specialist_memory --dry-run    # show target, delete nothing
"""

import shutil
import sys
import logging

from src.utils.memory_paths import local_memory_store_dir

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DEFAULT_GROUP = "bi-specialist"


def clear_local_memory(group_id: str, dry_run: bool = False) -> bool:
    """Delete the default-local memory store directory for ``group_id``.

    Returns True if a directory was found (and removed unless dry_run).
    """
    store_dir = local_memory_store_dir(group_id)
    if not store_dir.exists():
        logger.info("No local memory store found for group '%s' at %s — nothing to clear.",
                    group_id, store_dir)
        return False
    if dry_run:
        logger.info("[dry-run] Would delete local memory store for '%s': %s",
                    group_id, store_dir)
        return True
    shutil.rmtree(store_dir)
    logger.info("Deleted local memory store for group '%s': %s", group_id, store_dir)
    return True


def main() -> int:
    args = [a for a in sys.argv[1:]]
    dry_run = "--dry-run" in args
    positional = [a for a in args if not a.startswith("--")]
    group_id = positional[0] if positional else DEFAULT_GROUP
    clear_local_memory(group_id, dry_run=dry_run)
    logger.info(
        "Done. Note: this clears LOCAL (default) memory only. For a Databricks "
        "Vector Search / Lakebase memory backend, drop & recreate the index via "
        "the memory-backend admin API instead."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
