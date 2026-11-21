"""Item IDs for a serialized Genie space (pure).

The Genie API requires every example, snippet, question and instruction to carry
an ID, and orders each list by it. Items written by hand in the YAML files may
lack one; ``ensure_ids`` assigns fresh 32-character hex IDs and sorts the lists.
"""

from __future__ import annotations

import copy
import uuid

#: Paths (from the top of ``serialized_space``) of the lists that carry IDs.
ID_LIST_PATHS: tuple[tuple[str, ...], ...] = (
    ("config", "sample_questions"),
    ("instructions", "text_instructions"),
    ("instructions", "example_question_sqls"),
    ("instructions", "sql_functions"),
    ("instructions", "join_specs"),
    ("instructions", "sql_snippets", "filters"),
    ("instructions", "sql_snippets", "expressions"),
    ("instructions", "sql_snippets", "measures"),
    ("benchmarks", "questions"),
)


def _item_id(item: object) -> str:
    if isinstance(item, dict):
        value = item.get("id")
        return value if isinstance(value, str) else ""
    return ""


def _holder(root: dict[str, object], path: tuple[str, ...]) -> dict[str, object] | None:
    node: object = root
    for key in path:
        if not isinstance(node, dict):
            return None
        node = node.get(key)
    return node if isinstance(node, dict) else None


def ensure_ids(serialized: dict[str, object]) -> tuple[dict[str, object], int]:
    """A copy of ``serialized`` with every listed item given an ID, plus the count.

    The input is not modified. Each ID list is sorted ascending by ID.
    """
    result: dict[str, object] = copy.deepcopy(serialized)
    assigned = 0
    for path in ID_LIST_PATHS:
        holder = _holder(result, path[:-1])
        if holder is None:
            continue
        items = holder.get(path[-1])
        if not isinstance(items, list):
            continue
        for item in items:
            if isinstance(item, dict) and not _item_id(item):
                item["id"] = uuid.uuid4().hex
                assigned += 1
        holder[path[-1]] = sorted(items, key=_item_id)
    return result, assigned
