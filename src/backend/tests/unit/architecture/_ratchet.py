"""Shrink-only baselines shared by the ratchet tests in this directory.

A baseline is a flat JSON object ``{key: count}`` recorded from the tree. The
tests fail when a count GROWS (or a new key appears) and when one SHRINKS without
the baseline being lowered, so ground gained cannot be quietly given back.

Regenerate with the owning test module's ``--update`` entry point, e.g.::

    uv run python tests/unit/architecture/test_ruff_ratchet.py --update

``--update`` only ever LOWERS the baseline: it takes ``min(recorded, current)``
and drops keys that reached zero. It never records a new key or a higher count,
so the command cannot be used to absorb a regression.
"""

from __future__ import annotations

import json
import pathlib
from collections.abc import Mapping


def load(path: pathlib.Path) -> dict[str, int]:
    return json.loads(path.read_text())


def shrunk(baseline: Mapping[str, int], current: Mapping[str, int]) -> dict[str, int]:
    """The baseline lowered to ``current`` wherever the tree improved."""
    return {
        key: min(count, current[key])
        for key, count in sorted(baseline.items())
        if current.get(key, 0) > 0
    }


def save(path: pathlib.Path, data: Mapping[str, int]) -> None:
    path.write_text(json.dumps(dict(sorted(data.items())), indent=2) + "\n")


def update(path: pathlib.Path, current: Mapping[str, int]) -> None:
    before = load(path)
    after = shrunk(before, current)
    save(path, after)
    print(
        f"{path.name}: {len(before)} -> {len(after)} entries, "
        f"{sum(before.values())} -> {sum(after.values())} total"
    )


def grown(baseline: Mapping[str, int], current: Mapping[str, int]) -> list[str]:
    return [
        f"{key}: {baseline.get(key, 0)} -> {count}"
        for key, count in sorted(current.items())
        if count > baseline.get(key, 0)
    ]


def stale(baseline: Mapping[str, int], current: Mapping[str, int]) -> list[str]:
    return [
        f"{key}: {count} -> {current.get(key, 0)}"
        for key, count in sorted(baseline.items())
        if current.get(key, 0) < count
    ]
