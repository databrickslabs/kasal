"""Deploy planning for a Genie space (pure apart from the injected HTTP reads).

Target resolution, in order: the ``space_id`` in ``header.yaml`` when that space
still exists in this workspace; else the one space whose title matches; else a
new space. Two spaces with the same title are refused, not guessed between.
"""

from __future__ import annotations

import json
from dataclasses import dataclass

from src.services.tools.genie_space_sync_utils.genie_api import (
    GenieHttp,
    get_space,
    list_spaces,
)
from src.services.tools.genie_space_sync_utils.space_files import SpaceFilesError


@dataclass(frozen=True)
class DeployTarget:
    operation: str  # "create" or "update"
    space_id: str  # empty for "create"


def resolve_target(http: GenieHttp, header_space_id: str, title: str) -> DeployTarget:
    """Decide whether the deploy creates a space or updates an existing one.

    Only reads: safe to call in a dry run.
    """
    if (
        header_space_id
        and get_space(http, header_space_id, serialized=False) is not None
    ):
        return DeployTarget("update", header_space_id)
    matches = [
        str(space["space_id"])
        for space in list_spaces(http)
        if space.get("title") == title and space.get("space_id")
    ]
    if len(matches) > 1:
        raise SpaceFilesError(
            f"{len(matches)} spaces are titled {title!r}; set space_id in header.yaml"
        )
    if matches:
        return DeployTarget("update", matches[0])
    return DeployTarget("create", "")


def build_payload(
    header: dict[str, object], serialized: dict[str, object], warehouse_id: str
) -> dict[str, object]:
    """The create / update body the Genie API expects."""
    description = header.get("description")
    return {
        "warehouse_id": warehouse_id,
        "title": str(header.get("title") or "").strip(),
        "description": description if isinstance(description, str) else "",
        "serialized_space": json.dumps(serialized),
    }
