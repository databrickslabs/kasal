"""Build-time metadata for the `kasal` distribution.

Reads the runtime dependency list from src/backend/pyproject.toml so the
backend remains the single source of truth — a dependency added there ships in
the next wheel without anyone remembering a second list.
"""

import tomllib
from pathlib import Path

from hatchling.metadata.plugin.interface import MetadataHookInterface


class BackendDependencies(MetadataHookInterface):
    def update(self, metadata: dict) -> None:
        backend = Path(self.root) / "src" / "backend" / "pyproject.toml"
        with backend.open("rb") as f:
            project = tomllib.load(f)["project"]
        metadata["dependencies"] = project["dependencies"]
