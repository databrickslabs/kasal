"""A crew's confirmed labels, kept in the MLflow Prompt Registry.

Why the registry and not assessments on a trace: labels are entered BEFORE a
run, often before the crew has any trace to attach them to, and there is one
label set per crew. The registry is the primitive Kasal already uses for the
judges: versioned (who changed a label, and when, is the version history),
one load and one write, the same grant on Unity Catalog and the same calls on
a local server, and no trace search on the read path.

One prompt per crew AND workspace: ``kasal_labels__crew_<12hex>__<group>`` (on
UC with the ``catalog.schema.`` prefix). The template is JSON
``{"expected_facts": [...], "expected_response": "..."}``; each version is
tagged ``kasal_kind=labels``, ``kasal_crew_id`` and ``kasal_group_id``. A read
returns nothing unless both tags match the caller, so a workspace never reads
another's labels even where they share a registry.

Only labels the user confirmed are stored. Suggestions (``review``) are not.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import TYPE_CHECKING, Dict, Iterable, List, Optional, Tuple

from src.services.prompt_optimization.gepa.registry_errors import (
    is_permission_denied,
    prompt_registry_grant_hint,
)
from src.services.prompt_optimization.judge_registry import latest_version_number
from src.services.prompt_optimization.labels.review import (
    TAG_CREW,
    TAG_GROUP,
    group_tag,
)

if TYPE_CHECKING:
    from mlflow import MlflowClient

PROMPT_PREFIX = "kasal_labels__"
TAG_KIND = "kasal_kind"
KIND = "labels"


@dataclass(frozen=True)
class CrewLabels:
    """The points a crew's deliverable must contain, and/or its expected answer."""

    expected_facts: Tuple[str, ...] = ()
    expected_response: str = ""

    @classmethod
    def of(cls, facts: Iterable[str], response: Optional[str] = "") -> CrewLabels:
        """Trimmed, with blank facts dropped."""
        return cls(
            tuple(f.strip() for f in facts if f and f.strip()),
            (response or "").strip(),
        )

    def is_empty(self) -> bool:
        return not (self.expected_facts or self.expected_response)

    def expectations(self) -> Dict[str, object]:
        """The mlflow expectation fields that are set (a blank one would pass
        Correctness' check and grade against nothing)."""
        row: Dict[str, object] = {}
        if self.expected_facts:
            row["expected_facts"] = list(self.expected_facts)
        if self.expected_response:
            row["expected_response"] = self.expected_response
        return row

    def as_dict(self) -> Dict[str, object]:
        return {
            "expected_facts": list(self.expected_facts),
            "expected_response": self.expected_response,
        }


def _parse(template: object) -> CrewLabels:
    payload = json.loads(str(template))
    if not isinstance(payload, dict):
        raise ValueError("Stored labels are not a JSON object")
    facts = payload.get("expected_facts") or []
    facts_list: List[str] = (
        [str(f) for f in facts] if isinstance(facts, list) else [str(facts)]
    )
    return CrewLabels.of(facts_list, str(payload.get("expected_response") or ""))


class LabelStore:
    """Load and save crew labels in one prompt registry. Blocking — use inside
    ``asyncio.to_thread`` within ``mlflow_session(backend)``."""

    def __init__(
        self,
        registry_uri: str,
        uc_schema: Optional[str] = None,
        client: Optional[MlflowClient] = None,
    ) -> None:
        if client is None:
            import mlflow

            client = mlflow.MlflowClient(registry_uri=registry_uri)
        self._client = client
        self._uc_schema = uc_schema

    def prompt_name(self, crew_id: str, group_id: Optional[str]) -> str:
        crew = str(crew_id).replace("-", "")[:12]
        group = "".join(c if c.isalnum() else "_" for c in group_tag(group_id))
        base = f"{PROMPT_PREFIX}crew_{crew}__{group}"
        return f"{self._uc_schema}.{base}" if self._uc_schema else base

    def load(self, crew_id: str, group_id: Optional[str]) -> Optional[CrewLabels]:
        """The newest confirmed labels, or None (none saved, or not this
        crew's and workspace's)."""
        name = self.prompt_name(crew_id, group_id)
        number = latest_version_number(self._client, name)
        if number is None:
            return None
        version = self._client.load_prompt(name, version=number, allow_missing=True)
        if version is None:
            return None
        tags = dict(getattr(version, "tags", None) or {})
        if tags.get(TAG_CREW) != str(crew_id) or tags.get(TAG_GROUP) != group_tag(
            group_id
        ):
            return None
        return _parse(getattr(version, "template", ""))

    def save(self, crew_id: str, group_id: Optional[str], labels: CrewLabels) -> bool:
        """Store ``labels`` as a new version. False (no write) when unchanged."""
        current = self.load(crew_id, group_id)
        if current == labels or (current is None and labels.is_empty()):
            return False
        name = self.prompt_name(crew_id, group_id)
        try:
            self._client.register_prompt(
                name=name,
                template=json.dumps(labels.as_dict()),
                commit_message="labels confirmed in Kasal's Optimize dialog",
                tags={
                    TAG_KIND: KIND,
                    TAG_CREW: str(crew_id),
                    TAG_GROUP: group_tag(group_id),
                },
            )
        except Exception as exc:
            if self._uc_schema and is_permission_denied(exc):
                raise ValueError(prompt_registry_grant_hint(name)) from exc
            raise
        return True
