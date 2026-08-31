"""Judges live in the MLflow Prompt Registry.

A judge is three strings — a name, plain-language instructions with
``{{ outputs }}``, and a Kasal model key — and it is invoked ON DEMAND through
LLMManager (``crew_runner`` while GEPA scores candidates, ``memalign_bridge``
while MemAlign distils grades). The Prompt Registry is MLflow's primitive for
exactly that kind of thing: a versioned, governed definition. On Databricks it
is Unity Catalog — the same schema grant GEPA's crew prompts already need — and
on a local server it is the OSS registry.

Why not ``make_judge().register()`` (the scorer registry): on Databricks that
registry IS the experiment's monitoring job. Every write PATCHes the job's
scheduled-scorer list (403 unless the app SP holds job permissions), and an
existing name cannot be re-registered at all ("has already been registered"),
so create, assign, update, delete and MemAlign's re-register all failed there.
Kasal never wanted monitoring — judges are only ever run from the Optimize
dialog. See issue #7.

Naming: prompt ``kasal_judge__<full_name>``, where ``full_name`` is the judge's
identity everywhere else (assessment names on graded traces, the dialog): a
bare name for a library judge, ``crew_<12hex>__<name>`` for a crew's copy. On
UC the three-level ``catalog.schema.`` prefix is added.

Each version carries tags ``kasal_model`` (the Kasal key), ``kasal_crew`` (the
crew id or empty) and ``kasal_kind=judge``. MemAlign's learned guidelines are
stored inside the template in MLflow's own block format, so a judge's history
reads naturally in the registry UI.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from typing import Any, Dict, Iterator, List, Optional, Tuple

from src.services.prompt_optimization.gepa.registry_errors import (
    is_permission_denied,
    prompt_registry_grant_hint,
)

logger = logging.getLogger(__name__)

PROMPT_PREFIX = "kasal_judge__"
TAG_KIND = "kasal_kind"
TAG_MODEL = "kasal_model"
TAG_CREW = "kasal_crew"
KIND = "judge"
#: Prompts per search page; judges number in the tens, not thousands.
PAGE_SIZE = 200
#: Header of the guideline block MemAlign folds into a judge's instructions.
GUIDELINES_HEADER = "Distilled Guidelines"

_CREW_NAME = re.compile(r"^crew_([0-9a-f]{1,12})__(.+)$")


def split_crew_name(full_name: str) -> Tuple[Optional[str], str]:
    """``(crew id, display name)`` — crew id is None for a library judge."""
    match = _CREW_NAME.match(full_name)
    if match:
        return match.group(1), match.group(2)
    return None, full_name


def uc_schema_of(registry_uri: str, sample_prompt_name: str) -> Optional[str]:
    """``catalog.schema`` from a three-level UC prompt name (the one
    ``_resolve_registry`` returns); None for a local registry."""
    if registry_uri != "databricks-uc" or "." not in sample_prompt_name:
        return None
    return sample_prompt_name.rsplit(".", 1)[0]


def with_guidelines(base: str, guidelines: List[str]) -> str:
    """Render instructions + guidelines exactly as mlflow's
    ``MemoryAugmentedJudge.instructions`` does."""
    text = base.rstrip()
    if not guidelines:
        return text
    block = "".join(f"  - {g}\n" for g in guidelines)
    return f"{text}\n\n{GUIDELINES_HEADER} ({len(guidelines)}):\n{block}"


def strip_guidelines(instructions: str) -> Tuple[str, List[str]]:
    """Inverse of :func:`with_guidelines`: ``(base instructions, guidelines)``."""
    head, marker, tail = instructions.partition(f"\n\n{GUIDELINES_HEADER} (")
    if not marker:
        return instructions, []
    lines = tail.split("\n", 1)[1] if "\n" in tail else ""
    guidelines = [
        line.strip()[2:].strip()
        for line in lines.splitlines()
        if line.strip().startswith("- ")
    ]
    return head.rstrip(), [g for g in guidelines if g]


@dataclass
class JudgeSpec:
    """A judge as stored: its identity, criteria and Kasal model key."""

    full_name: str
    instructions: str
    model: Optional[str]
    version: Optional[int] = None

    @property
    def name(self) -> str:
        # The registry identity — what the runner and alignment read, and
        # what the scorer objects' ``.name`` used to be.
        return self.full_name

    @property
    def crew_id(self) -> Optional[str]:
        return split_crew_name(self.full_name)[0]

    @property
    def display_name(self) -> str:
        return split_crew_name(self.full_name)[1]

    def as_dict(self) -> Dict[str, Any]:
        return {
            "name": self.display_name,
            "full_name": self.full_name,
            "crew_id": self.crew_id,
            "model": self.model,
            # Full text (bounded): the edit dialog round-trips this — a
            # truncated copy would corrupt the judge on save.
            "instructions": (self.instructions or "")[:4000],
        }


class JudgeRegistry:
    """Judge CRUD over one MLflow prompt registry.

    Blocking — construct and use inside ``asyncio.to_thread``, within
    ``mlflow_session(backend)`` so the calls carry the backend's auth.
    """

    def __init__(
        self, registry_uri: str, uc_schema: Optional[str] = None, client: Any = None
    ):
        self._registry_uri = registry_uri
        self._uc_schema = uc_schema
        if client is None:
            import mlflow

            client = mlflow.MlflowClient(registry_uri=registry_uri)
        self._client = client

    # ------------------------------------------------------------- naming
    def prompt_name(self, full_name: str) -> str:
        base = f"{PROMPT_PREFIX}{full_name}"
        return f"{self._uc_schema}.{base}" if self._uc_schema else base

    @staticmethod
    def full_name_of(prompt_name: str) -> Optional[str]:
        """Judge identity from a registry prompt name; None for other prompts
        (GEPA's crew prompts share the schema)."""
        leaf = prompt_name.rsplit(".", 1)[-1]
        if not leaf.startswith(PROMPT_PREFIX):
            return None
        return leaf[len(PROMPT_PREFIX) :] or None

    def _search_filter(self) -> str:
        if self._uc_schema:
            catalog, schema = self._uc_schema.split(".", 1)
            return f"catalog = '{catalog}' AND schema = '{schema}'"
        return f"name LIKE '{PROMPT_PREFIX}%'"

    def _prompt_names(self) -> Iterator[str]:
        token = None
        while True:
            page = self._client.search_prompts(
                filter_string=self._search_filter(),
                max_results=PAGE_SIZE,
                page_token=token,
            )
            for prompt in page:
                yield prompt.name
            token = getattr(page, "token", None)
            if not token:
                return

    # --------------------------------------------------------------- reads
    def list(self, crew_prefix: Optional[str] = None) -> List[JudgeSpec]:
        """Every judge, or only one crew's (``crew_prefix`` = 'crew_<id>__')."""
        out: List[JudgeSpec] = []
        for prompt_name in self._prompt_names():
            full_name = self.full_name_of(prompt_name)
            if not full_name or (crew_prefix and not full_name.startswith(crew_prefix)):
                continue
            spec = self.load(full_name)
            if spec is not None:
                out.append(spec)
        return out

    def _latest_version_number(self, name: str) -> Optional[int]:
        """Newest version number of a prompt, or None when it does not exist.

        Resolved explicitly rather than via ``load_prompt(name)``: without a
        version that call asks the store for an ALIAS named "latest", which
        the OSS server treats as "newest" but Unity Catalog looks up literally
        — no such alias, so every judge read as missing there. The two stores
        also return different shapes here (a list of versions vs. a response
        proto carrying ``prompt_versions``); both are read.
        """
        try:
            result = self._client.search_prompt_versions(name, max_results=PAGE_SIZE)
        except Exception as exc:  # noqa: BLE001 — UC raises for an unknown prompt
            text = str(exc).lower()
            if "not exist" in text or "not found" in text:
                return None
            raise
        items = getattr(result, "prompt_versions", None)
        if items is None:
            items = list(result or [])
        numbers = []
        for item in items:
            try:
                numbers.append(int(getattr(item, "version", None)))
            except (TypeError, ValueError):
                continue
        return max(numbers) if numbers else None

    def load(self, full_name: str) -> Optional[JudgeSpec]:
        """The judge's newest version, or None."""
        name = self.prompt_name(full_name)
        number = self._latest_version_number(name)
        if number is None:
            return None
        version = self._client.load_prompt(name, version=number, allow_missing=True)
        return None if version is None else self._spec(full_name, version)

    @staticmethod
    def _spec(full_name: str, version: Any) -> JudgeSpec:
        tags = dict(getattr(version, "tags", None) or {})
        template = getattr(version, "template", "")
        number = getattr(version, "version", None)
        return JudgeSpec(
            full_name=full_name,
            instructions=template if isinstance(template, str) else str(template),
            model=tags.get(TAG_MODEL) or None,
            version=int(number) if number is not None else None,
        )

    # -------------------------------------------------------------- writes
    def save(
        self,
        full_name: str,
        instructions: str,
        model: Optional[str],
        commit_message: Optional[str] = None,
    ) -> JudgeSpec:
        """Register a new version (the first one creates the prompt)."""
        name = self.prompt_name(full_name)
        crew_id, _ = split_crew_name(full_name)
        try:
            version = self._client.register_prompt(
                name=name,
                template=instructions,
                commit_message=commit_message,
                tags={TAG_KIND: KIND, TAG_MODEL: model or "", TAG_CREW: crew_id or ""},
            )
        except Exception as exc:
            if self._uc_schema and is_permission_denied(exc):
                raise ValueError(prompt_registry_grant_hint(name)) from exc
            raise
        return self._spec(full_name, version)

    def delete(self, full_name: str) -> bool:
        """Delete the judge with all its versions. False when it does not exist."""
        name = self.prompt_name(full_name)
        latest = self._latest_version_number(name)
        if latest is None:
            return False
        try:
            if self._uc_schema:
                # UC refuses to delete a prompt that still has versions; the
                # OSS registry drops them with the prompt.
                for number in range(1, latest + 1):
                    try:
                        self._client.delete_prompt_version(name, str(number))
                    except Exception as exc:  # noqa: BLE001 — gaps are fine
                        if (
                            "not exist" in str(exc).lower()
                            or "not found" in str(exc).lower()
                        ):
                            continue
                        raise
            self._client.delete_prompt(name)
        except Exception as exc:
            if self._uc_schema and is_permission_denied(exc):
                raise ValueError(prompt_registry_grant_hint(name)) from exc
            raise
        return True
