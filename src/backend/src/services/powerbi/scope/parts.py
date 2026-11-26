"""An in-memory view of a Fabric ``getDefinition`` payload (TMDL model or PBIR report).

The reference tooling this package was ported from dumps every part to disk and
globs the folder. Kasal already holds the parts in memory (the Pipeline Config
Generator fetches them for its own enrichment), so the scope analysis reads them
here instead: paths are relative to the ``definition/`` root, ``glob`` matches one
path segment per ``*`` like ``pathlib.Path.glob``, and nothing touches the disk.
"""

from __future__ import annotations

import base64
import binascii
import fnmatch
import json
from collections.abc import Iterable, Mapping

_ROOT = "definition/"
# Packaged custom visuals, images and themes carry nothing the analysis reads.
_PACKAGED_RESOURCES = ("CustomVisuals/", "StaticResources/")


JsonObj = dict[str, object]


def _walk(value: object, path: tuple[str, ...]) -> object:
    for key in path:
        value = value.get(key) if isinstance(value, dict) else None
    return value


def obj(value: object, *path: str) -> JsonObj:
    """The dict at ``path`` inside parsed JSON, or ``{}`` (narrowing helper for PBIR walking)."""
    found = _walk(value, path)
    return found if isinstance(found, dict) else {}


def arr(value: object, *path: str) -> list[object]:
    """The list at ``path`` inside parsed JSON, or ``[]``."""
    found = _walk(value, path)
    return found if isinstance(found, list) else []


def text_at(value: object, *path: str) -> str | None:
    """The string at ``path`` inside parsed JSON, or None."""
    found = _walk(value, path)
    return found if isinstance(found, str) else None


def _relative(path: str) -> str:
    return path[len(_ROOT) :] if path.startswith(_ROOT) else path


def _decode(payload: object) -> object:
    """A part payload as text or parsed JSON: base64 text is decoded, JSON text parsed."""
    if isinstance(payload, (dict, list)) or payload is None:
        return payload
    text = str(payload)
    try:
        text = base64.b64decode(text, validate=True).decode("utf-8")
    except (binascii.Error, UnicodeDecodeError, ValueError):
        pass  # already plain text (tests, or a caller that decoded it)
    return text


class PartTree:
    """``{relative path: text or parsed JSON}`` with the two reads the analysis needs."""

    def __init__(self, files: Mapping[str, object]):
        self.files = {_relative(path): value for path, value in files.items()}

    @classmethod
    def from_parts(cls, parts: Iterable[Mapping[str, object]] | None) -> PartTree:
        """From a Fabric ``getDefinition`` parts list (``[{path, payload}]``)."""
        files = {}
        for part in parts or []:
            path = str(part.get("path") or "")
            if path and not path.startswith(_PACKAGED_RESOURCES):
                files[path] = _decode(part.get("payload"))
        return cls(files)

    @classmethod
    def from_report_definition(
        cls, report_def: Mapping[str, object] | None
    ) -> PartTree:
        """From the report definition the config generator keeps (``{definition: {parts}}`` or ``{parts}``)."""
        if not report_def:
            return cls({})
        definition = obj(report_def, "definition") or dict(report_def)
        return cls.from_parts(
            [p for p in arr(definition, "parts") if isinstance(p, dict)]
        )

    def __bool__(self) -> bool:
        return bool(self.files)

    def glob(self, pattern: str) -> list[str]:
        """Sorted paths matching ``pattern`` segment by segment (``pages/*/page.json``)."""
        wanted = pattern.split("/")
        return sorted(
            path
            for path in self.files
            if len(segments := path.split("/")) == len(wanted)
            and all(fnmatch.fnmatchcase(s, w) for s, w in zip(segments, wanted))
        )

    def text(self, path: str) -> str | None:
        value = self.files.get(path)
        if value is None:
            return None
        return value if isinstance(value, str) else json.dumps(value)

    def json(self, path: str) -> JsonObj:
        """The part at ``path`` parsed as a JSON object; ``{}`` when absent or not an object."""
        value = self.files.get(path)
        if isinstance(value, str):
            try:
                value = json.loads(value)
            except ValueError:
                return {}
        return value if isinstance(value, dict) else {}
