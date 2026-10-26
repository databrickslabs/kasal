"""Incremental JSON surface scanning. Completed values are decoded exactly once."""

from __future__ import annotations

import json
from typing import Any


class PartialSurface:
    __slots__ = ("surface_kind", "root", "components", "data_model", "complete")

    def __init__(self) -> None:
        self.surface_kind: str | None = None
        self.root: str | None = None
        self.components: list[dict[str, Any]] = []
        self.data_model: dict[str, Any] = {}
        self.complete = False


class IncrementalSurfaceParser:
    """Consume deltas without rescanning an unfinished string or container.

    Only the open value is buffered as characters. Complete components and model
    entries are retained for reconnect snapshots. Malformed JSON stops this
    best-effort parser; the composer's validated final surface remains authoritative.
    """

    def __init__(self) -> None:
        self.part = PartialSurface()
        self.new_keys: list[str] = []
        self._context = "top"
        self._state = "preamble"
        self._key = ""
        self._token: list[str] = []
        self._token_kind = ""
        self._depth = 0
        self._quoted = False
        self._escaped = False
        self._failed = False

    def feed(self, chunk: str) -> PartialSurface:
        self.new_keys = []
        if self._failed or self.part.complete:
            return self.part
        try:
            for char in chunk:
                self._consume(char)
        except (ValueError, TypeError):
            self._failed = True
        return self.part

    def _finish_token(self) -> None:
        value = json.loads("".join(self._token))
        self._token = []
        if self._state == "key":
            if not isinstance(value, str):
                raise ValueError("Expected a key")
            self._key = value
            self._state = "colon"
            return
        if self._context == "components":
            if isinstance(value, dict):
                self.part.components.append(value)
        elif self._context == "model":
            if self._key not in self.part.data_model:
                self.new_keys.append(self._key)
            self.part.data_model[self._key] = value
        elif self._key == "surfaceKind" and isinstance(value, str):
            self.part.surface_kind = value
        elif self._key == "root" and isinstance(value, str):
            self.part.root = value
        self._state = "between"

    def _consume(self, char: str) -> None:
        if self.part.complete:
            return
        if self._token:
            if self._token_kind == "primitive" and char in ",}] \t\r\n":
                self._finish_token()
                self._consume(char)
                return
            self._token.append(char)
            if self._quoted:
                if self._escaped:
                    self._escaped = False
                elif char == "\\":
                    self._escaped = True
                elif char == '"':
                    self._quoted = False
                    if self._token_kind == "string":
                        self._finish_token()
            elif char == '"':
                self._quoted = True
            elif char in "[{":
                self._depth += 1
            elif char in "]}":
                self._depth -= 1
                if self._depth == 0:
                    self._finish_token()
            return
        if self._state == "preamble":
            if char == "{":
                self._state = "between"
            return
        if char.isspace():
            return
        if self._state == "colon":
            if char != ":":
                raise ValueError("Expected a colon")
            self._state = "value"
            return
        if self._state == "between":
            if char == ",":
                return
            if char == ("]" if self._context == "components" else "}"):
                if self._context == "top":
                    self.part.complete = True
                else:
                    self._context = "top"
                return
            self._state = "value" if self._context == "components" else "key"
        if self._state == "value" and self._context == "top":
            if self._key in ("components", "dataModel"):
                if char != ("[" if self._key == "components" else "{"):
                    raise ValueError("Expected a container")
                self._context = "components" if self._key == "components" else "model"
                self._state = "between"
                return
        if self._state == "key" and char != '"':
            raise ValueError("Expected a string key")
        self._token = [char]
        self._quoted = char == '"'
        self._escaped = False
        self._depth = 1 if char in "[{" else 0
        self._token_kind = (
            "string" if self._quoted else "container" if self._depth else "primitive"
        )
