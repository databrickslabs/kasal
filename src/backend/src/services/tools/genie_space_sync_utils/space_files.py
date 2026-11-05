"""Genie space <-> YAML file layout (pure, no I/O).

The layout is the one the reference ``genie_pbi_as_code`` repository keeps per
space::

    header.yaml        space_id, title, description
    config.yaml        sample_questions
    data_sources.yaml  tables and metric views, with their column configs
    instructions.yaml  text instructions, example SQL, SQL snippets, join specs
    benchmarks.yaml    benchmark questions

The Genie API stores text fields (SQL, instructions, questions, ...) as
``list[str]``. On disk they are one block scalar; on deploy they are split back
into a one-element list. ``serialized_to_files`` (extract output) and
``files_to_definition`` (deploy input) round-trip the serialized space.
"""

from __future__ import annotations

import json

import yaml
from yaml.nodes import ScalarNode

SPACE_FILES: tuple[str, ...] = (
    "header.yaml",
    "config.yaml",
    "data_sources.yaml",
    "instructions.yaml",
    "benchmarks.yaml",
)

#: Fields the API keeps as ``list[str]`` but the files store as one string.
TEXT_LIST_FIELDS: frozenset[str] = frozenset(
    {"content", "sql", "question", "usage_guidance", "instruction", "description"}
)

#: File name -> the top-level section of ``serialized_space`` it carries.
_SECTION_FILES: tuple[tuple[str, str], ...] = (
    ("config.yaml", "config"),
    ("data_sources.yaml", "data_sources"),
    ("instructions.yaml", "instructions"),
    ("benchmarks.yaml", "benchmarks"),
)

SPACE_API_VERSION = 2


class SpaceFilesError(ValueError):
    """The supplied files do not describe a space that can be read or deployed."""


class _TextLiteral(str):
    """Marks a multi-line string that must be written as a ``|`` block scalar."""


def _represent_text(dumper: yaml.SafeDumper, data: str) -> ScalarNode:
    style = "|" if isinstance(data, _TextLiteral) or "\n" in data else None
    return dumper.represent_scalar("tag:yaml.org,2002:str", data, style=style)


class _BlockDumper(yaml.SafeDumper):
    """Writes multi-line text as block scalars, everything else as plain YAML."""


_BlockDumper.add_representer(str, _represent_text)
# PyYAML looks representers up by exact type, so the marker subclass needs its own.
_BlockDumper.add_representer(_TextLiteral, _represent_text)


def _literal(text: str) -> str:
    return _TextLiteral(text) if "\n" in text else text


def _is_text_list(value: object) -> bool:
    return (
        isinstance(value, list)
        and bool(value)
        and all(isinstance(item, str) for item in value)
    )


def join_text_lists(data: object) -> object:
    """Join ``list[str]`` text fields into one string each (recursively)."""
    if isinstance(data, dict):
        joined: dict[str, object] = {}
        for key, item in data.items():
            if key in TEXT_LIST_FIELDS and _is_text_list(item):
                lines = [
                    str(line).replace("\r\n", "\n").replace("\r", "\n").rstrip()
                    for line in item
                ]
                joined[str(key)] = _literal("\n".join(lines).rstrip())
            else:
                joined[str(key)] = join_text_lists(item)
        return joined
    if isinstance(data, list):
        return [join_text_lists(item) for item in data]
    return data


def split_text_lists(data: object) -> object:
    """Wrap each text-field string back into a one-element list (recursively)."""
    if isinstance(data, dict):
        split: dict[str, object] = {}
        for key, item in data.items():
            if key in TEXT_LIST_FIELDS and isinstance(item, str):
                split[str(key)] = [item]
            else:
                split[str(key)] = split_text_lists(item)
        return split
    if isinstance(data, list):
        return [split_text_lists(item) for item in data]
    return data


def dump_yaml(data: object) -> str:
    """YAML text for one file; key order is kept, multi-line text is a block."""
    text: str = yaml.dump(
        join_text_lists(data),
        Dumper=_BlockDumper,
        allow_unicode=True,
        sort_keys=False,
        default_flow_style=False,
    )
    return text


def load_yaml(name: str, text: str) -> dict[str, object]:
    """A file's YAML as a mapping; an empty file is an empty mapping."""
    loaded: object = yaml.safe_load(text) if text.strip() else {}
    if loaded is None:
        return {}
    if not isinstance(loaded, dict):
        raise SpaceFilesError(f"{name} must hold a YAML mapping")
    return {str(key): value for key, value in loaded.items()}


def serialized_to_files(
    serialized: dict[str, object], header: dict[str, object]
) -> dict[str, str]:
    """Split a ``serialized_space`` (plus its header) into the five YAML files."""
    files = {"header.yaml": dump_yaml(header)}
    for file_name, section in _SECTION_FILES:
        content = serialized.get(section)
        files[file_name] = dump_yaml(content if isinstance(content, dict) else {})
    return files


def parse_space_files(raw: object) -> dict[str, str]:
    """Accept a JSON object (or dict) of file name -> YAML text; reject anything else."""
    decoded: object = json.loads(raw) if isinstance(raw, str) else raw
    if not isinstance(decoded, dict):
        raise SpaceFilesError("space_files must be a JSON object of file name -> YAML")
    files: dict[str, str] = {}
    for name, text in decoded.items():
        if name not in SPACE_FILES:
            raise SpaceFilesError(
                f"unknown space file {name!r}; expected one of {', '.join(SPACE_FILES)}"
            )
        if not isinstance(text, str):
            raise SpaceFilesError(f"{name} must be YAML text")
        files[str(name)] = text
    return files


def files_to_definition(
    files: dict[str, str],
) -> tuple[dict[str, object], dict[str, object]]:
    """``(header, serialized_space)`` rebuilt from the YAML files, API-shaped."""
    header = load_yaml("header.yaml", files.get("header.yaml", ""))
    serialized: dict[str, object] = {"version": SPACE_API_VERSION}
    for file_name, section in _SECTION_FILES:
        loaded = load_yaml(file_name, files.get(file_name, ""))
        serialized[section] = split_text_lists(loaded)
    return header, serialized


def definition_title(header: dict[str, object]) -> str:
    title = header.get("title")
    return title.strip() if isinstance(title, str) else ""


def _count(value: object) -> int:
    return len(value) if isinstance(value, list) else 0


def summarize_definition(serialized: dict[str, object]) -> dict[str, int]:
    """How many of each object a serialized space holds (for plans and reports)."""
    data_sources = _section(serialized, "data_sources")
    instructions = _section(serialized, "instructions")
    snippets = _section(instructions, "sql_snippets")
    return {
        "tables": _count(data_sources.get("tables")),
        "metric_views": _count(data_sources.get("metric_views")),
        "sample_questions": _count(
            _section(serialized, "config").get("sample_questions")
        ),
        "text_instructions": _count(instructions.get("text_instructions")),
        "example_sqls": _count(instructions.get("example_question_sqls")),
        "sql_snippets": sum(_count(snippets.get(kind)) for kind in snippets),
        "benchmarks": _count(_section(serialized, "benchmarks").get("questions")),
    }


def _section(parent: dict[str, object], key: str) -> dict[str, object]:
    value = parent.get(key)
    return value if isinstance(value, dict) else {}
