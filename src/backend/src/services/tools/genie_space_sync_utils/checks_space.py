"""Checks of one serialized Genie space against the deployed Unity Catalog objects.

Each check returns verdicts; none raises for a bad space, only records it. The
SQL checks run through an injected ``SqlFn`` and keep only the statement's row
count (``COUNT(*)`` around the statement), never its rows.
"""

from __future__ import annotations

import re

from src.services.tools.genie_space_sync_utils.metric_views import ViewCatalog
from src.services.tools.genie_space_sync_utils.sql_checks import (
    count_statement,
    percent_scaling,
    referenced_tables,
    snippet_statement,
)
from src.services.tools.genie_space_sync_utils.sql_results import (
    SqlFn,
    count_of,
    error_of,
)
from src.services.tools.genie_space_sync_utils.verdicts import (
    FAIL,
    PASS,
    REVIEW,
    Verdict,
    short,
)

_SQL_COMMENT = re.compile(r"--(.*)")
_WORD = re.compile(r"\b[a-z][a-z0-9_]*")
_LABEL_CHARS = 70
SqlItem = tuple[str, str, str]  # (kind, label, sql text)


def _dicts(value: object) -> list[dict[str, object]]:
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, dict)]


def _section(parent: dict[str, object], key: str) -> dict[str, object]:
    value = parent.get(key)
    return value if isinstance(value, dict) else {}


def _text(value: object) -> str:
    if isinstance(value, list):
        return "".join(item for item in value if isinstance(item, str))
    return value if isinstance(value, str) else ""


def _label(value: object) -> str:
    return short(_text(value), _LABEL_CHARS)


def source_entries(serialized: dict[str, object]) -> list[dict[str, object]]:
    data_sources = _section(serialized, "data_sources")
    return _dicts(data_sources.get("tables")) + _dicts(data_sources.get("metric_views"))


def table_names(serialized: dict[str, object]) -> list[str]:
    names: list[str] = []
    for entry in source_entries(serialized):
        name = str(entry.get("identifier") or "")
        if name and name not in names:
            names.append(name)
    return names


def sql_items(serialized: dict[str, object]) -> list[SqlItem]:
    """Every example question SQL and every benchmark SQL answer."""
    instructions = _section(serialized, "instructions")
    items: list[SqlItem] = [
        ("example", _label(item.get("question")), _text(item.get("sql")))
        for item in _dicts(instructions.get("example_question_sqls"))
    ]
    for question in _dicts(_section(serialized, "benchmarks").get("questions")):
        for answer in _dicts(question.get("answer")):
            if answer.get("format") == "SQL":
                items.append(
                    (
                        "benchmark",
                        _label(question.get("question")),
                        _text(answer.get("content")),
                    )
                )
    return items


def snippet_items(serialized: dict[str, object]) -> list[tuple[str, str, str, object]]:
    """(kind, display name, sql, raw snippet) for every SQL snippet."""
    snippets = _section(_section(serialized, "instructions"), "sql_snippets")
    return [
        (kind, str(item.get("display_name") or ""), _text(item.get("sql")), item)
        for kind, group in snippets.items()
        for item in _dicts(group)
    ]


def check_data_sources(
    serialized: dict[str, object], catalog: ViewCatalog
) -> list[Verdict]:
    """Each source must be readable, and every column setting must name a real column."""
    verdicts: list[Verdict] = []
    for entry in source_entries(serialized):
        name = str(entry.get("identifier") or "")
        view = catalog.view(name)
        if view is None:
            detail = catalog.errors.get(name, "not readable")
            verdicts.append(Verdict("data_source", name, FAIL, detail))
            continue
        missing = [
            column
            for config in _dicts(entry.get("column_configs"))
            if (column := str(config.get("column_name") or ""))
            and column.lower() not in view.columns
        ]
        if missing:
            detail = f"column settings on missing columns: {missing}"
            verdicts.append(Verdict("data_source", name, FAIL, detail))
        else:
            verdicts.append(Verdict("data_source", name, PASS))
    return verdicts


def check_sql_items(serialized: dict[str, object], sql_fn: SqlFn) -> list[Verdict]:
    """Runs each example and benchmark SQL as a row count; flags unattached tables."""
    attached = {name.lower() for name in table_names(serialized)}
    verdicts: list[Verdict] = []
    for kind, label, sql in sql_items(serialized):
        unattached = sorted(referenced_tables(sql) - attached)
        if unattached:
            detail = f"uses tables the space does not attach: {unattached}"
            verdicts.append(Verdict(f"{kind}_tables", label, REVIEW, detail))
        result = sql_fn(count_statement(sql))
        count = count_of(result)
        if count is None:
            verdicts.append(Verdict(f"{kind}_sql", label, FAIL, error_of(result)))
        else:
            verdicts.append(Verdict(f"{kind}_sql", label, PASS, f"{count} rows"))
    return verdicts


def check_snippets(serialized: dict[str, object], sql_fn: SqlFn) -> list[Verdict]:
    """Runs each SQL snippet (filter, measure, expression) against its view."""
    tables = table_names(serialized)
    verdicts: list[Verdict] = []
    for kind, name, sql, _raw in snippet_items(serialized):
        check = f"snippet_{kind}"
        try:
            statement = snippet_statement(kind, sql, tables)
        except ValueError as exc:
            verdicts.append(Verdict(check, name, FAIL, short(exc)))
            continue
        if statement is None:
            verdicts.append(Verdict(check, name, REVIEW, "unknown snippet kind"))
            continue
        result = sql_fn(statement)
        if count_of(result) is None:
            verdicts.append(Verdict(check, name, FAIL, error_of(result)))
        else:
            verdicts.append(Verdict(check, name, PASS))
    return verdicts


def check_percent_scaling(
    serialized: dict[str, object], catalog: ViewCatalog
) -> list[Verdict]:
    """A percentage measure multiplied by 100 is a double scaling: fail. Other
    same-line scaling of such a measure (old/new fallbacks) is a review hint."""
    percent = catalog.percent_measures(table_names(serialized))
    if not percent:
        return []
    texts: list[tuple[str, str, str]] = sql_items(serialized) + [
        (f"snippet_{kind}", name, sql)
        for kind, name, sql, _raw in snippet_items(serialized)
    ]
    verdicts: list[Verdict] = []
    for kind, label, sql in texts:
        direct, computed = percent_scaling(sql, percent)
        if direct:
            detail = f"multiplies a percentage measure by 100: {direct}"
            verdicts.append(Verdict(f"{kind}_percent", label, FAIL, detail))
        if computed:
            detail = f"scales an expression with percentage measures by 100: {computed}"
            verdicts.append(Verdict(f"{kind}_percent", label, REVIEW, detail))
    return verdicts


def prose_texts(serialized: dict[str, object]) -> list[tuple[str, str]]:
    """Text written for Genie in words, and the SQL comments, with where each comes from."""
    instructions = _section(serialized, "instructions")
    examples = _dicts(instructions.get("example_question_sqls"))
    texts = [
        ("instructions", _text(item.get("content")))
        for item in _dicts(instructions.get("text_instructions"))
    ]
    texts += [
        (f"guidance: {_label(item.get('question'))}", _text(item.get("usage_guidance")))
        for item in examples
    ]
    texts += [
        (
            f"snippet: {name}",
            _text(raw.get("instruction") if isinstance(raw, dict) else None),
        )
        for _kind, name, _sql, raw in snippet_items(serialized)
    ]
    texts += [
        (
            f"sql comments: {_label(item.get('question'))}",
            "\n".join(_SQL_COMMENT.findall(_text(item.get("sql")))),
        )
        for item in examples
    ]
    return texts


def unknown_identifiers(text: str, known: set[str], tables: list[str]) -> list[str]:
    """snake_case words of a text that name no known column, measure or table."""
    lowered = [table.lower() for table in tables]
    words = {
        word
        for word in _WORD.findall(text.lower())
        if "_" in word and "__" not in word and not word.endswith("_")
    }
    return sorted(
        word
        for word in words
        if word not in known and not any(word in table for table in lowered)
    )


def check_identifiers(
    serialized: dict[str, object], catalog: ViewCatalog
) -> list[Verdict]:
    """Review hints: identifiers in prose that match no deployed name."""
    tables = table_names(serialized)
    known = catalog.known_names(tables)
    verdicts: list[Verdict] = []
    for where, text in prose_texts(serialized):
        unknown = unknown_identifiers(text, known, tables)
        if unknown:
            detail = f"identifiers that match no column, measure or table: {unknown}"
            verdicts.append(Verdict("identifier", where, REVIEW, detail))
    return verdicts
