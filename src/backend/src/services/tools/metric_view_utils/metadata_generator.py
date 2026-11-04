"""Metadata Generator — generate display names, synonyms, formats for UC Metric Views."""

from __future__ import annotations

import re

from .measure_format import derive_measure_format


class MetadataGenerator:
    """Generate display_name, synonyms, and format metadata for UC Metric View."""

    def __init__(
        self,
        column_metadata: dict | None = None,
        measure_metadata: dict | None = None,
        dimension_metadata: dict | None = None,
        comment_overrides: dict | None = None,
        dimension_exclusions: dict | None = None,
        dimension_order: dict | None = None,
        name_prefixes_to_strip: tuple | None = None,
        pbi_format_strings: dict[str, str] | None = None,
    ) -> None:
        self._column_metadata = column_metadata or {}
        self._measure_metadata: dict[str, dict] = measure_metadata or {}
        self._dimension_metadata: dict[str, dict] = dimension_metadata or {}
        self._comment_overrides = comment_overrides or {}
        self._dimension_exclusions: dict[str, set[str]] = dimension_exclusions or {}
        self._dimension_order: dict[str, list[str]] = dimension_order or {}
        self._name_prefixes = name_prefixes_to_strip or ()
        # Power BI measure name -> formatString (harvested from TMDL).
        self._pbi_format_strings: dict[str, str] = pbi_format_strings or {}

    def get_dimension_meta(self, col_name: str, table_key: str = "") -> dict:
        """Get metadata for a dimension column."""
        result: dict = {}
        # Check table-specific overrides first
        if table_key and table_key in self._dimension_metadata:
            override: dict = self._dimension_metadata[table_key].get(col_name, {})
            if override:
                return override
        # Check global column metadata
        key = col_name.lower()
        if key in self._column_metadata:
            display_name, synonyms = self._column_metadata[key]
            result["display_name"] = display_name
            result["synonyms"] = synonyms
        else:
            result["display_name"] = self._humanize(col_name)
        return result

    def get_measure_meta(
        self,
        measure_name: str,
        table_key: str = "",
        sql_expr: str = "",
        pbi_name: str = "",
    ) -> dict:
        """Get metadata for a measure, including its display ``format``.

        The format is derived by :func:`measure_format.derive_measure_format`
        from the Power BI formatString (when ``pbi_name`` has one), then the
        name and expression heuristics. A table override that carries its own
        ``format`` wins over all of it.
        """
        # Check table-specific overrides first
        if table_key and table_key in self._measure_metadata:
            override: dict = self._measure_metadata[table_key].get(measure_name, {})
            if override:
                return override
        result: dict = {}
        result["display_name"] = self._humanize(measure_name)
        format_string = self._pbi_format_strings.get(pbi_name or measure_name)
        fmt = derive_measure_format(measure_name, sql_expr, format_string)
        if fmt is not None:
            result["format"] = fmt
        return result

    def get_dimension_order(self, table_key: str) -> list[str]:
        """Get per-table dimension ordering."""
        return self._dimension_order.get(table_key, [])

    def _humanize(self, name: str) -> str:
        """Convert snake_case/technical name to human-readable display name."""
        for prefix in self._name_prefixes:
            if name.lower().startswith(prefix) and len(name) > len(prefix) + 3:
                name = name[len(prefix) :]
        name = re.sub(r"^\d{3,6}_?", "", name)
        if not name:
            return "Measure"
        parts = name.replace("_", " ").split()
        return " ".join(p.capitalize() for p in parts)
