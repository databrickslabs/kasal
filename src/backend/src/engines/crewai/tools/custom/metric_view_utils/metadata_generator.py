"""Metadata Generator — generate display names, synonyms, formats for UC Metric Views."""
from __future__ import annotations

import re

# Column metadata patterns
_PERCENTAGE_MEASURE_PATTERNS = re.compile(r'(?:_pct|percent|ratio|rate|share)(?:_|$)', re.IGNORECASE)
_CURRENCY_MEASURE_PATTERNS = re.compile(r'(?:cost|revenue|spend|price|amount|value|budget)(?:_|$)', re.IGNORECASE)


class MetadataGenerator:
    """Generate display_name, synonyms, and format metadata for UC Metric View."""

    def __init__(self, column_metadata: dict | None = None,
                 measure_metadata: dict | None = None,
                 dimension_metadata: dict | None = None,
                 comment_overrides: dict | None = None,
                 dimension_exclusions: dict | None = None,
                 dimension_order: dict | None = None):
        self._column_metadata = column_metadata or {}
        self._measure_metadata = measure_metadata or {}
        self._dimension_metadata = dimension_metadata or {}
        self._comment_overrides = comment_overrides or {}
        self._dimension_exclusions = dimension_exclusions or {}
        self._dimension_order = dimension_order or {}

    def get_dimension_meta(self, col_name: str, table_key: str = '') -> dict:
        """Get metadata for a dimension column."""
        result = {}
        # Check table-specific overrides first
        if table_key and table_key in self._dimension_metadata:
            override = self._dimension_metadata[table_key].get(col_name, {})
            if override:
                return override
        # Check global column metadata
        key = col_name.lower()
        if key in self._column_metadata:
            display_name, synonyms = self._column_metadata[key]
            result['display_name'] = display_name
            result['synonyms'] = synonyms
        else:
            result['display_name'] = self._humanize(col_name)
        return result

    def get_measure_meta(self, measure_name: str, table_key: str = '', sql_expr: str = '') -> dict:
        """Get metadata for a measure."""
        # Check table-specific overrides first
        if table_key and table_key in self._measure_metadata:
            override = self._measure_metadata[table_key].get(measure_name, {})
            if override:
                return override
        result = {}
        result['display_name'] = self._humanize(measure_name)
        if _PERCENTAGE_MEASURE_PATTERNS.search(measure_name):
            result['format'] = {
                'type': 'percentage',
                'decimal_places': {'type': 'exact', 'places': 2},
            }
        elif _CURRENCY_MEASURE_PATTERNS.search(measure_name):
            result['format'] = {
                'type': 'number',
                'decimal_places': {'type': 'exact', 'places': 2},
            }
        return result

    def get_comment_override(self, table_key: str) -> str | None:
        """Get per-table comment override."""
        return self._comment_overrides.get(table_key)

    def get_dimension_exclusions(self, table_key: str) -> set[str]:
        """Get per-table dimension exclusions."""
        return self._dimension_exclusions.get(table_key, set())

    def get_dimension_order(self, table_key: str) -> list[str]:
        """Get per-table dimension ordering."""
        return self._dimension_order.get(table_key, [])

    @staticmethod
    def _humanize(name: str) -> str:
        """Convert snake_case/technical name to human-readable display name."""
        for prefix in ('bic_', 'khr', 'kco', 'kpe', 'kfx'):
            if name.lower().startswith(prefix) and len(name) > len(prefix) + 3:
                name = name[len(prefix):]
        name = re.sub(r'^\d{3,6}_?', '', name)
        if not name:
            return 'Measure'
        parts = name.replace('_', ' ').split()
        return ' '.join(p.capitalize() for p in parts)
