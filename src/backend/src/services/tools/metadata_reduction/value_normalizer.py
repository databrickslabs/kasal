"""
Filter value normalizer for Power BI metadata reduction.

Validates and corrects user-provided filter values against actual column values
from sample data and slicer distinct values. Uses a four-step deterministic
approach before optional LLM fallback.

Borrows patterns from the IDOR_2.0 value_normalizer_agent.py.
"""

import difflib
import re
import logging
from typing import Dict, List, Optional, Any

logger = logging.getLogger(__name__)

# Adjectival suffixes for demonym matching (ordered longest → shortest)
_ADJECTIVAL_SUFFIXES = ("ish", "ian", "ese", "ean", "an", "ic", "er", "i")

# Stopwords to skip during token-word matching
_MATCH_STOPWORDS = frozenset({
    "and", "or", "of", "the", "in", "at", "a", "an", "for", "bu", "bv",
    "vs", "by", "to", "is", "are", "was", "be", "per", "all",
})

# difflib thresholds
_TYPO_CUTOFF = 0.55
_TOKEN_WORD_CUTOFF = 0.70


class ValueNormalizer:
    """Normalize user-provided filter values against actual column values."""

    def normalize_filter_values(
        self,
        active_filters: Dict[str, Any],
        sample_data: Dict[str, List[Dict]],
        slicers: List[Dict],
        columns: Optional[List[Dict]] = None,
    ) -> Dict[str, Any]:
        """For each active filter, validate the value against known values.

        Args:
            active_filters: Dict mapping "Table.Column" → filter value(s).
            sample_data: Dict mapping table name → list of row dicts.
            slicers: List of slicer dicts with 'table', 'column', 'values' keys.
            columns: Optional column metadata for type-aware normalization.

        Returns:
            Dict with same keys as active_filters but corrected values,
            plus a '_normalization_log' key with per-filter details.
        """
        if not active_filters:
            return {"_normalization_log": []}

        normalized = {}
        log_entries: List[Dict] = []

        for filter_key, filter_value in active_filters.items():
            # Parse "Table.Column" or "'Table'[Column]" format
            table_name, column_name = self._parse_filter_key(filter_key)
            if not table_name or not column_name:
                normalized[filter_key] = filter_value
                log_entries.append({
                    "filter": filter_key,
                    "status": "skipped",
                    "reason": "Could not parse filter key",
                })
                continue

            # Collect known values from sample_data and slicers
            known_values = self._collect_known_values(
                table_name, column_name, sample_data, slicers
            )

            if not known_values:
                # No reference values — pass through unchanged
                normalized[filter_key] = filter_value
                log_entries.append({
                    "filter": filter_key,
                    "status": "passthrough",
                    "reason": "No reference values available",
                })
                continue

            # Normalize single value or list of values
            if isinstance(filter_value, list):
                corrected = []
                for v in filter_value:
                    result = self._correct_single_value(str(v), known_values)
                    corrected.append(result["corrected"])
                    log_entries.append({
                        "filter": filter_key,
                        "original": v,
                        **result,
                    })
                normalized[filter_key] = corrected
            else:
                result = self._correct_single_value(str(filter_value), known_values)
                normalized[filter_key] = result["corrected"]
                log_entries.append({
                    "filter": filter_key,
                    "original": filter_value,
                    **result,
                })

        normalized["_normalization_log"] = log_entries
        return normalized

    def _correct_single_value(
        self, user_value: str, available_values: List[str]
    ) -> Dict[str, Any]:
        """Four-step deterministic value correction.

        1. Exact match
        2. difflib close match (cutoff 0.55)
        3. Demonym/suffix stripping
        4. Token-word matching

        Returns dict with 'corrected', 'status', 'method' keys.
        """
        # Step 0: Exact match (case-insensitive)
        lower_map = {v.lower(): v for v in available_values}
        if user_value.lower() in lower_map:
            return {
                "corrected": lower_map[user_value.lower()],
                "status": "exact_match",
                "method": "exact",
            }

        # Step 1: difflib close match
        matches = difflib.get_close_matches(
            user_value, available_values, n=1, cutoff=_TYPO_CUTOFF
        )
        if matches:
            return {
                "corrected": matches[0],
                "status": "corrected",
                "method": "difflib_close_match",
            }

        # Step 2: Demonym/suffix stripping
        result = self._demonym_stem_match(user_value, available_values)
        if result:
            return {
                "corrected": result,
                "status": "corrected",
                "method": "demonym_stem",
            }

        # Step 3: Token-word matching
        result = self._token_word_match(user_value, available_values)
        if result:
            return {
                "corrected": result,
                "status": "corrected",
                "method": "token_word",
            }

        # No match found — pass through original
        return {
            "corrected": user_value,
            "status": "unresolved",
            "method": "none",
        }

    @staticmethod
    def _demonym_stem_match(
        user_value: str, candidates: List[str]
    ) -> Optional[str]:
        """Strip adjectival suffixes (ish, ian, ese, ean, an, ic, er, i) and match.

        Requires exactly one match to avoid ambiguity.
        """
        norm = user_value.lower().strip()
        for suffix in _ADJECTIVAL_SUFFIXES:
            if norm.endswith(suffix):
                stem = norm[: len(norm) - len(suffix)]
                if len(stem) < 2:
                    continue
                matches = [
                    v for v in candidates
                    if v.lower().startswith(stem)
                ]
                if len(matches) == 1:
                    return matches[0]
        return None

    @staticmethod
    def _token_word_match(
        user_value: str, candidates: List[str]
    ) -> Optional[str]:
        """Tokenize user input, find embedded words in canonical values.

        Tries exact word match, suffix-stripped prefix, abbreviation, then difflib.
        Requires exactly one matching candidate to avoid ambiguity.
        """
        tokens = re.split(r"\s+", user_value.lower().strip())
        meaningful = [
            t for t in tokens
            if t and t not in _MATCH_STOPWORDS and len(t) >= 3
        ]

        if not meaningful:
            return None

        value_words = {
            v: {w.lower() for w in re.split(r"[\s\-_/]+", v) if len(w) >= 2}
            for v in candidates
        }

        for token in meaningful:
            matched = []

            for v, words in value_words.items():
                # (a) Exact word match
                if token in words:
                    matched.append(v)
                    continue

                # (b) Suffix stripping + prefix
                found_via_suffix = False
                for suffix in _ADJECTIVAL_SUFFIXES:
                    if token.endswith(suffix):
                        stem = token[: len(token) - len(suffix)]
                        if len(stem) >= 2 and any(w.startswith(stem) for w in words):
                            matched.append(v)
                            found_via_suffix = True
                            break
                if found_via_suffix:
                    continue

                # (c) Abbreviation check
                if any(len(w) >= 3 and token.startswith(w) for w in words):
                    matched.append(v)
                    continue

                # (d) difflib fuzzy match
                close = difflib.get_close_matches(
                    token, list(words), n=1, cutoff=_TOKEN_WORD_CUTOFF
                )
                if close:
                    matched.append(v)

            if len(matched) == 1:
                return matched[0]

        return None

    @staticmethod
    def _parse_filter_key(key: str) -> tuple:
        """Parse a filter key into (table_name, column_name).

        Supports formats:
        - "Table.Column"
        - "'Table'[Column]"
        - "Table[Column]"
        """
        # Try 'Table'[Column] format
        m = re.match(r"'([^']+)'\[([^\]]+)\]", key)
        if m:
            return m.group(1), m.group(2)

        # Try Table[Column] format
        m = re.match(r"([^\[]+)\[([^\]]+)\]", key)
        if m:
            return m.group(1).strip(), m.group(2)

        # Try Table.Column format
        parts = key.split(".", 1)
        if len(parts) == 2:
            return parts[0].strip(), parts[1].strip()

        return None, None

    @staticmethod
    def _collect_known_values(
        table_name: str,
        column_name: str,
        sample_data: Dict[str, List[Dict]],
        slicers: List[Dict],
    ) -> List[str]:
        """Collect known values for a column from sample data and slicers."""
        values = set()

        # From sample data
        table_samples = sample_data.get(table_name, [])
        for row in table_samples:
            val = row.get(column_name)
            if val is not None:
                values.add(str(val))

        # From slicers
        for slicer in slicers:
            if (
                slicer.get("table", "").lower() == table_name.lower()
                and slicer.get("column", "").lower() == column_name.lower()
            ):
                for v in slicer.get("values", []):
                    if v is not None:
                        values.add(str(v))

        return sorted(values)
