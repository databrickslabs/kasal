"""
Dimension Resolver for Power BI Metadata Reduction.

Resolves user question keywords to concrete table-qualified column bindings.
Provides an explicit contract between user terminology and model columns so
the DAX skeleton builder can emit table-qualified GROUP BY columns instead
of bare strings.

All logic is deterministic (no LLM).

Author: Kasal Team
Date: 2026
"""

import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

try:
    from rapidfuzz import fuzz as _rfuzz

    def _token_set_ratio(a: str, b: str) -> float:
        return float(_rfuzz.token_set_ratio(a.lower(), b.lower()))

except ImportError:
    import difflib

    def _token_set_ratio(a: str, b: str) -> float:  # type: ignore[misc]
        return difflib.SequenceMatcher(None, a.lower(), b.lower()).ratio() * 100


@dataclass
class DimensionBinding:
    """Explicit binding between a user question keyword and a model column."""

    user_term: str          # Token from user question, e.g. "Business Unit"
    resolved_table: str     # e.g. "dim_Country"
    resolved_column: str    # e.g. "Business Unit"
    confidence: float       # 0.0 – 1.0
    sample_values: List[str] = field(default_factory=list)  # Up to 5 sample values

    def to_dict(self) -> Dict[str, Any]:
        return {
            "user_term": self.user_term,
            "resolved_table": self.resolved_table,
            "resolved_column": self.resolved_column,
            "confidence": round(self.confidence, 4),
            "sample_values": self.sample_values,
        }


class DimensionResolver:
    """Resolve user question keywords to table-qualified column bindings."""

    def resolve(
        self,
        keywords: List[str],
        selected_tables: List[Dict],
        sample_data: Optional[Dict[str, Any]] = None,
        field_synonyms: Optional[Dict[str, List[str]]] = None,
        threshold: float = 0.70,
    ) -> List[DimensionBinding]:
        """Resolve keywords to DimensionBindings.

        Args:
            keywords: List of keyword strings from question_intent (excluding measure names).
            selected_tables: Reduced table list (each with 'name' and 'columns').
            sample_data: Optional sample data dict keyed by "Table[Column]".
            field_synonyms: Optional synonym map keyed by "Table[Column]" → [synonyms].
            threshold: Minimum confidence score (0–1) to accept a binding.

        Returns:
            List of DimensionBinding (one per matched keyword, deduplicated).
        """
        sample_data = sample_data or {}
        field_synonyms = field_synonyms or {}
        bindings: List[DimensionBinding] = []
        bound_cols: set = set()  # prevent duplicate column bindings

        for keyword in keywords:
            if not keyword or len(keyword) < 2:
                continue

            best: Optional[Tuple[float, str, str]] = None  # (score, table, column)

            for table in selected_tables:
                table_name = table.get("name", "")
                cols = table.get("columns", [])

                for col in cols:
                    col_name = col if isinstance(col, str) else col.get("name", "")
                    if not col_name:
                        continue

                    col_key = f"{table_name}[{col_name}]"

                    # Exact match → 1.0
                    if keyword.lower() == col_name.lower():
                        score = 1.0
                    else:
                        # Fuzzy token set ratio (0–100 → 0–1)
                        score = _token_set_ratio(keyword, col_name) / 100.0

                    # Synonym boost: if column has synonyms and any synonym matches
                    synonyms = field_synonyms.get(col_key, [])
                    for syn in synonyms:
                        syn_score = _token_set_ratio(keyword, syn) / 100.0
                        if syn_score > score:
                            score = syn_score

                    if score >= threshold:
                        if best is None or score > best[0]:
                            best = (score, table_name, col_name)

            if best is None:
                continue

            score, table_name, col_name = best
            col_key = f"{table_name}[{col_name}]"

            if col_key in bound_cols:
                # Already bound this column — skip duplicate
                continue
            bound_cols.add(col_key)

            # Collect sample values for verification
            sample_entry = sample_data.get(col_key, {})
            sample_values: List[str] = []
            if isinstance(sample_entry, dict):
                raw_vals = sample_entry.get("sample_values", [])
                sample_values = [str(v) for v in raw_vals[:5]]

            binding = DimensionBinding(
                user_term=keyword,
                resolved_table=table_name,
                resolved_column=col_name,
                confidence=round(score, 4),
                sample_values=sample_values,
            )
            bindings.append(binding)
            logger.debug(
                f"[DimensionResolver] '{keyword}' → '{table_name}'['{col_name}'] "
                f"(confidence={score:.2f})"
            )

        logger.info(
            f"[DimensionResolver] Resolved {len(bindings)}/{len(keywords)} keywords to dimension bindings"
        )
        return bindings
