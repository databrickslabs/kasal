"""
Fuzzy scoring engine for Power BI metadata reduction.

Scores semantic model elements (tables, measures, columns) against a user question
using fuzzy string matching. Borrows patterns from the IDOR_2.0 reference implementation.

Dependencies: rapidfuzz (optional, falls back to difflib)
"""

import re
import unicodedata
import logging
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

try:
    from rapidfuzz import fuzz as _rfuzz

    def _fuzzy_score(query: str, candidate: str, threshold: int = 70) -> float:
        score = _rfuzz.token_set_ratio(query.lower(), candidate.lower())
        return score if score >= threshold else 0.0

except ImportError:
    import difflib

    logger.warning("rapidfuzz not installed — falling back to difflib (slower)")

    def _fuzzy_score(query: str, candidate: str, threshold: int = 70) -> float:  # type: ignore[misc]
        ratio = difflib.SequenceMatcher(None, query.lower(), candidate.lower()).ratio() * 100
        return ratio if ratio >= threshold else 0.0


# Common stopwords to filter from user questions
_STOPWORDS = frozenset({
    "a", "an", "and", "are", "as", "at", "be", "by", "do", "for", "from",
    "get", "has", "have", "how", "i", "if", "in", "is", "it", "its", "me",
    "much", "my", "no", "not", "of", "on", "or", "our", "per", "show",
    "tell", "the", "to", "us", "was", "we", "what", "when", "where",
    "which", "who", "why", "will", "with", "would", "yes",
    "can", "could", "does", "each", "many", "should", "than", "that",
    "their", "them", "then", "there", "these", "this", "those", "very",
})

# Warehouse prefixes stripped during normalization
_TABLE_PREFIXES = re.compile(
    r"^(dim|fact|fct|lkp|ref|tbl|bridge|br|agg|stg|raw|src|vw)[\s_]?",
    re.IGNORECASE,
)


class FuzzyScorer:
    """Score semantic model elements against a user question using fuzzy matching."""

    def __init__(self, synonym_threshold: int = 70, boost_min: float = 60.0):
        self.synonym_threshold = synonym_threshold
        self.boost_min = boost_min

    def score_table(
        self,
        table: dict,
        question_tokens: List[str],
        sample_data: Optional[Dict[str, Any]] = None,
    ) -> float:
        """Score a table's relevance to the question.

        Matches against: table name, column names, column descriptions,
        measure names, measure descriptions, synonyms, AND sample data values.
        """
        if not question_tokens:
            return 0.0

        candidates: List[str] = []

        # Table name (normalized)
        table_name = table.get("name", "")
        if table_name:
            candidates.append(self.normalize_text(table_name))

        # Purpose / grain (enriched metadata — populated by LLM enrichment step)
        for field in ("purpose", "grain", "description"):
            val = table.get(field)
            if val:
                candidates.append(val)

        # Column names + descriptions + synonyms
        # Columns can be dicts ({"name": "Revenue", ...}) or plain strings ("Revenue")
        for col in table.get("columns", []):
            if isinstance(col, str):
                candidates.append(self.normalize_text(col))
            elif isinstance(col, dict):
                col_name = col.get("name", "")
                if col_name:
                    candidates.append(self.normalize_text(col_name))
                col_desc = col.get("description", "")
                if col_desc:
                    candidates.append(col_desc)
                for syn in col.get("synonyms", []):
                    candidates.append(syn)

        # Measure names + descriptions + synonyms
        # Measures can be dicts or plain strings
        for measure in table.get("measures", []):
            if isinstance(measure, str):
                candidates.append(self.normalize_text(measure))
            elif isinstance(measure, dict):
                m_name = measure.get("name", "")
                if m_name:
                    candidates.append(self.normalize_text(m_name))
                m_desc = measure.get("description", "")
                if m_desc:
                    candidates.append(m_desc)
                for syn in measure.get("synonyms", []):
                    candidates.append(syn)

        # Sample data values: keys are "TableName[ColumnName]" → {sample_values: [...]}
        # Include sample values as candidates so value-based matches (e.g. "Ireland")
        # boost the table's score.
        if sample_data and table_name:
            for key, entry in sample_data.items():
                # Extract table name from "TableName[ColumnName]" format
                sd_table = key.split("[")[0] if "[" in key else key
                if sd_table != table_name:
                    continue
                values = entry.get("sample_values", []) if isinstance(entry, dict) else []
                for val in values:
                    if isinstance(val, str) and val:
                        candidates.append(val.lower())

        return self._best_score(question_tokens, candidates)

    def score_measure(self, measure: dict, question_tokens: List[str]) -> float:
        """Score a measure's relevance to the question."""
        if not question_tokens:
            return 0.0

        candidates: List[str] = []
        m_name = measure.get("name", "")
        if m_name:
            candidates.append(self.normalize_text(m_name))
        m_desc = measure.get("description", "")
        if m_desc:
            candidates.append(m_desc)
        for syn in measure.get("synonyms", []):
            candidates.append(syn)

        return self._best_score(question_tokens, candidates)

    def _best_score(self, tokens: List[str], candidates: List[str]) -> float:
        """Return the best fuzzy score across all token-candidate pairs."""
        if not candidates:
            return 0.0
        max_score = 0.0
        for token in tokens:
            for candidate in candidates:
                score = _fuzzy_score(token, candidate, threshold=0)
                if score > max_score:
                    max_score = score
        return max_score

    @staticmethod
    def normalize_text(text: str) -> str:
        """Normalize text for comparison.

        1. Unicode NFKD (strip accents)
        2. Lowercase
        3. Strip warehouse prefixes: Dim, Fact, Fct, Lkp, Ref, tbl_, etc.
        4. Replace underscores/hyphens with spaces
        5. Remove non-alphanumeric (except spaces)
        6. Collapse whitespace
        """
        if not text:
            return ""
        # NFKD decomposition → strip combining chars
        nfkd = unicodedata.normalize("NFKD", text)
        stripped = "".join(c for c in nfkd if not unicodedata.combining(c))
        lower = stripped.lower()
        # Strip warehouse prefixes
        lower = _TABLE_PREFIXES.sub("", lower)
        # Replace separators with spaces
        lower = re.sub(r"[_\-/\\]", " ", lower)
        # Remove non-alphanumeric except spaces
        lower = re.sub(r"[^a-z0-9\s]", "", lower)
        # Collapse whitespace
        return re.sub(r"\s+", " ", lower).strip()

    def extract_question_tokens(
        self,
        question: str,
        business_terms: Optional[Dict[str, List[str]]] = None,
    ) -> List[str]:
        """Extract meaningful tokens from the user question.

        Removes stopwords, normalizes, returns unique tokens.
        Expands abbreviations via business_terms dict (e.g. {"BU": ["Business Unit"]}).
        """
        normalized = self.normalize_text(question)
        words = normalized.split()
        seen: set = set()
        tokens: List[str] = []
        for w in words:
            if w not in _STOPWORDS and len(w) >= 2 and w not in seen:
                seen.add(w)
                tokens.append(w)

        # Expand business terms: if a question token matches an abbreviation key,
        # add the expansion phrases as additional tokens.
        if business_terms:
            # Build case-insensitive lookup
            terms_lower = {k.lower(): v for k, v in business_terms.items()}
            # Check both raw question words and normalized tokens
            raw_words = {w.lower() for w in question.split()}
            for abbrev, expansions in terms_lower.items():
                if abbrev in raw_words or abbrev in seen:
                    for expansion in expansions:
                        norm_exp = self.normalize_text(expansion)
                        if norm_exp and norm_exp not in seen:
                            seen.add(norm_exp)
                            tokens.append(norm_exp)
                            logger.info(
                                f"[FuzzyScorer] Business term expansion: "
                                f"'{abbrev}' → '{expansion}'"
                            )

        return tokens

    def rank_tables(
        self,
        tables: List[dict],
        question: str,
        sample_data: Optional[Dict[str, Any]] = None,
        business_terms: Optional[Dict[str, List[str]]] = None,
    ) -> List[Dict]:
        """Score and rank all tables against the question.

        Returns list of dicts: {"table": <table_dict>, "score": float, "likely_relevant": bool}
        sorted by score descending.
        """
        tokens = self.extract_question_tokens(question, business_terms=business_terms)
        results = []
        for table in tables:
            score = self.score_table(table, tokens, sample_data=sample_data)
            results.append({
                "table": table,
                "score": score,
                "likely_relevant": score >= self.boost_min,
            })
        results.sort(key=lambda x: x["score"], reverse=True)
        return results

    def rank_measures(
        self,
        measures: List[dict],
        question: str,
        business_terms: Optional[Dict[str, List[str]]] = None,
    ) -> List[Dict]:
        """Score and rank all measures against the question.

        Returns list of dicts: {"measure": <measure_dict>, "score": float}
        sorted by score descending.
        """
        tokens = self.extract_question_tokens(question, business_terms=business_terms)
        results = []
        for measure in measures:
            score = self.score_measure(measure, tokens)
            results.append({
                "measure": measure,
                "score": score,
            })
        results.sort(key=lambda x: x["score"], reverse=True)
        return results
