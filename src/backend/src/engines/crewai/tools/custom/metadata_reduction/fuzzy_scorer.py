"""
Fuzzy scoring engine for Power BI metadata reduction.

Scores semantic model elements (tables, measures, columns) against a user question
using fuzzy string matching. Borrows patterns from the IDOR_2.0 reference implementation.

Dependencies: rapidfuzz (optional, falls back to difflib)
"""

import re
import unicodedata
import logging
from typing import Any, Dict, List, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from .question_preprocessor import QuestionIntent

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

    def score_column(
        self,
        column: dict,
        question_tokens: List[str],
        sample_data: Optional[Dict[str, Any]] = None,
        table_name: str = "",
    ) -> float:
        """Score a single column's relevance to the question.

        Matches against: column name, description, synonyms, sample values.
        """
        if not question_tokens:
            return 0.0

        candidates: List[str] = []

        col_name = column.get("name", "") if isinstance(column, dict) else str(column)
        if col_name:
            candidates.append(self.normalize_text(col_name))

        if isinstance(column, dict):
            col_desc = column.get("description", "")
            if col_desc:
                candidates.append(col_desc)
            for syn in column.get("synonyms", []):
                candidates.append(syn)

        # Include sample data values for this column
        if sample_data and table_name and col_name:
            key = f"{table_name}[{col_name}]"
            entry = sample_data.get(key)
            if entry and isinstance(entry, dict):
                for val in entry.get("sample_values", []):
                    if isinstance(val, str) and val:
                        candidates.append(val.lower())

        return self._best_score(question_tokens, candidates)

    def reduce_columns(
        self,
        table: dict,
        question_tokens: List[str],
        sample_data: Optional[Dict[str, Any]] = None,
        kept_relationship_cols: Optional[set] = None,
        filter_columns: Optional[set] = None,
        has_time_intelligence: bool = False,
        threshold: float = 50.0,
    ) -> List:
        """Reduce a table's columns to only question-relevant ones.

        Always keeps:
        - Columns referenced in kept relationships (primary/foreign keys)
        - Columns referenced in active/default filters
        - Date/time columns if time intelligence detected in question
        - Columns scoring above threshold

        Safety: if reduction would remove >80% of columns, keep all.
        Returns the filtered column list (same type as input: dicts or strings).
        """
        columns = table.get("columns", [])
        if not columns or not question_tokens:
            return columns

        table_name = table.get("name", "")
        kept_rel_cols = kept_relationship_cols or set()
        filter_cols = filter_columns or set()

        # Date/time column heuristic
        _date_keywords = {"date", "year", "month", "quarter", "week", "day", "time", "period", "calendar"}

        kept = []
        for col in columns:
            col_name = col.get("name", "") if isinstance(col, dict) else str(col)
            col_name_lower = col_name.lower()

            # Always keep: relationship columns
            if col_name in kept_rel_cols:
                kept.append(col)
                continue

            # Always keep: filter-referenced columns
            if col_name in filter_cols:
                kept.append(col)
                continue

            # Always keep: date/time columns if time intelligence detected
            if has_time_intelligence:
                normalized = self.normalize_text(col_name)
                if any(kw in normalized for kw in _date_keywords):
                    kept.append(col)
                    continue

            # Score-based inclusion
            score = self.score_column(
                col, question_tokens,
                sample_data=sample_data,
                table_name=table_name,
            )
            if score >= threshold:
                kept.append(col)

        # Safety: if we'd remove >80% of columns, keep all
        if len(columns) > 0 and len(kept) < len(columns) * 0.2:
            logger.info(
                f"[COLUMN_REDUCTION] Safety override: {table_name} would keep "
                f"only {len(kept)}/{len(columns)} cols (<20%), keeping all"
            )
            return columns

        if len(kept) < len(columns):
            logger.info(
                f"[COLUMN_REDUCTION] {table_name} "
                f"{len(columns)}→{len(kept)} columns"
            )

        return kept

    def rank_tables(
        self,
        tables: List[dict],
        question: str,
        sample_data: Optional[Dict[str, Any]] = None,
        business_terms: Optional[Dict[str, List[str]]] = None,
        question_intent: Optional["QuestionIntent"] = None,
    ) -> List[Dict]:
        """Score and rank all tables against the question.

        Returns list of dicts: {"table": <table_dict>, "score": float, "likely_relevant": bool}
        sorted by score descending.

        If question_intent is provided, tables containing matched measures or dimensions
        receive a scoring boost.
        """
        tokens = self.extract_question_tokens(question, business_terms=business_terms)
        # Build boost sets from preprocessor intent
        intent_measure_names = set()
        intent_dim_names = set()
        if question_intent:
            intent_measure_names = {m.lower() for m in (question_intent.measures or [])}
            intent_dim_names = {d.lower() for d in (question_intent.dimensions or [])}

        results = []
        for table in tables:
            score = self.score_table(table, tokens, sample_data=sample_data)

            # Boost tables containing intent-matched measures or dimensions
            if intent_measure_names or intent_dim_names:
                table_measures = {
                    (m.get("name", "") if isinstance(m, dict) else str(m)).lower()
                    for m in table.get("measures", [])
                }
                table_cols = {
                    (c.get("name", "") if isinstance(c, dict) else str(c)).lower()
                    for c in table.get("columns", [])
                }
                if table_measures & intent_measure_names:
                    score = max(score, self.boost_min + 10)
                if table_cols & intent_dim_names:
                    score = max(score, self.boost_min + 5)

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
        question_intent: Optional["QuestionIntent"] = None,
    ) -> List[Dict]:
        """Score and rank all measures against the question.

        Returns list of dicts: {"measure": <measure_dict>, "score": float}
        sorted by score descending.

        If question_intent is provided, measures matching intent.measures receive a boost.
        """
        tokens = self.extract_question_tokens(question, business_terms=business_terms)
        intent_measure_names = set()
        if question_intent:
            intent_measure_names = {m.lower() for m in (question_intent.measures or [])}

        results = []
        for measure in measures:
            score = self.score_measure(measure, tokens)

            # Boost measures matching preprocessor intent
            if intent_measure_names:
                m_name = measure.get("name", "").lower()
                if m_name in intent_measure_names:
                    score = max(score, self.boost_min + 15)

            results.append({
                "measure": measure,
                "score": score,
            })
        results.sort(key=lambda x: x["score"], reverse=True)
        return results
