"""Prompt text for the DAX-to-SQL LLM fallback (skill corpus + system prompts).

The vendored DAX / UC-metric-view skill corpus is loaded once at import and the
system prompts (single-measure and batch) are assembled from it. Pure string
construction: no network, no LLM client. ``dax_llm_fallback`` re-exports the
names it uses, so existing imports keep resolving there.
"""

from __future__ import annotations

import logging
import os

logger = logging.getLogger(__name__)


# ── Skill corpus (LLM-first) ────────────────────────────────────────────────
# The engineering DAX-translation + UC-metric-view skill files are vendored
# under skills/. Loaded once at module import and embedded as a STABLE system
# prefix marked with Anthropic cache_control:ephemeral so the serving endpoint
# caches it (full price once per run, ~10% per subsequent measure). The dax/
# files teach WHAT to translate; the uc-metric-views/ files teach HOW to write
# the target YAML — the LLM needs both to emit deployable, correct output.
_SKILLS_DIR = os.path.join(os.path.dirname(__file__), "skills")
_SKILL_FILES = (
    ("dax", "SKILL.md"),
    ("dax", "PATTERNS.md"),
    ("dax", "UNSUPPORTED.md"),
    ("dax", "EDGE_CASES.md"),
    ("uc-metric-views", "SYNTAX.md"),
    ("uc-metric-views", "WINDOW.md"),
    ("uc-metric-views", "LOD.md"),
    ("uc-metric-views", "JOINS.md"),
    ("uc-metric-views", "COMPOSABILITY.md"),
    ("uc-metric-views", "MATERIALIZATION.md"),
)


def _load_skill_corpus() -> str:
    """Concatenate the vendored skill .md files into one corpus string.

    Missing files are skipped with a warning (fail-open — a partial corpus still
    beats none). Returns '' if the skills dir is absent, in which case the
    translator falls back to the terse built-in guidance.
    """
    parts: list[str] = []
    for group, name in _SKILL_FILES:
        path = os.path.join(_SKILLS_DIR, group, name)
        try:
            with open(path, encoding="utf-8") as fh:
                parts.append(f"# === {group}/{name} ===\n\n{fh.read().strip()}")
        except FileNotFoundError:
            logger.warning("[DAX_LLM] skill file missing (skipped): %s/%s", group, name)
        except Exception as e:  # noqa: BLE001
            logger.warning(
                "[DAX_LLM] skill file unreadable (%s/%s): %s", group, name, e
            )
    return "\n\n".join(parts)


# Loaded once at import. If empty (skills not vendored), the terse fallback
# system prompt below is used instead.
_SKILL_CORPUS = _load_skill_corpus()

# Terse built-in guidance used ONLY when the skill corpus is unavailable.
_FALLBACK_INSTRUCTIONS = """You are an expert DAX-to-Spark-SQL translator for Databricks Unity Catalog Metric Views.

Given a DAX measure expression, translate it to the equivalent Spark SQL expression.

Rules:
1. Use `source.column_name` for fact table columns
2. Use `alias.column_name` for joined dimension/fact columns
3. Use `MEASURE(measure_name)` to reference other translated measures
4. Use `SUM(source.col) FILTER (WHERE condition)` for filtered aggregations
5. Use `expr / NULLIF(expr, 0)` instead of DIVIDE()
6. Use bare column names in FILTER clauses (no table prefix)
7. DAX functions → SQL equivalents:
   - SUM(Table[col]) → SUM(source.col)
   - CALCULATE(expr, filter) → expr FILTER (WHERE filter)
   - DIVIDE(a, b) → a / NULLIF(b, 0)
   - DISTINCTCOUNTNOBLANK → COUNT(DISTINCT col)
   - SUMX(FILTER(T, cond), T[col]) → SUM(source.col) FILTER (WHERE cond)
   - COUNTX(FILTER(T, cond), T[col]) → COUNT(source.col) FILTER (WHERE cond)
   - AVERAGEX(FILTER(T, cond), T[col]) → AVG(source.col) FILTER (WHERE cond)
   - Table[col] = "val" → source.col = 'val'
   - Table[col] in {"a","b"} → source.col IN ('a', 'b')
   - && → AND, || → OR
8. If the expression CANNOT be translated to UC Metric View SQL, return success=false.
9. Use snake_case for measure names."""

# Two SQL rules restated where the model attends to them most (the full
# examples live in the skill corpus dax/PATTERNS.md §0, but a rule there can get
# diluted — these two are the ones that were violated in practice).
_SQL_RULES = """

CRITICAL SQL rules for every "sql_expr" (see skill corpus §0 for detail):
1. Qualify EVERY source column as source.<col> — including inside FILTER (WHERE ...)
   and CASE WHEN predicates. (Unqualified defaults to source, but always write the
   explicit source. prefix.)
2. SINGLE-SOURCE only: never SELECT FROM another table in a subquery — no
   "... IN (SELECT ... FROM other_table)", no "... = (SELECT ... FROM other_table)".
   Only source and declared join aliases are valid namespaces. If a measure needs a
   table that is neither source nor a declared join, set success=false with
   dax_class="architecture_change" and explain the source-view change needed — do
   NOT fabricate a cross-table subquery.
3. DAX BLANK inequality semantics: DAX `col <> "X"` is TRUE for BLANK rows, but
   SQL `col <> 'X'` DROPS NULLs (so the filter silently returns 0). Preserve DAX
   semantics with the NULL-aware form: `(source.col IS NULL OR source.col <> 'X')`.
   (`col = "X"` needs no such guard — both DAX and SQL exclude the blank/NULL row.)
4. Group-then-aggregate (SUMMARIZE + DISTINCTCOUNT): a measure that groups by a
   score and counts distinct ids per group, then sums, is counting DISTINCT
   (score, id) PAIRS — emit
   `COUNT(DISTINCT CASE WHEN <cond> THEN concat(CAST(source.score AS STRING),'|',CAST(source.id AS STRING)) END)`,
   never a row-level SUM or a product of counts.
5. TIME-INTELLIGENCE — prior year/period, YTD, QTD, MTD and the DAX functions
   SAMEPERIODLASTYEAR, DATEADD, PARALLELPERIOD, PREVIOUSYEAR, TOTALYTD/DATESYTD —
   is expressible as a WINDOW FUNCTION over the period column (single-source, no
   subquery, no new table):
     - prior period:  LAG(SUM(source.<x>)) OVER (ORDER BY source.<period>)
     - prior year:    LAG(SUM(source.<x>), <periods_per_year>) OVER (ORDER BY source.<period>)
     - YTD:           SUM(SUM(source.<x>)) OVER (PARTITION BY source.<year> ORDER BY source.<period>)
   Use the period/fiscal column shown in the context (e.g. fiscper, fiscal_year); a
   declared calendar self-join on a prior-period key (e.g. date_py) is also legal.
   See the skill corpus WINDOW.md / UNSUPPORTED.md. WHETHER to translate it this
   run is set by the "Time-intelligence" directive in the user message — a window
   over the wrong period count is silently wrong, so it is only enabled when PBI
   reconciliation is validating; when that directive says NOT validated, decline
   it (success=false, architecture_change)."""

# The JSON output contract (shared by corpus + fallback prompts). Adds the
# 7-category `dax_class` provenance label alongside the existing fields.
_OUTPUT_CONTRACT = """
Classify each measure into exactly one `dax_class`:
- "translatable_direct": a direct aggregation / simple expression
- "composed": references other measures via MEASURE()
- "filtered": CALCULATE/FILTER context that maps to a FILTER (WHERE ...) clause
- "architecture_change": needs a window/LOD/join restructure (still emittable)
- "display_layer": a formatting/UI artifact (FORMAT, color, label) — not a real metric
- "unsupported": PBI-specific semantics with no UCMV equivalent (ALLSELECTED, USERELATIONSHIP on inactive rels, visual-level filters, recursive iteration)
- "out_of_scope": not a measure translation task

ALWAYS respond with valid JSON (no markdown code blocks):
{
  "success": true/false,
  "sql_expr": "the SQL expression" or null,
  "dax_class": "one of the 7 categories above",
  "confidence": "high"/"medium"/"low",
  "explanation": "brief explanation of the translation",
  "error": "reason if success=false" or null
}""" + _SQL_RULES

# Corpus-backed system prompt when skills are vendored; terse otherwise. The
# corpus is the STABLE prefix that gets cache_control:ephemeral at call time.
if _SKILL_CORPUS:
    _SYSTEM_PROMPT = (
        "You are an expert Power BI DAX → Databricks UC Metric View translator. "
        "Use the following skill corpus (engineering's DAX-translation decision "
        "framework + UC-metric-view target-language spec) as your authoritative "
        "guide for WHAT to translate and HOW to write the target YAML/SQL.\n\n"
        f"{_SKILL_CORPUS}\n\n"
        "----\n"
        "Translate the given DAX measure to a Spark SQL expression for a UC Metric "
        "View, following the corpus above." + _OUTPUT_CONTRACT
    )
else:
    _SYSTEM_PROMPT = _FALLBACK_INSTRUCTIONS + "\n" + _OUTPUT_CONTRACT

# Batch output contract: the model is handed SEVERAL measures in one call (so the
# ~14k-token skill corpus is sent ONCE and amortised across the whole batch,
# instead of re-sent per measure — Databricks silently drops Anthropic prompt
# caching, so per-measure calls pay the full corpus every time). It must return a
# JSON ARRAY, one object per measure, echoing the measure name so results map back.
_BATCH_OUTPUT_CONTRACT = """

You are given SEVERAL DAX measures in one request. Respond with a SINGLE valid
JSON ARRAY (no markdown code fences), containing exactly one object per measure
you were given:
[
  {
    "measure_name": "<echo the EXACT measure name you were given>",
    "success": true or false,
    "sql_expr": "the Spark SQL expression" or null,
    "dax_class": "translatable_direct|composed|filtered|architecture_change|display_layer|unsupported|out_of_scope",
    "confidence": "high" or "medium" or "low",
    "explanation": "brief explanation of the translation",
    "error": "reason if success=false" or null
  }
]
Include EVERY measure exactly once. Do not merge, skip, or invent measures.
Classify each into exactly one dax_class (same definitions as above).""" + _SQL_RULES

# Corpus-backed batch system prompt (corpus sent ONCE per batch call).
if _SKILL_CORPUS:
    _SYSTEM_PROMPT_BATCH = (
        "You are an expert Power BI DAX → Databricks UC Metric View translator. "
        "Use the following skill corpus (engineering's DAX-translation decision "
        "framework + UC-metric-view target-language spec) as your authoritative "
        "guide for WHAT to translate and HOW to write the target YAML/SQL.\n\n"
        f"{_SKILL_CORPUS}\n\n"
        "----\n"
        "Translate EACH of the given DAX measures to a Spark SQL expression for a "
        "UC Metric View, following the corpus above." + _BATCH_OUTPUT_CONTRACT
    )
else:
    _SYSTEM_PROMPT_BATCH = _FALLBACK_INSTRUCTIONS + "\n" + _BATCH_OUTPUT_CONTRACT
