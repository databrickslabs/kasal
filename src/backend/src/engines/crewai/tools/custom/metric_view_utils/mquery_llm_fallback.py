"""LLM-powered fallback for raw Power Query M that the SQL parser can't read.

Opt-in module: only invoked when ``use_llm_fallback=True`` in pipeline config.

Problem it solves
-----------------
UCMV's MQueryParser understands transpiled **SQL** only. When a table's source is
raw Power Query M (``let Source = Sql.Database(...) in ...``) with no embedded
native SQL, the parser extracts no FROM clause and no aggregate columns, so the
table is neither a fact nor has a source → 0 views. This module asks an LLM to
translate the M expression into a minimal Spark SQL ``SELECT ... FROM
catalog.schema.table`` that the parser CAN read, recovering the source table (and
any obvious aggregation) so fact detection can fire.

Fail-open: any LLM error leaves the table exactly as it was (still non-fact) — it
never blocks the tables that parsed fine.
"""
from __future__ import annotations

import hashlib
import json
import logging
import re
from collections import OrderedDict

logger = logging.getLogger(__name__)

_RUN_CACHE_MAX = 128

_SYSTEM_PROMPT = """You are an expert at reading Power Query M (the "let ... in" language used by \
Power BI / Fabric dataflows) and extracting the underlying data source as Spark SQL for Databricks \
Unity Catalog.

You are GIVEN a table's Power Query M source expression. Your job is to produce the equivalent
Spark SQL SELECT that reads the SAME underlying table, so a downstream parser can identify the
source table and (if present) its aggregations.

Rules:
1. Identify the underlying physical source (schema/table) the M expression reads from.
   - Sql.Database("srv","db",[Query="SELECT ..."]) → use the embedded SELECT verbatim.
   - Source{[Schema="s",Item="t"]}[Data] → `SELECT * FROM s.t`.
   - Databricks.Query / Databricks.Catalogs paths → the referenced catalog.schema.table.
   - A native query embedded anywhere in the M → return that query.
2. Prefer a fully-qualified name (schema.table or catalog.schema.table). If only a bare table
   name is present, return `SELECT * FROM <table>`.
3. Preserve any GROUP BY / SUM / aggregation that is genuinely present in an embedded native query.
   Do NOT invent aggregations that are not in the M — if the source is a plain table read, return
   `SELECT * FROM <table>` (no fabricated GROUP BY).
4. Return ONLY standard Spark SQL. No M functions, no Power Query syntax.
5. If the M expression does NOT read a queryable relational source (e.g. it is a hand-authored
   table, a web/JSON/Excel source, or pure in-memory transformation with no table), return
   success=false.

ALWAYS respond with valid JSON (no markdown code blocks):
{
  "success": true/false,
  "source_sql": "SELECT ... FROM schema.table" or null,
  "source_table": "schema.table" or null,
  "confidence": "high"/"medium"/"low",
  "explanation": "brief explanation",
  "error": "reason if success=false" or null
}"""


def _content_hash(text: str) -> str:
    """SHA-256 hash for cache key."""
    return hashlib.sha256(text.encode()).hexdigest()[:16]


def _build_user_prompt(table_name: str, mquery: str) -> str:
    """Build the user prompt with the table name and its M expression."""
    # Cap the M body so a pathological expression can't blow the token budget.
    body = mquery if len(mquery) <= 6000 else mquery[:6000] + "\n... [truncated]"
    return f"""Extract the Spark SQL source for this Power BI table.

## Table
{table_name}

## Power Query M expression
{body}

## Instructions
Return JSON with success, source_sql (a Spark SQL SELECT), source_table, confidence, explanation."""


def _parse_response(response_text: str) -> dict:
    """Parse and validate LLM response JSON (tolerates markdown fences)."""
    try:
        text = response_text.strip()
        if text.startswith('```json'):
            text = text.split('```json')[1].split('```')[0].strip()
        elif text.startswith('```'):
            text = text.split('```')[1].split('```')[0].strip()
        return json.loads(text)
    except (json.JSONDecodeError, IndexError):
        return {'success': False, 'error': 'Failed to parse LLM response'}


_M_LEFTOVER = re.compile(
    r'\b(let\b|Sql\.Database|Value\.NativeQuery|Table\.|Source\s*\{|\[Schema\s*=|#")',
)


def _validate_source_sql(sql: str) -> bool:
    """Reject output that still contains M constructs or has no FROM."""
    if not sql or _M_LEFTOVER.search(sql):
        return False
    return bool(re.search(r'\bFROM\b', sql, re.IGNORECASE))


async def _call_llm(prompt: str, system_prompt: str, model: str) -> dict:
    """Call LLM via LLMManager.completion() — auth handled internally."""
    from src.core.llm_manager import LLMManager
    from src.utils.telemetry import get_user_agent_header, KasalProduct

    try:
        content = await LLMManager.completion(
            messages=[
                {'role': 'system', 'content': system_prompt},
                {'role': 'user', 'content': prompt},
            ],
            model=model,
            temperature=0.1,
            max_tokens=1500,
            extra_headers=get_user_agent_header(KasalProduct.POWERBI),
        )
        return {'content': content}
    except Exception as e:  # noqa: BLE001 — fail-open
        logger.warning(f"[MQUERY_LLM] API call failed: {e}")
        return {'content': None, 'error': str(e)}


async def translate_mquery_to_sql(
    table_name: str,
    mquery: str,
    model: str = 'databricks-claude-sonnet-4-5',
    cache: OrderedDict | None = None,
) -> dict:
    """Translate a raw M expression to a Spark SQL source SELECT.

    Returns a dict: {success, source_sql, source_table, confidence, explanation}.
    Fail-open — on any error returns {'success': False, ...}.
    """
    _cache = cache if cache is not None else OrderedDict()
    cache_key = _content_hash(mquery)
    if cache_key in _cache:
        _cache.move_to_end(cache_key)
        return _cache[cache_key]

    response = await _call_llm(_build_user_prompt(table_name, mquery), _SYSTEM_PROMPT, model)
    if not response.get('content'):
        result = {'success': False, 'error': response.get('error', 'no response')}
    else:
        parsed = _parse_response(response['content'])
        sql = parsed.get('source_sql')
        if parsed.get('success') and sql and _validate_source_sql(sql):
            result = {
                'success': True,
                'source_sql': sql,
                'source_table': parsed.get('source_table'),
                'confidence': parsed.get('confidence', 'medium'),
                'explanation': parsed.get('explanation', ''),
            }
            logger.info(
                "[MQUERY_LLM] %s → %s (confidence=%s)",
                table_name, sql[:80], result['confidence'],
            )
        else:
            reason = parsed.get('error') or parsed.get('explanation') or 'not a queryable source'
            result = {'success': False, 'error': reason}
            logger.info("[MQUERY_LLM] Could not translate %s: %s", table_name, reason)

    if len(_cache) >= _RUN_CACHE_MAX:
        _cache.popitem(last=False)
    _cache[cache_key] = result
    return result


async def recover_sources_with_llm(
    mquery_entries: list[dict],
    model: str = 'databricks-claude-sonnet-4-5',
) -> tuple[list[dict], int]:
    """Best-effort: rewrite raw-M entries' transpiled_sql to a Spark SQL SELECT.

    Takes UCMV mquery entries ([{table_name, transpiled_sql, validation_passed}]),
    finds those that look like raw Power Query M, and replaces transpiled_sql with
    an LLM-derived SQL SELECT so MQueryParser can read them. Entries that already
    look like SQL, or that the LLM can't translate, are left unchanged.

    Returns (updated_entries, recovered_count). Processed sequentially to avoid
    rate limiting; fail-open per entry.
    """
    from .mquery_parser import looks_like_raw_mquery

    cache: OrderedDict = OrderedDict()
    recovered = 0
    out: list[dict] = []
    for entry in mquery_entries or []:
        if not isinstance(entry, dict):
            out.append(entry)
            continue
        sql = (entry.get('transpiled_sql') or '').strip()
        table_name = entry.get('table_name') or ''
        if not sql or not table_name or not looks_like_raw_mquery(sql):
            out.append(entry)
            continue
        try:
            res = await translate_mquery_to_sql(table_name, sql, model=model, cache=cache)
        except Exception as e:  # noqa: BLE001 — fail-open
            logger.warning("[MQUERY_LLM] recovery failed for %s: %s", table_name, e)
            out.append(entry)
            continue
        if res.get('success') and res.get('source_sql'):
            new_entry = {**entry, 'transpiled_sql': res['source_sql'], 'validation_passed': 'Yes'}
            out.append(new_entry)
            recovered += 1
        else:
            out.append(entry)

    if recovered:
        logger.info("[MQUERY_LLM] Recovered SQL source for %d/%d raw-M table(s)",
                    recovered, len(mquery_entries or []))
    return out, recovered
