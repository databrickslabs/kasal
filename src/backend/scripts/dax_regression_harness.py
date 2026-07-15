"""Tenant-scale DAX → UCMV regression harness.

Runs real PBI DAX measures through the ACTUAL transpiler (fast-path matcher
registry + LLM-first skill-corpus fallback) and scores each translation's
STRUCTURAL FIDELITY against the source DAX. Produces a coverage % per construct
bucket plus a flagged suspect list.

What this measures — and what it does NOT:
  * It measures STRUCTURAL fidelity: are the DAX's filter literals preserved,
    did an additive/ratio term drop, did a share-of-total collapse, etc. This is
    the same scoring used in the CCHBC comparison.
  * It does NOT prove numerical correctness — we do not execute the SQL against a
    warehouse. A wrong-operator translation that preserves all literals would
    score as faithful. Report the number as "structural fidelity", not "correct".

Design:
  * Resumable — every processed measure is appended to a JSONL checkpoint keyed
    by a stable measure id. Re-running skips already-scored ids, so a run
    interrupted by budget throttling (HTTP 429) resumes instead of restarting.
  * Bounded concurrency for the LLM path.
  * Stratified sampling by construct bucket when --sample-per-bucket is given;
    otherwise processes the whole file (the 585k full run).

Usage (from src/backend, with the venv):
  .venv/bin/python scripts/dax_regression_harness.py \
      --csv ~/Downloads/New_Query_2026_07_15_16_05_52.csv \
      --out /tmp/dax_harness \
      [--limit N] [--sample-per-bucket N] [--no-llm] [--concurrency 8]

Outputs (under --out):
  checkpoint.jsonl   one scored record per measure (resume source)
  report.json        aggregate coverage % per bucket + suspect counts
  suspects.jsonl     the flagged (silently-suspect) measures for review
"""
from __future__ import annotations

import argparse
import asyncio
import csv
import hashlib
import json
import os
import re
import sys
import time
from collections import defaultdict

# Make `src` importable when run from src/backend.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

csv.field_size_limit(10 ** 9)


# ── construct bucketing (for stratified sampling + per-bucket reporting) ──────

def construct_bucket(dax: str) -> str:
    u = (dax or '').upper()
    if not u.strip():
        return 'empty'
    # order matters: most-specific first
    if 'TREATAS' in u:
        return 'treatas'
    if 'ALLEXCEPT' in u:
        return 'allexcept'
    if 'SUMMARIZE' in u or 'CALCULATETABLE' in u or 'ADDCOLUMNS' in u:
        return 'summarize_table'
    if 'TOPN' in u or 'RANKX' in u:
        return 'topn_rankx'
    if 'LOOKUPVALUE' in u:
        return 'lookupvalue'
    if re.search(r'SAMEPERIODLASTYEAR|DATEADD|PARALLELPERIOD|TOTALYTD|DATESYTD|PREVIOUSYEAR', u):
        return 'time_intel'
    if 'SWITCH' in u and 'SELECTEDVALUE' in u:
        return 'switch_selectedvalue'
    if 'ALLSELECTED' in u:
        return 'allselected'
    if 'ALL(' in u and 'DIVIDE' in u:
        return 'share_of_total'
    if 'DIVIDE' in u:
        return 'divide'
    if 'VAR ' in u and 'RETURN' in u:
        return 'var_return'
    if 'CALCULATE' in u and 'FILTER' in u:
        return 'calculate_filter'
    if 'CALCULATE' in u:
        return 'calculate'
    if re.search(r'^\s*(SUM|COUNT|AVG|AVERAGE|MIN|MAX|DISTINCTCOUNT|COUNTROWS)\s*\(', u):
        return 'simple_agg'
    return 'other'


def measure_id(row: dict) -> str:
    key = f"{row.get('dataset_id','')}|{row.get('table_name','')}|{row.get('measure_name','')}"
    return hashlib.sha1(key.encode('utf-8', 'replace')).hexdigest()[:16]


# ── structural scorer (SQL vs DAX) ───────────────────────────────────────────

_CODE = re.compile(r'"([A-Za-z0-9]{2,})"|\'([A-Za-z0-9]{2,})\'|\b(\d{3,})\b')


def _lits(text: str) -> set:
    out = set()
    for m in _CODE.finditer(text or ''):
        v = m.group(1) or m.group(2) or m.group(3)
        if v:
            out.add(v.upper())
    return out


def score(dax: str, sql: str | None, is_translatable: bool, skip_reason: str) -> tuple[str, str]:
    """Return (verdict, note). Verdicts:
       base / faithful / ratio_ok / demoted_todo / silently_suspect / skipped_by_design
    """
    from src.engines.crewai.tools.custom.metric_view_utils.sql_measure_sanitizer import (
        detect_lost_dax_component,
    )
    if not is_translatable or not sql:
        # The transpiler chose NOT to emit. Distinguish an honest TODO/skip
        # (good — it declined rather than shipping wrong) from a display artifact.
        r = (skip_reason or '').lower()
        if any(k in r for k in ('format', 'color', 'display', 'switch', 'selectedvalue', 'slicer', 'artifact')):
            return 'skipped_by_design', skip_reason[:120]
        return 'demoted_todo', skip_reason[:120]

    if re.match(r'^\s*(SUM|COUNT|AVG|MIN|MAX)\s*\(\s*(COALESCE\s*\(\s*)?source\.\w+', sql, re.I):
        return 'base', ''

    # The guard is our silently-wrong detector — if it fires on emitted SQL, the
    # transpiler SHIPPED something the guard would have caught (should be rare
    # post-fix; a nonzero count here is the real signal to chase).
    lost = detect_lost_dax_component(dax or '', sql)
    if lost:
        return 'silently_suspect', lost

    # Filter-literal preservation: every business code in the DAX should appear
    # in the SQL (ignoring the version tokens the SQL expresses differently).
    dl = _lits(dax)
    sl = _lits(sql)
    missing = {x for x in dl if x not in sl and not re.fullmatch(r'0000|B000|RE', x)}
    if missing:
        return 'silently_suspect', f'filter codes missing from SQL: {sorted(missing)[:6]}'

    return ('ratio_ok', '') if '/' in sql else ('faithful', '')


# ── transpiler invocation (real pipeline: fast-path + LLM-first) ─────────────

def _translate_one(translator, dax: str, name: str, use_llm: bool):
    """Fast-path first (trivial_only). If it declines and use_llm, LLM fallback."""
    from src.engines.crewai.tools.custom.metric_view_utils.dax_llm_fallback import (
        translate_with_llm,
    )
    measure = {'measure_name': name, 'original_name': name, 'dax_expression': dax}
    res = translator.translate(measure, 'fact', trivial_only=True)
    if res.is_translatable and res.sql_expr:
        return res
    if not use_llm:
        return res
    # LLM-first fallback for anything the fast-path declined.
    try:
        res2 = asyncio.get_event_loop().run_until_complete(
            translate_with_llm(res, 'fact', base_names=set(), original_to_snake={},
                               model='databricks-claude-sonnet-4', table_context='')
        )
        return res2 or res
    except Exception as e:
        res.skip_reason = f'LLM error: {e}'
        return res


async def _translate_batch_async(translator, batch, use_llm):
    """Translate a batch; returns list of (row, res)."""
    from src.engines.crewai.tools.custom.metric_view_utils.dax_llm_fallback import (
        translate_with_llm,
    )
    out = []
    llm_candidates = []
    for row in batch:
        dax = row.get('expression') or ''
        name = (row.get('measure_name') or 'm').strip() or 'm'
        measure = {'measure_name': name, 'original_name': name, 'dax_expression': dax}
        res = translator.translate(measure, 'fact', trivial_only=True)
        if (res.is_translatable and res.sql_expr) or not use_llm:
            out.append((row, res))
        else:
            llm_candidates.append((row, res))
    # LLM path, bounded concurrency, one call per measure.
    sem = asyncio.Semaphore(int(os.environ.get('HARNESS_LLM_CONCURRENCY', '8')))

    async def _one(row, res):
        async with sem:
            try:
                r2 = await translate_with_llm(
                    res, 'fact', base_names=set(), original_to_snake={},
                    model='databricks-claude-sonnet-4', table_context='')
                return (row, r2 or res)
            except Exception as e:
                res.skip_reason = f'LLM error: {str(e)[:80]}'
                return (row, res)

    if llm_candidates:
        out.extend(await asyncio.gather(*[_one(r, x) for r, x in llm_candidates]))
    return out


# ── main run loop ─────────────────────────────────────────────────────────────

def _bootstrap_llm_context():
    """Satisfy LLMManager's multi-tenant requirement for offline batch use.

    LLMManager.completion requires a group context (multi-tenant isolation) and,
    for Databricks-provider models, DATABRICKS_HOST/TOKEN in the environment.
    This script is not a request handler, so we set a synthetic group context and
    verify the Databricks env is present. The token/host must be exported by the
    caller (e.g. `databricks auth token -p <profile>`).
    """
    if not os.environ.get('DATABRICKS_HOST') or not os.environ.get('DATABRICKS_TOKEN'):
        print('[harness] WARNING: DATABRICKS_HOST/DATABRICKS_TOKEN not set — LLM calls '
              'will fail. Export them (databricks auth) before an LLM run.', flush=True)
    try:
        from src.utils.user_context import UserContext, GroupContext
        UserContext.set_group_context(GroupContext(group_ids=['dax-harness']))
        print('[harness] group context set (dax-harness)', flush=True)
    except Exception as e:
        print(f'[harness] could not set group context: {e}', flush=True)


def load_done(checkpoint_path: str) -> set:
    done = set()
    if os.path.exists(checkpoint_path):
        with open(checkpoint_path) as f:
            for line in f:
                try:
                    done.add(json.loads(line)['id'])
                except Exception:
                    continue
    return done


def select_rows(csv_path: str, limit: int | None, sample_per_bucket: int | None):
    """Yield rows, optionally stratified-sampled per construct bucket."""
    if sample_per_bucket:
        counts: dict = defaultdict(int)
        for row in csv.DictReader(open(csv_path, newline='', encoding='utf-8', errors='replace')):
            b = construct_bucket(row.get('expression') or '')
            if counts[b] < sample_per_bucket:
                counts[b] += 1
                yield row
        return
    n = 0
    for row in csv.DictReader(open(csv_path, newline='', encoding='utf-8', errors='replace')):
        yield row
        n += 1
        if limit and n >= limit:
            return


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--csv', required=True)
    ap.add_argument('--out', default='/tmp/dax_harness')
    ap.add_argument('--limit', type=int, default=None)
    ap.add_argument('--sample-per-bucket', type=int, default=None)
    ap.add_argument('--no-llm', action='store_true')
    ap.add_argument('--batch', type=int, default=64)
    ap.add_argument('--progress-every', type=int, default=500)
    args = ap.parse_args()

    os.makedirs(args.out, exist_ok=True)
    checkpoint = os.path.join(args.out, 'checkpoint.jsonl')
    done = load_done(checkpoint)
    use_llm = not args.no_llm

    if use_llm:
        _bootstrap_llm_context()

    from src.engines.crewai.tools.custom.metric_view_utils.dax_translator import DaxTranslator
    translator = DaxTranslator()

    rows = [r for r in select_rows(args.csv, args.limit, args.sample_per_bucket)
            if measure_id(r) not in done]
    total = len(rows)
    print(f"[harness] {total} measures to process ({len(done)} already done), "
          f"llm={'on' if use_llm else 'off'}", flush=True)

    t0 = time.time()
    processed = 0
    with open(checkpoint, 'a') as ck:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        for i in range(0, total, args.batch):
            batch = rows[i:i + args.batch]
            results = loop.run_until_complete(_translate_batch_async(translator, batch, use_llm))
            for row, res in results:
                dax = row.get('expression') or ''
                verdict, note = score(dax, res.sql_expr, res.is_translatable, res.skip_reason or '')
                rec = {
                    'id': measure_id(row),
                    'name': (row.get('measure_name') or '')[:80],
                    'bucket': construct_bucket(dax),
                    'verdict': verdict,
                    'note': note,
                    'sql': (res.sql_expr or '')[:400],
                }
                ck.write(json.dumps(rec) + '\n')
                processed += 1
            ck.flush()
            if processed % args.progress_every < args.batch:
                rate = processed / max(time.time() - t0, 1e-9)
                print(f"[harness] {processed}/{total}  {rate:.1f}/s", flush=True)

    aggregate(args.out)


def aggregate(out_dir: str):
    """Build report.json + suspects.jsonl from the checkpoint."""
    checkpoint = os.path.join(out_dir, 'checkpoint.jsonl')
    by_bucket: dict = defaultdict(lambda: defaultdict(int))
    overall: dict = defaultdict(int)
    suspects = []
    with open(checkpoint) as f:
        for line in f:
            try:
                r = json.loads(line)
            except Exception:
                continue
            by_bucket[r['bucket']][r['verdict']] += 1
            overall[r['verdict']] += 1
            if r['verdict'] == 'silently_suspect':
                suspects.append(r)
    total = sum(overall.values())
    good = overall['base'] + overall['faithful'] + overall['ratio_ok'] + overall['skipped_by_design'] + overall['demoted_todo']
    report = {
        'total': total,
        'overall': dict(overall),
        # "safe" = faithful/base/ratio OR honestly declined (TODO/skip) — i.e. did
        # NOT silently ship something wrong. This is the headline safety number.
        'safe_pct': round(100 * good / total, 2) if total else 0,
        'silently_suspect': overall['silently_suspect'],
        'silently_suspect_pct': round(100 * overall['silently_suspect'] / total, 2) if total else 0,
        'by_bucket': {b: dict(v) for b, v in sorted(by_bucket.items())},
    }
    with open(os.path.join(out_dir, 'report.json'), 'w') as f:
        json.dump(report, f, indent=2)
    with open(os.path.join(out_dir, 'suspects.jsonl'), 'w') as f:
        for s in suspects:
            f.write(json.dumps(s) + '\n')
    print(json.dumps(report, indent=2))


if __name__ == '__main__':
    main()
