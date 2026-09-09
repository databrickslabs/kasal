"""Tests for UCMV re-evaluation: capability fingerprint, the retry engine, and the tool.

The feature re-tries ONLY previously-failed measures against today's transpiler and
reports which are now recoverable. See
src/docs/powerbi/ucmv-reevaluation-recoverable-measures.md
"""
import json
from unittest.mock import patch

from src.services.tools.metric_view_utils import capability_version as cv
from src.services.tools.metric_view_utils.reevaluate import (
    IMPOSSIBLE_CATEGORIES,
    RETRYABLE_CATEGORIES,
    is_retryable,
    reevaluate_measures,
    review_key,
)
from src.services.tools.ucmv_reevaluation_tool import UCMVReevaluationTool


# --------------------------------------------------------------------------
# Capability fingerprint
# --------------------------------------------------------------------------
class TestCapabilityFingerprint:
    def test_fingerprint_is_stable_and_non_trivial(self):
        cv.capability_fingerprint.cache_clear()
        fp1 = cv.capability_fingerprint()
        fp2 = cv.capability_fingerprint()
        assert fp1 == fp2
        assert fp1 != cv._UNKNOWN
        assert len(fp1) == 16

    def test_summary_reports_real_capability_surface(self):
        s = cv.capability_summary()
        assert s['pattern_count'] > 10, 'translator should register many patterns'
        assert s['skill_file_count'] >= 8, 'skill corpus should be present'
        # Corpus is markdown skills + the function-reference JSON data file
        # (function_ref_retriever's long-tail source, part of the capability surface).
        assert all(f.endswith(('.md', '.json')) for f in s['skill_files'])

    def test_change_detection(self):
        current = cv.capability_fingerprint()
        # Unknown/missing stored fingerprint → assume a gain is possible.
        assert cv.has_capability_changed(None) is True
        assert cv.has_capability_changed('') is True
        assert cv.has_capability_changed(cv._UNKNOWN) is True
        # Different → changed. Same → nothing to gain.
        assert cv.has_capability_changed('0000000000000000') is True
        assert cv.has_capability_changed(current) is False


# --------------------------------------------------------------------------
# Retry gating
# --------------------------------------------------------------------------
class TestRetryGating:
    def test_impossible_and_retryable_are_disjoint(self):
        assert not (IMPOSSIBLE_CATEGORIES & RETRYABLE_CATEGORIES)

    def test_permanent_categories_are_skipped_by_default(self):
        item = {'original_name': 'C', 'dax_expression': 'IF(1,"a","b")',
                'category': 'display artifact'}
        ok, why = is_retryable(item)
        assert ok is False
        assert 'permanent limitation' in why

    def test_permanent_categories_can_be_opted_into(self):
        item = {'original_name': 'C', 'dax_expression': 'IF(1,"a","b")',
                'category': 'display artifact'}
        ok, _ = is_retryable(item, include_impossible=True)
        assert ok is True

    def test_missing_dax_is_not_retryable(self):
        ok, why = is_retryable({'original_name': 'X', 'dax_expression': '   ',
                                'category': 'complex DAX — needs manual translation'})
        assert ok is False
        assert 'no DAX' in why

    def test_reviewer_dismissed_measures_are_suppressed(self):
        item = {'table_key': 't', 'original_name': 'M', 'dax_expression': 'SUM(t[a])',
                'category': 'complex DAX — needs manual translation'}
        for status in ('wont_fix', 'hand_written'):
            ok, why = is_retryable(item, review={review_key(item): {'status': status}})
            assert ok is False, status
            assert 'dismissed' in why
        # A non-settling status must NOT suppress
        ok, _ = is_retryable(item, review={review_key(item): {'status': 'todo'}})
        assert ok is True

    def test_llm_declined_classes_are_not_retried(self):
        """The LLM's own verdict is authoritative: if the LLM-first translator already
        classified a measure as untranslatable, retrying at the SAME capability level
        re-derives the same 'no' (and costs tokens with use_llm=true).

        REGRESSION: before this gate, 73 of 104 real recorded failures were retried
        even though the LLM had already declined 95 of them — the sweep looked busy
        but proposed nothing.
        """
        for cls in ('unsupported', 'display_layer', 'out_of_scope', 'architecture_change'):
            item = {'original_name': 'M', 'dax_expression': 'SUM(t[a])',
                    'category': 'unassigned', 'dax_class': cls}
            ok, why = is_retryable(item)
            assert ok is False, cls
            assert 'LLM already declined' in why

    def test_llm_declined_can_be_opted_into(self):
        item = {'original_name': 'M', 'dax_expression': 'SUM(t[a])',
                'category': 'unassigned', 'dax_class': 'unsupported'}
        assert is_retryable(item, include_impossible=True)[0] is True

    def test_silent_wrong_guard_rejections_ARE_retried(self):
        """Measures blocked by the silent-wrong guard (dropped ratio denominator,
        dropped additive term, unresolved measure ref) are genuine candidates — the
        LLM did not refuse them, the output guard did."""
        for cls in (None, 'composed', 'translatable_direct'):
            item = {
                'original_name': 'Total_NSR', 'dax_expression': 'var a=...\nreturn a+b',
                'category': 'unassigned', 'dax_class': cls,
                'skip_reason': 'TODO — not emitted (would be silently wrong/invalid: '
                               'additive term dropped)',
            }
            assert is_retryable(item)[0] is True, cls

    def test_review_key_matches_frontend_contract(self):
        assert review_key({'table_key': 'fact_x', 'original_name': 'My Measure'}) == 'fact_x::My Measure'


# --------------------------------------------------------------------------
# Re-evaluation engine
# --------------------------------------------------------------------------
class TestReevaluateMeasures:
    def _simple(self, name='Total Amount', refs=5):
        # A plain SUM: today's deterministic fast-path handles this.
        return {
            'table_key': 'fact_sales', 'original_name': name,
            'dax_expression': 'SUM(fact_sales[amount])',
            'skip_reason': 'no matching pattern',
            'category': 'complex DAX — needs manual translation',
            'referenced_by': refs,
        }

    def test_recovers_a_now_translatable_measure(self):
        out = reevaluate_measures([self._simple()], {})
        assert out['counts']['recovered'] == 1
        rec = out['newly_translatable'][0]
        assert rec['original_name'] == 'Total Amount'
        assert rec['new_sql']  # real SQL produced
        assert rec['previous_skip_reason'] == 'no matching pattern'
        assert rec['referenced_by'] == 5

    def test_never_retries_permanent_or_dismissed(self):
        items = [
            self._simple(),
            {'table_key': 'fact_sales', 'original_name': 'Color',
             'dax_expression': 'IF(1,"g","r")', 'category': 'display artifact',
             'referenced_by': 0},
            self._simple(name='Dismissed', refs=9),
        ]
        review = {'fact_sales::Dismissed': {'status': 'wont_fix'}}
        out = reevaluate_measures(items, {}, review=review)
        assert out['counts']['retried'] == 1
        assert out['counts']['skipped'] == 2
        names = {s['original_name'] for s in out['skipped']}
        assert names == {'Color', 'Dismissed'}

    def test_results_ordered_by_impact(self):
        out = reevaluate_measures(
            [self._simple(name='low', refs=1), self._simple(name='high', refs=42)], {}
        )
        assert [r['original_name'] for r in out['newly_translatable']] == ['high', 'low']

    def test_limit_applies_after_impact_ordering(self):
        items = [self._simple(name='low', refs=1), self._simple(name='high', refs=42)]
        out = reevaluate_measures(items, {}, limit=1)
        assert out['counts']['retried'] == 1
        # The high-impact one must be the one that got the budget.
        assert out['newly_translatable'][0]['original_name'] == 'high'

    def test_untranslatable_measure_reports_still_failing(self):
        item = {
            'table_key': 'fact_sales', 'original_name': 'Weird',
            # Deliberately not something the fast-path can do.
            'dax_expression': 'CALCULATE(SUMX(FILTER(ALL(x), x[a]>1), [m]), TREATAS({1}, y[b]))',
            'category': 'complex DAX — needs manual translation', 'referenced_by': 2,
        }
        out = reevaluate_measures([item], {})
        assert out['counts']['recovered'] == 0
        assert out['counts']['still_failing'] == 1

    def test_empty_input_is_safe(self):
        out = reevaluate_measures([], {})
        assert out['counts'] == {
            'considered': 0, 'retried': 0, 'recovered': 0,
            'still_failing': 0, 'skipped': 0,
        }

    def test_ignores_non_dict_rows(self):
        out = reevaluate_measures([self._simple(), 'garbage', 42], {})  # type: ignore[list-item]
        assert out['counts']['considered'] == 1


# --------------------------------------------------------------------------
# Tool
# --------------------------------------------------------------------------
class TestUCMVReevaluationTool:
    def _record(self, dataset_id, fingerprint, items):
        """An execution-sourced run dict (executionhistory is the sole source)."""
        return {
            'source': 'execution',
            'key': dataset_id,
            'label': dataset_id,
            'created_at': '2026-07-01',
            'items': items,
            'fingerprint': fingerprint,
            'config': {},
        }

    def _items(self):
        return [{
            'table_key': 'fact_sales', 'original_name': 'Total Amount',
            'dax_expression': 'SUM(fact_sales[amount])',
            'skip_reason': 'no matching pattern',
            'category': 'complex DAX — needs manual translation', 'referenced_by': 7,
        }]

    def _run_with(self, executions, **kwargs):
        """Drive the execution path (the tool's sole source).

        untranslatable_items lands in executionhistory.result, which
        _load_executions harvests; the tool reads nothing else.
        """
        async def _fake_load(self, ids, gid, maxd):
            return executions

        with patch.object(UCMVReevaluationTool, '_load_executions', _fake_load):
            return json.loads(UCMVReevaluationTool()._run(group_id='g1', **kwargs))

    def test_reports_recoveries_for_a_stale_run(self):
        out = self._run_with([self._record('ds-old', 'oldfingerprint00', self._items())])
        assert out['summary']['measures_recovered'] == 1
        ds = out['datasets'][0]
        assert ds['dataset_id'] == 'ds-old'
        assert ds['fingerprint_changed'] is True
        assert ds['newly_translatable'][0]['new_sql']

    def test_skips_runs_at_the_current_capability_level(self):
        current = cv.capability_fingerprint()
        out = self._run_with([self._record('ds-cur', current, self._items())])
        ds = out['datasets'][0]
        assert ds['fingerprint_changed'] is False
        assert ds['newly_translatable'] == []
        assert 'nothing to gain' in ds['note']
        assert out['summary']['measures_recovered'] == 0

    def test_force_retries_even_at_current_capability(self):
        """`force` exists so the sweep can be verified (and re-checked after a change
        the fingerprint didn't capture) without editing stored rows."""
        current = cv.capability_fingerprint()
        rec = [self._record('ds-cur', current, self._items())]
        without = self._run_with(rec)
        forced = self._run_with(rec, force=True)
        assert without['summary']['measures_recovered'] == 0
        assert forced['summary']['measures_recovered'] == 1
        assert forced['settings']['force'] is True

    def test_run_without_stored_items_is_reported_not_crashed(self):
        out = self._run_with([self._record('ds-empty', 'oldfingerprint00', [])])
        ds = out['datasets'][0]
        assert ds['newly_translatable'] == []
        assert out['summary']['measures_recovered'] == 0

    def test_reports_capability_and_settings(self):
        out = self._run_with([self._record('ds', 'old0', self._items())], use_llm=False)
        assert out['capability']['fingerprint'] == cv.capability_fingerprint()
        # use_llm must default OFF so a scheduled sweep burns no tokens.
        assert out['settings']['use_llm'] is False

    def test_load_failure_is_reported_not_raised(self):
        """A dead DB must produce an error report, never an exception that kills a
        scheduled sweep. The execution scan is the sole source, so its failure surfaces."""
        async def _boom(self, ids, gid, maxd):
            raise RuntimeError('db down')

        with patch.object(UCMVReevaluationTool, '_load_executions', _boom):
            out = json.loads(UCMVReevaluationTool()._run(group_id='g1'))
        assert 'db down' in out['error']
        assert out['datasets'] == []

    def test_unknown_fingerprint_is_labelled_honestly(self):
        """A run with no recorded fingerprint is retried (a gain can't be ruled out),
        but the report must NOT imply the transpiler is known to have improved."""
        ex = {
            'source': 'execution', 'key': 'job-2', 'label': 'old run',
            'created_at': '2026-07-01', 'items': self._items(),
            'fingerprint': None, 'config': {},
        }

        async def _execs(self, ids, gid, maxd):
            return [ex]

        with patch.object(UCMVReevaluationTool, '_load_executions', _execs):
            out = json.loads(UCMVReevaluationTool()._run(group_id='g1'))
        ds = out['datasets'][0]
        assert ds['fingerprint_changed'] is True
        assert 'pre-dates' in ds['fingerprint_note']

    def test_harvest_finds_items_nested_in_json_strings(self):
        """Tool outputs are embedded as JSON *strings* inside the execution blob, so a
        plain .get() misses them — the harvester must recurse and parse."""
        blob = {
            'content': {
                'tool_output': json.dumps({
                    'yaml': {}, 'untranslatable_items': [
                        {'original_name': 'M1', 'dax_expression': 'SUM(t[a])'},
                    ],
                }),
            },
        }
        found: list = []
        UCMVReevaluationTool._harvest_untranslatable(blob, found)
        assert len(found) == 1
        assert found[0]['original_name'] == 'M1'

    def test_parse_dataset_ids_accepts_json_csv_and_list(self):
        t = UCMVReevaluationTool()
        assert t._parse_dataset_ids('["a","b"]') == ['a', 'b']
        assert t._parse_dataset_ids('c, d') == ['c', 'd']
        assert t._parse_dataset_ids(['e']) == ['e']
        assert t._parse_dataset_ids(None) == []
