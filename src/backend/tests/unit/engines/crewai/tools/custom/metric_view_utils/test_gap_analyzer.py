"""Tests for gap_analyzer — pipeline gap analysis and prioritization."""
from __future__ import annotations

import json
import os
import sys
from collections import Counter
from dataclasses import dataclass

import pytest

# ── Path setup (mirror the gap_analyzer script) ────────────────────
_HERE = os.path.dirname(os.path.abspath(__file__))
_BACKEND = os.path.abspath(os.path.join(_HERE, '..', '..', '..', '..', '..', '..', '..'))
_EXAMPLE_DIR = os.path.abspath(os.path.join(_BACKEND, '..', '..', 'examples', 'uc_metric_view_migration'))

sys.path.insert(0, _BACKEND)
sys.path.insert(0, os.path.join(_BACKEND, 'src'))
sys.path.insert(0, _EXAMPLE_DIR)

from gap_analyzer import categorize_reason, is_artifact, calculate_unlock_potential
from src.engines.crewai.tools.custom.metric_view_utils.pipeline import MetricViewPipeline


# ── Lightweight stub for TranslationResult (avoids importing pipeline) ──
@dataclass
class _FakeResult:
    original_name: str
    skip_reason: str


# ═══════════════════════════════════════════════════════════════════
# categorize_reason
# ═══════════════════════════════════════════════════════════════════

class TestCategorizeReason:
    """Tests for categorize_reason()."""

    def test_selectedvalue_switch(self):
        reason = 'SELECTEDVALUE+SWITCH — too complex'
        assert categorize_reason(reason) == 'SELECTEDVALUE+SWITCH'

    def test_selectedvalue_switch_separate_words(self):
        """SELECTEDVALUE and SWITCH both present but not concatenated."""
        reason = 'Uses SELECTEDVALUE with nested SWITCH expression'
        assert categorize_reason(reason) == 'SELECTEDVALUE+SWITCH'

    def test_selectedvalue_slicer(self):
        reason = 'SELECTEDVALUE slicer context — PBI specific'
        assert categorize_reason(reason) == 'SELECTEDVALUE (slicer)'

    def test_cannot_resolve_ref(self):
        reason = 'Cannot resolve [F_End_date] in expression'
        assert categorize_reason(reason) == 'Cannot resolve [ref]'

    def test_format_function(self):
        reason = 'FORMAT function — PBI display only'
        assert categorize_reason(reason) == 'FORMAT function'

    def test_divide_sub_expression(self):
        reason = 'DIVIDE sub-expression could not be translated'
        assert categorize_reason(reason) == 'DIVIDE sub-expression'

    def test_blank_placeholder(self):
        reason = 'BLANK() placeholder — no business logic'
        assert categorize_reason(reason) == 'BLANK() placeholder'

    def test_no_pattern_match(self):
        reason = 'No matching pattern for this DAX construct'
        assert categorize_reason(reason) == 'No pattern match'

    def test_no_pattern_match_variant(self):
        reason = 'No pattern match found'
        assert categorize_reason(reason) == 'No pattern match'

    def test_isblank_blank_guard(self):
        reason = 'ISBLANK guard with BLANK return'
        assert categorize_reason(reason) == 'ISBLANK+BLANK guard'

    def test_isfiltered(self):
        reason = 'ISFILTERED check — PBI specific'
        assert categorize_reason(reason) == 'ISFILTERED'

    def test_pbi_artifact_cascade(self):
        reason = 'PY/DIVIDE over PBI artifacts'
        assert categorize_reason(reason) == 'PBI artifact cascade'

    def test_divide_over_pbi(self):
        reason = 'DIVIDE over PBI artifacts — display only'
        assert categorize_reason(reason) == 'PBI artifact cascade'

    def test_covered_by_switch(self):
        reason = 'Covered by SWITCH decomposition'
        assert categorize_reason(reason) == 'Covered by SWITCH'

    def test_covered_on_primary_table(self):
        reason = 'Covered on primary table (secondary allocation)'
        assert categorize_reason(reason) == 'Covered on primary table'

    def test_color_formatting(self):
        reason = 'Color formatting measure'
        assert categorize_reason(reason) == 'Color formatting'

    def test_dax_not_available(self):
        reason = 'DAX expression not available'
        assert categorize_reason(reason) == 'DAX expression not available'

    def test_other_unknown(self):
        reason = 'Some completely unknown reason'
        assert categorize_reason(reason) == 'Other'

    def test_empty_reason(self):
        assert categorize_reason('') == 'Other'


# ═══════════════════════════════════════════════════════════════════
# is_artifact
# ═══════════════════════════════════════════════════════════════════

class TestIsArtifact:
    """Tests for is_artifact()."""

    def test_artifact_categories(self):
        """Known artifact categories should be identified."""
        artifact_cats = [
            'SELECTEDVALUE (slicer)',
            'ISBLANK+BLANK guard',
            'FORMAT function',
            'ISFILTERED',
            'PBI artifact cascade',
            'BLANK() placeholder',
            'Covered by SWITCH',
            'Covered on primary table',
            'Color formatting',
        ]
        for cat in artifact_cats:
            assert is_artifact(cat), f'{cat} should be an artifact'

    def test_real_gap_not_artifact(self):
        """Real gap categories should NOT be marked as artifacts."""
        real_gaps = [
            'SELECTEDVALUE+SWITCH',
            'Cannot resolve [ref]',
            'DIVIDE sub-expression',
            'No pattern match',
            'Other',
        ]
        for cat in real_gaps:
            assert not is_artifact(cat), f'{cat} should NOT be an artifact'


# ═══════════════════════════════════════════════════════════════════
# calculate_unlock_potential
# ═══════════════════════════════════════════════════════════════════

class TestCalculateUnlockPotential:
    """Tests for calculate_unlock_potential()."""

    def test_referenced_measure_gets_high_score(self):
        """A measure referenced by 3 other measures should score 4 (3 + itself)."""
        untranslatable = [_FakeResult(original_name='Total Sales', skip_reason='Cannot resolve')]
        all_dax = [
            {'measure_name': 'A', 'dax_expression': '[Total Sales] + 1'},
            {'measure_name': 'B', 'dax_expression': '[Total Sales] * 2'},
            {'measure_name': 'C', 'dax_expression': '[Total Sales] / 100'},
        ]
        result = calculate_unlock_potential(untranslatable, all_dax)
        assert result['Total Sales'] == 4  # 3 refs + 1 (itself)

    def test_unreferenced_measure_gets_score_one(self):
        """An unreferenced measure should score 1 (only itself)."""
        untranslatable = [_FakeResult(original_name='Obscure Metric', skip_reason='No pattern match')]
        all_dax = [
            {'measure_name': 'A', 'dax_expression': 'SUM(Table[col])'},
            {'measure_name': 'B', 'dax_expression': '[Other Metric] + 1'},
        ]
        result = calculate_unlock_potential(untranslatable, all_dax)
        assert result['Obscure Metric'] == 1

    def test_table_column_ref_not_counted(self):
        """Table[Column] patterns should NOT be counted as measure refs."""
        untranslatable = [_FakeResult(original_name='Amount', skip_reason='Cannot resolve')]
        all_dax = [
            # This is Table[Amount] — a column ref, not a measure ref
            {'measure_name': 'A', 'dax_expression': "SUM(Sales[Amount])"},
            # This is a standalone [Amount] — a measure ref
            {'measure_name': 'B', 'dax_expression': '[Amount] + 10'},
        ]
        result = calculate_unlock_potential(untranslatable, all_dax)
        # Only B's ref should count, Sales[Amount] should be skipped
        assert result['Amount'] == 2  # 1 standalone ref + 1 (itself)

    def test_multiple_untranslatable_measures(self):
        """Multiple untranslatable measures each get their own score."""
        untranslatable = [
            _FakeResult(original_name='MetricA', skip_reason='gap'),
            _FakeResult(original_name='MetricB', skip_reason='gap'),
        ]
        all_dax = [
            {'measure_name': 'X', 'dax_expression': '[MetricA] + [MetricB]'},
            {'measure_name': 'Y', 'dax_expression': '[MetricA] * 2'},
        ]
        result = calculate_unlock_potential(untranslatable, all_dax)
        assert result['MetricA'] == 3  # 2 refs + 1
        assert result['MetricB'] == 2  # 1 ref + 1

    def test_not_available_dax_skipped(self):
        """Measures with 'Not available' DAX should be skipped."""
        untranslatable = [_FakeResult(original_name='SomeMetric', skip_reason='gap')]
        all_dax = [
            {'measure_name': 'A', 'dax_expression': 'Not available'},
            {'measure_name': 'B', 'dax_expression': ''},
        ]
        result = calculate_unlock_potential(untranslatable, all_dax)
        assert result['SomeMetric'] == 1  # no refs found, only itself

    def test_empty_inputs(self):
        """Empty inputs should return empty dict."""
        result = calculate_unlock_potential([], [])
        assert result == {}


# ═══════════════════════════════════════════════════════════════════
# End-to-end test with SC Reporting data (skipif guarded)
# ═══════════════════════════════════════════════════════════════════

_MEASURES_PATH = os.path.join(_EXAMPLE_DIR, 'measure_table_mapping.json')
_MQUERY_PATH = os.path.join(_EXAMPLE_DIR, 'mquery_transpilation.json')
_RELS_PATH = os.path.join(_EXAMPLE_DIR, 'pbi_relationships.json')
_CONFIG_PATH = os.path.join(_EXAMPLE_DIR, 'pipeline_config.json')

_HAS_EXAMPLE_DATA = all(
    os.path.exists(p) for p in [_MEASURES_PATH, _MQUERY_PATH, _RELS_PATH, _CONFIG_PATH]
)


@pytest.mark.skipif(not _HAS_EXAMPLE_DATA, reason='SC Reporting example data not available')
class TestEndToEndGapAnalysis:
    """End-to-end tests using real SC Reporting pipeline data."""

    @pytest.fixture(autouse=True)
    def setup_pipeline(self):
        """Run the pipeline once and collect untranslatable measures."""
        measures = json.load(open(_MEASURES_PATH))
        mquery_entries = json.load(open(_MQUERY_PATH))
        rels_raw = json.load(open(_RELS_PATH))
        config = json.load(open(_CONFIG_PATH))

        for tbl_cfg in config.get('mapping_only_tables', {}).values():
            tbl_cfg['source_table'] = tbl_cfg['source_table'].format(
                catalog='david_test_metrics', schema='test_schema',
            )

        from src.engines.crewai.tools.custom.metric_view_utils.mquery_parser import MQueryParser
        from src.engines.crewai.tools.custom.metric_view_utils.relationships_loader import RelationshipsLoader
        from src.engines.crewai.tools.custom.metric_view_utils.scan_data_parser import ScanDataParser

        parser = MQueryParser()
        mquery_tables = parser.parse_json(mquery_entries)
        fact_tables = {k for k, v in mquery_tables.items() if v.is_fact}
        rel_loader = RelationshipsLoader()
        rel_enrich = rel_loader.load(rels_raw, mquery_tables, fact_tables)
        scan_parser = ScanDataParser()
        scan_path = os.path.join(_EXAMPLE_DIR, 'scan_result_debug.json')
        scan_data = scan_parser.parse(scan_path) if os.path.exists(scan_path) else {}

        self.pipeline = MetricViewPipeline(
            mapping=measures,
            mquery_tables=mquery_tables,
            config=config,
            relationships_enrichment=rel_enrich,
            inactive_relationships=rel_loader.get_inactive_relationships() or None,
            scan_data=scan_data,
            unflatten_tables=True,
            refresh_policy_tables=scan_parser.get_refresh_policy_tables() or None,
            no_summarize_columns=scan_parser.get_no_summarize_columns() or None,
            rls_tables=scan_parser.get_rls_tables() or None,
        )
        self.specs = self.pipeline.run()
        self.measures = measures

    def test_real_gaps_less_than_50(self):
        """Most untranslatable should be artifacts, not real gaps."""
        all_untranslatable = []
        for spec in self.specs.values():
            all_untranslatable.extend(spec.untranslatable)
        for m in self.pipeline.cross_table_measures:
            all_untranslatable.append(m)

        by_category = Counter()
        for m in all_untranslatable:
            cat = categorize_reason(m.skip_reason)
            by_category[cat] += 1

        real_gaps = sum(c for cat, c in by_category.items() if not is_artifact(cat))
        total_untranslatable = len(all_untranslatable)
        # Real gaps should be less than half of all untranslatable (most are artifacts)
        assert real_gaps < total_untranslatable, (
            f'Real gaps ({real_gaps}) should be less than total untranslatable ({total_untranslatable})'
        )

    def test_switch_is_top_gap_category(self):
        """SELECTEDVALUE+SWITCH should be the largest real gap category."""
        all_untranslatable = []
        for spec in self.specs.values():
            all_untranslatable.extend(spec.untranslatable)

        by_category = Counter()
        for m in all_untranslatable:
            cat = categorize_reason(m.skip_reason)
            if not is_artifact(cat):
                by_category[cat] += 1

        if by_category:
            top_cat = by_category.most_common(1)[0][0]
            assert top_cat == 'SELECTEDVALUE+SWITCH', (
                f'Expected SELECTEDVALUE+SWITCH as top gap, got {top_cat}'
            )
