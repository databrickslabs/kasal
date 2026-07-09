"""Tests for measure usage counter (derive_measure_usage).

Counts measure→measure references (DAX dependency in-degree) so reviewers can
prioritize gaps: a TODO that many measures depend on blocks that many downstream
translations. Measure references only — NOT dashboard/visual usage.
"""
import os
import sys

import pytest

# generate_config is a standalone script loaded from the custom-tools dir
# (the tool does `import generate_config`), so mirror that on sys.path.
_CUSTOM_TOOLS = os.path.join(
    os.path.dirname(__file__),
    '../../../../../../src/engines/crewai/tools/custom',
)
sys.path.insert(0, os.path.abspath(_CUSTOM_TOOLS))
import generate_config as gc  # noqa: E402


class TestDeriveMeasureUsage:
    def test_epl_example(self):
        """EPL used in one other measure → EPL count 1; the referencing measure 0."""
        usage = gc.derive_measure_usage([
            {'measure_name': 'EPL', 'expression': 'SUM(t[x])'},
            {'measure_name': 'Margin', 'expression': '[EPL] / [Revenue]'},
            {'measure_name': 'Revenue', 'expression': 'SUM(t[r])'},
        ])
        assert usage['EPL'] == 1
        assert usage['Revenue'] == 1
        assert usage['Margin'] == 0

    def test_every_measure_present_even_at_zero(self):
        usage = gc.derive_measure_usage([
            {'measure_name': 'A', 'expression': 'SUM(t[a])'},
            {'measure_name': 'B', 'expression': 'SUM(t[b])'},
        ])
        # Every real measure present at 0; snaked aliases ('a','b') added too.
        assert usage['A'] == 0 and usage['B'] == 0
        assert usage['a'] == 0 and usage['b'] == 0

    def test_table_column_refs_not_counted(self):
        """Table[col] must NOT be counted — only [MeasureRef] to a known measure."""
        usage = gc.derive_measure_usage([
            {'measure_name': 'Sales', 'expression': 'SUM(Sales[Amount]) + SUM(Sales[Tax])'},
        ])
        # 'Amount'/'Tax' aren't measures; 'Sales' preceded by nothing-as-measure-ref
        # here is a Table[col] pattern, so no self/other count.
        assert usage['Sales'] == 0

    def test_multiple_dependents_accumulate(self):
        usage = gc.derive_measure_usage([
            {'measure_name': 'Base', 'expression': 'SUM(t[x])'},
            {'measure_name': 'D1', 'expression': '[Base] * 2'},
            {'measure_name': 'D2', 'expression': '[Base] + 1'},
            {'measure_name': 'D3', 'expression': '[Base] - [D1]'},
        ])
        assert usage['Base'] == 3   # D1, D2, D3
        assert usage['D1'] == 1     # D3

    def test_same_ref_twice_in_one_measure_counts_once(self):
        """A measure referencing [Base] twice still counts as one dependent."""
        usage = gc.derive_measure_usage([
            {'measure_name': 'Base', 'expression': 'SUM(t[x])'},
            {'measure_name': 'Ratio', 'expression': '[Base] / ([Base] + 1)'},
        ])
        assert usage['Base'] == 1

    def test_self_reference_ignored(self):
        usage = gc.derive_measure_usage([
            {'measure_name': 'Recursive', 'expression': '[Recursive] + SUM(t[x])'},
        ])
        assert usage['Recursive'] == 0

    def test_cross_table_reference_counted(self):
        """References cross tables — a measure on table B referencing a measure
        on table A still counts for A (usage is global, not per-table)."""
        usage = gc.derive_measure_usage([
            {'measure_name': 'FactA_M', 'table_name': 'FactA', 'expression': 'SUM(a[x])'},
            {'measure_name': 'FactB_M', 'table_name': 'FactB', 'expression': '[FactA_M] * 2'},
        ])
        assert usage['FactA_M'] == 1

    def test_empty_expression_and_missing(self):
        usage = gc.derive_measure_usage([
            {'measure_name': 'Empty', 'expression': ''},
            {'measure_name': 'NoExpr'},
        ])
        # 'Empty' snakes to itself ('empty'); 'NoExpr' → 'no_expr' alias.
        assert usage == {'Empty': 0, 'NoExpr': 0, 'empty': 0, 'no_expr': 0}

    def test_snake_case_alias_added(self):
        """switch_decompositions entries are snake_cased, so the usage map must
        also resolve under the snaked name (F_Start_date → f_start_date)."""
        usage = gc.derive_measure_usage([
            {'measure_name': 'F_Start_date', 'expression': 'SUM(t[x])'},
            {'measure_name': 'Wrapper', 'expression': '[F_Start_date] + 1'},
        ])
        assert usage['F_Start_date'] == 1        # original PBI name
        assert usage['f_start_date'] == 1        # snaked alias (same count)

    def test_alias_never_clobbers_a_real_measure(self):
        """If a real measure already owns the snaked key, its own count wins —
        the alias must not overwrite it."""
        usage = gc.derive_measure_usage([
            # 'Foo_Bar' snakes to 'foo_bar', which is ALSO a real measure name.
            {'measure_name': 'Foo_Bar', 'expression': 'SUM(t[x])'},
            {'measure_name': 'foo_bar', 'expression': 'SUM(t[y])'},
            {'measure_name': 'RefsFooBar', 'expression': '[foo_bar] * 2'},
        ])
        # 'foo_bar' is referenced once (by RefsFooBar); the alias from 'Foo_Bar'
        # (count 0) must NOT clobber that real 1.
        assert usage['foo_bar'] == 1
