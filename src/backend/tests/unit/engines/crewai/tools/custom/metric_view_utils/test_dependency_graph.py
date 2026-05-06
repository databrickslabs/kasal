"""Tests for measure dependency graph — topological sort + cycle detection."""
import pytest
from src.engines.crewai.tools.custom.metric_view_utils.dependency_graph import (
    _find_measure_refs,
    build_dependency_graph,
)


class TestFindMeasureRefs:
    def test_simple_ref(self):
        refs = _find_measure_refs('[Total Sales] + 1', {'Total Sales', 'Other'})
        assert refs == {'Total Sales'}

    def test_table_col_excluded(self):
        refs = _find_measure_refs('SUM(Sales[Amount])', {'Amount', 'Sales'})
        assert refs == set()  # Sales[Amount] is Table[col], not a measure ref

    def test_multiple_refs(self):
        refs = _find_measure_refs('[A] - [B] + [C]', {'A', 'B', 'C', 'D'})
        assert refs == {'A', 'B', 'C'}

    def test_unknown_refs_excluded(self):
        refs = _find_measure_refs('[Known] + [Unknown]', {'Known'})
        assert refs == {'Known'}

    def test_self_ref_included_in_extraction(self):
        # Self-refs are excluded by build_dependency_graph, not _find_measure_refs
        refs = _find_measure_refs('[Self]', {'Self'})
        assert refs == {'Self'}

    def test_divide_pattern(self):
        refs = _find_measure_refs('DIVIDE([Num], [Den])', {'Num', 'Den'})
        assert refs == {'Num', 'Den'}

    def test_no_refs(self):
        refs = _find_measure_refs('SUM(Table[Col]) + 1', {'Total'})
        assert refs == set()


class TestBuildDependencyGraph:
    def test_simple_chain(self):
        measures = [
            {'measure_name': 'A', 'dax_expression': 'SUM(T[x])'},
            {'measure_name': 'B', 'dax_expression': '[A] + 1'},
            {'measure_name': 'C', 'dax_expression': '[B] * [A]'},
        ]
        graph = build_dependency_graph(measures)
        assert graph['leaves'] == ['A']
        assert graph['topo_order'].index('A') < graph['topo_order'].index('B')
        assert graph['topo_order'].index('B') < graph['topo_order'].index('C')
        assert graph['cycles'] == []

    def test_no_dependencies(self):
        measures = [
            {'measure_name': 'X', 'dax_expression': 'SUM(T[a])'},
            {'measure_name': 'Y', 'dax_expression': 'SUM(T[b])'},
        ]
        graph = build_dependency_graph(measures)
        assert set(graph['leaves']) == {'X', 'Y'}
        assert len(graph['topo_order']) == 2
        assert graph['cycles'] == []

    def test_cycle_detection(self):
        measures = [
            {'measure_name': 'A', 'dax_expression': '[B] + 1'},
            {'measure_name': 'B', 'dax_expression': '[A] + 1'},
        ]
        graph = build_dependency_graph(measures)
        assert len(graph['cycles']) >= 1
        cycle_members = set()
        for cycle in graph['cycles']:
            cycle_members.update(cycle)
        assert 'A' in cycle_members
        assert 'B' in cycle_members

    def test_diamond_dependency(self):
        measures = [
            {'measure_name': 'Base', 'dax_expression': 'SUM(T[x])'},
            {'measure_name': 'Left', 'dax_expression': '[Base] * 2'},
            {'measure_name': 'Right', 'dax_expression': '[Base] * 3'},
            {'measure_name': 'Top', 'dax_expression': '[Left] + [Right]'},
        ]
        graph = build_dependency_graph(measures)
        order = graph['topo_order']
        assert order.index('Base') < order.index('Left')
        assert order.index('Base') < order.index('Right')
        assert order.index('Left') < order.index('Top')
        assert order.index('Right') < order.index('Top')

    def test_empty_input(self):
        graph = build_dependency_graph([])
        assert graph['topo_order'] == []
        assert graph['leaves'] == []
        assert graph['cycles'] == []

    def test_mixed_translatable_and_refs(self):
        measures = [
            {'measure_name': 'Sales', 'dax_expression': 'SUM(Fact[Amount])'},
            {'measure_name': 'Cost', 'dax_expression': 'SUM(Fact[Cost])'},
            {'measure_name': 'Profit', 'dax_expression': '[Sales] - [Cost]'},
            {'measure_name': 'Margin', 'dax_expression': 'DIVIDE([Profit], [Sales])'},
        ]
        graph = build_dependency_graph(measures)
        order = graph['topo_order']
        assert order.index('Sales') < order.index('Profit')
        assert order.index('Cost') < order.index('Profit')
        assert order.index('Profit') < order.index('Margin')
        assert graph['cycles'] == []

    def test_roots_identified(self):
        measures = [
            {'measure_name': 'A', 'dax_expression': 'SUM(T[x])'},
            {'measure_name': 'B', 'dax_expression': '[A] + 1'},
        ]
        graph = build_dependency_graph(measures)
        assert 'B' in graph['roots']  # nothing depends on B
        assert 'A' not in graph['roots']  # B depends on A
