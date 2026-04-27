"""Unit tests for MeasureDependencyResolver in metadata_reduction package."""

import pytest
from src.engines.crewai.tools.custom.metadata_reduction.dependency_resolver import (
    MeasureDependencyResolver,
)


def _make_measures_and_tables():
    """Create a sample set of measures and tables for testing."""
    measures = [
        {"name": "Total Revenue", "expression": "SUM(Sales[Revenue])", "table": "Sales"},
        {"name": "Total Cost", "expression": "SUM(Sales[Cost])", "table": "Sales"},
        {"name": "Gross Profit", "expression": "[Total Revenue] - [Total Cost]", "table": "Sales"},
        {"name": "Profit Margin", "expression": "DIVIDE([Gross Profit], [Total Revenue])", "table": "Sales"},
        {"name": "Country Count", "expression": "DISTINCTCOUNT(Geography[Country])", "table": "Geography"},
        {"name": "Standalone", "expression": "42", "table": "Other"},
    ]
    tables = [
        {"name": "Sales", "measures": measures[:4]},
        {"name": "Geography", "measures": [measures[4]]},
        {"name": "Other", "measures": [measures[5]]},
    ]
    return measures, tables


class TestBuildGraph:
    def test_builds_dependency_graph(self):
        measures, tables = _make_measures_and_tables()
        resolver = MeasureDependencyResolver(measures, tables)
        graph = resolver.dependency_graph

        assert "Total Revenue" in graph
        assert graph["Total Revenue"] == set()
        assert graph["Total Cost"] == set()
        assert "Total Revenue" in graph["Gross Profit"]
        assert "Total Cost" in graph["Gross Profit"]
        assert "Gross Profit" in graph["Profit Margin"]
        assert "Total Revenue" in graph["Profit Margin"]

    def test_ignores_column_references(self):
        measures = [
            {"name": "M1", "expression": "SUM('Sales'[Revenue])", "table": "Sales"},
        ]
        tables = [{"name": "Sales", "measures": measures}]
        resolver = MeasureDependencyResolver(measures, tables)
        # 'Sales'[Revenue] is a qualified column ref, not a measure ref
        assert resolver.dependency_graph["M1"] == set()

    def test_ignores_parameter_references(self):
        measures = [
            {"name": "M1", "expression": "SUM([@Value])", "table": "Sales"},
        ]
        tables = [{"name": "Sales", "measures": measures}]
        resolver = MeasureDependencyResolver(measures, tables)
        assert resolver.dependency_graph["M1"] == set()

    def test_handles_empty_expression(self):
        measures = [
            {"name": "M1", "expression": "", "table": "Sales"},
            {"name": "M2", "expression": None, "table": "Sales"},
        ]
        tables = [{"name": "Sales", "measures": measures}]
        resolver = MeasureDependencyResolver(measures, tables)
        assert resolver.dependency_graph["M1"] == set()
        assert resolver.dependency_graph["M2"] == set()

    def test_excludes_self_references(self):
        measures = [
            {"name": "M1", "expression": "[M1] + 1", "table": "Sales"},
        ]
        tables = [{"name": "Sales", "measures": measures}]
        resolver = MeasureDependencyResolver(measures, tables)
        assert "M1" not in resolver.dependency_graph["M1"]


class TestResolve:
    def test_resolves_direct_dependencies(self):
        measures, tables = _make_measures_and_tables()
        resolver = MeasureDependencyResolver(measures, tables)
        result = resolver.resolve(["Gross Profit"])

        names = {m["name"] for m in result}
        assert "Gross Profit" in names
        assert "Total Revenue" in names
        assert "Total Cost" in names

    def test_resolves_transitive_dependencies(self):
        measures, tables = _make_measures_and_tables()
        resolver = MeasureDependencyResolver(measures, tables)
        result = resolver.resolve(["Profit Margin"])

        names = {m["name"] for m in result}
        # Profit Margin -> Gross Profit -> Total Revenue, Total Cost
        assert "Profit Margin" in names
        assert "Gross Profit" in names
        assert "Total Revenue" in names
        assert "Total Cost" in names

    def test_does_not_include_unrelated(self):
        measures, tables = _make_measures_and_tables()
        resolver = MeasureDependencyResolver(measures, tables)
        result = resolver.resolve(["Profit Margin"])

        names = {m["name"] for m in result}
        assert "Country Count" not in names
        assert "Standalone" not in names

    def test_standalone_measure_resolves_alone(self):
        measures, tables = _make_measures_and_tables()
        resolver = MeasureDependencyResolver(measures, tables)
        result = resolver.resolve(["Standalone"])

        assert len(result) == 1
        assert result[0]["name"] == "Standalone"

    def test_marks_dependency_of(self):
        measures, tables = _make_measures_and_tables()
        resolver = MeasureDependencyResolver(measures, tables)
        result = resolver.resolve(["Gross Profit"])

        dep_measures = [m for m in result if "_dependency_of" in m]
        assert len(dep_measures) >= 2  # Total Revenue and Total Cost

    def test_handles_circular_dependencies(self):
        measures = [
            {"name": "A", "expression": "[B] + 1", "table": "T"},
            {"name": "B", "expression": "[C] + 1", "table": "T"},
            {"name": "C", "expression": "[A] + 1", "table": "T"},
        ]
        tables = [{"name": "T", "measures": measures}]
        resolver = MeasureDependencyResolver(measures, tables)
        # Should not infinite loop
        result = resolver.resolve(["A"])
        names = {m["name"] for m in result}
        assert "A" in names
        assert "B" in names
        assert "C" in names

    def test_multiple_selected_measures(self):
        measures, tables = _make_measures_and_tables()
        resolver = MeasureDependencyResolver(measures, tables)
        result = resolver.resolve(["Total Revenue", "Country Count"])

        names = {m["name"] for m in result}
        assert "Total Revenue" in names
        assert "Country Count" in names
        assert len(result) == 2

    def test_empty_selection(self):
        measures, tables = _make_measures_and_tables()
        resolver = MeasureDependencyResolver(measures, tables)
        result = resolver.resolve([])
        assert result == []

    def test_unknown_measure_ignored(self):
        measures, tables = _make_measures_and_tables()
        resolver = MeasureDependencyResolver(measures, tables)
        result = resolver.resolve(["Nonexistent Measure"])
        assert result == []


class TestGetTablesForMeasures:
    def test_returns_correct_tables(self):
        measures, tables = _make_measures_and_tables()
        resolver = MeasureDependencyResolver(measures, tables)

        table_set = resolver.get_tables_for_measures(["Total Revenue", "Country Count"])
        assert "Sales" in table_set
        assert "Geography" in table_set

    def test_empty_list_returns_empty(self):
        measures, tables = _make_measures_and_tables()
        resolver = MeasureDependencyResolver(measures, tables)
        assert resolver.get_tables_for_measures([]) == set()

    def test_unknown_measure_ignored(self):
        measures, tables = _make_measures_and_tables()
        resolver = MeasureDependencyResolver(measures, tables)
        result = resolver.get_tables_for_measures(["Nonexistent"])
        assert result == set()


class TestMeasureIndexing:
    def test_indexes_from_flat_list(self):
        measures = [
            {"name": "M1", "expression": "1", "table": "T1"},
        ]
        resolver = MeasureDependencyResolver(measures, [])
        assert "M1" in resolver._measure_map

    def test_indexes_from_table_embedded(self):
        tables = [
            {"name": "T1", "measures": [{"name": "M1", "expression": "1"}]},
        ]
        resolver = MeasureDependencyResolver([], tables)
        assert "M1" in resolver._measure_map

    def test_flat_list_takes_precedence(self):
        measures = [
            {"name": "M1", "expression": "flat_expr", "table": "T1"},
        ]
        tables = [
            {"name": "T1", "measures": [{"name": "M1", "expression": "table_expr"}]},
        ]
        resolver = MeasureDependencyResolver(measures, tables)
        assert resolver._measure_map["M1"]["expression"] == "flat_expr"
