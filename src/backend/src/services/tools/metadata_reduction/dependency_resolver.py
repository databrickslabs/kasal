"""
DAX measure dependency resolver.

Parses DAX expressions to build a dependency graph, then resolves
transitive dependencies so the reducer can auto-include measures
that are referenced by the LLM-selected ones.

Borrows patterns from the IDOR_2.0 reference implementation.
"""

import re
import logging
from typing import Dict, List, Set

logger = logging.getLogger(__name__)

# Pattern to extract [MeasureName] references from DAX expressions.
# Excludes:
#   - Qualified column refs like 'Table'[Column] (preceded by ')
#   - Parameters like [@Param] (preceded by @)
_BRACKET_REF_PATTERN = re.compile(r"(?<!')(?<!\w)\[([^\]]+)\]")


class MeasureDependencyResolver:
    """Resolve transitive measure dependencies from DAX expressions."""

    def __init__(self, all_measures: List[dict], all_tables: List[dict]):
        """
        Args:
            all_measures: Flat list of measure dicts with 'name', 'expression', 'table'.
            all_tables: List of table dicts (used to map measures back to their tables).
        """
        self._measure_map: Dict[str, dict] = {}
        self._measure_to_table: Dict[str, str] = {}

        # Build measure index from flat measure list
        for m in all_measures:
            self._measure_map[m["name"]] = m
            if "table" in m:
                self._measure_to_table[m["name"]] = m["table"]

        # Also index from table-embedded measures
        for table in all_tables:
            for m in table.get("measures", []):
                if m["name"] not in self._measure_map:
                    self._measure_map[m["name"]] = m
                if m["name"] not in self._measure_to_table:
                    self._measure_to_table[m["name"]] = table["name"]

        self._dep_graph = self._build_graph()

    def _build_graph(self) -> Dict[str, Set[str]]:
        """Parse DAX expressions to find measure references.

        Pattern: [MeasureName] — bracketed names that match known measure names.
        Filters out qualified column references and parameter references.
        """
        graph: Dict[str, Set[str]] = {}
        known_names = set(self._measure_map.keys())

        for name, measure in self._measure_map.items():
            expression = measure.get("expression", "") or ""
            refs = self._extract_references(expression)
            # Only keep references that are actually known measures
            graph[name] = refs & known_names - {name}

        return graph

    @staticmethod
    def _extract_references(dax_expression: str) -> Set[str]:
        """Extract [MeasureName] references from a DAX expression."""
        if not dax_expression:
            return set()
        matches = _BRACKET_REF_PATTERN.findall(dax_expression)
        return {
            m for m in matches
            if "." not in m and not m.startswith("@")
        }

    def resolve(self, selected_measure_names: List[str]) -> List[dict]:
        """Given selected measure names, return all measures including
        transitive dependencies.

        Returns list of measure dicts (including dependency-added ones).
        Also adds a '_dependency_of' field to auto-included measures.
        """
        all_needed: Set[str] = set(selected_measure_names)
        dependencies_added: List[str] = []

        for name in selected_measure_names:
            transitive = self._get_all_dependencies(name)
            for dep in transitive:
                if dep not in all_needed:
                    all_needed.add(dep)
                    dependencies_added.append(dep)

        if dependencies_added:
            logger.info(
                "Dependency resolver auto-included %d measures: %s",
                len(dependencies_added),
                dependencies_added,
            )

        result = []
        for name in all_needed:
            measure = self._measure_map.get(name)
            if measure:
                m_copy = dict(measure)
                if name in dependencies_added:
                    m_copy["_dependency_of"] = [
                        sel for sel in selected_measure_names
                        if name in self._get_all_dependencies(sel)
                    ]
                result.append(m_copy)

        return result

    def _get_all_dependencies(
        self, measure_name: str, visited: Set[str] | None = None
    ) -> Set[str]:
        """Recursively resolve all transitive dependencies.

        Uses a visited set to handle circular references.
        """
        if visited is None:
            visited = set()
        if measure_name in visited:
            return set()
        visited.add(measure_name)

        direct_deps = self._dep_graph.get(measure_name, set())
        all_deps = set(direct_deps)
        for dep in direct_deps:
            all_deps |= self._get_all_dependencies(dep, visited)

        return all_deps

    def get_tables_for_measures(self, measure_names: List[str]) -> Set[str]:
        """Return the set of table names that contain the given measures."""
        tables = set()
        for name in measure_names:
            table = self._measure_to_table.get(name)
            if table:
                tables.add(table)
        return tables

    @property
    def dependency_graph(self) -> Dict[str, Set[str]]:
        """Expose the dependency graph for inspection."""
        return dict(self._dep_graph)
