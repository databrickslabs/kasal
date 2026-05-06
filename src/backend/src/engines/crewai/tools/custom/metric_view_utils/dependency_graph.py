"""Measure Dependency Graph — topological sort for ordered measure translation.

Implements Kahn's algorithm for topological sorting of measure-to-measure
dependencies, with cycle detection. Used by the pipeline's Pass 2 to ensure
measures are translated in the correct order (leaves first, composed measures last).
"""
from __future__ import annotations

import re
from collections import deque


def _find_measure_refs(dax: str, all_measure_names: set[str]) -> set[str]:
    """Extract measure references from a DAX expression.

    Matches [MeasureName] patterns but NOT Table[Column] patterns.
    Only returns names that exist in all_measure_names.

    Args:
        dax: DAX expression string
        all_measure_names: Set of known measure names to validate against

    Returns:
        Set of referenced measure names
    """
    refs: set[str] = set()
    for m in re.finditer(r'\[([^\[\]]+)\]', dax):
        ref_name = m.group(1)
        # Skip Table[col] patterns: preceded by a word character (table name)
        before = dax[:m.start()].rstrip()
        if before and re.search(r'\w$', before):
            continue
        if ref_name in all_measure_names:
            refs.add(ref_name)
    return refs


def build_dependency_graph(measures: list[dict]) -> dict:
    """Build a measure dependency DAG with topological ordering.

    Args:
        measures: List of measure dicts with 'measure_name'/'name' and
                  'dax_expression' fields

    Returns:
        Dict with:
        - adjacency: {measure_name: [measures_it_depends_on]}
        - reverse_adjacency: {measure_name: [measures_that_depend_on_it]}
        - topo_order: topologically sorted list (leaves first)
        - leaves: measures with zero measure dependencies
        - roots: measures that nothing depends on
        - cycles: list of cyclic measure groups (empty if acyclic)
    """
    # Build name set
    all_names: set[str] = set()
    name_to_dax: dict[str, str] = {}
    for m in measures:
        name = m.get('measure_name', m.get('name', m.get('original_name', '')))
        if name:
            all_names.add(name)
            name_to_dax[name] = m.get('dax_expression', '')

    # Build adjacency: measure → [measures it depends on]
    adjacency: dict[str, list[str]] = {}
    reverse_adj: dict[str, list[str]] = {}
    in_degree: dict[str, int] = {}

    for name in all_names:
        dax = name_to_dax.get(name, '')
        deps = _find_measure_refs(dax, all_names - {name})  # exclude self-refs
        adjacency[name] = sorted(deps)
        in_degree.setdefault(name, 0)
        for dep in deps:
            reverse_adj.setdefault(dep, []).append(name)
            in_degree[name] = in_degree.get(name, 0) + 1

    # Ensure all names in in_degree
    for name in all_names:
        in_degree.setdefault(name, 0)
        reverse_adj.setdefault(name, [])

    # Kahn's algorithm — topological sort
    queue: deque[str] = deque()
    for name, deg in in_degree.items():
        if deg == 0:
            queue.append(name)

    topo_order: list[str] = []
    while queue:
        node = queue.popleft()
        topo_order.append(node)
        for dependent in reverse_adj.get(node, []):
            in_degree[dependent] -= 1
            if in_degree[dependent] == 0:
                queue.append(dependent)

    # Cycle detection: any node not in topo_order is in a cycle
    in_topo = set(topo_order)
    cyclic = [name for name in all_names if name not in in_topo]
    cycles: list[list[str]] = []
    if cyclic:
        # Group connected cyclic nodes
        visited: set[str] = set()
        for name in cyclic:
            if name not in visited:
                group = _find_cycle_group(name, adjacency, set(cyclic), visited)
                if group:
                    cycles.append(sorted(group))

    leaves = [name for name in topo_order if not adjacency.get(name)]
    roots = [name for name in topo_order if not reverse_adj.get(name)]

    return {
        'adjacency': adjacency,
        'reverse_adjacency': reverse_adj,
        'topo_order': topo_order,
        'leaves': leaves,
        'roots': roots,
        'cycles': cycles,
    }


def _find_cycle_group(
    start: str,
    adjacency: dict[str, list[str]],
    cyclic_set: set[str],
    visited: set[str],
) -> list[str]:
    """Find a connected group of cyclic nodes starting from 'start'."""
    group: list[str] = []
    stack = [start]
    while stack:
        node = stack.pop()
        if node in visited:
            continue
        if node not in cyclic_set:
            continue
        visited.add(node)
        group.append(node)
        for dep in adjacency.get(node, []):
            if dep in cyclic_set and dep not in visited:
                stack.append(dep)
    return group
