import type { Node, Edge } from 'reactflow';

// Horizontal spacing (px) between node origins on the loaded row. Comfortably
// clears a crew node's width so adjacent nodes don't overlap.
const HORIZONTAL_NODE_SPACING = 360;
// Single shared y so every loaded node sits at the same height; fitView then
// centers the whole row vertically in the viewport.
const ROW_Y = 0;

// True when the loaded nodes already carry a real saved layout worth preserving
// (more than one node, sitting at distinct positions). A flow saved with branches
// must retain its exact placement; only degenerate loads (a single crew node opened
// from the catalog, or every node piled at the same/default point) get auto-arranged.
export const hasSavedLayout = (nodes: Node[]): boolean => {
  if (nodes.length <= 1) return false;
  const positions = new Set(
    nodes.map(n => `${Math.round(n.position?.x ?? 0)},${Math.round(n.position?.y ?? 0)}`),
  );
  return positions.size > 1;
};

// Lay nodes out left-to-right on one row, ordered by edge direction (topological)
// so a flow with no meaningful saved layout reads as a clean horizontal pipeline
// instead of a stack. All nodes share ROW_Y; combined with the subsequent fitView
// this leaves the row horizontally and vertically centered.
export const layoutFlowHorizontally = (nodes: Node[], edges: Edge[]): Node[] => {
  if (nodes.length === 0) return nodes;

  const indexById = new Map<string, number>(nodes.map((n, i) => [n.id, i]));
  const indegree = new Map<string, number>(nodes.map(n => [n.id, 0]));
  const adjacency = new Map<string, string[]>();
  edges.forEach(e => {
    if (!indexById.has(e.source) || !indexById.has(e.target)) return;
    adjacency.set(e.source, [...(adjacency.get(e.source) || []), e.target]);
    indegree.set(e.target, (indegree.get(e.target) || 0) + 1);
  });

  // Kahn's algorithm; ties resolved by original insertion order for stability.
  const ready = nodes.filter(n => (indegree.get(n.id) || 0) === 0).map(n => n.id);
  const ordered: string[] = [];
  const seen = new Set<string>();
  while (ready.length) {
    ready.sort((a, b) => (indexById.get(a) ?? 0) - (indexById.get(b) ?? 0));
    const id = ready.shift() as string;
    seen.add(id);
    ordered.push(id);
    (adjacency.get(id) || []).forEach(t => {
      indegree.set(t, (indegree.get(t) || 0) - 1);
      if ((indegree.get(t) || 0) <= 0 && !seen.has(t)) ready.push(t);
    });
  }
  // Append any nodes dropped by cycles, preserving their original order.
  nodes.forEach(n => { if (!seen.has(n.id)) ordered.push(n.id); });

  const columnByNodeId = new Map(ordered.map((id, i) => [id, i] as const));
  return nodes.map(n => ({
    ...n,
    position: { x: (columnByNodeId.get(n.id) ?? 0) * HORIZONTAL_NODE_SPACING, y: ROW_Y },
  }));
};
