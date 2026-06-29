import { Node, Edge } from 'reactflow';
import { layoutFlowHorizontally, hasSavedLayout } from './WorkflowEventHandlers';

const node = (id: string, x = 0, y = 0): Node => ({
  id,
  type: 'crewNode',
  position: { x, y },
  data: { label: id },
});

const edge = (source: string, target: string): Edge => ({
  id: `${source}-${target}`,
  source,
  target,
});

describe('layoutFlowHorizontally', () => {
  it('returns nodes unchanged when there are none', () => {
    expect(layoutFlowHorizontally([], [])).toEqual([]);
  });

  it('places every node at the same height (single centered row)', () => {
    const result = layoutFlowHorizontally(
      [node('a', 10, 500), node('b', 800, 30), node('c', 50, -200)],
      [],
    );
    const ys = result.map(n => n.position.y);
    expect(new Set(ys).size).toBe(1);
  });

  it('orders nodes left-to-right following edge direction', () => {
    // Saved positions are deliberately scrambled (c left-most, a right-most).
    const nodes = [node('a', 900, 0), node('b', 400, 0), node('c', 0, 0)];
    const edges = [edge('a', 'b'), edge('b', 'c')];
    const result = layoutFlowHorizontally(nodes, edges);
    const byId = Object.fromEntries(result.map(n => [n.id, n.position.x]));
    // a -> b -> c, so x should increase in that order regardless of saved x.
    expect(byId.a).toBeLessThan(byId.b);
    expect(byId.b).toBeLessThan(byId.c);
  });

  it('spaces nodes evenly along the row', () => {
    const result = layoutFlowHorizontally(
      [node('a'), node('b'), node('c')],
      [edge('a', 'b'), edge('b', 'c')],
    );
    const xs = result.map(n => n.position.x).sort((p, q) => p - q);
    expect(xs[0]).toBe(0);
    expect(xs[1] - xs[0]).toBe(xs[2] - xs[1]);
  });

  it('still lays out disconnected nodes without dropping any', () => {
    const result = layoutFlowHorizontally([node('a'), node('b'), node('c')], []);
    expect(result).toHaveLength(3);
    expect(new Set(result.map(n => n.position.x)).size).toBe(3);
  });

  it('does not hang or drop nodes when edges form a cycle', () => {
    const result = layoutFlowHorizontally(
      [node('a'), node('b'), node('c')],
      [edge('a', 'b'), edge('b', 'c'), edge('c', 'a')],
    );
    expect(result).toHaveLength(3);
  });

  it('ignores edges that reference a node not in the set', () => {
    // Edge target "ghost" is not among the nodes — it must be skipped, not crash.
    const result = layoutFlowHorizontally(
      [node('a', 0, 0), node('b', 0, 0)],
      [edge('a', 'ghost'), edge('ghost', 'b')],
    );
    expect(result).toHaveLength(2);
    // With no valid edges both are roots → laid out as a row at distinct x.
    expect(new Set(result.map(n => n.position.x)).size).toBe(2);
  });
});

describe('hasSavedLayout', () => {
  it('is false for an empty or single-node flow (auto-arrange those)', () => {
    expect(hasSavedLayout([])).toBe(false);
    expect(hasSavedLayout([node('a', 100, 200)])).toBe(false);
  });

  it('is false when every node is piled at the same point (degenerate load)', () => {
    expect(hasSavedLayout([node('a', 250, 150), node('b', 250, 150)])).toBe(false);
  });

  it('is true for a real saved layout with distinct branch positions', () => {
    // A saved flow with branches: nodes at genuinely different coordinates.
    const nodes = [node('a', 0, 0), node('b', 300, -120), node('c', 300, 120)];
    expect(hasSavedLayout(nodes)).toBe(true);
  });
});
