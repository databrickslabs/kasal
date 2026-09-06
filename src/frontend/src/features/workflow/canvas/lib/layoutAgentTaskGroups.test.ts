import { describe, expect, it } from 'vitest';
import type { Node } from 'reactflow';
import { layoutAgentTaskGroups } from './layoutAgentTaskGroups';
const agent = (id: string, height = 172): Node => ({ id, type: 'agentNode', position: { x: 100, y: 100 }, width: 200, height, data: {} });
const task = (id: string, height = 174): Node => ({ id, type: 'taskNode', position: { x: 450, y: 100 }, width: 270, height, data: {} });
const center = (node: Node) => node.position.y + node.height! / 2;
const byId = (nodes: Node[], id: string) => nodes.find(node => node.id === id)!;

describe('Agent and task group alignment', () => {
  it.each([1, 2, 3, 4])('centers one agent against %i tasks', count => {
    const nodes = [agent('owner'), ...Array.from({ length: count }, (_, i) => task(`work-${i}`, i % 2 ? 194 : 174))];
    const before = structuredClone(nodes);
    const edges = nodes.slice(1).map(node => ({ source: 'owner', target: node.id }));
    const arranged = layoutAgentTaskGroups(nodes, edges, 'horizontal', { x: 100, y: 100 });
    const centers = nodes.slice(1).map(node => center(byId(arranged, node.id)));
    const middle = Math.floor(count / 2);
    const expected = count % 2 ? centers[middle] : (centers[middle - 1] + centers[middle]) / 2;
    expect(center(byId(arranged, 'owner'))).toBe(expected);
    expect(nodes).toEqual(before);
  });

  it('aligns a second agent with its last task after a two-task group', () => {
    const nodes = [agent('a'), agent('b'), task('one'), task('two'), task('three')];
    const edges = [{ source: 'a', target: 'one' }, { source: 'a', target: 'two' }, { source: 'b', target: 'three' }];
    const result = layoutAgentTaskGroups(nodes, edges, 'horizontal', { x: 100, y: 100 });
    expect(center(byId(result, 'a'))).toBe((center(byId(result, 'one')) + center(byId(result, 'two'))) / 2);
    expect(center(byId(result, 'b'))).toBe(center(byId(result, 'three')));
    expect(byId(result, 'three').position.y).toBeGreaterThan(byId(result, 'two').position.y + 174);
  });

  it('keeps tall agents and shared/duplicate task links from overlapping or duplicating nodes', () => {
    const nodes = [agent('a', 500), agent('b', 400), task('one'), task('two')];
    const edges = [{ source: 'a', target: 'one' }, { source: 'a', target: 'one' }, { source: 'b', target: 'one' }, { source: 'b', target: 'two' }];
    const result = layoutAgentTaskGroups(nodes, edges, 'horizontal', { x: 100, y: 100 });
    expect(result.map(node => node.id)).toEqual(nodes.map(node => node.id));
    expect(byId(result, 'b').position.y).toBeGreaterThanOrEqual(byId(result, 'a').position.y + 500 + 80);
    expect(center(byId(result, 'b'))).toBe(center(byId(result, 'two')));
  });

  it('re-centers an existing group when another task is added', () => {
    const edges = [{ source: 'a', target: 'one' }];
    const initial = layoutAgentTaskGroups([agent('a'), task('one')], edges, 'horizontal', { x: 100, y: 100 });
    const updated = layoutAgentTaskGroups([...initial, task('two')], [...edges, { source: 'a', target: 'two' }], 'horizontal', { x: 100, y: 100 });
    expect(center(byId(updated, 'a'))).toBe((center(byId(updated, 'one')) + center(byId(updated, 'two'))) / 2);
    expect(byId(updated, 'a').position.y).toBeGreaterThan(byId(initial, 'a').position.y);
  });
});
