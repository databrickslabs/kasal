import { describe, it, expect } from 'vitest';
import { Node } from 'reactflow';
import { CanvasLayoutManager } from './CanvasLayoutManager';

const node = (id: string, type: string, x: number, y: number, height?: number, width?: number): Node => ({ id, type, position: { x, y }, data: {}, height, width });

describe('Appending individual builder nodes', () => {
  const manager = () => new CanvasLayoutManager({ margin: 20, minNodeSpacing: 50 });

  it.each(['agentNode', 'taskNode'])('stacks %s below existing peers, including a zero x coordinate', type => {
    const layout = manager();
    const existing = [node('first', type, 0, -100, 300), node('second', type, 0, 100, 100)];
    const before = structuredClone(existing);
    const position = type === 'agentNode' ? layout.getAgentNodePosition(existing) : layout.getTaskNodePosition(existing);
    expect(position.x).toBe(0);
    expect(position.y).toBeGreaterThanOrEqual(250);
    expect(existing).toEqual(before);
  });

  it('repeated tasks without agents get distinct, vertically stacked positions', () => {
    const layout = manager();
    const nodes: Node[] = [];
    for (let i = 0; i < 4; i++) {
      const position = layout.getTaskNodePosition(nodes);
      if (i) {
        expect(position.x).toBe(nodes[0].position.x);
        expect(position.y).toBeGreaterThanOrEqual(nodes[i - 1].position.y + 230 + 50);
      }
      nodes.push(node(String(i), 'taskNode', position.x, position.y));
    }
  });

  it('clears tall, manually positioned cards of other types in the next slot', () => {
    const position = manager().getAgentNodePosition([
      node('agent', 'agentNode', 100, 0, 210),
      node('obstacle', 'taskNode', 80, 250, 340, 310),
      node('next-obstacle', 'taskNode', 100, 680, 200, 270),
    ]);
    expect(position).toEqual({ x: 100, y: 930 });
  });

  it.each(['horizontal', 'vertical'] as const)('stacks new tasks below the existing task in %s mode', layoutOrientation => {
    const layout = manager();
    layout.updateUIState({ layoutOrientation });
    const existing = [node('agent', 'agentNode', 100, 100, 210), node('task', 'taskNode', 460, 310, 280)];
    expect(layout.getTaskNodePosition(existing)).toEqual({ x: 460, y: 640 });
  });

  it('places a first task beside its agent with space for measured width', () => {
    const layout = manager();
    layout.updateUIState({ layoutOrientation: 'horizontal' });
    const position = layout.getTaskNodePosition([node('agent', 'agentNode', 0, 0, 210, 320)]);
    expect(position).toEqual({ x: 370, y: 0 });
  });
});
