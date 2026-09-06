import type { Node } from 'reactflow';

type Connection = { source: string; target: string };
type Orientation = 'horizontal' | 'vertical';

/** Arrange each agent beside the median center of its own ordered task group. */
export function layoutAgentTaskGroups(
  nodes: Node[], edges: Connection[], orientation: Orientation,
  origin: { x: number; y: number }, gap = 80,
): Node[] {
  const agents = nodes.filter(node => node.type === 'agentNode' && !node.hidden);
  const tasks = nodes.filter(node => node.type === 'taskNode' && !node.hidden);
  const agentIds = new Set(agents.map(node => node.id));
  const taskIds = new Set(tasks.map(node => node.id));
  const owners = new Map<string, string>();
  for (const edge of edges) {
    // Node types, rather than ID prefixes, determine the connection's meaning.
    if (agentIds.has(edge.source) && taskIds.has(edge.target) && !owners.has(edge.target)) owners.set(edge.target, edge.source);
  }
  // Index once, preserving canvas order and the latest workspace's linear grouping cost.
  const groups = new Map<string, Node[]>();
  for (const task of tasks) {
    const owner = owners.get(task.id);
    if (!owner) continue;
    const group = groups.get(owner) || [];
    group.push(task);
    groups.set(owner, group);
  }
  const horizontal = orientation === 'horizontal';
  const size = (node: Node) => ({ width: node.width || (node.type === 'taskNode' ? 270 : 200), height: node.height || (node.type === 'taskNode' ? 230 : 210) });
  const span = (node: Node) => horizontal ? size(node).height : size(node).width;
  const agentDepth = Math.max(0, ...agents.map(node => horizontal ? size(node).width : size(node).height));
  const agentCross = horizontal ? origin.x : origin.y;
  const taskCross = agents.length ? agentCross + agentDepth + 100 : agentCross;
  let cursor = horizontal ? origin.y : origin.x;
  const positions = new Map<string, { x: number; y: number }>();
  const position = (cross: number, along: number) => horizontal ? { x: cross, y: along } : { x: along, y: cross };

  for (const agent of agents) {
    const group = groups.get(agent.id) || [];
    if (!group.length) {
      positions.set(agent.id, position(agentCross, cursor));
      cursor += span(agent) + gap;
      continue;
    }
    let end = cursor;
    const starts = group.map(task => { const start = end; end += span(task) + gap; return start; });
    const centers = group.map((task, i) => starts[i] + span(task) / 2);
    const middle = Math.floor(group.length / 2);
    const center = group.length % 2 ? centers[middle] : (centers[middle - 1] + centers[middle]) / 2;
    const agentStart = center - span(agent) / 2;
    // A tall agent must not extend into the preceding group.
    const offset = Math.max(0, cursor - agentStart);
    group.forEach((task, i) => positions.set(task.id, position(taskCross, starts[i] + offset)));
    positions.set(agent.id, position(agentCross, agentStart + offset));
    cursor = Math.max(end - gap + offset, agentStart + offset + span(agent)) + gap;
  }
  for (const task of tasks.filter(task => !owners.has(task.id))) {
    positions.set(task.id, position(taskCross, cursor));
    cursor += span(task) + gap;
  }
  return nodes.map(node => {
    const next = positions.get(node.id);
    return next && (next.x !== node.position.x || next.y !== node.position.y) ? { ...node, position: next } : node;
  });
}
