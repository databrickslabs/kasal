import type { Edge, Node } from 'reactflow';

/** Group connected tasks once, retaining canvas order and suppressing duplicate edges. */
export function tasksByAgent(tasks: Node[], edges: Edge[]): Map<string, Node[]> {
  const agentsByTask = new Map<string, Set<string>>();
  for (const edge of edges) {
    if (!edge.source?.startsWith('agent-') || !edge.target?.startsWith('task-')) continue;
    const agents = agentsByTask.get(edge.target) ?? new Set<string>();
    agents.add(edge.source);
    agentsByTask.set(edge.target, agents);
  }
  const result = new Map<string, Node[]>();
  for (const task of tasks) {
    for (const agent of agentsByTask.get(task.id) ?? []) {
      const connected = result.get(agent) ?? [];
      connected.push(task);
      result.set(agent, connected);
    }
  }
  return result;
}
