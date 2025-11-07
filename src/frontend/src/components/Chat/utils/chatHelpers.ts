import { Node, Edge } from 'reactflow';

export const hasCrewContent = (nodes: Node[]) => {
  const hasAgents = nodes.some(node => node.type === 'agentNode');
  const hasTask = nodes.some(node => node.type === 'taskNode');
  return hasAgents && hasTask;
};

/**
 * Detects if the given nodes represent a crew generation (multiple agents AND multiple tasks)
 * vs individual node generation (single agent or single task)
 *
 * @param nodes - Array of nodes to check
 * @returns true if this is a crew generation (multiple agents AND multiple tasks)
 */
export const isCrewGeneration = (nodes: Node[]): boolean => {
  const agentCount = nodes.filter(n => n.type === 'agentNode').length;
  const taskCount = nodes.filter(n => n.type === 'taskNode').length;
  return agentCount > 1 && taskCount > 1;
};

/**
 * Handles node generation callback with automatic detection of crew vs individual generation.
 * For crew generation: replaces all nodes/edges (canvas should be cleared first)
 * For individual generation: appends with deduplication
 *
 * @param newNodes - New nodes to add
 * @param newEdges - New edges to add
 * @param setNodes - State setter for nodes
 * @param setEdges - State setter for edges
 */
export const handleNodesGenerated = (
  newNodes: Node[],
  newEdges: Edge[],
  setNodes: React.Dispatch<React.SetStateAction<Node[]>>,
  setEdges: React.Dispatch<React.SetStateAction<Edge[]>>
): void => {
  if (isCrewGeneration(newNodes)) {
    // For crew generation, REPLACE all nodes/edges (canvas was already cleared)
    setNodes(newNodes);
    setEdges(newEdges);
  } else {
    // For individual agent/task generation, APPEND with deduplication
    setNodes(currentNodes => {
      const existingIds = new Set(currentNodes.map(n => n.id));
      const uniqueNewNodes = newNodes.filter(n => !existingIds.has(n.id));
      return [...currentNodes, ...uniqueNewNodes];
    });
    setEdges(currentEdges => {
      const existingEdgeKeys = new Set(currentEdges.map(e => `${e.source}-${e.target}`));
      const uniqueNewEdges = newEdges.filter(e => !existingEdgeKeys.has(`${e.source}-${e.target}`));
      return [...currentEdges, ...uniqueNewEdges];
    });
  }
};

export const isExecuteCommand = (message: string) => {
  const trimmed = message.trim().toLowerCase();
  return trimmed === 'execute crew' || trimmed === 'ec' || trimmed === 'run' || trimmed === 'execute' || trimmed.startsWith('ec ') || trimmed.startsWith('execute crew ');
};

export const extractJobIdFromCommand = (message: string): string | null => {
  const trimmed = message.trim().toLowerCase();
  if (trimmed.startsWith('ec ')) {
    return message.trim().substring(3).trim();
  }
  if (trimmed.startsWith('execute crew ')) {
    return message.trim().substring(13).trim();
  }
  return null;
};