import { Connection, Edge } from 'reactflow';

/**
 * Generates a standardized edge ID based on the connection parameters
 * Format: reactflow__edge-${source}-${target}-${sourceHandle || ''}-${targetHandle || ''}-${randomSuffix}
 * Added a random suffix to ensure uniqueness even for the same connection parameters
 */
export const generateEdgeId = (connection: Connection): string => {
  const { source, target, sourceHandle, targetHandle } = connection;
  // Add a random suffix to ensure uniqueness
  const randomSuffix = Math.random().toString(36).substring(2, 8);
  return `reactflow__edge-${source}-${target}-${sourceHandle || ''}-${targetHandle || ''}-${randomSuffix}`;
};

/**
 * Checks if an edge with the given connection parameters already exists
 */
export const edgeExists = (
  edges: Edge[],
  connection: Connection
): boolean => {
  const edgeId = generateEdgeId(connection);
  return edges.some(edge => edge.id === edgeId);
};

/**
 * Creates a new edge with standardized properties
 */
export const createEdge = (
  connection: Connection,
  type = 'default',
  animated = false,
  style: Record<string, string | number> = {}
): Edge => {
  if (!connection.source || !connection.target) {
    throw new Error('Source and target are required for creating an edge');
  }

  return {
    id: generateEdgeId(connection),
    source: connection.source,
    target: connection.target,
    sourceHandle: connection.sourceHandle || null,
    targetHandle: connection.targetHandle || null,
    type,
    animated,
    style
  };
}; 