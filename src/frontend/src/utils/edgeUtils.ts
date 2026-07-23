import { Connection, Edge } from 'reactflow';
import {
  shouldEdgeBeAnimated,
  getDefaultHandles,
  getEdgeStyle
} from '../config/edgeConfig';

/**
 * Generates a standardized edge ID based on the connection parameters
 * Format: reactflow__edge-${source}-${target}-${sourceHandle || 'default'}-${targetHandle || 'default'}
 * Deterministic ID generation to prevent duplicate edges
 */
export const generateEdgeId = (connection: Connection): string => {
  const { source, target, sourceHandle, targetHandle } = connection;
  // Use deterministic ID without random suffix to prevent duplicates
  return `reactflow__edge-${source}-${target}-${sourceHandle || 'default'}-${targetHandle || 'default'}`;
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
 * Creates a new edge with standardized properties using centralized configuration
 */
export const createEdge = (
  connection: Connection,
  type = 'default',
  animated?: boolean,
  style: Record<string, string | number> = {},
  layout?: 'horizontal' | 'vertical'
): Edge => {
  if (!connection.source || !connection.target) {
    throw new Error('Source and target are required for creating an edge');
  }

  // Get default handles from centralized config (with layout consideration)
  const defaultHandles = getDefaultHandles(connection.source, connection.target, layout);
  const sourceHandle = connection.sourceHandle ?? defaultHandles.sourceHandle;
  const targetHandle = connection.targetHandle ?? defaultHandles.targetHandle;

  // Determine if edge should be animated (use provided value or auto-detect)
  const shouldAnimate = animated !== undefined
    ? animated
    : shouldEdgeBeAnimated(connection.source, connection.target);

  // Get edge style from centralized config
  const edgeStyle = getEdgeStyle(connection.source, connection.target, shouldAnimate, style);

  return {
    id: generateEdgeId({ ...connection, sourceHandle, targetHandle }),
    source: connection.source,
    target: connection.target,
    sourceHandle,
    targetHandle,
    type,
    animated: shouldAnimate,
    style: edgeStyle
  };
};