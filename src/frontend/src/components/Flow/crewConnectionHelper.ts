import { Edge, Connection } from 'reactflow';

/**
 * Handles connections between crew nodes.
 * This function should be used in the onConnect callback of ReactFlow.
 * 
 * @param params The connection parameters from ReactFlow
 * @param edges The current edges in the flow
 * @returns An array of updated edges with the new connection
 */
export const handleCrewConnection = (params: Connection, edges: Edge[]): Edge[] => {
  const { source, target, sourceHandle, targetHandle } = params;
  
  // If source or target is missing, return the edges as-is
  if (!source || !target) {
    return edges;
  }
  
  // Check if this is a connection between crew nodes (based on node IDs)
  const isCrewConnection = source.includes('crew') && target.includes('crew');
  
  if (isCrewConnection) {
    // Check if we need to swap the connection direction
    // (if source handle has "-target" in its ID)
    if (sourceHandle?.includes('-target')) {
      // Create a modified params with swapped direction
      const baseSourceHandle = sourceHandle.replace('-target', '');
      const baseTargetHandle = targetHandle?.includes('-target') 
        ? targetHandle.replace('-target', '') 
        : targetHandle;
      
      // Use undefined instead of null for compatibility with Edge type
      const swappedSource = target;
      const swappedTarget = source;
      const swappedSourceHandle = baseTargetHandle || undefined;
      const swappedTargetHandle = baseSourceHandle ? `${baseSourceHandle}-target` : undefined;
      
      // Create a new edge with the swapped connection and crewEdge type
      const newEdge: Edge = {
        id: `${swappedSource}-${swappedTarget}-${swappedSourceHandle || ''}-${swappedTargetHandle || ''}`,
        source: swappedSource,
        target: swappedTarget,
        sourceHandle: swappedSourceHandle,
        targetHandle: swappedTargetHandle,
        type: 'crewEdge',
      };
      
      // Check if this exact connection already exists
      const connectionExists = edges.some(
        edge => 
          edge.source === newEdge.source && 
          edge.target === newEdge.target &&
          edge.sourceHandle === newEdge.sourceHandle && 
          edge.targetHandle === newEdge.targetHandle
      );
      
      if (!connectionExists) {
        // Add the new edge to the existing edges
        return [...edges, newEdge];
      }
      
      return edges; // Return without adding if the connection already exists
    }
    
    // Original direction is fine - create a new edge with the crewEdge type
    const newEdge: Edge = {
      id: `${source}-${target}-${sourceHandle || ''}-${targetHandle || ''}`,
      source: source,
      target: target,
      sourceHandle,
      targetHandle,
      type: 'crewEdge', // Use our custom edge type
    };
    
    // Check if this exact connection already exists
    const connectionExists = edges.some(
      edge => 
        edge.source === newEdge.source && 
        edge.target === newEdge.target &&
        edge.sourceHandle === newEdge.sourceHandle && 
        edge.targetHandle === newEdge.targetHandle
    );
    
    if (!connectionExists) {
      // Add the new edge to the existing edges
      return [...edges, newEdge];
    }
  }
  
  // For non-crew connections or connections that already exist
  // Create the edge with the default edge type
  const newEdge: Edge = {
    id: `${source}-${target}-${sourceHandle || ''}-${targetHandle || ''}`,
    source: source,
    target: target,
    sourceHandle,
    targetHandle,
  };
  
  // Check if connection already exists
  const connectionExists = edges.some(
    edge => 
      edge.source === newEdge.source && 
      edge.target === newEdge.target &&
      edge.sourceHandle === newEdge.sourceHandle && 
      edge.targetHandle === newEdge.targetHandle
  );
  
  if (!connectionExists) {
    return [...edges, newEdge];
  }
  
  return edges;
};

/**
 * Instructions to integrate this helper:
 * 
 * 1. In your ReactFlow component, import this function:
 *    import { handleCrewConnection } from '../Flow/crewConnectionHelper';
 * 
 * 2. Use it in the onConnect callback:
 *    const onConnect = useCallback((params: Connection) => {
 *      setEdges(edges => handleCrewConnection(params, edges));
 *    }, [setEdges]);
 * 
 * 3. Make sure to set the edgeTypes in your ReactFlow component:
 *    <ReactFlow
 *      ...
 *      edgeTypes={{
 *        default: AnimatedEdge,
 *        crewEdge: CrewEdge
 *      }}
 *      onConnect={onConnect}
 *      ...
 *    />
 */ 