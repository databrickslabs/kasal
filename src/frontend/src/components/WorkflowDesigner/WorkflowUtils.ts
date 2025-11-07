import { Edge, Connection, Node } from 'reactflow';
import { v4 as uuidv4 } from 'uuid';
import { createEdge } from '../../utils/edgeUtils';
import { FlowConfiguration, FlowFormData } from '../../types/flow';

// Add a function at the top level to intercept ResizeObserver errors globally
export const setupResizeObserverErrorHandling = (): void => {
  if (typeof window !== 'undefined' && !Object.prototype.hasOwnProperty.call(window, '_resizeObserverHandled')) {
    // Set a flag to prevent multiple handlers
    Object.defineProperty(window, '_resizeObserverHandled', {
      value: true,
      writable: false,
      configurable: false
    });

    // Add global error event handler
    window.addEventListener('error', (event) => {
      if (event.message && 
          (event.message.includes('ResizeObserver loop') || 
           event.message.includes('ResizeObserver Loop'))) {
        event.stopImmediatePropagation();
        event.preventDefault();
        return false;
      }
    });

    // Add unhandled rejection handler
    window.addEventListener('unhandledrejection', (event) => {
      if (event.reason && 
          typeof event.reason.message === 'string' && 
          (event.reason.message.includes('ResizeObserver loop') || 
           event.reason.message.includes('ResizeObserver Loop'))) {
        event.preventDefault();
        return false;
      }
    });

    // Replace console.error to filter out ResizeObserver errors
    const originalConsoleError = console.error;
    console.error = function(...args) {
      if (args.length > 0 && 
          typeof args[0] === 'string' && 
          (args[0].includes('ResizeObserver loop') || 
           args[0].includes('ResizeObserver Loop'))) {
        // Suppress this error
        return;
      }
      originalConsoleError.apply(console, args);
    };
  }
};

// Add debounce utility function with proper typing
export const debounce = <T extends (...args: unknown[]) => unknown>(fn: T, ms = 300) => {
  let timeoutId: ReturnType<typeof setTimeout>;
  return function(this: unknown, ...args: Parameters<T>) {
    clearTimeout(timeoutId);
    timeoutId = setTimeout(() => fn.apply(this, args), ms);
  };
};

// Create a new flow node
export const createFlowNode = (flowData: FlowFormData, position: { x: number; y: number }) => {
  const flowId = `flow-${Date.now()}`;
  
  // Create a standard crew node
  return {
    id: flowId,
    type: 'crewNode',
    position,
    data: {
      id: flowId,
      label: flowData.name,
      crewName: flowData.crewName,
      crewId: flowData.crewRef || flowId,
      type: flowData.type,
    }
  };
};

// Process a flow selection into nodes and edges with new IDs
export const processFlowSelection = (
  flowNodes: Node[], 
  flowEdges: Edge[], 
  flowConfig?: FlowConfiguration
) => {
  // Create copies of nodes and edges with new IDs to prevent duplicates
  const idMap = new Map<string, string>();
  
  const newNodes = flowNodes.map(node => {
    const oldId = node.id;
    const newId = uuidv4();
    idMap.set(oldId, newId);
    
    return {
      ...node,
      id: newId,
      position: {
        x: node.position.x,
        y: node.position.y
      },
      data: {
        ...node.data
      }
    };
  });
  
  // Create edges with updated source/target IDs
  const newEdges: Edge[] = flowEdges.map(edge => {
    const newSource = idMap.get(edge.source) || edge.source;
    const newTarget = idMap.get(edge.target) || edge.target;
    
    const connection: Connection = {
      source: newSource,
      target: newTarget,
      sourceHandle: null,
      targetHandle: null
    };
    
    return createEdge(connection, 'animated', true, { stroke: '#9c27b0' });
  });

  return { newNodes, newEdges };
};

// Create new edges ensuring no duplicates
export const createUniqueEdges = (
  newEdges: Edge[],
  existingEdges: Edge[]
): Edge[] => {
  // Create a Set of existing edge IDs for quick lookup
  const existingEdgeIds = new Set(existingEdges.map(e => e.id));
  
  // Create a Set of existing edge source-target pairs to prevent duplicates
  const existingEdgePairs = new Set(existingEdges.map(e => `${e.source}-${e.target}`));
  
  // Filter out any new edges that would create duplicates
  return newEdges.filter(edge => {
    const edgePair = `${edge.source}-${edge.target}`;
    return !existingEdgeIds.has(edge.id) && !existingEdgePairs.has(edgePair);
  });
}; 