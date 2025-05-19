import { useCallback, useRef, useEffect } from 'react';
import { Node, ReactFlowInstance } from 'reactflow';

interface UseNodePositioningProps {
  nodes: Node[];
  setNodes: (nodes: Node[] | ((nodes: Node[]) => Node[])) => void;
  manuallyPositionedNodes: string[];
  panelState: string;
  areFlowsVisible: boolean;
  isDraggingPanel: boolean;
  getActiveFlowInstance: () => ReactFlowInstance | null;
}

export const useNodePositioning = ({
  nodes,
  setNodes,
  manuallyPositionedNodes,
  panelState,
  areFlowsVisible,
  isDraggingPanel,
  getActiveFlowInstance,
}: UseNodePositioningProps) => {
  const updateNodePositionsTimeoutRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const unmountedRef = useRef<boolean>(false);

  // Update the useEffect cleanup that handles component unmount
  useEffect(() => {
    // Component mount
    unmountedRef.current = false;
    
    return () => {
      // Component unmount
      unmountedRef.current = true;
    };
  }, []);

  // Effect to handle node positioning
  useEffect(() => {
    // Skip positioning if there are no nodes or flag is disabled
    if (!nodes.length) return;

    // Skip updating positions during active drag operations
    if (isDraggingPanel) return;

    // Create a set of manually positioned node IDs
    const manuallyPositionedSet = new Set(manuallyPositionedNodes);

    // Skip node positioning for recently added nodes to avoid interference
    const minTimeSinceAddition = 1000; // 1 second
    const now = Date.now();
    
    const recentlyAddedNodeIds = new Set(
      nodes
        .filter(node => 
          node.data?.addedTimestamp && 
          now - node.data.addedTimestamp < minTimeSinceAddition
        )
        .map(node => node.id)
    );

    // Debounce the function to reduce the number of calls
    if (updateNodePositionsTimeoutRef.current) {
      clearTimeout(updateNodePositionsTimeoutRef.current);
    }

    updateNodePositionsTimeoutRef.current = setTimeout(() => {
      if (unmountedRef.current) return; // Skip if component is unmounted
      
      let hasChanges = false;
      const nodesWithValidPositions = nodes.map(node => {
        // Skip any manually positioned nodes or recently added nodes
        if (manuallyPositionedSet.has(node.id) || recentlyAddedNodeIds.has(node.id)) {
          return node;
        }
        
        // Skip if position is already valid
        if (
          node.position && 
          isFinite(node.position.x) && 
          isFinite(node.position.y) &&
          Math.abs(node.position.x) < 10000 &&
          Math.abs(node.position.y) < 10000
        ) {
          return node;
        }

        hasChanges = true;
        return {
          ...node,
          position: {
            x: Math.random() * 300, 
            y: Math.random() * 300
          }
        };
      });

      // Only update if we actually changed anything
      if (hasChanges) {
        setNodes(nodesWithValidPositions);
        
        // After setting nodes with valid positions, fit view if necessary
        // Use a longer timeout to ensure nodes are rendered properly
        setTimeout(() => {
          if (unmountedRef.current) return;
          
          const instance = getActiveFlowInstance();
          if (instance) {
            instance.fitView({ padding: 0.2, includeHiddenNodes: false });
          }
        }, 300);
      }
    }, 200); // 200ms debounce

    return () => {
      if (updateNodePositionsTimeoutRef.current) {
        clearTimeout(updateNodePositionsTimeoutRef.current);
      }
    };
  }, [
    nodes, 
    setNodes, 
    panelState,
    areFlowsVisible,
    isDraggingPanel,
    manuallyPositionedNodes,
    getActiveFlowInstance,
  ]);

  return {
    updateNodePositionsTimeoutRef,
    unmountedRef,
  };
}; 