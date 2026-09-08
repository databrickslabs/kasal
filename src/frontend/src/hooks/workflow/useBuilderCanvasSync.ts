import { useEffect, useRef, useCallback } from 'react';
import { Node, Edge } from 'reactflow';
import { useBuilderCanvasStore } from '../../app/sessions/builderCanvasStore';

interface UseBuilderCanvasSyncProps {
  nodes: Node[];
  edges: Edge[];
  setNodes: React.Dispatch<React.SetStateAction<Node[]>>;
  setEdges: React.Dispatch<React.SetStateAction<Edge[]>>;
}

// Helper function to compare nodes for actual content changes
const nodesHaveChanged = (oldNodes: Node[], newNodes: Node[]): boolean => {
  if (oldNodes.length !== newNodes.length) return true;
  
  // Create maps for efficient lookup
  const oldNodeMap = new Map(oldNodes.map(node => [node.id, node]));
  
  // Check each node for changes
  for (const newNode of newNodes) {
    const oldNode = oldNodeMap.get(newNode.id);
    if (!oldNode) return true; // New node added
    
    // Compare essential properties that indicate actual changes
    if (
      oldNode.type !== newNode.type ||
      Math.abs(oldNode.position.x - newNode.position.x) > 0.1 ||
      Math.abs(oldNode.position.y - newNode.position.y) > 0.1 ||
      JSON.stringify(oldNode.data) !== JSON.stringify(newNode.data)
    ) {
      return true;
    }
  }
  
  return false;
};

// Helper function to compare edges for actual content changes
const edgesHaveChanged = (oldEdges: Edge[], newEdges: Edge[]): boolean => {
  if (oldEdges.length !== newEdges.length) return true;
  
  // Create maps for efficient lookup
  const oldEdgeMap = new Map(oldEdges.map(edge => [edge.id, edge]));
  
  // Check each edge for changes
  for (const newEdge of newEdges) {
    const oldEdge = oldEdgeMap.get(newEdge.id);
    if (!oldEdge) return true; // New edge added
    
    // Compare essential properties that indicate actual changes
    if (
      oldEdge.source !== newEdge.source ||
      oldEdge.target !== newEdge.target ||
      oldEdge.type !== newEdge.type ||
      JSON.stringify(oldEdge.data) !== JSON.stringify(newEdge.data)
    ) {
      return true;
    }
  }
  
  return false;
};

export const useBuilderCanvasSync = ({ nodes, edges, setNodes, setEdges }: UseBuilderCanvasSyncProps) => {
  const {
    activeCanvasId,
    getActiveCanvas,
    updateCanvasNodes,
    updateCanvasEdges,
    updateCanvasCrewInfo
  } = useBuilderCanvasStore();

  // Keep track of whether we're currently loading crew data or switching tabs
  const isLoadingCrewRef = useRef(false);
  const isSwitchingTabsRef = useRef(false);
  const lastActiveTabIdRef = useRef<string | null>(null);
  const lastNodesRef = useRef<Node[]>([]);
  const lastEdgesRef = useRef<Edge[]>([]);

  // Update refs when nodes/edges change
  useEffect(() => {
    if (!isSwitchingTabsRef.current) {
      lastNodesRef.current = nodes;
      lastEdgesRef.current = edges;
    }
  }, [nodes, edges]);

  // Save current state for a specific tab
  const saveStateForTab = useCallback((tabId: string, nodesToSave: Node[], edgesToSave: Edge[]) => {
    if (tabId && !isLoadingCrewRef.current) {
      updateCanvasNodes(tabId, nodesToSave);
      updateCanvasEdges(tabId, edgesToSave);
    }
  }, [updateCanvasNodes, updateCanvasEdges]);

  // Sync tab data to flow manager when active tab changes
  useEffect(() => {
    const timers: ReturnType<typeof setTimeout>[] = [];
    const later = (callback: () => void, delay: number) => timers.push(setTimeout(callback, delay));
    if (activeCanvasId !== lastActiveTabIdRef.current) {
      // Don't interfere if we're currently loading a crew
      if (isLoadingCrewRef.current) {
        lastActiveTabIdRef.current = activeCanvasId;
        return;
      }

      // Save current state to the previous tab before switching
      if (lastActiveTabIdRef.current && !isLoadingCrewRef.current) {
        saveStateForTab(lastActiveTabIdRef.current, lastNodesRef.current, lastEdgesRef.current);
      }

      // Tab is changing
      isSwitchingTabsRef.current = true;
      
      const activeTab = getActiveCanvas();
      if (!activeTab) {
        setNodes([]); setEdges([]);
        lastNodesRef.current = []; lastEdgesRef.current = [];
        isSwitchingTabsRef.current = false;
      }
      if (activeTab) {
        
        // Create deep copies to ensure proper restoration
        const restoredNodes = activeTab.nodes.map(node => ({
          ...node,
          position: { ...node.position },
          data: { ...node.data }
        }));
        
        const restoredEdges = activeTab.edges.map(edge => ({
          ...edge,
          data: edge.data ? { ...edge.data } : undefined
        }));
        
        // Only restore if we're not loading a crew
        if (!isLoadingCrewRef.current) {
          // Set the nodes and edges with proper state restoration
          setNodes(restoredNodes);
          setEdges(restoredEdges);
          
          // Update refs immediately
          lastNodesRef.current = restoredNodes;
          lastEdgesRef.current = restoredEdges;
          
          // For new empty tabs, ensure the canvas is cleared
          if (restoredNodes.length === 0 && restoredEdges.length === 0) {
            // Force clear the canvas for empty tabs
            later(() => {
              setNodes([]);
              setEdges([]);
              lastNodesRef.current = [];
              lastEdgesRef.current = [];
            }, 50);
          }
          
          // Trigger fitView after nodes are restored to ensure proper viewport
          later(() => {
            if (restoredNodes.length > 0) {
              window.dispatchEvent(new CustomEvent('fitViewToNodes', { bubbles: true }));
            }
          }, 300);
          
          // Also trigger a ReactFlow instance update to ensure proper synchronization
          later(() => {
            window.dispatchEvent(new CustomEvent('updateReactFlowInstance', { 
              detail: { nodes: restoredNodes, edges: restoredEdges }
            }));
          }, 100);
        }
        
        // Reset the switching flag after a delay to allow ReactFlow to process
        later(() => {
          isSwitchingTabsRef.current = false;
        }, 500);
      }
      
      // Update the last active tab reference
      lastActiveTabIdRef.current = activeCanvasId;
    }
    return () => timers.forEach(clearTimeout);
  }, [activeCanvasId, getActiveCanvas, setNodes, setEdges, saveStateForTab]);

  // Save current state before tab switch
  const saveCurrentState = useCallback(() => {
    if (activeCanvasId && !isLoadingCrewRef.current && !isSwitchingTabsRef.current) {
      saveStateForTab(activeCanvasId, lastNodesRef.current, lastEdgesRef.current);
    }
  }, [activeCanvasId, saveStateForTab]);

  // Save state before unload
  useEffect(() => {
    const handleBeforeUnload = () => {
      if (activeCanvasId) {
        saveStateForTab(activeCanvasId, lastNodesRef.current, lastEdgesRef.current);
      }
    };

    window.addEventListener('beforeunload', handleBeforeUnload);
    return () => {
      window.removeEventListener('beforeunload', handleBeforeUnload);
    };
  }, [activeCanvasId, saveStateForTab]);

  // Sync flow manager changes back to active tab (with debouncing)
  const syncNodesToTab = useCallback(() => {
    if (activeCanvasId && !isLoadingCrewRef.current && !isSwitchingTabsRef.current) {
      const activeTab = getActiveCanvas();
      if (activeTab && nodesHaveChanged(activeTab.nodes, nodes)) {
        updateCanvasNodes(activeCanvasId, nodes);
      }
    }
  }, [nodes, activeCanvasId, updateCanvasNodes, getActiveCanvas]);

  const syncEdgesToTab = useCallback(() => {
    if (activeCanvasId && !isLoadingCrewRef.current && !isSwitchingTabsRef.current) {
      const activeTab = getActiveCanvas();
      if (activeTab && edgesHaveChanged(activeTab.edges, edges)) {
        updateCanvasEdges(activeCanvasId, edges);
      }
    }
  }, [edges, activeCanvasId, updateCanvasEdges, getActiveCanvas]);

  // Use separate effects with debouncing to avoid excessive updates
  useEffect(() => {
    const timeoutId = setTimeout(syncNodesToTab, 300);
    return () => clearTimeout(timeoutId);
  }, [syncNodesToTab]);

  useEffect(() => {
    const timeoutId = setTimeout(syncEdgesToTab, 300);
    return () => clearTimeout(timeoutId);
  }, [syncEdgesToTab]);

  // Listen for crew save complete events
  useEffect(() => {
    const handleSaveCrewComplete = (event: CustomEvent<{ crewId: string; crewName: string; tabId?: string }>) => {
      if (event.detail) {
        const { crewId, crewName, tabId } = event.detail;
        const targetTabId = tabId || activeCanvasId; // Use specified tab ID or current active tab
        
        if (targetTabId && crewId && crewName && !isLoadingCrewRef.current) {
          updateCanvasCrewInfo(targetTabId, crewId, crewName);
        } else if (isLoadingCrewRef.current) {
          // Skip update while loading crew
        }
      }
    };

    window.addEventListener('saveCrewComplete', handleSaveCrewComplete as EventListener);
    
    return () => {
      window.removeEventListener('saveCrewComplete', handleSaveCrewComplete as EventListener);
    };
  }, [activeCanvasId, updateCanvasCrewInfo]);

  // Listen for crew load events to prevent marking as dirty during load
  useEffect(() => {
    const handleCrewLoadStart = () => {
      isLoadingCrewRef.current = true;
    };

    const handleCrewLoadComplete = () => {
      setTimeout(() => {
        isLoadingCrewRef.current = false;
      }, 200); // Small delay to ensure all updates are processed
    };

    window.addEventListener('crewLoadStarted', handleCrewLoadStart);
    window.addEventListener('crewLoadCompleted', handleCrewLoadComplete);
    
    return () => {
      window.removeEventListener('crewLoadStarted', handleCrewLoadStart);
      window.removeEventListener('crewLoadCompleted', handleCrewLoadComplete);
    };
  }, []);

  // Listen for clear canvas events (for new tabs)
  useEffect(() => {
    const handleClearCanvas = (event: CustomEvent<{ tabId: string }>) => {
      if (event.detail.tabId === activeCanvasId) {
        setNodes([]);
        setEdges([]);
      }
    };

    window.addEventListener('clearCanvas', handleClearCanvas as EventListener);
    
    return () => {
      window.removeEventListener('clearCanvas', handleClearCanvas as EventListener);
    };
  }, [activeCanvasId, setNodes, setEdges]);

  return {
    activeCanvasId,
    getActiveCanvas,
    saveCurrentState
  };
}; 
