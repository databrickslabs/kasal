import { useEffect, useRef, useCallback } from 'react';
import { Node, Edge } from 'reactflow';
import { useTabManagerStore } from '../../store/tabManager';

interface UseTabSyncProps {
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

export const useTabSync = ({ nodes, edges, setNodes, setEdges }: UseTabSyncProps) => {
  const {
    activeTabId,
    getActiveTab,
    updateTabNodes,
    updateTabEdges,
    updateTabCrewInfo
  } = useTabManagerStore();

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
      console.log('Saving state for tab:', tabId, 'with', nodesToSave.length, 'nodes and', edgesToSave.length, 'edges');
      updateTabNodes(tabId, nodesToSave);
      updateTabEdges(tabId, edgesToSave);
    }
  }, [updateTabNodes, updateTabEdges]);

  // Sync tab data to flow manager when active tab changes
  useEffect(() => {
    if (activeTabId !== lastActiveTabIdRef.current) {
      // Save current state to the previous tab before switching
      if (lastActiveTabIdRef.current) {
        saveStateForTab(lastActiveTabIdRef.current, lastNodesRef.current, lastEdgesRef.current);
      }

      // Tab is changing
      isSwitchingTabsRef.current = true;
      
      const activeTab = getActiveTab();
      if (activeTab) {
        console.log('Switching to tab:', activeTab.name, 'with', activeTab.nodes.length, 'nodes and', activeTab.edges.length, 'edges');
        
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
        
        // Set the nodes and edges with proper state restoration
        setNodes(restoredNodes);
        setEdges(restoredEdges);
        
        // Update refs immediately
        lastNodesRef.current = restoredNodes;
        lastEdgesRef.current = restoredEdges;
        
        // For new empty tabs, ensure the canvas is cleared
        if (restoredNodes.length === 0 && restoredEdges.length === 0) {
          // Force clear the canvas for empty tabs
          setTimeout(() => {
            setNodes([]);
            setEdges([]);
            lastNodesRef.current = [];
            lastEdgesRef.current = [];
          }, 50);
        }
        
        // Trigger fitView after nodes are restored to ensure proper viewport
        setTimeout(() => {
          if (restoredNodes.length > 0) {
            window.dispatchEvent(new CustomEvent('fitViewToNodes', { bubbles: true }));
          }
        }, 300);
        
        // Also trigger a ReactFlow instance update to ensure proper synchronization
        setTimeout(() => {
          window.dispatchEvent(new CustomEvent('updateReactFlowInstance', { 
            detail: { nodes: restoredNodes, edges: restoredEdges }
          }));
        }, 100);
        
        // Reset the switching flag after a delay to allow ReactFlow to process
        setTimeout(() => {
          isSwitchingTabsRef.current = false;
        }, 500);
      }
      
      // Update the last active tab reference
      lastActiveTabIdRef.current = activeTabId;
    }
  }, [activeTabId, getActiveTab, setNodes, setEdges, saveStateForTab]);

  // Save current state before tab switch
  const saveCurrentState = useCallback(() => {
    if (activeTabId && !isLoadingCrewRef.current && !isSwitchingTabsRef.current) {
      saveStateForTab(activeTabId, lastNodesRef.current, lastEdgesRef.current);
    }
  }, [activeTabId, saveStateForTab]);

  // Save state before unload
  useEffect(() => {
    const handleBeforeUnload = () => {
      if (activeTabId) {
        saveStateForTab(activeTabId, lastNodesRef.current, lastEdgesRef.current);
      }
    };

    window.addEventListener('beforeunload', handleBeforeUnload);
    return () => {
      window.removeEventListener('beforeunload', handleBeforeUnload);
    };
  }, [activeTabId, saveStateForTab]);

  // Sync flow manager changes back to active tab (with debouncing)
  const syncNodesToTab = useCallback(() => {
    if (activeTabId && !isLoadingCrewRef.current && !isSwitchingTabsRef.current) {
      const activeTab = getActiveTab();
      if (activeTab && nodesHaveChanged(activeTab.nodes, nodes)) {
        console.log('Auto-syncing', nodes.length, 'nodes to tab:', activeTab.name);
        updateTabNodes(activeTabId, nodes);
      }
    }
  }, [nodes, activeTabId, updateTabNodes, getActiveTab]);

  const syncEdgesToTab = useCallback(() => {
    if (activeTabId && !isLoadingCrewRef.current && !isSwitchingTabsRef.current) {
      const activeTab = getActiveTab();
      if (activeTab && edgesHaveChanged(activeTab.edges, edges)) {
        console.log('Auto-syncing', edges.length, 'edges to tab:', activeTab.name);
        updateTabEdges(activeTabId, edges);
      }
    }
  }, [edges, activeTabId, updateTabEdges, getActiveTab]);

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
    const handleSaveCrewComplete = (event: CustomEvent<{ crewId: string; crewName: string }>) => {
      if (activeTabId && event.detail) {
        const { crewId, crewName } = event.detail;
        if (crewId && crewName) {
          updateTabCrewInfo(activeTabId, crewId, crewName);
        }
      }
    };

    window.addEventListener('saveCrewComplete', handleSaveCrewComplete as EventListener);
    
    return () => {
      window.removeEventListener('saveCrewComplete', handleSaveCrewComplete as EventListener);
    };
  }, [activeTabId, updateTabCrewInfo]);

  // Listen for crew load events to prevent marking as dirty during load
  useEffect(() => {
    const handleCrewLoad = () => {
      isLoadingCrewRef.current = true;
      setTimeout(() => {
        isLoadingCrewRef.current = false;
      }, 1000); // Give more time for crew loading to complete
    };

    window.addEventListener('crewLoadStarted', handleCrewLoad);
    
    return () => {
      window.removeEventListener('crewLoadStarted', handleCrewLoad);
    };
  }, []);

  // Listen for clear canvas events (for new tabs)
  useEffect(() => {
    const handleClearCanvas = (event: CustomEvent<{ tabId: string }>) => {
      if (event.detail.tabId === activeTabId) {
        console.log('Clearing canvas for new tab:', event.detail.tabId);
        setNodes([]);
        setEdges([]);
      }
    };

    window.addEventListener('clearCanvas', handleClearCanvas as EventListener);
    
    return () => {
      window.removeEventListener('clearCanvas', handleClearCanvas as EventListener);
    };
  }, [activeTabId, setNodes, setEdges]);

  return {
    activeTabId,
    getActiveTab,
    saveCurrentState
  };
}; 