import { useCallback, useState, useEffect, useRef } from 'react';
import { Node, Edge, OnSelectionChangeParams, ReactFlowInstance } from 'reactflow';
import { FlowConfiguration } from '../../types/workflow/flow';
import { CrewTask } from '../../types/workflow/crewPlan';
import { v4 as uuidv4 } from 'uuid';
import { useFlowStateStore, DeclaredFlowState } from '../../store/flowState';
import { useTabManagerStore } from '../../store/tabManager';
import { CanvasLayoutManager } from '../../features/workflow/canvas/lib/CanvasLayoutManager';
import { useUILayoutStore } from '../../store/uiLayout';

// Context menu handlers
export const useContextMenuHandlers = () => {
  const [paneContextMenu, setPaneContextMenu] = useState<{
    mouseX: number;
    mouseY: number;
  } | null>(null);

  const handlePaneContextMenu = useCallback((event: React.MouseEvent) => {
    event.preventDefault();
    setPaneContextMenu({
      mouseX: event.clientX,
      mouseY: event.clientY,
    });
  }, []);

  const handlePaneContextMenuClose = useCallback(() => {
    setPaneContextMenu(null);
  }, []);

  return {
    paneContextMenu,
    handlePaneContextMenu,
    handlePaneContextMenuClose
  };
};

// Flow instance management
export const useFlowInstanceHandlers = () => {
  const crewFlowInstanceRef = useRef<ReactFlowInstance | null>(null);
  const flowFlowInstanceRef = useRef<ReactFlowInstance | null>(null);

  const handleCrewFlowInit = useCallback((instance: ReactFlowInstance) => {
    crewFlowInstanceRef.current = instance;
    
    // Dispatch event to notify that ReactFlow is initialized
    setTimeout(() => {
      window.dispatchEvent(new CustomEvent('crewFlowInitialized', { 
        detail: { instance } 
      }));
    }, 100);
  }, []);

  const handleFlowFlowInit = useCallback((instance: ReactFlowInstance) => {
    flowFlowInstanceRef.current = instance;
  }, []);

  return {
    crewFlowInstanceRef,
    flowFlowInstanceRef,
    handleCrewFlowInit,
    handleFlowFlowInit
  };
};

// Selection change handler
export const useSelectionChangeHandler = (
  setSelectedEdges: (edges: Edge[]) => void
) => {
  return useCallback((params: OnSelectionChangeParams) => {
    setSelectedEdges(params.edges || []);
  }, [setSelectedEdges]);
};

// Horizontal spacing (px) between node origins on the loaded row. Comfortably
// clears a crew node's width so adjacent nodes don't overlap.
const HORIZONTAL_NODE_SPACING = 360;
// Single shared y so every loaded node sits at the same height; fitView then
// centers the whole row vertically in the viewport.
const ROW_Y = 0;

// True when the loaded nodes already carry a real saved layout worth preserving
// (more than one node, sitting at distinct positions). A flow saved with branches
// must retain its exact placement; only degenerate loads (a single crew node opened
// from the catalog, or every node piled at the same/default point) get auto-arranged.
export const hasSavedLayout = (nodes: Node[]): boolean => {
  if (nodes.length <= 1) return false;
  const positions = new Set(
    nodes.map(n => `${Math.round(n.position?.x ?? 0)},${Math.round(n.position?.y ?? 0)}`),
  );
  return positions.size > 1;
};

// Lay nodes out left-to-right on one row, ordered by edge direction (topological)
// so a flow with no meaningful saved layout reads as a clean horizontal pipeline
// instead of a stack. All nodes share ROW_Y; combined with the subsequent fitView
// this leaves the row horizontally and vertically centered.
export const layoutFlowHorizontally = (nodes: Node[], edges: Edge[]): Node[] => {
  if (nodes.length === 0) return nodes;

  const indexById = new Map<string, number>(nodes.map((n, i) => [n.id, i]));
  const indegree = new Map<string, number>(nodes.map(n => [n.id, 0]));
  const adjacency = new Map<string, string[]>();
  edges.forEach(e => {
    if (!indexById.has(e.source) || !indexById.has(e.target)) return;
    adjacency.set(e.source, [...(adjacency.get(e.source) || []), e.target]);
    indegree.set(e.target, (indegree.get(e.target) || 0) + 1);
  });

  // Kahn's algorithm; ties resolved by original insertion order for stability.
  const ready = nodes.filter(n => (indegree.get(n.id) || 0) === 0).map(n => n.id);
  const ordered: string[] = [];
  const seen = new Set<string>();
  while (ready.length) {
    ready.sort((a, b) => (indexById.get(a) ?? 0) - (indexById.get(b) ?? 0));
    const id = ready.shift() as string;
    seen.add(id);
    ordered.push(id);
    (adjacency.get(id) || []).forEach(t => {
      indegree.set(t, (indegree.get(t) || 0) - 1);
      if ((indegree.get(t) || 0) <= 0 && !seen.has(t)) ready.push(t);
    });
  }
  // Append any nodes dropped by cycles, preserving their original order.
  nodes.forEach(n => { if (!seen.has(n.id)) ordered.push(n.id); });

  const columnByNodeId = new Map(ordered.map((id, i) => [id, i] as const));
  return nodes.map(n => ({
    ...n,
    position: { x: (columnByNodeId.get(n.id) ?? 0) * HORIZONTAL_NODE_SPACING, y: ROW_Y },
  }));
};

// CrewFlowSelectionDialog tab indices: Crews=0, Agents=1, Tasks=2, Flows=3.
export const CATALOG_CREWS_TAB = 0;
export const CATALOG_FLOWS_TAB = 3;

// Pick the catalog tab matching the active canvas: the Flows tab on the flow
// canvas, the Crews tab on the crew canvas. Keeps "Load from Catalog" in sync
// with where the user is (loading a flow from the flow canvas, not crews).
export const catalogTabForCanvas = (areFlowsVisible: boolean): number =>
  areFlowsVisible ? CATALOG_FLOWS_TAB : CATALOG_CREWS_TAB;

export interface CatalogDialogSetters {
  setInitialTab: (tab: number) => void;
  setShowOnlyTab: (tab: number) => void;
  setOpen: (open: boolean) => void;
}

// Open the crew/flow catalog dialog pinned to the tab matching the active canvas.
export const openCatalogForCanvas = (
  areFlowsVisible: boolean,
  setters: CatalogDialogSetters,
): void => {
  const targetTab = catalogTabForCanvas(areFlowsVisible);
  setters.setInitialTab(targetTab);
  setters.setShowOnlyTab(targetTab);
  setters.setOpen(true);
};

// Flow selection handler - loads flow into FlowCanvas (separate from crew tabs)
export const useFlowSelectHandler = (
  setFlowNodes: React.Dispatch<React.SetStateAction<Node[]>>,
  setFlowEdges: React.Dispatch<React.SetStateAction<Edge[]>>
) => {
  return useCallback((flowNodes: Node[], flowEdges: Edge[], flowConfig?: FlowConfiguration) => {
    console.log('WorkflowDesigner - Handling flow select (loading into FlowCanvas):', { flowNodes, flowEdges, flowConfig });

    // Carry the flow's DECLARED state onto the tab. Channel names are rederived
    // from the canvas on every save, but reducers and the conversational flag
    // are the author's decisions — without this, opening a conversational flow
    // and saving it would quietly demote it to a one-shot.
    const activeTabId = useTabManagerStore.getState().activeTabId;
    const declaredState = (flowConfig as { state?: DeclaredFlowState } | undefined)?.state;
    if (activeTabId) {
      if (declaredState) {
        useFlowStateStore.getState().setDeclared(activeTabId, declaredState);
      } else {
        // A flow that declares nothing must not inherit the last flow opened
        // in this tab.
        useFlowStateStore.getState().clearDeclared(activeTabId);
      }
    }

    // Create copies of nodes and edges with new IDs to prevent duplicates
    const idMap = new Map<string, string>();

    const newNodes = flowNodes.map(node => {
      const oldId = node.id;
      const newId = uuidv4();
      idMap.set(oldId, newId);

      // Find tasks for this crew from flowConfig.listeners (fallback only)
      let flowConfigTasks: CrewTask[] = [];
      if (flowConfig?.listeners && node.data?.crewName) {
        const listener = flowConfig.listeners.find(l => l.crewName === node.data.crewName);
        if (listener && listener.tasks) {
          flowConfigTasks = listener.tasks;
        }
      }

      // Remove flow_id and flowId from node data to prevent referencing non-existent flows
      const { flow_id, flowId: _oldFlowId, ...cleanNodeData } = node.data || {};

      // CRITICAL: Preserve ALL node data properties from the saved node
      // Only use flowConfigTasks as fallback if allTasks is not already in node data
      const preservedAllTasks = cleanNodeData.allTasks || flowConfigTasks;

      return {
        ...node,  // Preserve all top-level node properties (style, width, height, etc.)
        id: newId,
        position: {
          x: node.position.x,
          y: node.position.y
        },
        data: {
          ...cleanNodeData,  // Preserve ALL data properties (allTasks, selectedTasks, order, etc.)
          allTasks: preservedAllTasks  // Use saved allTasks or fallback to flowConfig
        }
      };
    });

    // Create edges with updated source/target IDs while preserving all edge data
    const newEdges: Edge[] = flowEdges.map(edge => {
      const newSource = idMap.get(edge.source) || edge.source;
      const newTarget = idMap.get(edge.target) || edge.target;

      // Remove flow_id and flowId from edge data to prevent referencing non-existent flows
      const edgeData = edge.data || {};
      const { flow_id, flowId: _oldFlowId, ...cleanEdgeData } = edgeData;

      // Preserve all edge properties including data, sourceHandle, targetHandle
      return {
        ...edge,  // Preserve all original edge properties
        id: edge.id || uuidv4(),
        source: newSource,
        target: newTarget,
        type: edge.type || 'animated',
        sourceHandle: edge.sourceHandle || null,
        targetHandle: edge.targetHandle || null,
        data: cleanEdgeData,  // Use cleaned edge data without flow references
        style: { stroke: '#9c27b0' }
      };
    });

    // Retain the saved branch placement when the flow has a real layout; only
    // auto-arrange into a centered horizontal row for degenerate loads (single
    // node, or everything stacked at one point). fitView centers either way.
    const laidOutNodes = hasSavedLayout(newNodes)
      ? newNodes
      : layoutFlowHorizontally(newNodes, newEdges);

    // CRITICAL: Set flow state directly (not crew state, not tabs)
    setFlowNodes(laidOutNodes);
    setFlowEdges(newEdges);

    console.log('Flow loaded into FlowCanvas:', { nodeCount: newNodes.length, edgeCount: newEdges.length });

    // Trigger events after state update
    setTimeout(() => {
      // First trigger fit view
      window.dispatchEvent(new CustomEvent('fitViewToNodes'));

      // Then dispatch a notification event for the flow loaded
      const flowName = flowConfig?.name || 'Flow';
      window.dispatchEvent(new CustomEvent('showNotification', {
        detail: { message: `${flowName} loaded successfully` }
      }));

      // Dispatch event to open the flow panel
      window.dispatchEvent(new CustomEvent('openFlowPanel'));
    }, 300);
  }, [setFlowNodes, setFlowEdges]);
};


// Handle crew flow dialog interactions
export const useCrewFlowDialogHandler = () => {
  const [isCrewFlowDialogOpen, setIsCrewFlowDialogOpen] = useState(false);
  
  // Function to open the dialog
  const openCrewOrFlowDialog = useCallback(() => {
    setIsCrewFlowDialogOpen(true);
  }, []);
  
  // Listen for openCrewFlowDialog events
  useEffect(() => {
    const handleOpenCrewFlowDialog = () => {
      openCrewOrFlowDialog();
    };
    
    window.addEventListener('openCrewFlowDialog', handleOpenCrewFlowDialog);
    
    return () => {
      window.removeEventListener('openCrewFlowDialog', handleOpenCrewFlowDialog);
    };
  }, [openCrewOrFlowDialog]);

  return {
    isCrewFlowDialogOpen,
    setIsCrewFlowDialogOpen,
    openCrewOrFlowDialog
  };
};

// Handle flow dialog specifically for flows
export const useFlowSelectionDialogHandler = () => {
  const [isFlowDialogOpen, setIsFlowDialogOpen] = useState(false);
  
  // Function to open the dialog
  const openFlowDialog = useCallback(() => {
    setIsFlowDialogOpen(true);
  }, []);
  
  // Listen for openFlowDialog events
  useEffect(() => {
    const handleOpenFlowDialog = () => {
      openFlowDialog();
    };
    
    window.addEventListener('openFlowDialog', handleOpenFlowDialog);
    
    return () => {
      window.removeEventListener('openFlowDialog', handleOpenFlowDialog);
    };
  }, [openFlowDialog]);

  return {
    isFlowDialogOpen,
    setIsFlowDialogOpen,
    openFlowDialog
  };
};


// Event binding handlers
export const useEventBindings = (
  handleRunClick: (executionType?: 'flow' | 'crew') => Promise<void>,
  setNodes: React.Dispatch<React.SetStateAction<Node[]>>,
  setEdges: React.Dispatch<React.SetStateAction<Edge[]>>,
) => {
  const handleRunClickWrapper = useCallback(async (executionType?: 'flow' | 'crew') => {
    if (executionType) {
      await handleRunClick(executionType);
    }
  }, [handleRunClick]);

  const handleCrewSelectWrapper = useCallback((nodes: Node[], edges: Edge[], crewName?: string, crewId?: string) => {
    console.log('WorkflowDesigner - Handling crew select:', { nodes, edges, crewName, crewId });

    // Notify that crew loading has started
    window.dispatchEvent(new CustomEvent('crewLoadStarted'));

    // Reorganize nodes using CanvasLayoutManager for consistent layout
    const layoutManager = new CanvasLayoutManager({ margin: 20, minNodeSpacing: 50 });
    const currentUIState = useUILayoutStore.getState().getUILayoutState();

    // Update screen dimensions to current window size
    currentUIState.screenWidth = window.innerWidth;
    currentUIState.screenHeight = window.innerHeight;

    layoutManager.updateUIState(currentUIState);

    // Reorganize nodes based on current layout orientation
    const reorganizedNodes = layoutManager.reorganizeNodes(nodes, 'crew', edges);

    console.log('📐 handleCrewSelectWrapper: Reorganized nodes using CanvasLayoutManager', {
      layoutOrientation: currentUIState.layoutOrientation,
      originalNodeCount: nodes.length,
      reorganizedNodeCount: reorganizedNodes.length,
      edgeCount: edges.length,
      originalPositions: nodes.map(n => ({ id: n.id, x: n.position.x, y: n.position.y })),
      newPositions: reorganizedNodes.map(n => ({ id: n.id, x: n.position.x, y: n.position.y }))
    });

    // Get the tab manager store
    const { createTab, updateTabNodes, updateTabEdges, setActiveTab, getActiveTab, updateTabCrewInfo } =
      useTabManagerStore.getState();

    // Save the current active tab ID before creating new one
    const previousActiveTabId = getActiveTab()?.id;
    console.log('Previous active tab ID:', previousActiveTabId);

    // Create a new tab for the loaded crew with the crew name. Force 'crew' view so
    // loading a crew always lands on the crew canvas even if the user was in flow mode.
    const actualCrewName = crewName || 'Loaded Crew';
    const newTabId = createTab(actualCrewName, 'crew');
    console.log('Created new tab with ID:', newTabId, 'and name:', actualCrewName);

    // Update edge handles and styles to match the current layout orientation
    const currentLayout = currentUIState.layoutOrientation || 'horizontal';

    const updatedEdges = edges.map(e => {
      const sourceNode = reorganizedNodes.find(n => n.id === e.source);
      const targetNode = reorganizedNodes.find(n => n.id === e.target);

      // Agent-to-task edges: change based on layout orientation
      if (sourceNode?.type === 'agentNode' && targetNode?.type === 'taskNode') {
        const agentSourceHandle = currentLayout === 'vertical' ? 'bottom' : 'right';
        const taskTargetHandle = currentLayout === 'vertical' ? 'top' : 'left';
        return {
          ...e,
          sourceHandle: agentSourceHandle,
          targetHandle: taskTargetHandle,
          style: {
            ...e.style,
            stroke: '#2196f3',
            strokeWidth: 2,
            // No strokeDasharray = solid line
          },
          animated: false
        };
      }

      // Task-to-task edges: ALWAYS horizontal (right → left) regardless of layout
      if (sourceNode?.type === 'taskNode' && targetNode?.type === 'taskNode') {
        return {
          ...e,
          sourceHandle: 'right',
          targetHandle: 'left',
          style: {
            ...e.style,
            stroke: '#2196f3',
            strokeWidth: 2,
            strokeDasharray: '12', // Dashed line
          },
          animated: true
        };
      }

      // Manager-to-agent edges: change based on layout orientation
      if (sourceNode?.type === 'managerNode' && targetNode?.type === 'agentNode') {
        const managerSourceHandle = currentLayout === 'vertical' ? 'bottom' : 'right';
        const agentTargetHandle = currentLayout === 'vertical' ? 'top' : 'left';
        return {
          ...e,
          sourceHandle: managerSourceHandle,
          targetHandle: agentTargetHandle,
          style: {
            ...e.style,
            stroke: '#2196f3',
            strokeWidth: 2,
          },
          animated: false
        };
      }

      return e;
    });

    console.log('🔗 Updated edge handles for layout:', {
      layoutOrientation: currentLayout,
      agentToTaskHandles: currentLayout === 'vertical' ? 'bottom→top' : 'right→left',
      taskToTaskHandles: 'right→left (always)',
      managerToAgentHandles: currentLayout === 'vertical' ? 'bottom→top' : 'right→left',
      edgeCount: updatedEdges.length
    });

    // Update the new tab with the reorganized nodes and updated edges BEFORE setting it as active
    // This ensures useTabSync will restore the correct positions when the tab becomes active
    updateTabNodes(newTabId, reorganizedNodes);
    updateTabEdges(newTabId, updatedEdges);

    // Verify the tab was updated correctly
    const updatedTab = useTabManagerStore.getState().getTab(newTabId);

    console.log('✅ Updated tab with reorganized nodes and edges before activation', {
      tabId: newTabId,
      tabNodeCount: updatedTab?.nodes.length,
      tabEdgeCount: updatedTab?.edges.length,
      reorganizedNodeCount: reorganizedNodes.length,
      updatedEdgeCount: updatedEdges.length
    });

    // If we have a crew name and ID, mark this tab as having loaded crew content
    if (crewName && crewId) {
      console.log('Marking tab as loaded crew with name:', crewName, 'and ID:', crewId);
      updateTabCrewInfo(newTabId, crewId, crewName);
    }

    // Use setTimeout to ensure Zustand state updates are processed before tab activation
    // This prevents useTabSync from seeing stale/empty tab data
    setTimeout(() => {
      // Now set the new tab as active
      setActiveTab(newTabId);
      console.log('Set new tab as active:', newTabId);

      // Directly set the nodes and edges to ensure they're displayed
      // This overrides any potential clearing from useTabSync
      setTimeout(() => {
        setNodes(reorganizedNodes);
        setEdges(updatedEdges);
        console.log('Set nodes and edges directly after tab activation');

        // Fit view after nodes are set
        setTimeout(() => {
          window.dispatchEvent(new CustomEvent('fitViewToNodes'));
        }, 200);

        // Notify that crew loading has completed
        setTimeout(() => {
          window.dispatchEvent(new CustomEvent('crewLoadCompleted'));
        }, 300);
      }, 50);
    }, 10);
  }, [setNodes, setEdges]);

  // Update event listeners to use the wrapper
  useEffect(() => {
    const handleExecuteCrew = () => {
      handleRunClickWrapper('crew');
    };
    
    const handleExecuteFlow = () => {
      handleRunClickWrapper('flow');
    };
    
    window.addEventListener('executeCrewEvent', handleExecuteCrew);
    window.addEventListener('executeFlowEvent', handleExecuteFlow);
    
    return () => {
      window.removeEventListener('executeCrewEvent', handleExecuteCrew);
      window.removeEventListener('executeFlowEvent', handleExecuteFlow);
    };
  }, [handleRunClickWrapper]);

  // Add an effect to listen for fitViewToNodes events
  useEffect(() => {
    const handleFitViewToNodes = () => {
      // Dispatch a custom event
      window.dispatchEvent(new CustomEvent('fitViewToNodesInternal'));
    };
    
    window.addEventListener('fitViewToNodes', handleFitViewToNodes);
    
    return () => {
      window.removeEventListener('fitViewToNodes', handleFitViewToNodes);
    };
  }, []);

  // Add effect to listen for openConfigAPIKeys events
  useEffect(() => {
    const handleOpenAPIKeys = () => {
      // Dispatch a custom event
      window.dispatchEvent(new CustomEvent('openConfigAPIKeysInternal'));
    };
    
    window.addEventListener('openConfigAPIKeys', handleOpenAPIKeys);
    
    return () => {
      window.removeEventListener('openConfigAPIKeys', handleOpenAPIKeys);
    };
  }, []);

  return {
    handleRunClickWrapper,
    handleCrewSelectWrapper
  };
};
