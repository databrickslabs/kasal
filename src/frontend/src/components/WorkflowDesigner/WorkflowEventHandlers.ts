import { useCallback, useState, useEffect, useRef } from 'react';
import { Node, Edge, OnSelectionChangeParams, ReactFlowInstance } from 'reactflow';
import { FlowConfiguration, FlowFormData } from '../../types/flow';
import { CrewTask } from '../../types/crewPlan';
import { v4 as uuidv4 } from 'uuid';
import { createEdge } from '../../utils/edgeUtils';
import { FlowService } from '../../api/FlowService';
import { createUniqueEdges } from './WorkflowUtils';
import { _generateCrewPositions, validateNodePositions } from '../../utils/flowWizardUtils';
import { useTabManagerStore } from '../../store/tabManager';
import { CanvasLayoutManager } from '../../utils/CanvasLayoutManager';
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

// Flow selection handler - loads flow into FlowCanvas (separate from crew tabs)
export const useFlowSelectHandler = (
  setFlowNodes: React.Dispatch<React.SetStateAction<Node[]>>,
  setFlowEdges: React.Dispatch<React.SetStateAction<Edge[]>>
) => {
  return useCallback((flowNodes: Node[], flowEdges: Edge[], flowConfig?: FlowConfiguration) => {
    console.log('WorkflowDesigner - Handling flow select (loading into FlowCanvas):', { flowNodes, flowEdges, flowConfig });

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

    // CRITICAL: Set flow state directly (not crew state, not tabs)
    setFlowNodes(newNodes);
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

// Flow addition handler
export const useFlowAddHandler = (
  setNodes: React.Dispatch<React.SetStateAction<Node[]>>,
  setEdges: React.Dispatch<React.SetStateAction<Edge[]>>,
  nodes: Node[],
  edges: Edge[],
  showErrorMessage: (message: string) => void
) => {
  return useCallback((flowData: FlowFormData, position: { x: number; y: number }) => {
    const flowId = `flow-${Date.now()}`;
    
    // Create a standard crew node
    const newNode = {
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
    
    // Add the new node to the canvas
    setNodes((nds) => nds.concat(newNode));
    
    // Check if this is a start flow with target crews
    const listenToArray = flowData.listenTo || [];
    
    if (flowData.type === 'start' && listenToArray.length > 0) {
      
      // Add target flow nodes and connect them with edges
      const newNodes: Node[] = [];
      const newEdges: Edge[] = [];
      
      // Calculate position offset for placing new nodes
      const offsetY = position.y + 150; // Place target nodes below the current node
      
      listenToArray.forEach((targetCrewName, index) => {
        // Check if a node with this crew name already exists
        const existingNode = nodes.find(node => 
          node.data?.crewName === targetCrewName && node.id !== flowId
        );
        
        if (existingNode) {
          const connection = {
            source: flowId,
            target: existingNode.id,
            sourceHandle: null,
            targetHandle: null
          };
          
          // Check if this edge already exists
          const existingEdge = edges.some(edge =>
            edge.source === connection.source && edge.target === connection.target
          );
          
          if (!existingEdge) {
            newEdges.push(createEdge(connection, 'animated', true, { stroke: '#9c27b0' }));
          }
        } else {
          // Create a new flow node for this target crew
          const newNodeId = `flow-${Date.now()}-${index}`;
          const offsetX = position.x + (index - (listenToArray.length - 1) / 2) * 200;
          
          newNodes.push({
            id: newNodeId,
            type: 'crewNode',
            position: { x: offsetX, y: offsetY },
            data: {
              id: newNodeId,
              label: targetCrewName,
              crewName: targetCrewName,
              crewId: newNodeId,
              type: 'normal',
            }
          });
          
          // Create edge to new node
          const connection = {
            source: flowId,
            target: newNodeId,
            sourceHandle: null,
            targetHandle: null
          };
          
          // Check if this edge already exists
          const existingEdge = edges.some(edge =>
            edge.source === connection.source && edge.target === connection.target
          );
          
          if (!existingEdge) {
            newEdges.push(createEdge(connection, 'animated', true, { stroke: '#9c27b0' }));
          }
        }
      });
      
      // Add new nodes and edges to the canvas
      if (newNodes.length > 0) {
        setNodes(nds => [...nds, ...newNodes]);
      }
      
      // Add all edges at once
      if (newEdges.length > 0) {
        setEdges(edges => createUniqueEdges(newEdges, edges));
      }
    }
  }, [setNodes, setEdges, nodes, edges]);
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

// Handle flow dialog for creating crews
export const useFlowDialogHandler = (
  setNodes: React.Dispatch<React.SetStateAction<Node[]>>,
  setEdges: React.Dispatch<React.SetStateAction<Edge[]>>,
  showErrorMessage: (message: string) => void
) => {
  // Define a proper type for the crew object
  interface CrewObject {
    id: number | string;
    name: string;
  }

  // Define a type for the flow save data
  interface FlowSaveData {
    name: string;
    crew_id: string;
    nodes: Node[];
    edges: Edge[];
    flowConfig: FlowConfiguration;
  }

  return useCallback((selectedCrews: CrewObject[], positions: Record<string, {x: number, y: number}>, flowConfig?: FlowConfiguration, shouldSave = false) => {
    // Create crew nodes all at once
    const newNodes = selectedCrews.map(crew => {
      const position = positions[crew.id.toString()];
      const nodeId = `crew-${Date.now()}-${crew.id}`;
      
      return {
        id: nodeId,
        type: 'crewNode',
        position,
        data: {
          id: crew.id.toString(),
          label: crew.name,
          crewName: crew.name,
          crewId: crew.id,
          // Store the flowConfig on the node for later retrieval if needed
          flowConfig: flowConfig
        }
      };
    });
    
    // Validate node positions
    const validatedNodes = validateNodePositions(newNodes);
    
    // Add all nodes at once
    setNodes(nodes => [...nodes, ...validatedNodes]);
    
    const newEdges: Edge[] = [];
    
    // If we have flow configuration, create the edges as well
    if (flowConfig && flowConfig.listeners) {
      // Create a map of crew IDs to node IDs for easy lookup
      const crewNodeMap = validatedNodes.reduce<Record<string | number, string>>((map, node) => {
        // Explicitly type the crewId value to handle both string and number
        const crewId: string | number = node.data.crewId;
        map[crewId.toString()] = node.id;
        return map;
      }, {});
      
      // Process listeners to create edges
      flowConfig.listeners.forEach(listener => {
        const sourceNodeId = crewNodeMap[listener.crewId.toString()];
        
        if (sourceNodeId && listener.tasks) {
          // For each task in the listener, create an edge
          listener.tasks.forEach(task => {
            // Access the agent_id property with a type cast
            interface TaskWithAgent { id: string; name: string; agent_id?: string }
            const taskWithAgent = task as TaskWithAgent;
            const targetCrewId = taskWithAgent.agent_id ? Number(taskWithAgent.agent_id) : null;
            
            if (targetCrewId && crewNodeMap[targetCrewId.toString()]) {
              const targetNodeId = crewNodeMap[targetCrewId.toString()];
              
              // Create edge from source (listener) to target (task's crew)
              const flowConnection = {
                source: sourceNodeId,
                target: targetNodeId,
                sourceHandle: null,
                targetHandle: null
              };
              
              newEdges.push(createEdge(flowConnection, 'animated', true, { stroke: '#9c27b0' }));
            }
          });
        }
      });
      
      // Add all edges at once
      if (newEdges.length > 0) {
        setEdges(edges => createUniqueEdges(newEdges, edges));
      }
    }
    
    // Save the flow if shouldSave is true
    if (shouldSave && flowConfig) {
      // Get the first crew's ID from selectedCrews
      const firstCrew = selectedCrews[0];
      
      if (!firstCrew || !firstCrew.id) {
        showErrorMessage('No valid crew found to associate with the flow');
        return;
      }
      
      // Use crew ID as is - it will be validated and converted to UUID in the service
      const crewId = firstCrew.id;
      
      // Save flow with associated nodes and edges
      const flowSaveData: FlowSaveData = {
        name: flowConfig.name,
        crew_id: String(crewId), // Convert to string to ensure consistency
        nodes: validatedNodes,
        edges: newEdges,
        flowConfig
      };
      
      // Save the flow to the database
      FlowService.saveFlow(flowSaveData)
        .then(result => {
          console.log('Flow saved successfully:', result);
          // Show a success message if needed
          showErrorMessage('Flow saved successfully');
        })
        .catch(error => {
          console.error('Error saving flow:', error);
          showErrorMessage(`Failed to save flow: ${error instanceof Error ? error.message : 'Unknown error'}`);
        });
    }
  }, [setNodes, setEdges, showErrorMessage]);
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

    // Create a new tab for the loaded crew with the crew name
    const actualCrewName = crewName || 'Loaded Crew';
    const newTabId = createTab(actualCrewName);
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

      return e;
    });

    console.log('🔗 Updated edge handles for layout:', {
      layoutOrientation: currentLayout,
      agentToTaskHandles: currentLayout === 'vertical' ? 'bottom→top' : 'right→left',
      taskToTaskHandles: 'right→left (always)',
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