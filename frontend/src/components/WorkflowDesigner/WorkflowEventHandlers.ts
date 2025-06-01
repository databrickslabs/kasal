import { useCallback, useState, useEffect, useRef } from 'react';
import { Node, Edge, OnSelectionChangeParams, ReactFlowInstance, Connection } from 'reactflow';
import { FlowConfiguration, FlowFormData } from '../../types/flow';
import { v4 as uuidv4 } from 'uuid';
import { createEdge } from '../../utils/edgeUtils';
import { FlowService } from '../../api/FlowService';
import { createUniqueEdges } from './WorkflowUtils';
import { _generateCrewPositions, validateNodePositions } from '../../utils/flowWizardUtils';
import { useTabManagerStore } from '../../store/tabManager';

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

// Flow selection handler
export const useFlowSelectHandler = (
  setNodes: React.Dispatch<React.SetStateAction<Node[]>>,
  setEdges: React.Dispatch<React.SetStateAction<Edge[]>>
) => {
  return useCallback((flowNodes: Node[], flowEdges: Edge[], flowConfig?: FlowConfiguration) => {
    // Delay processing to allow time for React to handle DOM updates
    setTimeout(() => {
      // Use a function to process nodes so we can handle them properly
      const processNodes = () => {
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
        
        // Add all nodes at once
        if (newNodes.length > 0) {
          setNodes(nodes => {
            // Check if the new nodes would create duplicates with existing nodes
            const existingLabels = new Set(nodes.map(n => n.data?.label));
            
            // Only add nodes that aren't duplicate labels or types
            const filteredNewNodes = newNodes.filter(node => {
              // Keep all nodes that aren't crew, agent, or task nodes
              if (!node.type?.includes('crewNode') && 
                  !node.type?.includes('agentNode') && 
                  !node.type?.includes('taskNode')) {
                return true;
              }
              
              // For crew, agent, or task nodes, check for duplicates
              return !existingLabels.has(node.data?.label);
            });
            
            return [...nodes, ...filteredNewNodes];
          });
        }
        
        // Process additional flow config if provided
        if (flowConfig) {
          // Update listeners with IDs for the newly added nodes
          if (flowConfig.listeners && flowConfig.listeners.length > 0) {
            newNodes.forEach(node => {
              if (node.data && node.data.listener) {
                // Locate the corresponding listener in the flow config
                const listenerFound = flowConfig.listeners?.find(l => 
                  l.crewId === String(node.data.crewRef) &&
                  l.tasks && 
                  Array.isArray(l.tasks) && 
                  l.tasks.some(t => node.data.listener.tasks.some((lt: { id: string }) => lt.id === t.id))
                );
                
                if (listenerFound) {
                  // Update listener tasks if needed
                }
              }
            });
          }
        }
        
        // Add all edges at once
        if (newEdges.length > 0) {
          setEdges(edges => createUniqueEdges(newEdges, edges));
        }
        
        // Trigger an event to fit view to nodes after a short delay
        setTimeout(() => {
          // First trigger fit view
          window.dispatchEvent(new CustomEvent('fitViewToNodes'));
          
          // Then dispatch a notification event for the flow loaded
          const flowName = flowConfig?.name || 'Flow';
          window.dispatchEvent(new CustomEvent('showNotification', { 
            detail: { message: `${flowName} loaded successfully` }
          }));
        }, 300);
      };
      
      // Allow the DOM to settle before adding new elements
      processNodes();
    }, 100);
  }, [setNodes, setEdges]);
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

  const handleCrewSelectWrapper = useCallback((nodes: Node[], edges: Edge[], crewName?: string) => {
    console.log('WorkflowDesigner - Handling crew select:', { nodes, edges, crewName });
    
    // Notify that crew loading has started
    window.dispatchEvent(new CustomEvent('crewLoadStarted'));
    
    // Get the tab manager store
    const { createTab, updateTabNodes, updateTabEdges, setActiveTab } = 
      useTabManagerStore.getState();
    
    // Create a new tab for the loaded crew
    const newTabId = createTab(crewName || 'Loaded Crew');
    
    // Set the new tab as active
    setActiveTab(newTabId);
    
    // Update the new tab with the loaded nodes and edges
    setTimeout(() => {
      updateTabNodes(newTabId, nodes);
      updateTabEdges(newTabId, edges);
      
      // Also update the current state to trigger re-render
      setNodes(nodes);
      setEdges(edges);
      
      // Fit view to the loaded nodes
      setTimeout(() => {
        window.dispatchEvent(new CustomEvent('fitViewToNodes'));
      }, 200);
      
      // Notify that crew loading has completed
      setTimeout(() => {
        window.dispatchEvent(new CustomEvent('crewLoadCompleted'));
      }, 500);
    }, 100);
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