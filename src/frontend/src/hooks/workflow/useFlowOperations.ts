import { useCallback } from 'react';
import { Node, Edge, Connection } from 'reactflow';
import { v4 as uuidv4 } from 'uuid';
import type { FlowFormData, FlowConfiguration } from '../../types/flow';
import { FlowService } from '../../api/FlowService';
import { createEdge, edgeExists } from '../../utils/edgeUtils';

interface UseFlowOperationsProps {
  nodes: Node[];
  edges: Edge[];
  setNodes: (nodes: Node[] | ((nodes: Node[]) => Node[])) => void;
  setEdges: (edges: Edge[] | ((edges: Edge[]) => Edge[])) => void;
  showErrorMessage: (message: string) => void;
}

export const useFlowOperations = ({
  nodes,
  edges,
  setNodes,
  setEdges,
  showErrorMessage,
}: UseFlowOperationsProps) => {
  const handleAddFlowNode = useCallback((flowData: FlowFormData, position: { x: number; y: number }) => {
    const flowId = `flow-${Date.now()}`;
    
    // Create a standard crew node
    const newNode: Node = {
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
    setNodes((nds: Node[]) => nds.concat(newNode));
    
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
          // Create edge to existing node
          const connection: Connection = {
            source: flowId,
            target: existingNode.id,
            sourceHandle: null,
            targetHandle: null
          };
          
          if (!edgeExists(edges, connection)) {
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
          const connection: Connection = {
            source: flowId,
            target: newNodeId,
            sourceHandle: null,
            targetHandle: null
          };
          
          newEdges.push(createEdge(connection, 'animated', true, { stroke: '#9c27b0' }));
        }
      });
      
      // Add new nodes and edges to the canvas
      if (newNodes.length > 0) {
        setNodes((nds: Node[]) => [...nds, ...newNodes]);
      }
      
      if (newEdges.length > 0) {
        setEdges((eds: Edge[]) => [...eds, ...newEdges]);
      }
    }
  }, [nodes, edges, setNodes, setEdges]);

  const handleFlowSelect = useCallback((flowNodes: Node[], flowEdges: Edge[], flowConfig?: FlowConfiguration) => {
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
        const newEdges = flowEdges.map(edge => {
          const newSource = idMap.get(edge.source) || edge.source;
          const newTarget = idMap.get(edge.target) || edge.target;
          
          return {
            ...edge,
            id: `e-${uuidv4()}`,
            source: newSource,
            target: newTarget
          };
        });
        
        // Add all nodes at once
        if (newNodes.length > 0) {
          setNodes((nodes: Node[]) => {
            // Check if the new nodes would create duplicates with existing nodes
            const existingLabels = new Set(nodes.map((n: Node) => n.data?.label));
            
            // Only add nodes that aren't duplicate labels
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
                  // This is where you'd handle any special processing for listeners
                }
              }
            });
          }
        }
        
        // Add all edges at once
        if (newEdges.length > 0) {
          setEdges((edges: Edge[]) => [...edges, ...newEdges]);
        }
      };
      
      // Allow the DOM to settle before adding new elements
      processNodes();
    }, 100);
  }, [setNodes, setEdges]);

  const handleSaveFlow = useCallback(async (flowSaveData: any) => {
    try {
      const result = await FlowService.saveFlow(flowSaveData);
      console.log('Flow saved successfully:', result);
      showErrorMessage('Flow saved successfully');
    } catch (error) {
      console.error('Error saving flow:', error);
      showErrorMessage(`Failed to save flow: ${error instanceof Error ? error.message : 'Unknown error'}`);
    }
  }, [showErrorMessage]);

  return {
    handleAddFlowNode,
    handleFlowSelect,
    handleSaveFlow,
  };
}; 