import { useEffect, useCallback, useRef } from 'react';
import { Node, Edge } from 'reactflow';
import { CanvasLayoutManager } from '../../utils/CanvasLayoutManager';
import { useUILayoutStore } from '../../store/uiLayout';
import { useCrewExecutionStore } from '../../store/crewExecution';

interface UseManagerNodeProps {
  nodes: Node[];
  edges: Edge[];
  setNodes: (nodes: Node[] | ((nodes: Node[]) => Node[])) => void;
  setEdges: (edges: Edge[] | ((edges: Edge[]) => Edge[])) => void;
}

/**
 * Hook to manage manager node creation/removal based on process type
 */
export const useManagerNode = ({ nodes, edges, setNodes, setEdges }: UseManagerNodeProps) => {
  console.log('[useManagerNode] 🚀 Hook called!');

  const { layoutOrientation } = useUILayoutStore();
  const { managerLLM, processType, managerNodeId, setManagerNodeId } = useCrewExecutionStore();

  // Track previous layout orientation to detect changes
  // Initialize with undefined so first change is detected
  const prevLayoutOrientationRef = useRef<'vertical' | 'horizontal' | undefined>(undefined);

  console.log('[useManagerNode] 📊 Current state:', {
    processType,
    managerNodeId,
    layoutOrientation,
    prevLayoutOrientation: prevLayoutOrientationRef.current,
    managerLLM,
    nodesCount: nodes.length,
    edgesCount: edges.length
  });

  /**
   * Create manager node and connect it to all agents
   */
  const createManagerNode = useCallback(() => {
    // Check if manager already exists
    const existingManager = nodes.find(n => n.type === 'managerNode');
    if (existingManager) {
      console.log('[useManagerNode] Manager node already exists');
      return;
    }

    // Get all agent nodes
    const agentNodes = nodes.filter(n => n.type === 'agentNode');

    if (agentNodes.length === 0) {
      console.log('[useManagerNode] No agents found, creating manager at default position');
    }

    // Create layout manager to calculate position
    const layoutManager = new CanvasLayoutManager({ margin: 20, minNodeSpacing: 50 });
    const currentUIState = useUILayoutStore.getState().getUILayoutState();
    currentUIState.screenWidth = window.innerWidth;
    currentUIState.screenHeight = window.innerHeight;
    layoutManager.updateUIState(currentUIState);

    const position = layoutManager.getManagerNodePosition(nodes, 'crew');

    // Create manager node
    const managerNode: Node = {
      id: 'manager-node',
      type: 'managerNode',
      position,
      data: {
        label: 'Manager Agent',
        llm: managerLLM || 'databricks-llama-4-maverick',
      },
    };

    // Create edges from manager to all agents
    const newEdges: Edge[] = agentNodes.map(agent => {
      const sourceHandle = layoutOrientation === 'vertical' ? 'bottom' : 'right';
      const targetHandle = layoutOrientation === 'vertical' ? 'top' : 'left';

      return {
        id: `manager-${agent.id}`,
        source: 'manager-node',
        target: agent.id,
        sourceHandle,
        targetHandle,
        type: 'default',
        style: { stroke: '#ff9800', strokeWidth: 2 },
        animated: false,
        label: '', // No label for manager-to-agent edges
      };
    });

    console.log('[useManagerNode] Creating manager node:', {
      position,
      agentCount: agentNodes.length,
      edgeCount: newEdges.length,
      layout: layoutOrientation
    });

    // Add manager node to the store
    setNodes([...nodes, managerNode]);

    // Add edges to the store
    setEdges([...edges, ...newEdges]);

    // Store manager node ID in Zustand
    setManagerNodeId(managerNode.id);

    // Trigger reorganization after a short delay
    setTimeout(() => {
      window.dispatchEvent(new CustomEvent('recalculateNodePositions', {
        detail: { reason: 'manager-node-created' }
      }));
    }, 100);
  }, [nodes, edges, setNodes, setEdges, layoutOrientation, managerLLM, setManagerNodeId]);

  /**
   * Remove manager node and its connections
   */
  const removeManagerNode = useCallback(() => {
    // Find and remove manager node
    const managerNode = nodes.find(n => n.type === 'managerNode');
    if (!managerNode) {
      console.log('[useManagerNode] No manager node to remove');
      return;
    }

    console.log('[useManagerNode] Removing manager node');

    // Remove manager node from the store
    setNodes(nodes.filter(n => n.id !== managerNode.id));

    // Remove all edges connected to manager from the store
    setEdges(edges.filter(e =>
      e.source !== managerNode.id && e.target !== managerNode.id
    ));

    // Clear manager node ID from Zustand
    setManagerNodeId(null);
  }, [nodes, edges, setNodes, setEdges, setManagerNodeId]);

  /**
   * Update manager connections when agents are added/removed
   */
  const updateManagerConnections = useCallback(() => {
    const managerNode = nodes.find(n => n.type === 'managerNode');
    if (!managerNode) {
      return;
    }

    const agentNodes = nodes.filter(n => n.type === 'agentNode');

    // Remove old manager edges
    const nonManagerEdges = edges.filter(e => e.source !== managerNode.id);

    // Create new edges from manager to all agents
    const newManagerEdges: Edge[] = agentNodes.map(agent => {
      const sourceHandle = layoutOrientation === 'vertical' ? 'bottom' : 'right';
      const targetHandle = layoutOrientation === 'vertical' ? 'top' : 'left';

      return {
        id: `manager-${agent.id}`,
        source: managerNode.id,
        target: agent.id,
        sourceHandle,
        targetHandle,
        type: 'default',
        style: { stroke: '#ff9800', strokeWidth: 2 },
        animated: false,
        label: '', // No label for manager-to-agent edges
      };
    });

    console.log('[useManagerNode] Updating manager connections:', {
      agentCount: agentNodes.length,
      edgeCount: newManagerEdges.length
    });

    // Update edges in the store
    setEdges([...nonManagerEdges, ...newManagerEdges]);
  }, [nodes, edges, setEdges, layoutOrientation]);

  /**
   * Listen for process type changes via Zustand
   */
  useEffect(() => {
    const managerExists = nodes.some(n => n.type === 'managerNode');

    console.log('[useManagerNode] Effect triggered:', {
      processType,
      managerNodeId,
      managerExists,
      shouldCreate: processType === 'hierarchical' && !managerExists,
      shouldRemove: processType !== 'hierarchical' && managerExists
    });

    if (processType === 'hierarchical') {
      // Only create if manager doesn't actually exist in nodes
      if (!managerExists) {
        console.log('[useManagerNode] Creating manager node...');
        createManagerNode();
      } else {
        console.log('[useManagerNode] Manager already exists, skipping creation');
        // Ensure managerNodeId is set even if manager already exists
        if (!managerNodeId) {
          const existingManager = nodes.find(n => n.type === 'managerNode');
          if (existingManager) {
            console.log('[useManagerNode] Setting managerNodeId for existing manager:', existingManager.id);
            setManagerNodeId(existingManager.id);
          }
        }
      }
    } else {
      // Remove manager if it exists
      if (managerExists) {
        console.log('[useManagerNode] Removing manager node...');
        removeManagerNode();
      } else {
        console.log('[useManagerNode] No manager to remove');
      }
    }
  }, [processType, nodes, createManagerNode, removeManagerNode]);

  /**
   * Listen for layout orientation changes to update manager position and edge handles
   */
  useEffect(() => {
    const managerNode = nodes.find((n: Node) => n.type === 'managerNode');

    console.log('[useManagerNode] 🔄 Layout effect triggered:', {
      prevLayout: prevLayoutOrientationRef.current,
      currentLayout: layoutOrientation,
      hasManager: !!managerNode,
      processType,
      managerNodeId,
      willUpdate: prevLayoutOrientationRef.current !== layoutOrientation && !!managerNode && processType === 'hierarchical' && !!managerNodeId
    });

    // Only update if layout orientation actually changed
    if (prevLayoutOrientationRef.current === layoutOrientation) {
      console.log('[useManagerNode] ⏭️ Skipping - layout unchanged');
      return;
    }

    // Skip if no manager node exists yet
    if (!managerNode) {
      console.log('[useManagerNode] ⏭️ Skipping - no manager node');
      prevLayoutOrientationRef.current = layoutOrientation;
      return;
    }

    // Update if manager exists and we're in hierarchical mode (don't require managerNodeId)
    if (managerNode && processType === 'hierarchical') {
      console.log('[useManagerNode] ✅ Layout orientation changed, updating manager position and connections:', {
        from: prevLayoutOrientationRef.current,
        to: layoutOrientation,
        managerNodeId: managerNodeId || managerNode.id
      });

      const agentNodes = nodes.filter(n => n.type === 'agentNode');

      // Recalculate manager position for new layout
      const layoutManager = new CanvasLayoutManager({ margin: 20, minNodeSpacing: 50 });
      const currentUIState = useUILayoutStore.getState().getUILayoutState();
      currentUIState.screenWidth = window.innerWidth;
      currentUIState.screenHeight = window.innerHeight;
      // CRITICAL: Update the layout orientation in the UI state to the NEW orientation
      currentUIState.layoutOrientation = layoutOrientation;

      console.log('[useManagerNode] Before updateUIState:', {
        layoutOrientation,
        currentUIStateOrientation: currentUIState.layoutOrientation,
        agentPositions: agentNodes.map(a => ({ id: a.id.slice(-8), x: a.position.x, y: a.position.y }))
      });

      layoutManager.updateUIState(currentUIState);

      const newPosition = layoutManager.getManagerNodePosition(nodes, 'crew');

      console.log('[useManagerNode] Repositioning manager node:', {
        oldPosition: managerNode.position,
        newPosition,
        layout: layoutOrientation,
        expectedLayout: layoutOrientation === 'vertical' ? 'Manager Y < Agent Y' : 'Manager X < Agent X'
      });

      // Update manager node position
      const updatedNodes = nodes.map(node =>
        node.id === managerNode.id
          ? { ...node, position: newPosition }
          : node
      );
      setNodes(updatedNodes);

      // Remove old manager edges
      const nonManagerEdges = edges.filter(e => e.source !== managerNode.id);

      // Create new edges with updated handles
      const newManagerEdges: Edge[] = agentNodes.map(agent => {
        const sourceHandle = layoutOrientation === 'vertical' ? 'bottom' : 'right';
        const targetHandle = layoutOrientation === 'vertical' ? 'top' : 'left';

        return {
          id: `manager-${agent.id}`,
          source: managerNode.id,
          target: agent.id,
          sourceHandle,
          targetHandle,
          type: 'default',
          style: { stroke: '#ff9800', strokeWidth: 2 },
          animated: false,
          label: '', // No label for manager-to-agent edges
        };
      });

      // Update edges in the store
      setEdges([...nonManagerEdges, ...newManagerEdges]);

      // Update the ref
      prevLayoutOrientationRef.current = layoutOrientation;
    }
  }, [layoutOrientation, processType, managerNodeId, nodes, edges, setNodes, setEdges]);

  /**
   * Listen for node changes to update manager connections
   */
  useEffect(() => {
    const handleNodesChange = () => {
      const managerNode = nodes.find((n: Node) => n.type === 'managerNode');

      if (managerNode) {
        updateManagerConnections();
      }
    };

    // Debounce to avoid too many updates
    let timeoutId: NodeJS.Timeout;
    const debouncedHandler = () => {
      clearTimeout(timeoutId);
      timeoutId = setTimeout(handleNodesChange, 500);
    };

    window.addEventListener('nodesChanged', debouncedHandler);

    return () => {
      window.removeEventListener('nodesChanged', debouncedHandler);
      clearTimeout(timeoutId);
    };
  }, [nodes, updateManagerConnections]);

  return {
    createManagerNode,
    removeManagerNode,
    updateManagerConnections,
  };
};

