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
  const { layoutOrientation } = useUILayoutStore();
  const { managerLLM, processType, managerNodeId, setManagerNodeId, isLoadingCrew } = useCrewExecutionStore();

  // Track previous layout orientation to detect changes
  const prevLayoutOrientationRef = useRef<'vertical' | 'horizontal' | undefined>(undefined);

  // Track previous state to avoid unnecessary effect runs
  const prevProcessTypeRef = useRef(processType);
  const prevNodeCountRef = useRef(nodes.length);

  /**
   * Create manager node and connect it to all agents
   */
  const createManagerNode = useCallback(() => {
    // Check if manager already exists
    const existingManager = nodes.find(n => n.type === 'managerNode');
    if (existingManager) {
      return;
    }

    // Get all agent nodes
    const agentNodes = nodes.filter(n => n.type === 'agentNode');

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
        style: { stroke: '#2196f3', strokeWidth: 2 },
        animated: false,
        label: '',
      };
    });

    // Add manager node and edges
    setNodes([...nodes, managerNode]);
    setEdges([...edges, ...newEdges]);
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
    const managerNode = nodes.find(n => n.type === 'managerNode');
    if (!managerNode) {
      return;
    }

    setNodes(nodes.filter(n => n.id !== managerNode.id));
    setEdges(edges.filter(e =>
      e.source !== managerNode.id && e.target !== managerNode.id
    ));
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
    const nonManagerEdges = edges.filter(e => e.source !== managerNode.id);

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
        style: { stroke: '#2196f3', strokeWidth: 2 },
        animated: false,
        label: '',
      };
    });

    setEdges([...nonManagerEdges, ...newManagerEdges]);
  }, [nodes, edges, setEdges, layoutOrientation]);

  /**
   * Listen for process type changes via Zustand
   */
  useEffect(() => {
    // Skip if nothing relevant changed
    const managerExists = nodes.some(n => n.type === 'managerNode');
    const processTypeChanged = prevProcessTypeRef.current !== processType;
    const nodeCountChanged = prevNodeCountRef.current !== nodes.length;

    prevProcessTypeRef.current = processType;
    prevNodeCountRef.current = nodes.length;

    // Only act when process type changes or node count changes
    if (!processTypeChanged && !nodeCountChanged) {
      return;
    }

    if (isLoadingCrew) {
      return;
    }

    if (processType === 'hierarchical') {
      if (!managerExists) {
        createManagerNode();
      } else if (!managerNodeId) {
        const existingManager = nodes.find(n => n.type === 'managerNode');
        if (existingManager) {
          setManagerNodeId(existingManager.id);
        }
      }
    } else {
      if (managerExists) {
        removeManagerNode();
      }
    }
  }, [processType, nodes, createManagerNode, removeManagerNode, isLoadingCrew, managerNodeId, setManagerNodeId]);

  /**
   * Listen for layout orientation changes to update manager position and edge handles
   */
  useEffect(() => {
    // Only update if layout orientation actually changed
    if (prevLayoutOrientationRef.current === layoutOrientation) {
      return;
    }

    const managerNode = nodes.find((n: Node) => n.type === 'managerNode');

    // Skip if no manager node exists yet
    if (!managerNode) {
      prevLayoutOrientationRef.current = layoutOrientation;
      return;
    }

    // Update if manager exists and we're in hierarchical mode
    if (managerNode && processType === 'hierarchical') {
      const agentNodes = nodes.filter(n => n.type === 'agentNode');

      // Recalculate manager position for new layout
      const layoutManager = new CanvasLayoutManager({ margin: 20, minNodeSpacing: 50 });
      const currentUIState = useUILayoutStore.getState().getUILayoutState();
      currentUIState.screenWidth = window.innerWidth;
      currentUIState.screenHeight = window.innerHeight;
      currentUIState.layoutOrientation = layoutOrientation;

      layoutManager.updateUIState(currentUIState);

      const newPosition = layoutManager.getManagerNodePosition(nodes, 'crew');

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
          style: { stroke: '#2196f3', strokeWidth: 2 },
          animated: false,
          label: '',
        };
      });

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
