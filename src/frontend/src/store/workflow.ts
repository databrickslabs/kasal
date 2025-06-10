import { create } from 'zustand';
import { persist } from 'zustand/middleware';
import { Node, Edge, Connection } from 'reactflow';
import { FlowConfiguration } from '../types/flow';
import { createEdge, edgeExists } from '../utils/edgeUtils';

interface ContextMenuState {
  mouseX: number;
  mouseY: number;
  edgeId: string;
}

interface UIState {
  isMinimapVisible: boolean;
  controlsVisible: boolean;
}

interface WorkflowState {
  nodes: Node[];
  edges: Edge[];
  selectedEdges: Edge[];
  contextMenu: ContextMenuState | null;
  flowConfig: FlowConfiguration | null;
  draggedNodeIds: string[];
  manuallyPositionedNodes: string[];
  hasSeenTutorial: boolean;
  hasSeenHandlebar: boolean;
  showErrorMessage: (message: string) => void;
  uiState: UIState;
  
  // Actions
  setNodes: (nodes: Node[] | ((nodes: Node[]) => Node[])) => void;
  setEdges: (edges: Edge[] | ((edges: Edge[]) => Edge[])) => void;
  setSelectedEdges: (edges: Edge[]) => void;
  setContextMenu: (contextMenu: ContextMenuState | null) => void;
  setFlowConfig: (config: FlowConfiguration | null) => void;
  setDraggedNodeIds: (nodeIds: string[]) => void;
  addDraggedNodeId: (nodeId: string) => void;
  removeDraggedNodeId: (nodeId: string) => void;
  setManuallyPositionedNodes: (nodeIds: string[]) => void;
  addManuallyPositionedNodeId: (nodeId: string) => void;
  removeManuallyPositionedNodeId: (nodeId: string) => void;
  setHasSeenTutorial: (seen: boolean) => void;
  setHasSeenHandlebar: (seen: boolean) => void;
  setShowErrorMessage: (showErrorMessage: (message: string) => void) => void;
  setUIState: (changes: Partial<UIState>) => void;
  clearCanvas: () => void;
  clearWorkflow: () => void; // Alias for clearCanvas for compatibility
  deleteEdge: (edgeId: string) => void;
  addEdge: (connection: Connection) => void;
  updateNodePosition: (params: { nodeId: string; position: { x: number; y: number } }) => void;
}

const initialState = {
  nodes: [],
  edges: [],
  selectedEdges: [],
  contextMenu: null,
  flowConfig: null,
  draggedNodeIds: [],
  manuallyPositionedNodes: [],
  hasSeenTutorial: false,
  hasSeenHandlebar: false,
  showErrorMessage: (message: string) => console.error(message),
  uiState: {
    isMinimapVisible: false,
    controlsVisible: true
  },
};

export const useWorkflowStore = create<WorkflowState>()(
  persist(
    (set, get) => ({
      ...initialState,
      
      setNodes: (nodes) => 
        set((state) => {
          if (typeof nodes === 'function') {
            return { nodes: nodes(state.nodes) };
          }
          return { nodes };
        }),
      
      setEdges: (edges) => 
        set((state) => {
          if (typeof edges === 'function') {
            return { edges: edges(state.edges) };
          }
          return { edges };
        }),
      
      setSelectedEdges: (edges: Edge[]) => 
        set(() => ({ selectedEdges: edges })),
      
      setContextMenu: (contextMenu: ContextMenuState | null) => 
        set(() => ({ contextMenu })),
      
      setFlowConfig: (config: FlowConfiguration | null) => 
        set(() => ({ flowConfig: config })),
      
      setDraggedNodeIds: (nodeIds: string[]) => 
        set(() => ({ draggedNodeIds: nodeIds })),
      
      addDraggedNodeId: (nodeId: string) =>
        set((state) => ({
          draggedNodeIds: state.draggedNodeIds.includes(nodeId)
            ? state.draggedNodeIds
            : [...state.draggedNodeIds, nodeId]
        })),
      
      removeDraggedNodeId: (nodeId: string) =>
        set((state) => ({
          draggedNodeIds: state.draggedNodeIds.filter(id => id !== nodeId)
        })),
      
      setManuallyPositionedNodes: (nodeIds: string[]) => 
        set(() => ({ manuallyPositionedNodes: nodeIds })),
      
      addManuallyPositionedNodeId: (nodeId: string) =>
        set((state) => ({
          manuallyPositionedNodes: state.manuallyPositionedNodes.includes(nodeId)
            ? state.manuallyPositionedNodes
            : [...state.manuallyPositionedNodes, nodeId]
        })),
      
      removeManuallyPositionedNodeId: (nodeId: string) =>
        set((state) => ({
          manuallyPositionedNodes: state.manuallyPositionedNodes.filter(id => id !== nodeId)
        })),
        
      setHasSeenTutorial: (seen: boolean) => 
        set(() => ({ hasSeenTutorial: seen })),
        
      setHasSeenHandlebar: (seen: boolean) => 
        set(() => ({ hasSeenHandlebar: seen })),
      
      setShowErrorMessage: (showErrorMessage: (message: string) => void) => 
        set(() => ({ showErrorMessage })),
        
      setUIState: (changes: Partial<UIState>) =>
        set((state) => ({
          uiState: { ...state.uiState, ...changes }
        })),
      
      clearCanvas: () => 
        set(() => ({ 
          nodes: [], 
          edges: [],
          manuallyPositionedNodes: []
        })),
        
      clearWorkflow: () => 
        set(() => ({ 
          nodes: [], 
          edges: [],
          manuallyPositionedNodes: []
        })),
      
      deleteEdge: (edgeId: string) => 
        set((state) => ({ 
          edges: state.edges.filter((edge: Edge) => edge.id !== edgeId) 
        })),
      
      addEdge: (connection: Connection) => 
        set((state) => {
          const { source, target, sourceHandle, targetHandle } = connection;
          if (!source || !target) return state;

          const sourceNode = state.nodes.find((node: Node) => node.id === source);
          const targetNode = state.nodes.find((node: Node) => node.id === target);

          if (!sourceNode || !targetNode) return state;

          if (sourceNode.type === 'agentNode' && targetNode.type === 'agentNode') {
            return state; // Error will be handled by the component
          }

          // Check if edge already exists
          if (edgeExists(state.edges, connection)) {
            return state;
          }

          // Check if it's a crew connection needing direction swap
          if (sourceHandle?.includes('-target') && source.includes('crew-')) {
            const baseSourceHandle = sourceHandle.replace('-target', '');
            const baseTargetHandle = targetHandle?.includes('-target')
              ? targetHandle.replace('-target', '')
              : targetHandle;

            const swappedConnection: Connection = {
              source: target,
              target: source,
              sourceHandle: baseTargetHandle || null,
              targetHandle: baseSourceHandle ? `${baseSourceHandle}-target` : null
            };

            // Check if swapped edge exists
            if (edgeExists(state.edges, swappedConnection)) {
              return state;
            }

            return { 
              edges: [...state.edges, createEdge(swappedConnection, 'crewEdge', true)] 
            };
          }

          return { 
            edges: [...state.edges, createEdge(connection)] 
          };
        }),
      
      updateNodePosition: ({ nodeId, position }) => 
        set((state) => {
          const updatedNodes = state.nodes.map((node) => {
            if (node.id === nodeId) {
              return {
                ...node,
                position
              };
            }
            return node;
          });
          
          return { nodes: updatedNodes };
        }),
    }),
    {
      name: 'workflow-storage',
      partialize: (state) => ({
        // Don't persist nodes and edges - they should be managed by tab-manager-storage
        // nodes: state.nodes,
        // edges: state.edges,
        hasSeenTutorial: state.hasSeenTutorial,
        hasSeenHandlebar: state.hasSeenHandlebar,
        manuallyPositionedNodes: state.manuallyPositionedNodes
      }),
    }
  )
); 