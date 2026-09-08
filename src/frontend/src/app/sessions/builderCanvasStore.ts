import { create } from 'zustand';
import { Node, Edge } from 'reactflow';
import { v4 as uuidv4 } from 'uuid';
import { ReasoningConfig } from '../../types/workflow/crew';
import { useUILayoutStore } from '../../store/uiLayout';

// Execution configuration per canvas
export interface CanvasExecutionConfig {
  processType?: 'sequential' | 'hierarchical' | 'parallel';
  reasoningEnabled?: boolean;
  reasoningLLM?: string;
  reasoningConfig?: ReasoningConfig;  // Model reasoning/thinking budget
  managerLLM?: string;
  selectedModel?: string;
}

export interface BuilderCanvas {
  id: string;
  name: string;
  nodes: Node[];
  edges: Edge[];
  // Flow canvas nodes/edges (independent from crew canvas)
  flowNodes: Node[];
  flowEdges: Edge[];
  // View mode: which canvas is currently visible in this canvas
  viewMode: 'crew' | 'flow';
  isActive: boolean;
  isDirty: boolean; // Track if canvas has unsaved changes
  isSessionDraft?: boolean; // An empty session is listed only after work starts
  createdAt: Date;
  lastModified: Date;
  group_id: string; // Teamspace/group this canvas belongs to
  // Crew metadata
  savedCrewId?: string; // ID of the saved crew
  savedCrewName?: string; // Name of the saved crew
  lastSavedAt?: Date; // When the crew was last saved
  // Flow metadata
  savedFlowId?: string; // ID of the saved flow
  savedFlowName?: string; // Name of the saved flow
  // Chat session
  chatSessionId?: string; // ID of the chat session for this canvas
  // All runs belong to the session, even after editing or rerunning its canvas.
  executionJobIds?: string[];
  // Execution status
  executionStatus?: 'running' | 'completed' | 'failed';
  lastExecutionTime?: Date;
  // Execution configuration (per-canvas runtime settings)
  executionConfig?: CanvasExecutionConfig;
}

interface BuilderCanvasState {
  canvases: BuilderCanvas[];
  activeCanvasId: string | null;
  hydrated: boolean;


  // Actions
  createCanvas: (name?: string, viewMode?: 'crew' | 'flow', options?: { sessionDraft?: boolean }) => string;
  nameSessionFromPrompt: (sessionId: string, prompt: string) => void;
  closeCanvas: (canvasId: string) => void;
  setActiveCanvas: (canvasId: string) => void;
  updateCanvasName: (canvasId: string, name: string) => void;
  updateCanvasNodes: (canvasId: string, nodes: Node[]) => void;
  updateCanvasEdges: (canvasId: string, edges: Edge[]) => void;
  updateCanvasFlowNodes: (canvasId: string, flowNodes: Node[]) => void;
  updateCanvasFlowEdges: (canvasId: string, flowEdges: Edge[]) => void;
  updateCanvasViewMode: (canvasId: string, viewMode: 'crew' | 'flow') => void;
  markCanvasDirty: (canvasId: string) => void;
  markCanvasClean: (canvasId: string) => void;
  getActiveCanvas: () => BuilderCanvas | null;
  getCanvas: (canvasId: string) => BuilderCanvas | null;
  // Teamspace filtering
  getCanvasesForCurrentGroup: () => BuilderCanvas[];
  // New methods for crew management
  updateCanvasCrewInfo: (canvasId: string, crewId: string, crewName: string) => void;
  clearCanvasCrewInfo: (canvasId: string) => void;
  // New methods for flow management
  updateCanvasFlowInfo: (canvasId: string, flowId: string, flowName: string) => void;
  // New methods for execution status
  updateCanvasExecutionStatus: (canvasId: string, status: 'running' | 'completed' | 'failed') => void;
  // New methods for execution configuration (per-canvas runtime settings)
  updateCanvasExecutionConfig: (canvasId: string, config: Partial<CanvasExecutionConfig>) => void;
  getCanvasExecutionConfig: (canvasId: string) => CanvasExecutionConfig | undefined;
}

// Helper function to compare nodes for actual content changes
const nodesHaveActuallyChanged = (oldNodes: Node[], newNodes: Node[]): boolean => {
  if (oldNodes.length !== newNodes.length) return true;
  
  // Create maps for efficient lookup
  const oldNodeMap = new Map(oldNodes.map(node => [node.id, node]));
  const newNodeMap = new Map(newNodes.map(node => [node.id, node]));
  
  // Check if any nodes were added or removed
  if (oldNodeMap.size !== newNodeMap.size) return true;
  
  // Check each node for changes
  for (const newNode of newNodes) {
    const oldNode = oldNodeMap.get(newNode.id);
    if (!oldNode) return true; // New node added
    
    // Compare essential properties
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
const edgesHaveActuallyChanged = (oldEdges: Edge[], newEdges: Edge[]): boolean => {
  if (oldEdges.length !== newEdges.length) return true;
  
  // Create maps for efficient lookup
  const oldEdgeMap = new Map(oldEdges.map(edge => [edge.id, edge]));
  const newEdgeMap = new Map(newEdges.map(edge => [edge.id, edge]));
  
  // Check if any edges were added or removed
  if (oldEdgeMap.size !== newEdgeMap.size) return true;
  
  // Check each edge for changes
  for (const newEdge of newEdges) {
    const oldEdge = oldEdgeMap.get(newEdge.id);
    if (!oldEdge) return true; // New edge added
    
    // Compare essential properties
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

export const useBuilderCanvasStore = create<BuilderCanvasState>()(
  (set, get) => ({
      canvases: [],
      activeCanvasId: null,
      hydrated: false,

      createCanvas: (name?: string, viewMode?: 'crew' | 'flow', options?: { sessionDraft?: boolean }) => {
        const newCanvasId = uuidv4();
        // Get current group ID from localStorage
        const currentGroupId = localStorage.getItem('selectedGroupId') || '';

        // Count canvases for this group only
        const groupCanvases = get().canvases.filter(canvas => canvas.group_id === currentGroupId);
        const canvasName = name || `Canvas ${groupCanvases.length + 1}`;

        // A new canvas opens in the canvas the user is currently looking at, so adding
        // a canvas from the flow canvas stays in flow (not snapped back to crew). Callers
        // that load specific content (e.g. a crew) pass viewMode explicitly to override.
        const resolvedViewMode: 'crew' | 'flow' =
          viewMode ?? (useUILayoutStore.getState().areFlowsVisible ? 'flow' : 'crew');

        const newCanvas: BuilderCanvas = {
          id: newCanvasId,
          name: canvasName,
          nodes: [],
          edges: [],
          flowNodes: [],
          flowEdges: [],
          viewMode: resolvedViewMode,
          isActive: true,
          isDirty: false,
          isSessionDraft: options?.sessionDraft,
          createdAt: new Date(),
          lastModified: new Date(),
          group_id: currentGroupId,
          chatSessionId: newCanvasId
        };

        set(state => ({
          canvases: [
            ...state.canvases.map(canvas => ({ ...canvas, isActive: false })),
            newCanvas
          ],
          activeCanvasId: newCanvasId
        }));

        // Dispatch event to clear the canvas immediately
        setTimeout(() => {
          window.dispatchEvent(new CustomEvent('clearCanvas', {
            detail: { canvasId: newCanvasId }
          }));
        }, 0);

        return newCanvasId;
      },

      nameSessionFromPrompt: (sessionId, prompt) => {
        const title = prompt.trim().replace(/\s+/g, ' ').slice(0, 60);
        if (!title) return;
        set(state => ({ canvases: state.canvases.map(canvas =>
          canvas.chatSessionId === sessionId && canvas.isSessionDraft
            ? { ...canvas, name: title, isSessionDraft: false, lastModified: new Date() }
            : canvas
        ) }));
      },

      closeCanvas: (canvasId: string) => {
        set(state => {
          const canvasIndex = state.canvases.findIndex(canvas => canvas.id === canvasId);
          if (canvasIndex === -1) return state;

          const remainingCanvases = state.canvases.filter(canvas => canvas.id !== canvasId);
          let newActiveCanvasId = state.activeCanvasId;

          // If we're closing the active canvas, select another one
          if (state.activeCanvasId === canvasId) {
            if (remainingCanvases.length > 0) {
              // Select the canvas to the left, or the first canvas if we're closing the first one
              const newActiveIndex = canvasIndex > 0 ? canvasIndex - 1 : 0;
              newActiveCanvasId = remainingCanvases[newActiveIndex]?.id || null;
              
              // Mark the new active canvas
              remainingCanvases.forEach(canvas => {
                canvas.isActive = canvas.id === newActiveCanvasId;
              });
            } else {
              newActiveCanvasId = null;
            }
          }

          return {
            canvases: remainingCanvases,
            activeCanvasId: newActiveCanvasId
          };
        });
      },

      setActiveCanvas: (canvasId: string) => {
        set(state => ({
          canvases: state.canvases.map(canvas => ({
            ...canvas,
            isActive: canvas.id === canvasId
          })),
          activeCanvasId: canvasId
        }));
      },

      updateCanvasName: (canvasId: string, name: string) => {
        set(state => ({
          canvases: state.canvases.map(canvas =>
            canvas.id === canvasId
              ? { ...canvas, name, isSessionDraft: false, lastModified: new Date() }
              : canvas
          )
        }));
      },

      updateCanvasNodes: (canvasId: string, nodes: Node[]) => {
        set(state => {
          const canvas = state.canvases.find(t => t.id === canvasId);
          if (!canvas) return state;
          
          const nodesChanged = nodesHaveActuallyChanged(canvas.nodes, nodes);
          if (!nodesChanged) return state;
          const shouldMarkDirty = nodesChanged && (!canvas.savedCrewId || canvas.isDirty);
          
          // Clear execution status when nodes are meaningfully changed
          const shouldClearExecutionStatus = nodesChanged && canvas.executionStatus;
          
          return {
            canvases: state.canvases.map(t =>
              t.id === canvasId
                ? { 
                    ...t, 
                    nodes: nodes.map(node => ({
                      ...node,
                      position: { ...node.position },
                      data: { ...node.data }
                    })), 
                    isDirty: shouldMarkDirty, 
                    lastModified: new Date(),
                    // Clear execution status if nodes changed
                    executionStatus: shouldClearExecutionStatus ? undefined : t.executionStatus,
                    lastExecutionTime: shouldClearExecutionStatus ? undefined : t.lastExecutionTime
                  }
                : t
            )
          };
        });
      },

      updateCanvasEdges: (canvasId: string, edges: Edge[]) => {
        set(state => {
          const canvas = state.canvases.find(t => t.id === canvasId);
          if (!canvas) return state;

          const edgesChanged = edgesHaveActuallyChanged(canvas.edges, edges);
          if (!edgesChanged) return state;
          const shouldMarkDirty = edgesChanged && (!canvas.savedCrewId || canvas.isDirty);

          // Clear execution status when edges are meaningfully changed
          const shouldClearExecutionStatus = edgesChanged && canvas.executionStatus;

          return {
            canvases: state.canvases.map(t =>
              t.id === canvasId
                ? {
                    ...t,
                    edges: edges.map(edge => ({
                      ...edge,
                      data: edge.data ? { ...edge.data } : undefined
                    })),
                    isDirty: shouldMarkDirty,
                    lastModified: new Date(),
                    // Clear execution status if edges changed
                    executionStatus: shouldClearExecutionStatus ? undefined : t.executionStatus,
                    lastExecutionTime: shouldClearExecutionStatus ? undefined : t.lastExecutionTime
                  }
                : t
            )
          };
        });
      },

      updateCanvasFlowNodes: (canvasId: string, flowNodes: Node[]) => {
        set(state => {
          const canvas = state.canvases.find(t => t.id === canvasId);
          if (!canvas) return state;

          const nodesChanged = nodesHaveActuallyChanged(canvas.flowNodes, flowNodes);

          if (!nodesChanged) return state;

          return {
            canvases: state.canvases.map(t =>
              t.id === canvasId
                ? {
                    ...t,
                    flowNodes: flowNodes.map(node => ({
                      ...node,
                      position: { ...node.position },
                      data: { ...node.data }
                    })),
                    lastModified: new Date()
                  }
                : t
            )
          };
        });
      },

      updateCanvasFlowEdges: (canvasId: string, flowEdges: Edge[]) => {
        set(state => {
          const canvas = state.canvases.find(t => t.id === canvasId);
          if (!canvas) return state;

          const edgesChanged = edgesHaveActuallyChanged(canvas.flowEdges, flowEdges);

          if (!edgesChanged) return state;

          return {
            canvases: state.canvases.map(t =>
              t.id === canvasId
                ? {
                    ...t,
                    flowEdges: flowEdges.map(edge => ({
                      ...edge,
                      data: edge.data ? { ...edge.data } : undefined
                    })),
                    lastModified: new Date()
                  }
                : t
            )
          };
        });
      },

      updateCanvasViewMode: (canvasId: string, viewMode: 'crew' | 'flow') => {
        set(state => ({
          canvases: state.canvases.map(canvas =>
            canvas.id === canvasId
              ? { ...canvas, viewMode }
              : canvas
          )
        }));
      },

      markCanvasDirty: (canvasId: string) => {
        set(state => ({
          canvases: state.canvases.map(canvas =>
            canvas.id === canvasId
              ? { ...canvas, isDirty: true, lastModified: new Date() }
              : canvas
          )
        }));
      },

      markCanvasClean: (canvasId: string) => {
        set(state => ({
          canvases: state.canvases.map(canvas =>
            canvas.id === canvasId
              ? { ...canvas, isDirty: false }
              : canvas
          )
        }));
      },

      getActiveCanvas: () => {
        const state = get();
        return state.canvases.find(canvas => canvas.id === state.activeCanvasId) || null;
      },

      getCanvas: (canvasId: string) => {
        return get().canvases.find(canvas => canvas.id === canvasId) || null;
      },

      updateCanvasCrewInfo: (canvasId: string, crewId: string, crewName: string) => {
        set(state => ({
          canvases: state.canvases.map(canvas =>
            canvas.id === canvasId
              ? { 
                  ...canvas, 
                  savedCrewId: crewId,
                  savedCrewName: crewName,
                  lastSavedAt: new Date(),
                  isDirty: false,
                  name: crewName // Update canvas name to match crew name
                }
              : canvas
          )
        }));
      },

      clearCanvasCrewInfo: (canvasId: string) => {
        set(state => ({
          canvases: state.canvases.map(canvas =>
            canvas.id === canvasId
              ? { 
                  ...canvas, 
                  savedCrewId: undefined,
                  savedCrewName: undefined,
                  lastSavedAt: undefined
                }
              : canvas
          )
        }));
      },

      updateCanvasFlowInfo: (canvasId: string, flowId: string, flowName: string) => {
        set(state => ({
          canvases: state.canvases.map(canvas =>
            canvas.id === canvasId
              ? {
                  ...canvas,
                  savedFlowId: flowId,
                  savedFlowName: flowName,
                  lastSavedAt: new Date(),
                  isDirty: false,
                  name: flowName // Update canvas name to match flow name (mirrors crew save)
                }
              : canvas
          )
        }));
      },

      updateCanvasExecutionStatus: (canvasId: string, status: 'running' | 'completed' | 'failed') => {
        set(state => ({
          canvases: state.canvases.map(canvas =>
            canvas.id === canvasId
              ? { 
                  ...canvas, 
                  executionStatus: status,
                  lastExecutionTime: new Date(),
                  lastModified: new Date()
                }
              : canvas
          )
        }));
      },

      updateCanvasExecutionConfig: (canvasId: string, config: Partial<CanvasExecutionConfig>) => {
        set(state => ({
          canvases: state.canvases.map(canvas =>
            canvas.id === canvasId
              ? {
                  ...canvas,
                  executionConfig: {
                    ...canvas.executionConfig,
                    ...config
                  }
                }
              : canvas
          )
        }));
      },

      getCanvasExecutionConfig: (canvasId: string) => {
        const canvas = get().canvases.find(t => t.id === canvasId);
        return canvas?.executionConfig;
      },

      // Teamspace filtering methods
      getCanvasesForCurrentGroup: () => {
        const currentGroupId = localStorage.getItem('selectedGroupId') || '';
        return get().canvases.filter(canvas => canvas.group_id === currentGroupId);
      },

    })
);
