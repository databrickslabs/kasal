import { create } from 'zustand';
import { persist } from 'zustand/middleware';
import { Node, Edge } from 'reactflow';
import { v4 as uuidv4 } from 'uuid';

export interface TabData {
  id: string;
  name: string;
  nodes: Node[];
  edges: Edge[];
  isActive: boolean;
  isDirty: boolean; // Track if tab has unsaved changes
  createdAt: Date;
  lastModified: Date;
  // Crew metadata
  savedCrewId?: string; // ID of the saved crew
  savedCrewName?: string; // Name of the saved crew
  lastSavedAt?: Date; // When the crew was last saved
  // Chat session
  chatSessionId?: string; // ID of the chat session for this tab
  // Execution status
  executionStatus?: 'running' | 'completed' | 'failed';
  lastExecutionTime?: Date;
}

interface TabManagerState {
  tabs: TabData[];
  activeTabId: string | null;
  
  // Actions
  createTab: (name?: string) => string;
  closeTab: (tabId: string) => void;
  setActiveTab: (tabId: string) => void;
  updateTabName: (tabId: string, name: string) => void;
  updateTabNodes: (tabId: string, nodes: Node[]) => void;
  updateTabEdges: (tabId: string, edges: Edge[]) => void;
  markTabDirty: (tabId: string) => void;
  markTabClean: (tabId: string) => void;
  duplicateTab: (tabId: string) => string;
  getActiveTab: () => TabData | null;
  getTab: (tabId: string) => TabData | null;
  clearAllTabs: () => void;
  // New methods for crew management
  updateTabCrewInfo: (tabId: string, crewId: string, crewName: string) => void;
  clearTabCrewInfo: (tabId: string) => void;
  isTabSaved: (tabId: string) => boolean;
  // New methods for execution status
  updateTabExecutionStatus: (tabId: string, status: 'running' | 'completed' | 'failed') => void;
  clearTabExecutionStatus: (tabId: string) => void;
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

export const useTabManagerStore = create<TabManagerState>()(
  persist(
    (set, get) => ({
      tabs: [],
      activeTabId: null,

      createTab: (name?: string) => {
        const newTabId = uuidv4();
        const tabName = name || `Canvas ${get().tabs.length + 1}`;
        
        const newTab: TabData = {
          id: newTabId,
          name: tabName,
          nodes: [],
          edges: [],
          isActive: true,
          isDirty: false,
          createdAt: new Date(),
          lastModified: new Date(),
          chatSessionId: uuidv4() // Generate unique chat session for each tab
        };

        set(state => ({
          tabs: [
            ...state.tabs.map(tab => ({ ...tab, isActive: false })),
            newTab
          ],
          activeTabId: newTabId
        }));

        // Dispatch event to clear the canvas immediately
        setTimeout(() => {
          window.dispatchEvent(new CustomEvent('clearCanvas', { 
            detail: { tabId: newTabId }
          }));
        }, 0);

        return newTabId;
      },

      closeTab: (tabId: string) => {
        set(state => {
          const tabIndex = state.tabs.findIndex(tab => tab.id === tabId);
          if (tabIndex === -1) return state;

          const newTabs = state.tabs.filter(tab => tab.id !== tabId);
          let newActiveTabId = state.activeTabId;

          // If we're closing the active tab, select another one
          if (state.activeTabId === tabId) {
            if (newTabs.length > 0) {
              // Select the tab to the left, or the first tab if we're closing the first one
              const newActiveIndex = tabIndex > 0 ? tabIndex - 1 : 0;
              newActiveTabId = newTabs[newActiveIndex]?.id || null;
              
              // Mark the new active tab
              newTabs.forEach(tab => {
                tab.isActive = tab.id === newActiveTabId;
              });
            } else {
              newActiveTabId = null;
            }
          }

          return {
            tabs: newTabs,
            activeTabId: newActiveTabId
          };
        });
      },

      setActiveTab: (tabId: string) => {
        set(state => ({
          tabs: state.tabs.map(tab => ({
            ...tab,
            isActive: tab.id === tabId
          })),
          activeTabId: tabId
        }));
      },

      updateTabName: (tabId: string, name: string) => {
        set(state => ({
          tabs: state.tabs.map(tab =>
            tab.id === tabId
              ? { ...tab, name, lastModified: new Date() }
              : tab
          )
        }));
      },

      updateTabNodes: (tabId: string, nodes: Node[]) => {
        set(state => {
          const tab = state.tabs.find(t => t.id === tabId);
          if (!tab) return state;
          
          const nodesChanged = nodesHaveActuallyChanged(tab.nodes, nodes);
          const shouldMarkDirty = nodesChanged && (!tab.savedCrewId || tab.isDirty);
          
          // Clear execution status when nodes are meaningfully changed
          const shouldClearExecutionStatus = nodesChanged && tab.executionStatus;
          
          return {
            tabs: state.tabs.map(t =>
              t.id === tabId
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

      updateTabEdges: (tabId: string, edges: Edge[]) => {
        set(state => {
          const tab = state.tabs.find(t => t.id === tabId);
          if (!tab) return state;
          
          const edgesChanged = edgesHaveActuallyChanged(tab.edges, edges);
          const shouldMarkDirty = edgesChanged && (!tab.savedCrewId || tab.isDirty);
          
          // Clear execution status when edges are meaningfully changed
          const shouldClearExecutionStatus = edgesChanged && tab.executionStatus;
          
          return {
            tabs: state.tabs.map(t =>
              t.id === tabId
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

      markTabDirty: (tabId: string) => {
        set(state => ({
          tabs: state.tabs.map(tab =>
            tab.id === tabId
              ? { ...tab, isDirty: true, lastModified: new Date() }
              : tab
          )
        }));
      },

      markTabClean: (tabId: string) => {
        set(state => ({
          tabs: state.tabs.map(tab =>
            tab.id === tabId
              ? { ...tab, isDirty: false }
              : tab
          )
        }));
      },

      duplicateTab: (tabId: string) => {
        const sourceTab = get().getTab(tabId);
        if (!sourceTab) return '';

        const newTabId = uuidv4();
        const newTab: TabData = {
          id: newTabId,
          name: `${sourceTab.name} (Copy)`,
          nodes: sourceTab.nodes.map(node => ({
            ...node,
            id: uuidv4(), // Generate new IDs for duplicated nodes
            selected: false
          })),
          edges: sourceTab.edges.map(edge => ({
            ...edge,
            id: uuidv4(), // Generate new IDs for duplicated edges
            selected: false
          })),
          isActive: true,
          isDirty: true,
          createdAt: new Date(),
          lastModified: new Date(),
          // Don't copy crew info for duplicated tabs
          savedCrewId: undefined,
          savedCrewName: undefined,
          lastSavedAt: undefined,
          chatSessionId: uuidv4() // New chat session for duplicated tab
        };

        // Update edge references to point to new node IDs
        const nodeIdMap = new Map();
        sourceTab.nodes.forEach((oldNode, index) => {
          nodeIdMap.set(oldNode.id, newTab.nodes[index].id);
        });

        newTab.edges = newTab.edges.map(edge => ({
          ...edge,
          source: nodeIdMap.get(edge.source) || edge.source,
          target: nodeIdMap.get(edge.target) || edge.target
        }));

        set(state => ({
          tabs: [
            ...state.tabs.map(tab => ({ ...tab, isActive: false })),
            newTab
          ],
          activeTabId: newTabId
        }));

        return newTabId;
      },

      getActiveTab: () => {
        const state = get();
        return state.tabs.find(tab => tab.id === state.activeTabId) || null;
      },

      getTab: (tabId: string) => {
        return get().tabs.find(tab => tab.id === tabId) || null;
      },

      clearAllTabs: () => {
        set({
          tabs: [],
          activeTabId: null
        });
      },

      // New methods for crew management
      updateTabCrewInfo: (tabId: string, crewId: string, crewName: string) => {
        set(state => ({
          tabs: state.tabs.map(tab =>
            tab.id === tabId
              ? { 
                  ...tab, 
                  savedCrewId: crewId,
                  savedCrewName: crewName,
                  lastSavedAt: new Date(),
                  isDirty: false,
                  name: crewName // Update tab name to match crew name
                }
              : tab
          )
        }));
      },

      clearTabCrewInfo: (tabId: string) => {
        set(state => ({
          tabs: state.tabs.map(tab =>
            tab.id === tabId
              ? { 
                  ...tab, 
                  savedCrewId: undefined,
                  savedCrewName: undefined,
                  lastSavedAt: undefined
                }
              : tab
          )
        }));
      },

      isTabSaved: (tabId: string) => {
        const tab = get().getTab(tabId);
        return tab ? !tab.isDirty && !!tab.savedCrewId : false;
      },

      // New methods for execution status
      updateTabExecutionStatus: (tabId: string, status: 'running' | 'completed' | 'failed') => {
        set(state => ({
          tabs: state.tabs.map(tab =>
            tab.id === tabId
              ? { 
                  ...tab, 
                  executionStatus: status,
                  lastExecutionTime: new Date()
                }
              : tab
          )
        }));
      },

      clearTabExecutionStatus: (tabId: string) => {
        set(state => ({
          tabs: state.tabs.map(tab =>
            tab.id === tabId
              ? { 
                  ...tab, 
                  executionStatus: undefined,
                  lastExecutionTime: undefined
                }
              : tab
          )
        }));
      }
    }),
    {
      name: 'tab-manager-storage',
      partialize: (state) => ({
        tabs: state.tabs.map(tab => ({
          ...tab,
          createdAt: tab.createdAt.toISOString(),
          lastModified: tab.lastModified.toISOString(),
          lastSavedAt: tab.lastSavedAt?.toISOString(),
          lastExecutionTime: tab.lastExecutionTime?.toISOString(),
          // Don't persist 'running' status, only completed/failed
          executionStatus: tab.executionStatus === 'running' ? undefined : tab.executionStatus
        })),
        activeTabId: state.activeTabId
      }),
      onRehydrateStorage: () => (state) => {
        if (state) {
          // Deserialize dates and ensure proper object structure
          state.tabs = state.tabs.map(tab => ({
            ...tab,
            createdAt: new Date(tab.createdAt),
            lastModified: new Date(tab.lastModified),
            lastSavedAt: tab.lastSavedAt ? new Date(tab.lastSavedAt) : undefined,
            lastExecutionTime: tab.lastExecutionTime ? new Date(tab.lastExecutionTime) : undefined
          }));
        }
      }
    }
  )
); 