import { create } from 'zustand';
import { NodeActionsContextType } from '../types/crew';

interface NodeActionsState extends NodeActionsContextType {
  // State setters
  setHandleAgentEdit: (handler: (agentId: string) => void) => void;
  setHandleTaskEdit: (handler: (taskId: string) => void) => void;
  setHandleDeleteNode: (handler: (nodeId: string) => void) => void;
}

export const useNodeActionsStore = create<NodeActionsState>((set) => ({
  // Default implementations
  handleAgentEdit: (agentId: string) => {
    console.log('Agent edit not implemented', agentId);
  },
  handleTaskEdit: (taskId: string) => {
    console.log('Task edit not implemented', taskId);
  },
  handleDeleteNode: (nodeId: string) => {
    console.log('Delete node not implemented', nodeId);
  },
  
  // Setters for handlers
  setHandleAgentEdit: (handler) => set({ handleAgentEdit: handler }),
  setHandleTaskEdit: (handler) => set({ handleTaskEdit: handler }),
  setHandleDeleteNode: (handler) => set({ handleDeleteNode: handler })
})); 