import { create } from 'zustand';
import { persist } from 'zustand/middleware';

interface FlowConfigState {
  // CrewAI Engine settings
  crewAIFlowEnabled: boolean;

  // Visual editor toggle
  useVisualFlowEditor: boolean;

  // Actions
  setCrewAIFlowEnabled: (enabled: boolean) => void;
  setUseVisualFlowEditor: (enabled: boolean) => void;

  // Getters
  isFlowEnabled: () => boolean;
  isVisualEditorEnabled: () => boolean;
}

export const useFlowConfigStore = create<FlowConfigState>()(
  persist(
    (set, get) => ({
      // Default state
      crewAIFlowEnabled: false, // Default to disabled
      useVisualFlowEditor: true, // NEW: Default to visual editor

      // Actions
      setCrewAIFlowEnabled: (enabled: boolean) => {
        set({ crewAIFlowEnabled: enabled });
      },

      setUseVisualFlowEditor: (enabled: boolean) => {
        set({ useVisualFlowEditor: enabled });
      },

      // Getters
      isFlowEnabled: () => {
        return get().crewAIFlowEnabled;
      },

      isVisualEditorEnabled: () => {
        return get().useVisualFlowEditor;
      }
    }),
    {
      name: 'flow-config-storage',
      partialize: (state) => ({
        crewAIFlowEnabled: state.crewAIFlowEnabled,
        useVisualFlowEditor: state.useVisualFlowEditor
      })
    }
  )
); 