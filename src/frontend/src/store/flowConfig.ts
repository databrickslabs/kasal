import { create } from 'zustand';
import { persist } from 'zustand/middleware';

interface FlowConfigState {
  // Kasal Engine settings
  kasalFlowEnabled: boolean;

  // Actions
  setKasalFlowEnabled: (enabled: boolean) => void;

  // Getters
  isFlowEnabled: () => boolean;
}

export const useFlowConfigStore = create<FlowConfigState>()(
  persist(
    (set, get) => ({
      // Default state - Flow is enabled by default
      kasalFlowEnabled: true,

      // Actions
      setKasalFlowEnabled: (enabled: boolean) => {
        set({ kasalFlowEnabled: enabled });
      },

      // Getters
      isFlowEnabled: () => {
        return get().kasalFlowEnabled;
      }
    }),
    {
      name: 'flow-config-storage',
      partialize: (state) => ({
        kasalFlowEnabled: state.kasalFlowEnabled
      })
    }
  )
); 