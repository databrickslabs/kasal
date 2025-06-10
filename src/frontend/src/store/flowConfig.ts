import { create } from 'zustand';
import { persist } from 'zustand/middleware';

interface FlowConfigState {
  // CrewAI Engine settings
  crewAIFlowEnabled: boolean;
  
  // Actions
  setCrewAIFlowEnabled: (enabled: boolean) => void;
  
  // Getters
  isFlowEnabled: () => boolean;
}

export const useFlowConfigStore = create<FlowConfigState>()(
  persist(
    (set, get) => ({
      // Default state
      crewAIFlowEnabled: false, // Default to disabled
      
      // Actions
      setCrewAIFlowEnabled: (enabled: boolean) => {
        set({ crewAIFlowEnabled: enabled });
      },
      
      // Getters
      isFlowEnabled: () => {
        return get().crewAIFlowEnabled;
      }
    }),
    {
      name: 'flow-config-storage',
      partialize: (state) => ({
        crewAIFlowEnabled: state.crewAIFlowEnabled
      })
    }
  )
); 