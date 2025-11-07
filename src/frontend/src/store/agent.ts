import { create } from 'zustand';
import { AgentService, Agent } from '../api/AgentService';

interface AgentCache {
  [agentId: string]: {
    data: Agent;
    lastFetched: number;
    isLoading: boolean;
  };
}

interface AgentState {
  // Cache for agents by ID
  agentCache: AgentCache;

  // Loading states
  isLoadingList: boolean;

  // Actions
  getAgent: (agentId: string, forceRefresh?: boolean) => Promise<Agent | null>;
  updateAgent: (agentId: string, updates: Partial<Agent>) => void;
  removeAgent: (agentId: string) => void;
  clearCache: () => void;

  // Internal actions (don't call directly)
  _setAgentLoading: (agentId: string, loading: boolean) => void;
  _setAgentData: (agentId: string, agent: Agent) => void;
}

// Cache duration: 5 minutes
const CACHE_DURATION_MS = 5 * 60 * 1000;

const initialState = {
  agentCache: {},
  isLoadingList: false,
};

export const useAgentStore = create<AgentState>()((set, get) => ({
  ...initialState,

  getAgent: async (agentId: string, forceRefresh = false) => {
    const state = get();
    const cachedEntry = state.agentCache[agentId];

    // Check if we have valid cached data
    const now = Date.now();
    const isCacheValid = cachedEntry &&
                        cachedEntry.data &&
                        !forceRefresh &&
                        (now - cachedEntry.lastFetched) < CACHE_DURATION_MS;

    if (isCacheValid) {
      console.log(`[AgentStore] Using cached data for agent ${agentId}`);
      return cachedEntry.data;
    }

    // Check if we're already loading this agent
    if (cachedEntry?.isLoading) {
      console.log(`[AgentStore] Agent ${agentId} is already being fetched`);
      return cachedEntry.data || null;
    }

    // Set loading state
    get()._setAgentLoading(agentId, true);

    try {
      console.log(`[AgentStore] Fetching agent ${agentId} from API`);
      const agent = await AgentService.getAgent(agentId);

      if (agent) {
        get()._setAgentData(agentId, agent);
        return agent;
      }

      // If agent doesn't exist, remove from cache
      get().removeAgent(agentId);
      return null;
    } catch (error) {
      console.error(`[AgentStore] Failed to fetch agent ${agentId}:`, error);
      get()._setAgentLoading(agentId, false);
      return cachedEntry?.data || null;
    }
  },

  updateAgent: (agentId: string, updates: Partial<Agent>) => {
    set((state) => {
      const cachedEntry = state.agentCache[agentId];
      if (!cachedEntry) {
        console.warn(`[AgentStore] Cannot update non-existent agent ${agentId}`);
        return state;
      }

      return {
        agentCache: {
          ...state.agentCache,
          [agentId]: {
            ...cachedEntry,
            data: { ...cachedEntry.data, ...updates },
            lastFetched: Date.now(), // Update timestamp to keep cache fresh
          },
        },
      };
    });
  },

  removeAgent: (agentId: string) => {
    set((state) => {
      const { [agentId]: removed, ...remainingCache } = state.agentCache;
      return {
        agentCache: remainingCache,
      };
    });
  },

  clearCache: () => {
    console.log('[AgentStore] Clearing all cached agents');
    set({ agentCache: {} });
  },

  // Internal actions
  _setAgentLoading: (agentId: string, loading: boolean) => {
    set((state) => ({
      agentCache: {
        ...state.agentCache,
        [agentId]: {
          ...state.agentCache[agentId],
          isLoading: loading,
          data: state.agentCache[agentId]?.data || {} as Agent, // Preserve existing data
        },
      },
    }));
  },

  _setAgentData: (agentId: string, agent: Agent) => {
    set((state) => ({
      agentCache: {
        ...state.agentCache,
        [agentId]: {
          data: agent,
          lastFetched: Date.now(),
          isLoading: false,
        },
      },
    }));
  },
}));