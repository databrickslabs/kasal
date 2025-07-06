import { create } from 'zustand';
import { APIKeysService } from '../api';
import { ApiKey } from '../types/apiKeys';

interface APIKeysState {
  secrets: ApiKey[];
  loading: boolean;
  error: string | null;
  editDialogOpen: boolean;
  providerToEdit: string | null;
  fetchAPIKeys: () => Promise<void>;
  updateSecrets: (secrets: ApiKey[]) => void;
  clearError: () => void;
  openApiKeyEditor: (provider: string) => void;
  closeApiKeyEditor: () => void;
}

export const useAPIKeysStore = create<APIKeysState>((set) => ({
  secrets: [],
  loading: false,
  error: null,
  editDialogOpen: false,
  providerToEdit: null,

  fetchAPIKeys: async () => {
    set({ loading: true, error: null });
    try {
      const apiKeysService = APIKeysService.getInstance();
      const secrets = await apiKeysService.getAPIKeys();
      set({ secrets, loading: false });
    } catch (error) {
      set({ 
        loading: false, 
        error: error instanceof Error ? error.message : 'An unexpected error occurred'
      });
    }
  },

  updateSecrets: (secrets: ApiKey[]) => 
    set(() => ({ 
      secrets,
      error: null 
    })),

  clearError: () => 
    set(() => ({ error: null })),

  openApiKeyEditor: (provider: string) => 
    set(() => ({ 
      editDialogOpen: true, 
      providerToEdit: provider 
    })),

  closeApiKeyEditor: () => 
    set(() => ({ 
      editDialogOpen: false, 
      providerToEdit: null 
    })),
})); 