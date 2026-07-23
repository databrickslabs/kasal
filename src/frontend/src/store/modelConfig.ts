import { create } from 'zustand';
import { Models, ModelConfig } from '../types/models';
import { models as defaultModels } from '../config/models/models';

interface ModelConfigState {
  models: Models;
  currentEditModel: (ModelConfig & { key: string }) | null;
  editDialogOpen: boolean;
  isNewModel: boolean;
  loading: boolean;
  saving: boolean;
  modelsChanged: boolean;
  searchTerm: string;
  databricksEnabled: boolean;
  error: string | null;
  refreshKey: number;
  activeTab: number;
  setModels: (models: Models) => void;
  setCurrentEditModel: (model: (ModelConfig & { key: string }) | null) => void;
  setEditDialogOpen: (open: boolean) => void;
  setIsNewModel: (isNew: boolean) => void;
  setLoading: (loading: boolean) => void;
  setSaving: (saving: boolean) => void;
  setModelsChanged: (changed: boolean) => void;
  setSearchTerm: (term: string) => void;
  setDatabricksEnabled: (enabled: boolean) => void;
  setError: (error: string | null) => void;
  incrementRefreshKey: () => void;
  setActiveTab: (tab: number) => void;
  resetModelConfig: () => void;
}

export const useModelConfigStore = create<ModelConfigState>((set) => ({
  // Initial state
  models: defaultModels,
  currentEditModel: null,
  editDialogOpen: false,
  isNewModel: false,
  loading: true,
  saving: false,
  modelsChanged: false,
  searchTerm: '',
  databricksEnabled: false,
  error: null,
  refreshKey: 0,
  activeTab: 0,

  // Actions
  setModels: (models) => set({ models }),
  setCurrentEditModel: (model) => set({ currentEditModel: model }),
  setEditDialogOpen: (open) => set({ editDialogOpen: open }),
  setIsNewModel: (isNew) => set({ isNewModel: isNew }),
  setLoading: (loading) => set({ loading }),
  setSaving: (saving) => set({ saving }),
  setModelsChanged: (changed) => set({ modelsChanged: changed }),
  setSearchTerm: (term) => set({ searchTerm: term }),
  setDatabricksEnabled: (enabled) => set({ databricksEnabled: enabled }),
  setError: (error) => set({ error }),
  incrementRefreshKey: () => set((state) => ({ refreshKey: state.refreshKey + 1 })),
  setActiveTab: (tab) => set({ activeTab: tab }),
  resetModelConfig: () => set({
    models: defaultModels,
    currentEditModel: null,
    editDialogOpen: false,
    isNewModel: false,
    loading: true,
    saving: false,
    modelsChanged: false,
    searchTerm: '',
    databricksEnabled: false,
    error: null,
    refreshKey: 0,
    activeTab: 0,
  }),
})); 