import { useCallback } from 'react';
import { Models, ModelConfig } from '../../types/models';
import { useModelConfigStore } from '../../store/modelConfig';

export const useModelConfig = () => {
  const {
    models,
    currentEditModel,
    editDialogOpen,
    isNewModel,
    loading,
    saving,
    modelsChanged,
    searchTerm,
    databricksEnabled,
    error,
    refreshKey,
    activeTab,
    setModels,
    setCurrentEditModel,
    setEditDialogOpen,
    setIsNewModel,
    setLoading,
    setSaving,
    setModelsChanged,
    setSearchTerm,
    setDatabricksEnabled,
    setError,
    incrementRefreshKey,
    setActiveTab,
    resetModelConfig,
  } = useModelConfigStore();

  const handleSetModels = useCallback((models: Models) => {
    setModels(models);
  }, [setModels]);

  const handleSetCurrentEditModel = useCallback((model: (ModelConfig & { key: string }) | null) => {
    setCurrentEditModel(model);
  }, [setCurrentEditModel]);

  const handleSetEditDialogOpen = useCallback((open: boolean) => {
    setEditDialogOpen(open);
  }, [setEditDialogOpen]);

  const handleSetIsNewModel = useCallback((isNew: boolean) => {
    setIsNewModel(isNew);
  }, [setIsNewModel]);

  const handleSetLoading = useCallback((loading: boolean) => {
    setLoading(loading);
  }, [setLoading]);

  const handleSetSaving = useCallback((saving: boolean) => {
    setSaving(saving);
  }, [setSaving]);

  const handleSetModelsChanged = useCallback((changed: boolean) => {
    setModelsChanged(changed);
  }, [setModelsChanged]);

  const handleSetSearchTerm = useCallback((term: string) => {
    setSearchTerm(term);
  }, [setSearchTerm]);

  const handleSetDatabricksEnabled = useCallback((enabled: boolean) => {
    setDatabricksEnabled(enabled);
  }, [setDatabricksEnabled]);

  const handleSetError = useCallback((error: string | null) => {
    setError(error);
  }, [setError]);

  const handleIncrementRefreshKey = useCallback(() => {
    incrementRefreshKey();
  }, [incrementRefreshKey]);

  const handleSetActiveTab = useCallback((tab: number) => {
    setActiveTab(tab);
  }, [setActiveTab]);

  const handleResetModelConfig = useCallback(() => {
    resetModelConfig();
  }, [resetModelConfig]);

  return {
    // State
    models,
    currentEditModel,
    editDialogOpen,
    isNewModel,
    loading,
    saving,
    modelsChanged,
    searchTerm,
    databricksEnabled,
    error,
    refreshKey,
    activeTab,

    // Actions
    setModels: handleSetModels,
    setCurrentEditModel: handleSetCurrentEditModel,
    setEditDialogOpen: handleSetEditDialogOpen,
    setIsNewModel: handleSetIsNewModel,
    setLoading: handleSetLoading,
    setSaving: handleSetSaving,
    setModelsChanged: handleSetModelsChanged,
    setSearchTerm: handleSetSearchTerm,
    setDatabricksEnabled: handleSetDatabricksEnabled,
    setError: handleSetError,
    incrementRefreshKey: handleIncrementRefreshKey,
    setActiveTab: handleSetActiveTab,
    resetModelConfig: handleResetModelConfig,
  };
}; 