import { useEffect } from 'react';
import { useShallow } from 'zustand/react/shallow';
import { useModelsStore } from '../../store/models';
import { resolveBuilderModel } from '../../utils/modelFallback';

/**
 * The live enabled-model list for a model selector, from the shared models
 * store. Subscribing loads it when needed; it re-renders when an admin changes
 * the models (here or in another tab) without the selector remounting.
 *
 * `loading` is true only until the first list arrives: later refreshes swap the
 * list in place rather than blanking the selector.
 */
export function useEnabledModels() {
  return useModelsStore(useShallow((s) => ({
    models: s.modelRecord,
    rows: s.models,
    defaultModel: s.defaultModel,
    loaded: s.loadedForGroup !== null,
    loading: s.loadedForGroup === null && s.error === null,
    error: s.error,
  })));
}

/** Refresh the list when a model menu or dialog opens (throttled in the store). */
export function useRefreshModelsWhen(open: boolean): void {
  useEffect(() => {
    if (open) void useModelsStore.getState().ensureFresh();
  }, [open]);
}

/**
 * The builder assistant's model selector: the live list, plus keeping the
 * chosen model valid. A model that stops being enabled falls back to the
 * server default, else the first enabled model (`resolveBuilderModel`); a model
 * that is still enabled is never touched. Saved agents keep their own `llm` —
 * this only governs the assistant's generation model.
 */
export function useBuilderModels(selectedModel: string, setSelectedModel: (model: string) => void) {
  const { models, rows, defaultModel, loaded, loading } = useEnabledModels();
  useEffect(() => {
    if (!loaded) return;
    const next = resolveBuilderModel(selectedModel, rows, defaultModel);
    if (next && next !== selectedModel) setSelectedModel(next);
  }, [loaded, rows, defaultModel, selectedModel, setSelectedModel]);
  return { models, loadingModels: loading };
}
