import { beforeEach, describe, expect, it, vi } from 'vitest';
import { act, fireEvent, render, renderHook, screen, within } from '@testing-library/react';
import { currentModelsGroup, resetModelsStoreForTests, useModelsStore } from '../../store/models';
import { toModelRecord } from '../../api/config/EnabledModelsService';
import { useBuilderModels, useRefreshModelsWhen } from './useEnabledModels';
import LLMSelectionDialog from '../../features/workflow/agents/components/LLMSelectionDialog';

// Never hit the network: every test publishes the list itself.
vi.mock('../../api/config/EnabledModelsService', async (importOriginal) => ({
  ...(await importOriginal<typeof import('../../api/config/EnabledModelsService')>()),
  fetchEnabledModelRows: vi.fn(async () => []),
}));

const rows = (...keys: string[]) => keys.map((key, i) => ({
  id: i, key, name: key, provider: 'databricks', temperature: null, context_window: null,
  max_output_tokens: null, extended_thinking: false, enabled: true, created_at: '', updated_at: '',
}));

/** What a successful load leaves in the store (fresh, so nothing refetches). */
function publish(...keys: string[]) {
  const list = rows(...keys);
  act(() => useModelsStore.setState({
    models: list, modelRecord: toModelRecord(list), defaultModel: 'default-model',
    loadedForGroup: currentModelsGroup(), checkedGroup: currentModelsGroup(), checkedAt: Date.now(),
    error: null, loading: false,
  }));
}

describe('useBuilderModels', () => {
  beforeEach(() => resetModelsStoreForTests());

  it('re-renders with the new list without remounting', () => {
    const set = vi.fn();
    const { result } = renderHook(() => useBuilderModels('a', set));
    expect(result.current.loadingModels).toBe(true);
    publish('a', 'b');
    expect(Object.keys(result.current.models)).toEqual(['a', 'b']);
    expect(result.current.loadingModels).toBe(false);
    publish('a', 'b', 'c');
    expect(Object.keys(result.current.models)).toEqual(['a', 'b', 'c']);
    expect(set).not.toHaveBeenCalled();
  });

  it('falls back when the selected model is disabled: default, else first', () => {
    const set = vi.fn();
    const { rerender } = renderHook(({ model }) => useBuilderModels(model, set), {
      initialProps: { model: 'b' },
    });
    publish('a', 'b', 'default-model');
    expect(set).not.toHaveBeenCalled();
    publish('a', 'default-model');
    expect(set).toHaveBeenLastCalledWith('default-model');
    rerender({ model: 'gone' });
    publish('a');
    expect(set).toHaveBeenLastCalledWith('a');
  });

  it('changes nothing before the first list arrives', () => {
    const set = vi.fn();
    renderHook(() => useBuilderModels('whatever', set));
    expect(set).not.toHaveBeenCalled();
  });
});

describe('useRefreshModelsWhen', () => {
  beforeEach(() => resetModelsStoreForTests());

  it('asks for a throttled refresh each time the menu opens', () => {
    const ensureFresh = vi.fn(async () => undefined);
    const original = useModelsStore.getState().ensureFresh;
    useModelsStore.setState({ ensureFresh });
    try {
      const { rerender } = renderHook(({ open }) => useRefreshModelsWhen(open), {
        initialProps: { open: false },
      });
      expect(ensureFresh).not.toHaveBeenCalled();
      rerender({ open: true });
      expect(ensureFresh).toHaveBeenCalledTimes(1);
      rerender({ open: false });
      rerender({ open: true });
      expect(ensureFresh).toHaveBeenCalledTimes(2);
    } finally {
      useModelsStore.setState({ ensureFresh: original });
    }
  });
});

describe('LLMSelectionDialog', () => {
  beforeEach(() => resetModelsStoreForTests());

  it('lists the live enabled models and follows changes while open', () => {
    publish('a', 'b');
    render(<LLMSelectionDialog open onClose={() => undefined} onSelectLLM={() => undefined} currentLLM="a" />);
    fireEvent.mouseDown(screen.getByRole('combobox'));
    let listbox = screen.getByRole('listbox');
    expect(within(listbox).getAllByRole('option').map((o) => o.textContent)).toEqual(
      ['adatabricks', 'bdatabricks'],
    );
    publish('a', 'c');
    listbox = screen.getByRole('listbox');
    expect(within(listbox).getAllByRole('option').map((o) => o.textContent)).toEqual(
      ['adatabricks', 'cdatabricks'],
    );
  });
});
