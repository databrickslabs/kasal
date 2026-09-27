/**
 * Every model mutation on the Models page refreshes the shared models store
 * (and, through it, open chats, builders and other tabs) — workspace and
 * system scope alike.
 */
import { fireEvent, render, screen, waitFor, within } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import ModelConfiguration from './ModelConfiguration';
import { useModelConfigStore } from '../../../../store/modelConfig';

const { notifyModelsChanged, service } = vi.hoisted(() => {
  const models = {
    'model-a': { name: 'Alpha', provider: 'databricks', enabled: true },
    'model-b': { name: 'Beta', provider: 'databricks', enabled: false },
  };
  // Workspace view lists only models enabled system-wide.
  const globalModels = {
    'model-a': { name: 'Alpha', provider: 'databricks', enabled: true },
    'model-b': { name: 'Beta', provider: 'databricks', enabled: true },
  };
  return {
    notifyModelsChanged: vi.fn(async () => undefined),
    service: {
      getModels: vi.fn(async () => models),
      getGlobalModels: vi.fn(async () => globalModels),
      enableModel: vi.fn(async () => models),
      enableGlobalModel: vi.fn(async () => models),
      enableAllModels: vi.fn(async () => models),
      disableAllModels: vi.fn(async () => models),
      deleteModel: vi.fn(async () => models),
      saveModels: vi.fn(async () => models),
      createModel: vi.fn(async () => models),
    },
  };
});

vi.mock('../../../../store/models', () => ({ notifyModelsChanged }));
vi.mock('../../../../api/config/ModelService', () => ({
  ModelService: { getInstance: () => service },
}));
vi.mock('react-hot-toast', () => ({ default: { success: vi.fn(), error: vi.fn() } }));
vi.mock('react-i18next', () => ({
  useTranslation: () => ({
    t: (key: string, options?: { defaultValue?: string }) => options?.defaultValue ?? key,
  }),
}));

const rowFor = async (key: string) => (await screen.findByText(key)).closest('tr') as HTMLElement;

describe('ModelConfiguration → shared models store', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    useModelConfigStore.getState().resetModelConfig();
  });

  it('refreshes after a workspace enable/disable toggle', async () => {
    render(<ModelConfiguration mode="workspace" />);
    fireEvent.click(within(await rowFor('model-b')).getByRole('checkbox'));
    await waitFor(() => expect(service.enableModel).toHaveBeenCalledWith('model-b', true));
    await waitFor(() => expect(notifyModelsChanged).toHaveBeenCalledTimes(1));
  });

  it('refreshes after a system toggle', async () => {
    render(<ModelConfiguration mode="system" />);
    fireEvent.click(within(await rowFor('model-a')).getByRole('checkbox'));
    await waitFor(() => expect(service.enableGlobalModel).toHaveBeenCalledWith('model-a', false));
    await waitFor(() => expect(notifyModelsChanged).toHaveBeenCalledTimes(1));
  });

  it('refreshes after enable all and disable all', async () => {
    render(<ModelConfiguration mode="system" />);
    await rowFor('model-a');
    fireEvent.click(screen.getByRole('button', { name: 'configuration.models.enableAll' }));
    await waitFor(() => expect(notifyModelsChanged).toHaveBeenCalledTimes(1));
    fireEvent.click(screen.getByRole('button', { name: 'configuration.models.disableAll' }));
    await waitFor(() => expect(notifyModelsChanged).toHaveBeenCalledTimes(2));
  });

  it('refreshes after editing a model', async () => {
    render(<ModelConfiguration mode="system" />);
    const row = await rowFor('model-a');
    fireEvent.click(within(row).getAllByRole('button')[0]);
    fireEvent.click(await screen.findByRole('button', { name: 'Save' }));
    await waitFor(() => expect(service.saveModels).toHaveBeenCalled());
    await waitFor(() => expect(notifyModelsChanged).toHaveBeenCalledTimes(1));
  });

  it('refreshes after deleting a model', async () => {
    render(<ModelConfiguration mode="system" />);
    const row = await rowFor('model-b');
    fireEvent.click(within(row).getAllByRole('button')[1]);
    fireEvent.click(await screen.findByRole('button', { name: 'Delete' }));
    await waitFor(() => expect(service.deleteModel).toHaveBeenCalledWith('model-b'));
    await waitFor(() => expect(notifyModelsChanged).toHaveBeenCalled());
  });

  it('does not refresh when a mutation fails', async () => {
    service.enableModel.mockRejectedValueOnce(new Error('denied'));
    render(<ModelConfiguration mode="workspace" />);
    fireEvent.click(within(await rowFor('model-b')).getByRole('checkbox'));
    await waitFor(() => expect(service.enableModel).toHaveBeenCalled());
    expect(notifyModelsChanged).not.toHaveBeenCalled();
  });
});
