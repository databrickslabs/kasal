import { fireEvent, render, screen, waitFor, within } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import ChatInputPlusMenu from './ChatInputPlusMenu';
import { useCrewExecutionStore } from '../../../../store/crewExecution';

vi.mock('../../../../hooks/global/useReasoningSupport', () => ({
  useReasoningSupport: () => ({ supported: true, agentModelNames: ['model-a'] }),
  reasoningUnsupportedReason: () => 'Reasoning is unavailable for this model.',
}));

vi.mock('../../../../shared/api/client', () => ({ apiClient: { get: vi.fn().mockResolvedValue({ data: { effort_profiles: {
  low: { run_max_seconds: 180, max_iter: 8 }, medium: { run_max_seconds: 600, max_iter: 15 }, high: { run_max_seconds: 1200, max_iter: 30 },
} } }) } }));

const models = { 'model-a': { name: 'Model A', supports_reasoning_effort: true }, 'model-b': { name: 'Model B' } };
beforeEach(() => useCrewExecutionStore.setState({
  processType: 'sequential', managerLLM: '', reasoningEnabled: false,
  reasoningConfig: { reasoning_effort: 'low' },
}));

function setup(props = {}) {
  const onAddFiles = vi.fn();
  const onModelChange = vi.fn();
  render(<ChatInputPlusMenu models={models} selectedModel="model-a" onModelChange={onModelChange} onAddFiles={onAddFiles} {...props} />);
  fireEvent.click(screen.getByRole('button', { name: 'Files and run settings' }));
  return { onAddFiles, onModelChange };
}

describe('Builder composer settings', () => {
  it('keeps process choices inside their own panel and shows the selected value at the root', async () => {
    setup();
    expect(screen.queryByRole('menuitemradio')).not.toBeInTheDocument();
    fireEvent.click(screen.getByRole('menuitem', { name: 'Process type' }));
    expect(screen.getAllByRole('menuitemradio')).toHaveLength(3);
    expect(screen.getByRole('menuitemradio', { name: /Sequential/ })).toHaveAttribute('aria-checked', 'true');
    fireEvent.click(screen.getByRole('menuitemradio', { name: /Parallel/ }));
    expect(useCrewExecutionStore.getState().processType).toBe('parallel');
    expect(screen.getByRole('menuitem', { name: 'Process type' })).toHaveTextContent('Parallel');
    expect(screen.queryByRole('menuitemradio')).not.toBeInTheDocument();
    await waitFor(() => expect(screen.getByRole('menuitem', { name: 'Model' })).toHaveFocus());
  });

  it('offers a manager model for a hierarchical process without a second popup', () => {
    setup();
    fireEvent.click(screen.getByRole('menuitem', { name: 'Process type' }));
    fireEvent.click(screen.getByRole('menuitemradio', { name: /Hierarchical/ }));
    fireEvent.click(screen.getByRole('menuitem', { name: 'Manager model' }));
    expect(screen.getAllByRole('menu')).toHaveLength(1);
    fireEvent.click(screen.getByRole('menuitemradio', { name: 'Model B' }));
    expect(useCrewExecutionStore.getState().managerLLM).toBe('model-b');
    expect(screen.getByRole('menuitem', { name: 'Manager model' })).toHaveTextContent('Model B');
  });

  it('applies the shared effort profile and shows the selection at the root', async () => {
    setup();
    fireEvent.click(screen.getByRole('menuitem', { name: 'Effort' }));
    fireEvent.click(await screen.findByRole('menuitemradio', { name: /High/ }));
    expect(useCrewExecutionStore.getState().reasoningConfig.execution_effort).toEqual({ tier: 'high' });
    expect(screen.getByRole('menuitem', { name: 'Effort' })).toHaveTextContent('High');
  });

  it('inherits saved agents by default and can clear a run override', async () => {
    setup();
    expect(screen.getByRole('menuitem', { name: 'Effort' })).toHaveTextContent('Use agent settings');
    fireEvent.click(screen.getByRole('menuitem', { name: 'Effort' }));
    fireEvent.click(await screen.findByRole('menuitemradio', { name: /High/ }));
    fireEvent.click(screen.getByRole('menuitem', { name: 'Effort' }));
    fireEvent.click(screen.getByRole('menuitemradio', { name: /Use agent settings/ }));
    expect(useCrewExecutionStore.getState().reasoningConfig.execution_effort).toBeUndefined();
    expect(screen.getByRole('menuitem', { name: 'Effort' })).toHaveTextContent('Use agent settings');
  });

  it('supports keyboard navigation into and back out of a panel', () => {
    setup();
    fireEvent.keyDown(screen.getByRole('menuitem', { name: 'Process type' }), { key: 'ArrowRight' });
    expect(screen.getByRole('menu', { name: 'Process type' })).toBeVisible();
    fireEvent.keyDown(screen.getByRole('menuitem', { name: 'Back' }), { key: 'ArrowLeft' });
    expect(screen.getByRole('menuitem', { name: 'Process type' })).toBeVisible();
  });

  it('keeps the model picker and file attachment action working', async () => {
    const { onModelChange, onAddFiles } = setup();
    fireEvent.click(screen.getByRole('menuitem', { name: 'Model' }));
    fireEvent.click(screen.getByRole('menuitemradio', { name: 'Model B' }));
    expect(onModelChange).toHaveBeenCalledWith('model-b');
    fireEvent.click(screen.getByRole('menuitem', { name: 'Add files' }));
    expect(onAddFiles).toHaveBeenCalledOnce();
    await waitFor(() => expect(screen.queryByRole('menu')).not.toBeInTheDocument());
  });

  it('does not attach when the canvas is not ready', () => {
    const { onAddFiles } = setup({ attachDisabled: true, attachDisabledReason: 'Create a crew first.' });
    const row = within(screen.getByRole('menu')).getByRole('menuitem', { name: 'Add files' });
    expect(row).toHaveAttribute('aria-disabled', 'true');
    fireEvent.click(row);
    expect(onAddFiles).not.toHaveBeenCalled();
  });
});
