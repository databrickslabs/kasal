import { beforeEach, describe, it, expect, vi } from 'vitest';
import { useState } from 'react';
import { render, screen, fireEvent, act, waitFor } from '@testing-library/react';
import { useUILayoutStore } from '../../../../store/uiLayout';
import { CanvasAssistantLayout } from './CanvasAssistantLayout';
import { BuilderPreviewContext } from './BuilderPreviewContext';
import BuilderNodeEditor from './BuilderNodeEditor';
vi.mock('../../../chat/components/Preview/PreviewPanel', () => ({
  default: ({ content, onClose }: { content: { data: string }; onClose: () => void }) => <aside>Memory for {content.data}<button onClick={onClose}>Close preview</button></aside>,
}));
const createSchedule = vi.hoisted(() => vi.fn(async () => ({ name: 'Saved schedule' })));
const resumeCheckpoint = vi.hoisted(() => vi.fn(async () => ({ execution_id: 'resumed-run', status: 'RUNNING', run_name: 'News' })));
vi.mock('../../../../api/execution/ExecutionCheckpointService', () => ({ default: {
  getCheckpoint: vi.fn(async () => ({ kind: 'flow', run_name: 'News', completed_count: 2, unit_count: 3, resumable: true,
    execution_status: 'FAILED', units: [{ key: 'crew-news', name: 'Research', output_preview: 'News stories' }, { key: 'crew-slides', name: 'Presentation' }] })),
  resume: resumeCheckpoint,
} }));
vi.mock('../../../../api/execution/ScheduleService', () => ({ ScheduleService: { createScheduleFromExecution: createSchedule } }));
vi.mock('../../crews/components/CrewOptimizeDialog', () => ({ default: ({ crewId, embedded }: { crewId: string; embedded?: boolean }) => <div>Optimize {crewId} {embedded ? 'in pane' : 'in dialog'}</div> }));
vi.stubGlobal('ResizeObserver', class { observe() {} unobserve() {} disconnect() {} });
const props = {
  composer: <input aria-label="Draft" defaultValue="Keep this draft" />,
  response: <p>A response to review</p>, hasMessages: true, responseKey: 'reply-1', busy: false,
  dark: false, historyOpen: false, onHistory: vi.fn(), onNewChat: vi.fn(),
};
const wrapper = ({ children }: { children: React.ReactNode }) => <><div id="builder-assistant-response-host" /><div id="builder-assistant-composer-host" /><div><div data-testid="canvas-content">Canvas nodes</div><div id="builder-assistant-preview-host" /></div>{children}</>;
beforeEach(() => useUILayoutStore.setState({ assistantResponseFocused: false, assistantPanelSide: 'right', assistantPanelVisible: false, executionHistoryVisible: true }));
describe('Canvas assistant sidebar', () => {
  it('keeps checkpoint selection across canvas swaps and resumes the chosen historical run', async () => {
    const resumed = vi.fn();
    const response = <BuilderPreviewContext.Consumer>{preview => <button onClick={() => preview?.openCheckpoints?.('historical-run', resumed)}>Open checkpoints</button>}</BuilderPreviewContext.Consumer>;
    render(<CanvasAssistantLayout {...props} response={response} sessionKey="flow:one" />, { wrapper });
    fireEvent.click(screen.getByRole('button', { name: 'Open checkpoints' }));
    fireEvent.click(await screen.findByRole('radio', { name: /Redo from "Presentation"/ }));
    expect(screen.queryByRole('dialog')).toBeNull();
    fireEvent.click(screen.getByRole('tab', { name: 'Canvas' }));
    expect(screen.getByTestId('canvas-content')).toBeVisible();
    act(() => useUILayoutStore.getState().setAssistantPanelSide('left'));
    fireEvent.click(screen.getByRole('tab', { name: 'Checkpoints' }));
    expect(screen.getByRole('radio', { name: /Redo from "Presentation"/ })).toBeChecked();
    fireEvent.click(screen.getByRole('button', { name: 'Run from checkpoint' }));
    await waitFor(() => expect(resumeCheckpoint).toHaveBeenCalledWith('historical-run', 'crew-slides'));
    expect(resumed).toHaveBeenCalledWith('resumed-run');
    expect(screen.queryByRole('tab', { name: 'Checkpoints' })).toBeNull();
    expect(screen.getByTestId('canvas-content')).toBeVisible();
  });
  it('opens tabs on demand and switches between activity, the result and the same mounted canvas', () => {
    const response = <BuilderPreviewContext.Consumer>{preview => <>
      <button onClick={() => preview?.openResult?.({ type: 'ui', data: 'Mindmap', sourceMessageId: 'result' })}>Open result</button>
      <button onClick={() => preview?.openStep('run-one', { label: 'LLM response', detail: 'Reasoning text' })}>Open activity</button>
    </>}</BuilderPreviewContext.Consumer>;
    render(<CanvasAssistantLayout {...props} response={response} sessionKey="crew:one" />, { wrapper });
    const canvas = screen.getByTestId('canvas-content');
    expect(screen.queryByRole('tablist')).toBeNull();
    fireEvent.click(screen.getByRole('button', { name: 'Open result' }));
    expect(screen.getAllByRole('tab')).toHaveLength(2);
    expect(canvas).not.toBeVisible();
    expect(canvas.inert).toBe(true);
    fireEvent.click(screen.getByRole('button', { name: 'Open activity' }));
    expect(screen.getByRole('tab', { name: 'Run activity' })).toHaveAttribute('aria-selected', 'true');
    expect(screen.queryByRole('region', { name: 'Result preview' })).toBeNull();
    fireEvent.click(screen.getByRole('tab', { name: 'Result', exact: true }));
    expect(screen.getByRole('region', { name: 'Result preview' })).toHaveTextContent('Mindmap');
    fireEvent.click(screen.getByRole('tab', { name: 'Canvas', exact: true }));
    expect(screen.getByTestId('canvas-content')).toBe(canvas);
    expect(canvas).toBeVisible();
    expect(canvas.inert).not.toBe(true);
    fireEvent.click(screen.getByRole('tab', { name: 'Run activity' }));
    fireEvent.click(screen.getByRole('button', { name: 'Close Run activity' }));
    expect(screen.queryByRole('tab', { name: 'Run activity' })).toBeNull();
    expect(screen.getByRole('tab', { name: 'Result', exact: true })).toBeInTheDocument();
  });
  it('keeps a schedule draft across tabs and schedules the selected historical run', async () => {
    const created = vi.fn();
    const response = <BuilderPreviewContext.Consumer>{preview => <>
      <button onClick={() => preview?.openSchedule?.('historical-run', 'Original name', created)}>Schedule</button>
      <button onClick={() => preview?.openOptimize?.('saved-crew', 'My crew')}>Optimize</button>
    </>}</BuilderPreviewContext.Consumer>;
    render(<CanvasAssistantLayout {...props} response={response} sessionKey="crew:one" />, { wrapper });
    expect(screen.queryByRole('tab', { name: 'Schedule' })).toBeNull();
    fireEvent.click(screen.getByRole('button', { name: 'Schedule', exact: true }));
    expect(screen.queryByRole('dialog')).toBeNull();
    fireEvent.change(screen.getByDisplayValue('Original name'), { target: { value: 'My schedule draft' } });
    fireEvent.click(screen.getByRole('button', { name: 'Optimize', exact: true }));
    expect(screen.getByText('Optimize saved-crew in pane')).toBeVisible();
    fireEvent.click(screen.getByRole('tab', { name: 'Schedule', exact: true }));
    expect(screen.getByDisplayValue('My schedule draft')).toBeVisible();
    fireEvent.click(screen.getByRole('button', { name: 'Create schedule' }));
    await waitFor(() => expect(created).toHaveBeenCalledWith('Saved schedule'));
    expect(createSchedule).toHaveBeenCalledWith({ name: 'My schedule draft', execution_id: 'historical-run', cron_expression: '0 9 * * *' });
    expect(screen.queryByRole('tab', { name: 'Schedule', exact: true })).toBeNull();
    expect(screen.getByRole('tab', { name: 'Canvas', exact: true })).toHaveAttribute('aria-selected', 'true');
  });
  it.each(['agent', 'task', 'connection'] as const)('opens the %s form under Canvas and Back restores the same graph', async kind => {
    const closed = vi.fn();
    function Editor() {
      const [open, setOpen] = useState(false);
      return <><button onClick={() => setOpen(true)}>Edit node</button>
        <BuilderNodeEditor open={open} kind={kind} nodeId="node-1" label="Research" onClose={() => { closed(); setOpen(false); }}>
          <input aria-label="Node description" defaultValue="Saved description" />
        </BuilderNodeEditor></>;
    }
    render(<><CanvasAssistantLayout {...props} sessionKey="crew:one" /><Editor /></>, { wrapper });
    const canvas = screen.getByTestId('canvas-content');
    fireEvent.click(screen.getByRole('button', { name: 'Edit node' }));
    expect(await screen.findByRole('button', { name: 'Back to Canvas' })).toBeVisible();
    expect(screen.getAllByRole('tab').map(tab => tab.textContent)).toEqual(['Canvas']);
    expect(screen.getByRole('tab', { name: 'Canvas', exact: true })).toHaveAttribute('aria-selected', 'true');
    expect(screen.queryByRole('dialog')).toBeNull();
    expect(canvas).not.toBeVisible();
    expect(canvas.inert).toBe(true);
    fireEvent.change(screen.getByRole('textbox', { name: 'Node description' }), { target: { value: 'Unsaved draft' } });
    act(() => useUILayoutStore.getState().setAssistantPanelSide('left'));
    expect(screen.getByRole('textbox', { name: 'Node description' })).toHaveValue('Unsaved draft');
    fireEvent.click(screen.getByRole('button', { name: 'Back to Canvas' }));
    expect(closed).toHaveBeenCalledOnce();
    expect(screen.queryByRole('textbox', { name: 'Node description' })).toBeNull();
    expect(screen.getByTestId('canvas-content')).toBe(canvas);
    expect(canvas).toBeVisible();
    expect(canvas.inert).not.toBe(true);
    fireEvent.click(screen.getByRole('button', { name: 'Edit node' }));
    expect(screen.getByRole('textbox', { name: 'Node description' })).toHaveValue('Saved description');
    fireEvent.click(screen.getByRole('tab', { name: 'Canvas', exact: true }));
    expect(screen.queryByRole('button', { name: 'Back to Canvas' })).toBeNull();
    expect(canvas).toBeVisible();
  });
  it('closes a canvas form when a conversation action opens another view', async () => {
    const closed = vi.fn();
    function Editor() {
      const [open, setOpen] = useState(true);
      return <BuilderNodeEditor open={open} kind="task" nodeId="task-1" label="Research" onClose={() => { closed(); setOpen(false); }}>
        <input aria-label="Task draft" />
      </BuilderNodeEditor>;
    }
    const response = <BuilderPreviewContext.Consumer>{preview => <button onClick={() => preview?.openStep('run', { label: 'Response', detail: 'Details' })}>Open activity</button>}</BuilderPreviewContext.Consumer>;
    render(<><CanvasAssistantLayout {...props} response={response} sessionKey="crew:one" /><Editor /></>, { wrapper });
    await screen.findByRole('button', { name: 'Back to Canvas' });
    fireEvent.click(screen.getByRole('button', { name: 'Open activity' }));
    expect(closed).toHaveBeenCalledOnce();
    expect(screen.queryByRole('textbox', { name: 'Task draft' })).toBeNull();
    expect(screen.getAllByRole('tab').map(tab => tab.textContent)).toEqual(['Canvas', 'Run activity']);
    fireEvent.click(screen.getByRole('tab', { name: 'Canvas', exact: true }));
    expect(screen.getByTestId('canvas-content')).toBeVisible();
    expect(screen.queryByRole('button', { name: 'Back to Canvas' })).toBeNull();
  });
  it('opens a rich result beside either side of the conversation and clears it on a session switch', () => {
    const response = <BuilderPreviewContext.Consumer>{preview => <button onClick={() => preview?.openResult?.({ type: 'ui', data: 'Surface data', sourceMessageId: 'result-one' })}>Open result</button>}</BuilderPreviewContext.Consumer>;
    const view = render(<CanvasAssistantLayout {...props} response={response} sessionKey="crew:one" />, { wrapper });
    fireEvent.click(screen.getByRole('button', { name: 'Open result' }));
    const pane = screen.getByRole('region', { name: 'Result preview' });
    expect(pane.closest('#builder-assistant-preview-host')).not.toBeNull();
    act(() => useUILayoutStore.getState().setAssistantPanelSide('left'));
    expect(screen.getByRole('region', { name: 'Result preview' })).toBe(pane);
    view.rerender(<CanvasAssistantLayout {...props} response={response} sessionKey="flow:two" />);
    expect(screen.queryByRole('region', { name: 'Result preview' })).toBeNull();
  });
  it('opens run memory in the adjacent preview, follows swaps, and retains the fullscreen composer', async () => {
    const response = <BuilderPreviewContext.Consumer>{open => <button onClick={() => open?.openMemory('historical-run')}>Open run memory</button>}</BuilderPreviewContext.Consumer>;
    const { rerender } = render(<CanvasAssistantLayout {...props} response={response} sessionKey="crew:one" />, { wrapper });
    fireEvent.click(screen.getByRole('button', { name: 'Open run memory' }));
    const preview = screen.getByRole('region', { name: 'Run memory preview' });
    expect(preview.closest('#builder-assistant-preview-host')).not.toBeNull();
    expect(preview).toHaveTextContent('Memory for historical-run');
    expect(screen.queryByRole('region', { name: 'Expanded conversation' })).not.toBeInTheDocument();
    act(() => useUILayoutStore.getState().setAssistantPanelSide('left'));
    expect(screen.getByRole('region', { name: 'Run memory preview' })).toBe(preview);
    act(() => window.dispatchEvent(new Event('expandBuilderConversation')));
    expect(screen.getByRole('region', { name: 'Expanded conversation' })).toContainElement(screen.getByRole('region', { name: 'Run memory preview' }));
    expect(screen.getByRole('textbox', { name: 'Draft' })).toHaveValue('Keep this draft');
    fireEvent.click(screen.getByRole('button', { name: 'Back to canvas' }));
    await waitFor(() => expect(screen.queryByRole('region', { name: 'Expanded conversation' })).not.toBeInTheDocument());
    fireEvent.click(screen.getByRole('button', { name: 'Close preview' }));
    expect(screen.queryByRole('region', { name: 'Run memory preview' })).not.toBeInTheDocument();
    fireEvent.click(screen.getByRole('button', { name: 'Open run memory' }));
    act(() => useUILayoutStore.getState().setAssistantPanelVisible(false));
    expect(screen.queryByRole('region', { name: 'Run memory preview' })).not.toBeInTheDocument();
    act(() => useUILayoutStore.getState().setAssistantPanelVisible(true));
    expect(screen.getByRole('region', { name: 'Run memory preview' })).toBeVisible();
    rerender(<CanvasAssistantLayout {...props} response={response} sessionKey="flow:two" />);
    expect(screen.queryByRole('region', { name: 'Run memory preview' })).not.toBeInTheDocument();
  });
  it('opens on the right and can move left without losing the draft', () => {
    render(<CanvasAssistantLayout {...props} />, { wrapper });
    expect(screen.getByRole('region', { name: 'Conversation' })).toBeVisible();
    expect(useUILayoutStore.getState().assistantPanelSide).toBe('right');
    fireEvent.change(screen.getByRole('textbox', { name: 'Draft' }), { target: { value: 'An unsent edit' } });
    act(() => useUILayoutStore.getState().setAssistantPanelSide('left'));
    expect(useUILayoutStore.getState().assistantPanelSide).toBe('left');
    expect(useUILayoutStore.getState().executionHistoryVisible).toBe(true);
    act(() => useUILayoutStore.getState().setAssistantPanelSide('right'));
    expect(screen.getByRole('textbox', { name: 'Draft' })).toHaveValue('An unsent edit');
    expect(screen.queryByRole('button', { name: /Expand|Dock|Detach/ })).not.toBeInTheDocument();
  });
  it('closes and restores the answer without clearing the composer', () => {
    const { rerender } = render(<CanvasAssistantLayout {...props} />, { wrapper });
    act(() => useUILayoutStore.getState().setExecutionHistoryVisible(false));
    expect(screen.queryByRole('region', { name: 'Conversation' })).not.toBeInTheDocument();
    act(() => useUILayoutStore.getState().setAssistantPanelVisible(true));
    expect(screen.getByText('A response to review')).toBeVisible();
    act(() => useUILayoutStore.getState().setExecutionHistoryVisible(false));
    rerender(<CanvasAssistantLayout {...props} responseKey="reply-2" />);
    expect(screen.getByText('A response to review')).toBeVisible();
    expect(screen.getByRole('textbox', { name: 'Draft' })).toHaveValue('Keep this draft');
  });
  it('opens the response full screen and returns with the draft intact', async () => {
    render(<CanvasAssistantLayout {...props} />, { wrapper });
    fireEvent.change(screen.getByRole('textbox', { name: 'Draft' }), { target: { value: 'My unsent draft' } });
    act(() => window.dispatchEvent(new Event('expandBuilderConversation')));
    expect(screen.getByRole('region', { name: 'Expanded conversation' })).toBeVisible();
    expect(screen.getByText('A response to review')).toBeVisible();
    expect(screen.getByRole('textbox', { name: 'Draft' })).toHaveValue('My unsent draft');
    fireEvent.change(screen.getByRole('textbox', { name: 'Draft' }), { target: { value: 'Edited in full screen' } });
    fireEvent.click(screen.getByRole('button', { name: 'Back to canvas' }));
    await waitFor(() => expect(screen.queryByRole('region', { name: 'Expanded conversation' })).not.toBeInTheDocument());
    expect(screen.getByRole('textbox', { name: 'Draft' })).toHaveValue('Edited in full screen');
    expect(screen.getByText('A response to review')).toBeVisible();
    act(() => window.dispatchEvent(new Event('expandBuilderConversation')));
    fireEvent.keyDown(screen.getByRole('region', { name: 'Expanded conversation' }), { key: 'Escape' });
    await waitFor(() => expect(screen.queryByRole('region', { name: 'Expanded conversation' })).not.toBeInTheDocument());
    expect(screen.queryByRole('region', { name: 'Expanded conversation' })).not.toBeInTheDocument();
  });
  it('moves the same input into the response-focused area and restores its edits', () => {
    render(<CanvasAssistantLayout {...props} />, { wrapper });
    const input = screen.getByRole('textbox', { name: 'Draft' });
    fireEvent.change(input, { target: { value: 'Before focusing' } });
    act(() => useUILayoutStore.getState().setAssistantResponseFocused(true));
    expect(screen.getByRole('textbox', { name: 'Draft' })).toBe(input);
    expect(input.closest('#builder-assistant-composer-host')).not.toBeNull();
    fireEvent.change(input, { target: { value: 'Edited beside the canvas' } });
    act(() => useUILayoutStore.getState().setAssistantResponseFocused(false));
    expect(screen.getByRole('textbox', { name: 'Draft' })).toBe(input);
    expect(input).toHaveValue('Edited beside the canvas');
    expect(input.closest('[data-testid="canvas-assistant-dock"]')).not.toBeNull();
  });
  it('leaves new-session navigation to the shared sidebar', () => {
    render(<CanvasAssistantLayout {...props} busy />, { wrapper });
    expect(screen.queryByRole('button', { name: 'New Chat' })).not.toBeInTheDocument();
  });
  it('reveals the canvas for the tutorial without losing the input draft', () => {
    render(<CanvasAssistantLayout {...props} />, { wrapper });
    act(() => window.dispatchEvent(new Event('expandBuilderConversation')));
    expect(screen.getByRole('region', { name: 'Expanded conversation' })).toBeVisible();
    act(() => window.dispatchEvent(new Event('collapseBuilderConversation')));
    expect(screen.queryByRole('region', { name: 'Expanded conversation' })).toBeNull();
    expect(screen.getByRole('textbox', { name: 'Draft' })).toHaveValue('Keep this draft');
  });
});
