import { beforeEach, describe, it, expect, vi } from 'vitest';
import { render, screen, fireEvent, act, waitFor } from '@testing-library/react';
import { useUILayoutStore } from '../../../../store/uiLayout';
import { CanvasAssistantLayout } from './CanvasAssistantLayout';
import { BuilderPreviewContext } from './BuilderPreviewContext';
vi.mock('../../../chat/components/Preview/PreviewPanel', () => ({
  default: ({ content, onClose }: { content: { data: string }; onClose: () => void }) => <aside>Memory for {content.data}<button onClick={onClose}>Close preview</button></aside>,
}));
vi.stubGlobal('ResizeObserver', class { observe() {} disconnect() {} });
const props = {
  composer: <input aria-label="Draft" defaultValue="Keep this draft" />,
  response: <p>A response to review</p>, hasMessages: true, responseKey: 'reply-1', busy: false,
  dark: false, historyOpen: false, onHistory: vi.fn(), onNewChat: vi.fn(),
};
const wrapper = ({ children }: { children: React.ReactNode }) => <><div id="builder-assistant-response-host" /><div id="builder-assistant-composer-host" /><div id="builder-assistant-preview-host" />{children}</>;
beforeEach(() => useUILayoutStore.setState({ assistantResponseFocused: false, assistantPanelSide: 'right', assistantPanelVisible: false, executionHistoryVisible: true }));
describe('Canvas assistant sidebar', () => {
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
