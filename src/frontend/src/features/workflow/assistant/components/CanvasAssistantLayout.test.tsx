import { beforeEach, describe, it, expect, vi } from 'vitest';
import { render, screen, fireEvent, act, waitFor } from '@testing-library/react';
import { useUILayoutStore } from '../../../../store/uiLayout';
import { CanvasAssistantLayout } from './CanvasAssistantLayout';
vi.stubGlobal('ResizeObserver', class { observe() {} disconnect() {} });
const props = {
  composer: <input aria-label="Draft" defaultValue="Keep this draft" />,
  response: <p>A response to review</p>, hasMessages: true, responseKey: 'reply-1', busy: false,
  dark: false, historyOpen: false, onHistory: vi.fn(), onNewChat: vi.fn(), showEarlier: false, onToggleEarlier: vi.fn(),
};
const wrapper = ({ children }: { children: React.ReactNode }) => <><div id="builder-assistant-response-host" /><div id="builder-assistant-composer-host" />{children}</>;
beforeEach(() => useUILayoutStore.setState({ assistantResponseFocused: false, assistantPanelSide: 'right', assistantPanelVisible: false, executionHistoryVisible: true }));
describe('Canvas assistant sidebar', () => {
  it('opens on the right and can move left without losing the draft', () => {
    render(<CanvasAssistantLayout {...props} />, { wrapper });
    expect(screen.getByRole('region', { name: 'Kasal responses' })).toBeVisible();
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
    expect(screen.queryByRole('region', { name: 'Kasal responses' })).not.toBeInTheDocument();
    fireEvent.click(screen.getByRole('button', { name: 'Responses' }));
    expect(screen.getByText('A response to review')).toBeVisible();
    act(() => useUILayoutStore.getState().setExecutionHistoryVisible(false));
    rerender(<CanvasAssistantLayout {...props} responseKey="reply-2" />);
    expect(screen.getByText('A response to review')).toBeVisible();
    expect(screen.getByRole('textbox', { name: 'Draft' })).toHaveValue('Keep this draft');
  });
  it('opens the response full screen and returns with the draft intact', async () => {
    render(<CanvasAssistantLayout {...props} />, { wrapper });
    fireEvent.change(screen.getByRole('textbox', { name: 'Draft' }), { target: { value: 'My unsent draft' } });
    fireEvent.click(screen.getByRole('button', { name: 'Full screen responses' }));
    expect(screen.getByRole('dialog', { name: 'Full screen responses' })).toBeVisible();
    expect(screen.getByText('A response to review')).toBeVisible();
    expect(screen.getByRole('textbox', { name: 'Draft' })).toHaveValue('My unsent draft');
    fireEvent.change(screen.getByRole('textbox', { name: 'Draft' }), { target: { value: 'Edited in full screen' } });
    fireEvent.click(screen.getByRole('button', { name: 'Back to canvas' }));
    await waitFor(() => expect(screen.queryByRole('dialog')).not.toBeInTheDocument());
    expect(screen.getByRole('textbox', { name: 'Draft' })).toHaveValue('Edited in full screen');
    expect(screen.getByText('A response to review')).toBeVisible();
    fireEvent.click(screen.getByRole('button', { name: 'Full screen responses' }));
    fireEvent.keyDown(screen.getByRole('dialog'), { key: 'Escape' });
    await waitFor(() => expect(screen.queryByRole('dialog')).not.toBeInTheDocument());
    expect(screen.getByRole('button', { name: 'Full screen responses' })).toBeVisible();
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
  it('prevents starting a new canvas while generation is using the current canvas', () => {
    render(<CanvasAssistantLayout {...props} busy />, { wrapper });
    expect(screen.getByRole('button', { name: 'New Chat' })).toBeDisabled();
  });
});
