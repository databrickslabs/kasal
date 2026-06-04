import { describe, it, expect, beforeEach, vi } from 'vitest';
import { render, screen, fireEvent, within, waitFor } from '@testing-library/react';
// `within` is used in the menu-content assertions below.
import ModeSwitcher from './ModeSwitcher';

// --- Mock the stores the component depends on ---
const setAppMode = vi.fn();
let mockAppMode = 'crew';
let mockFlowEnabled = true;

vi.mock('../../store/uiLayout', () => ({
  useUILayoutStore: (selector: (s: unknown) => unknown) =>
    selector({ appMode: mockAppMode, setAppMode }),
}));

vi.mock('../../store/flowConfig', () => ({
  useFlowConfigStore: () => ({ crewAIFlowEnabled: mockFlowEnabled }),
}));

describe('ModeSwitcher', () => {
  beforeEach(() => {
    setAppMode.mockClear();
    mockAppMode = 'crew';
    mockFlowEnabled = true;
  });

  it('renders the grid trigger button (menu closed initially)', () => {
    render(<ModeSwitcher />);
    expect(screen.getByRole('button', { name: /Workspace mode: Agent Builder/i })).toBeInTheDocument();
    expect(screen.queryByText('Switch Mode')).not.toBeInTheDocument();
  });

  it('opens the menu with all three options when flow is enabled', () => {
    render(<ModeSwitcher />);
    fireEvent.click(screen.getByRole('button', { name: /Workspace mode/i }));
    expect(screen.getByText('Switch Mode')).toBeInTheDocument();
    const menu = screen.getByRole('menu');
    expect(within(menu).getByText('Agent Builder')).toBeInTheDocument();
    expect(within(menu).getByText('Flow Builder')).toBeInTheDocument();
    expect(within(menu).getByText('Chat')).toBeInTheDocument();
  });

  it('hides the Flow option when crewAIFlowEnabled is false', () => {
    mockFlowEnabled = false;
    render(<ModeSwitcher />);
    fireEvent.click(screen.getByRole('button', { name: /Workspace mode/i }));
    const menu = screen.getByRole('menu');
    expect(within(menu).getByText('Agent Builder')).toBeInTheDocument();
    expect(within(menu).queryByText('Flow Builder')).not.toBeInTheDocument();
    expect(within(menu).getByText('Chat')).toBeInTheDocument();
  });

  it('selecting Chat calls setAppMode and closes the menu', async () => {
    render(<ModeSwitcher />);
    fireEvent.click(screen.getByRole('button', { name: /Workspace mode/i }));
    fireEvent.click(screen.getByText('Chat'));
    expect(setAppMode).toHaveBeenCalledWith('chat');
    await waitFor(() => expect(screen.queryByText('Switch Mode')).not.toBeInTheDocument());
  });

  it('selecting Flow calls setAppMode("flow")', () => {
    render(<ModeSwitcher />);
    fireEvent.click(screen.getByRole('button', { name: /Workspace mode/i }));
    fireEvent.click(screen.getByText('Flow Builder'));
    expect(setAppMode).toHaveBeenCalledWith('flow');
  });

  it('shows the active label for the current mode (chat) and a checkmark on it', () => {
    mockAppMode = 'chat';
    render(<ModeSwitcher />);
    // Trigger tooltip label reflects active option
    expect(screen.getByRole('button', { name: /Workspace mode: Chat/i })).toBeInTheDocument();
    fireEvent.click(screen.getByRole('button', { name: /Workspace mode/i }));
    // MUI marks the active MenuItem with the Mui-selected class
    const chatItem = screen.getByText('Chat').closest('li');
    expect(chatItem?.className).toContain('Mui-selected');
  });

  it('falls back to the first option when appMode is not in the visible options', () => {
    // appMode is 'flow' but flow option is hidden -> activeOption falls back to Crew
    mockAppMode = 'flow';
    mockFlowEnabled = false;
    render(<ModeSwitcher />);
    expect(screen.getByRole('button', { name: /Workspace mode: Agent Builder/i })).toBeInTheDocument();
  });

  it('closes the menu via onClose (backdrop)', async () => {
    render(<ModeSwitcher />);
    fireEvent.click(screen.getByRole('button', { name: /Workspace mode/i }));
    expect(screen.getByText('Switch Mode')).toBeInTheDocument();
    // MUI Menu renders a backdrop; Escape triggers onClose
    fireEvent.keyDown(screen.getByRole('menu'), { key: 'Escape', code: 'Escape' });
    // After close (async transition) the heading is gone
    await waitFor(() => expect(screen.queryByText('Switch Mode')).not.toBeInTheDocument());
  });
});
