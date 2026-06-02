import { describe, it, expect, beforeEach, vi } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import ChatModeHeaderSlot from './ChatModeHeaderSlot';

const toggleSidebar = vi.fn();
let sidebarOpen = false;

vi.mock('./store/appStore', () => ({
  useAppStore: (selector: (s: unknown) => unknown) =>
    selector({ sidebarOpen, toggleSidebar }),
}));

describe('ChatModeHeaderSlot', () => {
  beforeEach(() => {
    toggleSidebar.mockClear();
    sidebarOpen = false;
  });

  it('shows "Show chat history" tooltip when sidebar is closed and toggles on click', () => {
    sidebarOpen = false;
    render(<ChatModeHeaderSlot />);
    const btn = screen.getByRole('button', { name: /Show chat history/i });
    fireEvent.click(btn);
    expect(toggleSidebar).toHaveBeenCalledTimes(1);
  });

  it('shows "Hide chat history" tooltip when sidebar is open', () => {
    sidebarOpen = true;
    render(<ChatModeHeaderSlot />);
    expect(screen.getByRole('button', { name: /Hide chat history/i })).toBeInTheDocument();
  });
});
