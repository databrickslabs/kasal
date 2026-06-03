import { describe, it, expect, vi } from 'vitest';
import { render, screen } from '@testing-library/react';
import TabBar from './TabBar';

/*
 * Focused test for the app-modes change to TabBar: a `leftSlot` prop renders
 * at the far-left of the bar, and a flex spacer keeps it right-alignable when
 * `hideTabsAndButtons` is set (chat mode). Not a full-coverage test of the
 * 870-line component — it pins the new behavior.
 */

const createTab = vi.fn();
vi.mock('../../store/tabManager', () => ({
  useTabManagerStore: () => ({
    tabs: [{ id: 't1', name: 'Main Canvas', isDirty: false }],
    activeTabId: 't1',
    createTab,
    closeTab: vi.fn(),
    setActiveTab: vi.fn(),
    updateTabName: vi.fn(),
    duplicateTab: vi.fn(),
    clearAllTabs: vi.fn(),
    clearTabExecutionStatus: vi.fn(),
    getTabsForCurrentGroup: () => [{ id: 't1', name: 'Main Canvas', isDirty: false }],
    switchToGroup: vi.fn(),
  }),
}));

vi.mock('../../hooks/workflow/useThemeManager', () => ({
  useThemeManager: () => ({ isDarkMode: false }),
}));

describe('TabBar — leftSlot (app-modes header)', () => {
  it('renders the leftSlot content', () => {
    render(<TabBar leftSlot={<div data-testid="left-slot">SLOT</div>} />);
    expect(screen.getByTestId('left-slot')).toBeInTheDocument();
  });

  it('renders leftSlot even when tabs/buttons are hidden (chat mode)', () => {
    render(
      <TabBar
        hideTabsAndButtons
        leftSlot={<div data-testid="left-slot">SLOT</div>}
      />,
    );
    expect(screen.getByTestId('left-slot')).toBeInTheDocument();
    // Tabs are hidden, so the tablist is not rendered
    expect(screen.queryByRole('tab')).toBeNull();
  });

  it('renders tabs when not hidden and no leftSlot provided', () => {
    render(<TabBar />);
    expect(screen.getByRole('tab', { name: /Main Canvas/i })).toBeInTheDocument();
  });
});
