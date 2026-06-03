import { describe, it, expect, vi } from 'vitest';
import { render, screen } from '@testing-library/react';
import RightSidebar from './RightSidebar';

/*
 * Focused test for the change made in the app-modes work: the "Show Workflow
 * Panel" (AccountTree / toggle-flows) button was REMOVED from the right sidebar
 * because Flow is now a top-level mode reached via the TabBar mode switcher.
 * This is not a full-coverage test of the 400-line component — it pins the
 * specific behavior we changed so it can't silently regress.
 */

vi.mock('../../store/permissions', () => ({
  usePermissionStore: () => ({ userRole: 'admin' }),
}));

vi.mock('../../store/tabManager', () => ({
  useTabManagerStore: () => ({ getActiveTab: () => null }),
}));

vi.mock('../CrewExport/ExportCrewDialog', () => ({
  default: () => null,
}));

describe('RightSidebar — flow-toggle removal', () => {
  const baseProps = {
    onOpenLogsDialog: vi.fn(),
    onToggleChat: vi.fn(),
    isChatOpen: true,
    setIsAgentDialogOpen: vi.fn(),
    setIsTaskDialogOpen: vi.fn(),
  };

  it('does NOT render a "Show/Hide Workflow Panel" toggle button', () => {
    render(
      <RightSidebar
        {...baseProps}
        areFlowsVisible={false}
        toggleFlowsVisibility={vi.fn()}
      />,
    );
    expect(screen.queryByRole('button', { name: /Show Workflow Panel/i })).toBeNull();
    expect(screen.queryByRole('button', { name: /Hide Workflow Panel/i })).toBeNull();
  });

  it('still renders core actions (catalog, schedules)', () => {
    render(<RightSidebar {...baseProps} areFlowsVisible={false} />);
    // Catalog button is always present
    expect(screen.getByRole('button', { name: /Open Catalog/i })).toBeInTheDocument();
  });

  it('ignores a passed toggleFlowsVisibility prop (no flow toggle wired)', () => {
    const toggleFlowsVisibility = vi.fn();
    render(
      <RightSidebar
        {...baseProps}
        areFlowsVisible
        toggleFlowsVisibility={toggleFlowsVisibility}
      />,
    );
    // There is no control that could call it
    expect(toggleFlowsVisibility).not.toHaveBeenCalled();
  });
});
