import { describe, it, expect, vi, afterEach } from 'vitest';
import { render, screen, fireEvent, act } from '@testing-library/react';
import RightSidebar from './RightSidebar';
import { useBuilderExecutionControls } from '../../store/builderExecutionControls';

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

vi.mock('../../features/workflow/export/components/ExportCrewDialog', () => ({
  default: () => null,
}));

describe('RightSidebar — flow-toggle removal', () => {
  afterEach(() => useBuilderExecutionControls.setState({ crew: null, flow: null }));
  const baseProps = {
    onOpenLogsDialog: vi.fn(),
    onToggleChat: vi.fn(),
    isChatOpen: true,
    setIsAgentDialogOpen: vi.fn(),
    setIsTaskDialogOpen: vi.fn(),
  };

  it.each(['crew', 'flow'] as const)('replaces Play with the active %s stop control', mode => {
    const stop = vi.fn().mockResolvedValue(undefined);
    useBuilderExecutionControls.setState({ [mode]: { jobId: 'active-job', stopping: false, stop } });
    render(<RightSidebar {...baseProps} hasCrewNodes hasFlowNodes areFlowsVisible={mode === 'flow'} />);
    fireEvent.click(screen.getByRole('button', { name: mode === 'flow' ? 'Stop Flow' : 'Stop Crew' }));
    expect(stop).toHaveBeenCalledOnce();
    act(() => useBuilderExecutionControls.setState({ [mode]: { jobId: 'active-job', stopping: true, stop } }));
    expect(screen.getByRole('button', { name: 'Stopping execution' })).toBeDisabled();
    act(() => useBuilderExecutionControls.setState({ [mode]: null }));
    expect(screen.getByRole('button', { name: mode === 'flow' ? 'Run Flow' : 'Run Crew' })).toBeEnabled();
  });

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

  it('leaves catalogs, logs and schedules in the shared left sidebar', () => {
    render(<RightSidebar {...baseProps} areFlowsVisible={false} />);
    expect(screen.queryByRole('button', { name: /Open Catalog/i })).not.toBeInTheDocument();
    expect(screen.queryByRole('button', { name: /View Assistant Logs|Schedules/i })).not.toBeInTheDocument();
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
