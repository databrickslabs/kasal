import { describe, it, expect, vi } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import RunActions from './ExecutionActions';
import type { Run } from '../../../api/execution/ExecutionHistoryService';
vi.mock('./CheckpointDialog', () => ({ default: ({ open, onResumed }: { open: boolean; onResumed: () => void }) => open ? <button onClick={onResumed}>Resume checkpoint</button> : null }));
vi.mock('./ExecutionStopButton', () => ({ default: () => null }));
vi.mock('../../../utils/pdfGenerator', () => ({ generateRunPDF: vi.fn() }));
const setup = (status: string) => {
  const run = { id: 'run-1', job_id: 'job-1', run_name: 'Research', status } as Run;
  const callbacks = { onViewResult: vi.fn(), onShowTrace: vi.fn(), onShowLogs: vi.fn(), onSchedule: vi.fn(), onDelete: vi.fn(), onStatusChange: vi.fn(), onShowDetails: vi.fn() };
  render(<RunActions compact run={run} {...callbacks} />);
  const menu = () => fireEvent.click(screen.getByRole('button', { name: 'Actions for Research' }));
  return { run, callbacks, menu };
};
describe('History run actions', () => {
  it('opens activity for active jobs and prevents unavailable result actions', () => {
    const { callbacks, menu } = setup('RUNNING');
    fireEvent.click(screen.getByRole('button', { name: 'Activity' }));
    expect(callbacks.onShowTrace).toHaveBeenCalledWith('run-1');
    menu();
    expect(screen.getByRole('menuitem', { name: 'View logs' })).toHaveAttribute('aria-disabled', 'true');
    expect(screen.queryByRole('menuitem', { name: 'Checkpoint and resume' })).not.toBeInTheDocument();
  });
  it('routes result, schedule, logs and delete to the correct run', () => {
    const { callbacks, run, menu } = setup('COMPLETED');
    fireEvent.click(screen.getByRole('button', { name: 'Result' }));
    expect(callbacks.onViewResult).toHaveBeenCalledWith(run);
    menu(); fireEvent.click(screen.getByRole('menuitem', { name: 'Schedule execution' }));
    expect(callbacks.onSchedule).toHaveBeenCalledWith(run);
    menu(); fireEvent.click(screen.getByRole('menuitem', { name: 'View logs' }));
    expect(callbacks.onShowLogs).toHaveBeenCalledWith('job-1');
    menu(); fireEvent.click(screen.getByRole('menuitem', { name: 'Delete run' }));
    expect(callbacks.onDelete).toHaveBeenCalledWith(run);
  });
  it('preserves checkpoint resume and refreshes after resuming', () => {
    const { callbacks, menu } = setup('FAILED');
    menu(); fireEvent.click(screen.getByRole('menuitem', { name: 'Checkpoint and resume' }));
    fireEvent.click(screen.getByRole('button', { name: 'Resume checkpoint' }));
    expect(callbacks.onStatusChange).toHaveBeenCalledWith('run-1', 'RUNNING');
  });
});
