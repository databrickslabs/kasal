import React from 'react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import { usePermissionStore } from '../../../../store/permissions';
import type { Run } from '../../../../types/execution/run';
const getRun = vi.hoisted(() => vi.fn());
const createSchedule = vi.hoisted(() => vi.fn(async () => ({ name: 'Saved schedule' })));
vi.mock('../../../../api/execution/ExecutionHistoryService', () => ({ runService: { getRunByJobId: getRun } }));
vi.mock('../../../../api/execution/ScheduleService', () => ({ ScheduleService: {
  createScheduleFromExecution: createSchedule, listSchedules: vi.fn(async () => []),
} }));
vi.mock('../../../chat/components/Preview/MemoryPane', () => ({ default: ({ runId }: { runId: string }) => <div>Memory for {runId}</div> }));
import BuilderRunActions, { runUsedMemory } from './BuilderRunActions';
const run = { job_id: 'older-run', status: 'completed', run_name: 'News', agents_yaml: '', inputs: { agents_yaml: { a: { memory: true } } } } as Run;
beforeEach(() => { vi.clearAllMocks(); getRun.mockResolvedValue(run); usePermissionStore.setState({ allowAgentBuilder: true }); });
describe('Builder completed-run actions', () => {
  it('opens memory and schedules the message execution, not the latest canvas', async () => {
    render(<BuilderRunActions jobId="older-run" />);
    fireEvent.click(await screen.findByRole('button', { name: 'View memory graph' }));
    expect(screen.getByText('Memory for older-run')).toBeInTheDocument();
    fireEvent.click(screen.getByRole('button', { name: 'Close memory' }));
    fireEvent.click(await screen.findByRole('button', { name: 'Schedule' }));
    expect(screen.getByRole('dialog', { name: 'Run this on a schedule' })).toBeInTheDocument();
    fireEvent.click(screen.getByRole('button', { name: 'Create schedule' }));
    await waitFor(() => expect(createSchedule).toHaveBeenCalledWith(expect.objectContaining({ execution_id: 'older-run' })));
  });
  it('offers scheduling but no memory graph when the saved run disabled memory', async () => {
    getRun.mockResolvedValue({ ...run, inputs: { agents_yaml: { a: { memory: false } } } });
    render(<BuilderRunActions jobId="older-run" />);
    expect(await screen.findByRole('button', { name: 'Schedule' })).toBeInTheDocument();
    expect(screen.queryByRole('button', { name: 'View memory graph' })).not.toBeInTheDocument();
  });
  it('does not offer completed-run actions for a failed execution', async () => {
    getRun.mockResolvedValue({ ...run, status: 'failed' });
    const { container } = render(<BuilderRunActions jobId="failed-run" />);
    await waitFor(() => expect(getRun).toHaveBeenCalledWith('failed-run'));
    expect(container).toBeEmptyDOMElement();
  });
  it('recognizes stored YAML and honors execution-wide memory disable', () => {
    expect(runUsedMemory({ ...run, inputs: undefined, agents_yaml: 'a:\n  memory: true' })).toBe(true);
    expect(runUsedMemory({ ...run, inputs: { ...run.inputs, disable_memory: true } })).toBe(false);
  });
});
