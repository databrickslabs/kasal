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
const getTraces = vi.hoisted(() => vi.fn(async () => ({ data: { traces: [] } })));
vi.mock('../../../../shared/api/client', () => ({ apiClient: { get: getTraces }, default: { get: getTraces } }));
vi.mock('../../../chat/store/appStore', () => ({ useAppStore: { getState: () => ({ loadCatalog: vi.fn() }) } }));
import BuilderRunActions, { runUsedMemory } from './BuilderRunActions';
import { BuilderPreviewContext } from './BuilderPreviewContext';
const run = { job_id: 'older-run', status: 'completed', run_name: 'News', agents_yaml: '', inputs: { agents_yaml: { a: { memory: true } } } } as Run;
beforeEach(() => { vi.clearAllMocks(); getRun.mockResolvedValue(run); getTraces.mockResolvedValue({ data: { traces: [] } }); usePermissionStore.setState({ allowAgentBuilder: true, allowFlowBuilder: true, userRole: 'admin' }); });
describe('Builder completed-run actions', () => {
  it.each(['crew', 'flow'])('opens checkpoints beside the %s conversation for its historical execution', async execution_type => {
    getRun.mockResolvedValue({ ...run, execution_type, tasks_yaml: 'first: {}\nsecond: {}' });
    const openCheckpoints = vi.fn();
    render(<BuilderPreviewContext.Provider value={{ openMemory: vi.fn(), openStep: vi.fn(), openCheckpoints }}><BuilderRunActions jobId="older-run" /></BuilderPreviewContext.Provider>);
    fireEvent.click(await screen.findByRole('button', { name: 'Checkpoints' }));
    expect(openCheckpoints).toHaveBeenCalledWith('older-run', expect.any(Function));
    expect(screen.queryByRole('dialog')).not.toBeInTheDocument();
  });
  it('offers checkpoint recovery for a failed multi-task execution without completed-run actions', async () => {
    getRun.mockResolvedValue({ ...run, status: 'failed', tasks_yaml: 'first: {}\nsecond: {}' });
    render(<BuilderPreviewContext.Provider value={{ openMemory: vi.fn(), openStep: vi.fn(), openCheckpoints: vi.fn() }}><BuilderRunActions jobId="failed-run" /></BuilderPreviewContext.Provider>);
    expect(await screen.findByRole('button', { name: 'Checkpoints' })).toBeInTheDocument();
    expect(screen.queryByRole('button', { name: 'Schedule' })).not.toBeInTheDocument();
  });
  it.each(['single-task', 'operator'])('hides checkpoints for %s', async reason => {
    getRun.mockResolvedValue({ ...run, tasks_yaml: reason === 'single-task' ? 'first: {}' : 'first: {}\nsecond: {}' });
    if (reason === 'operator') usePermissionStore.setState({ userRole: 'operator' });
    render(<BuilderPreviewContext.Provider value={{ openMemory: vi.fn(), openStep: vi.fn(), openCheckpoints: vi.fn() }}><BuilderRunActions jobId="older-run" /></BuilderPreviewContext.Provider>);
    await screen.findByRole('button', { name: 'Schedule' });
    expect(screen.queryByRole('button', { name: 'Checkpoints' })).not.toBeInTheDocument();
  });
  it('uses the adjacent builder preview for the selected historical run', async () => {
    const open = vi.fn();
    render(<BuilderPreviewContext.Provider value={{ openMemory: open, openStep: vi.fn() }}><BuilderRunActions jobId="older-run" /></BuilderPreviewContext.Provider>);
    fireEvent.click(await screen.findByRole('button', { name: 'View memory graph' }));
    expect(open).toHaveBeenCalledWith('older-run');
    expect(screen.queryByRole('dialog')).not.toBeInTheDocument();
  });
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
  it('shows the memory graph for a flow with memory traces and no agent YAML', async () => {
    getRun.mockResolvedValue({ ...run, execution_type: 'flow', inputs: { agents_yaml: {} } });
    getTraces.mockResolvedValue({ data: { traces: [{ id: 1 }] } } as any);
    render(<BuilderRunActions jobId="flow-run" />);
    fireEvent.click(await screen.findByRole('button', { name: 'View memory graph' }));
    expect(screen.getByText('Memory for flow-run')).toBeInTheDocument();
    expect(getTraces).toHaveBeenCalledWith('/traces/job/flow-run', expect.objectContaining({ params: { limit: 1, event_type_prefix: 'memory_' } }));
  });
  it('keeps catalog saving on generated plans, not completed runs', async () => {
    render(<BuilderRunActions jobId="older-run" />);
    await screen.findByRole('button', { name: 'Schedule' });
    expect(screen.queryByRole('button', { name: 'Save to catalog', exact: true })).not.toBeInTheDocument();
  });
});
