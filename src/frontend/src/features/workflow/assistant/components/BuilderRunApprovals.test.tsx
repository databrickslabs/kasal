import { act, fireEvent, render, screen, waitFor } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { HITLService, type HITLApprovalResponse, type ExecutionHITLStatus, type HITLActionResponse } from '../../../../api/execution/HITLService';
import BuilderRunApprovals from './BuilderRunApprovals';
import { BuilderPreviewContext } from './BuilderPreviewContext';

vi.mock('../../../../api/execution/HITLService', () => ({ HITLService: {
  getExecutionHITLStatus: vi.fn(), approveGate: vi.fn(), rejectGate: vi.fn(), getApproval: vi.fn(),
} }));
const api = vi.mocked(HITLService);
const approval = (kind = 'flow_gate'): HITLApprovalResponse => ({ id: 7, execution_id: 'job', status: 'pending', is_expired: false,
  gate_config: { kind, step_name: 'Send the report', task_name: 'Research', tool_name: 'Send email' },
  has_previous_crew_output: true,
} as HITLApprovalResponse);
const status = (item: HITLApprovalResponse): ExecutionHITLStatus => ({ execution_id: 'job', has_pending_approval: item.status === 'pending',
  pending_approval: item.status === 'pending' ? item : null, approval_history: [item], total_gates_passed: 0,
});
beforeEach(() => { vi.clearAllMocks(); api.getExecutionHITLStatus.mockResolvedValue(status(approval())); api.approveGate.mockResolvedValue({ success: true } as HITLActionResponse); api.rejectGate.mockResolvedValue({ success: true } as HITLActionResponse); });

describe('builder conversation approvals', () => {
  it.each([true, false])('polls pending approvals without SSE (running=%s) and stops after unmount', async running => {
    vi.useFakeTimers();
    try {
      const view = render(<BuilderRunApprovals jobId="job" running={running} />);
      await act(async () => { await Promise.resolve(); });
      api.getExecutionHITLStatus.mockClear();
      await act(async () => { await vi.advanceTimersByTimeAsync(5000); });
      expect(api.getExecutionHITLStatus).toHaveBeenCalledTimes(1);
      view.unmount();
      api.getExecutionHITLStatus.mockClear();
      await act(async () => { await vi.advanceTimersByTimeAsync(10000); });
      expect(api.getExecutionHITLStatus).not.toHaveBeenCalled();
    } finally { vi.useRealTimers(); }
  });
  it.each(['flow_gate', 'task_review', 'tool_call'])('restores a pending %s inline and submits exactly one decision', async kind => {
    api.getExecutionHITLStatus.mockResolvedValue(status(approval(kind)));
    render(<BuilderRunApprovals jobId="job" running />);
    const approve = await screen.findByRole('button', { name: 'Approve', exact: true });
    expect(screen.getAllByRole('button', { name: 'Approve', exact: true })).toHaveLength(1);
    expect(screen.queryByRole('dialog')).toBeNull();
    api.getExecutionHITLStatus.mockResolvedValue(status({ ...approval(kind), status: 'approved' } as HITLApprovalResponse));
    fireEvent.click(approve);
    await waitFor(() => expect(api.approveGate).toHaveBeenCalledWith(7, {}));
    expect(await screen.findByText('— approved')).toBeVisible();
    expect(screen.queryByRole('button', { name: 'Approve', exact: true })).toBeNull();
  });
  it('sends task-review feedback from the conversation', async () => {
    api.getExecutionHITLStatus.mockResolvedValue(status(approval('task_review')));
    render(<BuilderRunApprovals jobId="job" running />);
    fireEvent.click(await screen.findByRole('button', { name: 'Request changes' }));
    fireEvent.change(screen.getByRole('textbox'), { target: { value: 'Include the sources' } });
    fireEvent.click(screen.getByRole('button', { name: 'Send', exact: true }));
    await waitFor(() => expect(api.rejectGate).toHaveBeenCalledWith(7, { reason: 'Include the sources' }));
    expect(await screen.findByText(/changes requested: Include the sources/)).toBeVisible();
  });
  it('refreshes on matching live events and ignores another execution', async () => {
    render(<BuilderRunApprovals jobId="job" running />);
    await screen.findByRole('button', { name: 'Approve', exact: true });
    api.getExecutionHITLStatus.mockClear();
    act(() => window.dispatchEvent(new CustomEvent('hitlRequest', { detail: { job_id: 'other' } })));
    expect(api.getExecutionHITLStatus).not.toHaveBeenCalled();
    act(() => window.dispatchEvent(new CustomEvent('hitlRequest', { detail: { job_id: 'job' } })));
    await waitFor(() => expect(api.getExecutionHITLStatus).toHaveBeenCalledWith('job'));
  });
  it('shows decisions from history and no actions for expired gates', async () => {
    api.getExecutionHITLStatus.mockResolvedValue(status({ ...approval(), status: 'timeout' } as HITLApprovalResponse));
    render(<BuilderRunApprovals jobId="job" running={false} />);
    expect(await screen.findByText('— Approval expired')).toBeVisible();
    expect(screen.queryByRole('button', { name: 'Approve', exact: true })).toBeNull();
  });
  it('loads the output only on demand and opens it beside the conversation', async () => {
    api.getApproval.mockResolvedValue({ ...approval(), previous_crew_output: 'Full report for review' });
    const openResult = vi.fn();
    render(<BuilderPreviewContext.Provider value={{ openMemory: vi.fn(), openStep: vi.fn(), openResult }}><BuilderRunApprovals jobId="job" running /></BuilderPreviewContext.Provider>);
    fireEvent.click(await screen.findByRole('button', { name: 'Review output' }));
    await waitFor(() => expect(openResult).toHaveBeenCalledWith(expect.objectContaining({ data: 'Full report for review', sourceMessageId: 'job' })));
    expect(api.getApproval).toHaveBeenCalledWith(7, 'ui');
  });
  it('does not restore stale approval data after switching runs', async () => {
    let finish!: (value: ExecutionHITLStatus) => void;
    api.getExecutionHITLStatus.mockImplementationOnce(() => new Promise(resolve => { finish = resolve; }));
    const view = render(<BuilderRunApprovals jobId="job" running />);
    api.getExecutionHITLStatus.mockResolvedValue({ approval_history: [] } as unknown as ExecutionHITLStatus);
    view.rerender(<BuilderRunApprovals jobId="new-job" running />);
    await act(async () => finish(status(approval())));
    expect(screen.queryByRole('button', { name: 'Approve', exact: true })).toBeNull();
  });
  it('keeps the request actionable after a failed submission', async () => {
    api.approveGate.mockRejectedValue(new Error('Permission denied'));
    render(<BuilderRunApprovals jobId="job" running />);
    fireEvent.click(await screen.findByRole('button', { name: 'Approve', exact: true }));
    expect(await screen.findByText(/Permission denied/)).toBeVisible();
    expect(screen.getByRole('button', { name: 'Approve', exact: true })).toBeEnabled();
  });
});
