import { act, render, screen, waitFor } from '@testing-library/react';
import { beforeEach, expect, it, vi } from 'vitest';
import ToolApprovalListener from './ToolApprovalListener';
const state = vi.hoisted(() => ({ tabs: [] as Array<{ executionJobIds: string[] }> }));
const getStatus = vi.hoisted(() => vi.fn());
vi.mock('../../store/tabManager', () => ({ useTabManagerStore: { getState: () => state } }));
vi.mock('../../features/chat/store/executionStore', () => ({ useExecutionStore: { getState: () => ({ jobOwnerOf: () => null }) } }));
vi.mock('../../api/execution/HITLService', () => ({ HITLService: { getExecutionHITLStatus: getStatus } }));
vi.mock('../../features/approvals/components/HITLApprovalDialog', () => ({ default: () => <div role="dialog">Approval</div> }));
beforeEach(() => { vi.clearAllMocks(); state.tabs = []; getStatus.mockResolvedValue({ has_pending_approval: true, pending_approval: { is_expired: false } }); });
it('leaves builder-owned approvals in their conversations', () => {
  state.tabs = [{ executionJobIds: ['builder-job'] }];
  render(<ToolApprovalListener />);
  act(() => window.dispatchEvent(new CustomEvent('hitlRequest', { detail: { job_id: 'builder-job' } })));
  expect(getStatus).not.toHaveBeenCalled();
  expect(screen.queryByRole('dialog')).toBeNull();
});
it('keeps dialog support for executions outside builder sessions', async () => {
  render(<ToolApprovalListener />);
  act(() => window.dispatchEvent(new CustomEvent('hitlRequest', { detail: { job_id: 'external-job' } })));
  await waitFor(() => expect(screen.getByRole('dialog')).toBeVisible());
});
