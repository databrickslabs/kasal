/**
 * Inline approval line — approve/deny for tool_call gates and
 * approve/request-changes for task_review gates. Denying grows the SAME
 * activity line with an inline feedback input whose text becomes the
 * rejectGate reason (for task_review the backend re-runs the task with it).
 */
import React from 'react';
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';

const mockApproveGate = vi.fn();
const mockRejectGate = vi.fn();
vi.mock('../../../../api/execution/HITLService', () => ({
  HITLService: {
    approveGate: (...args: unknown[]) => mockApproveGate(...args),
    rejectGate: (...args: unknown[]) => mockRejectGate(...args),
  },
}));

const updateMessage = vi.fn();
vi.mock('../../store/sessionStore', () => ({
  useSessionStore: (selector: (s: { updateMessage: typeof updateMessage }) => unknown) =>
    selector({ updateMessage }),
}));

import ToolApprovalCard, { ToolApprovalData } from './ToolApprovalCard';

const TOOL_CALL: ToolApprovalData = {
  approval_id: 7,
  kind: 'tool_call',
  tool_name: 'SerperDevTool',
  agent_role: 'Researcher',
  tool_args: { q: 'kasal' },
};

const TASK_REVIEW: ToolApprovalData = {
  approval_id: 9,
  kind: 'task_review',
  task_name: 'Write summary',
  output_preview: 'Draft output…',
};

beforeEach(() => {
  vi.clearAllMocks();
  mockApproveGate.mockResolvedValue({});
  mockRejectGate.mockResolvedValue({});
});

describe('ToolApprovalCard', () => {
  it('approves without any feedback row', async () => {
    render(<ToolApprovalCard data={TOOL_CALL} messageId="m1" />);
    fireEvent.click(screen.getByRole('button', { name: 'Approve' }));

    await waitFor(() => expect(mockApproveGate).toHaveBeenCalledWith(7, {}));
    expect(updateMessage).toHaveBeenCalledWith('m1', {
      resultData: expect.objectContaining({ decided: 'approved' }),
    });
    expect(screen.queryByRole('textbox')).not.toBeInTheDocument();
  });

  it('Deny (tool_call) opens an inline optional-reason input; typed reason is sent', async () => {
    render(<ToolApprovalCard data={TOOL_CALL} messageId="m1" />);
    fireEvent.click(screen.getByRole('button', { name: 'Deny' }));

    // Action links are replaced by the feedback row on the same line.
    expect(screen.queryByRole('button', { name: 'Approve' })).not.toBeInTheDocument();
    const input = screen.getByPlaceholderText('Reason (optional)');
    fireEvent.change(input, { target: { value: 'wrong tool' } });
    fireEvent.click(screen.getByRole('button', { name: 'Send' }));

    await waitFor(() =>
      expect(mockRejectGate).toHaveBeenCalledWith(7, { reason: 'wrong tool' }),
    );
    expect(updateMessage).toHaveBeenCalledWith('m1', {
      resultData: expect.objectContaining({ decided: 'denied', decided_reason: 'wrong tool' }),
    });
  });

  it('empty tool_call reason falls back to the generic reason and persists no echo', async () => {
    render(<ToolApprovalCard data={TOOL_CALL} messageId="m1" />);
    fireEvent.click(screen.getByRole('button', { name: 'Deny' }));
    fireEvent.click(screen.getByRole('button', { name: 'Send' }));

    await waitFor(() =>
      expect(mockRejectGate).toHaveBeenCalledWith(7, { reason: 'Denied from chat' }),
    );
    const persisted = updateMessage.mock.calls[0][1].resultData as ToolApprovalData;
    expect(persisted.decided).toBe('denied');
    expect(persisted.decided_reason).toBeUndefined();
  });

  it('Request changes (task_review): Enter submits the typed feedback as the retry reason', async () => {
    render(<ToolApprovalCard data={TASK_REVIEW} messageId="m2" />);
    fireEvent.click(screen.getByRole('button', { name: 'Request changes' }));

    const input = screen.getByPlaceholderText('What should change?');
    fireEvent.change(input, { target: { value: 'Add citations to every claim' } });
    fireEvent.keyDown(input, { key: 'Enter' });

    await waitFor(() =>
      expect(mockRejectGate).toHaveBeenCalledWith(9, { reason: 'Add citations to every claim' }),
    );
    expect(updateMessage).toHaveBeenCalledWith('m2', {
      resultData: expect.objectContaining({
        decided: 'denied',
        decided_reason: 'Add citations to every claim',
      }),
    });
  });

  it('Escape cancels back to the Approve/Request changes state without submitting', () => {
    render(<ToolApprovalCard data={TASK_REVIEW} messageId="m2" />);
    fireEvent.click(screen.getByRole('button', { name: 'Request changes' }));

    const input = screen.getByPlaceholderText('What should change?');
    fireEvent.change(input, { target: { value: 'never mind' } });
    fireEvent.keyDown(input, { key: 'Escape' });

    expect(mockRejectGate).not.toHaveBeenCalled();
    expect(screen.queryByRole('textbox')).not.toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Approve' })).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Request changes' })).toBeInTheDocument();
  });

  it('decided task_review line echoes the first ~60 chars of the feedback', () => {
    const long = 'x'.repeat(80);
    render(
      <ToolApprovalCard
        data={{ ...TASK_REVIEW, decided: 'denied', decided_reason: long }}
        messageId="m2"
      />,
    );
    expect(
      screen.getByText(`— changes requested: ${'x'.repeat(60)}…`),
    ).toBeInTheDocument();
    expect(screen.queryByRole('button')).not.toBeInTheDocument();
  });

  it('decided task_review without stored feedback keeps the generic retry note', () => {
    render(
      <ToolApprovalCard data={{ ...TASK_REVIEW, decided: 'denied' }} messageId="m2" />,
    );
    expect(screen.getByText('— changes requested, the task retries')).toBeInTheDocument();
  });

  it('shows the error on the line when the reject call fails', async () => {
    mockRejectGate.mockRejectedValue(new Error('gate expired'));
    render(<ToolApprovalCard data={TOOL_CALL} messageId="m1" />);
    fireEvent.click(screen.getByRole('button', { name: 'Deny' }));
    fireEvent.click(screen.getByRole('button', { name: 'Send' }));

    await waitFor(() => expect(screen.getByText(/gate expired/)).toBeInTheDocument());
    expect(updateMessage).not.toHaveBeenCalled();
    // Input stays open so the user can retry or cancel.
    expect(screen.getByPlaceholderText('Reason (optional)')).toBeInTheDocument();
  });
});
