/**
 * Unit tests for HITLApprovalDialog component.
 *
 * Tests the functionality of the HITL approval dialog including
 * loading states, approval/rejection workflows, and error handling.
 */
import React from 'react';
import { render, screen, fireEvent, waitFor, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { ThemeProvider } from '@mui/material/styles';
import { createTheme } from '@mui/material/styles';
import { vi, describe, it, expect, beforeEach, afterEach } from 'vitest';

import HITLApprovalDialog from '../HITLApprovalDialog';
import {
  HITLApprovalStatus,
  HITLRejectionAction,
  HITLApprovalResponse,
  ExecutionHITLStatus,
} from '../../../api/HITLService';

// Must use vi.hoisted for variables referenced in vi.mock
const mocks = vi.hoisted(() => ({
  mockGetExecutionHITLStatus: vi.fn(),
  mockApproveGate: vi.fn(),
  mockRejectGate: vi.fn(),
}));

// Mock HITLService
vi.mock('../../../api/HITLService', async (importOriginal) => {
  const actual = await importOriginal<typeof import('../../../api/HITLService')>();
  return {
    ...actual,
    HITLService: {
      getExecutionHITLStatus: mocks.mockGetExecutionHITLStatus,
      approveGate: mocks.mockApproveGate,
      rejectGate: mocks.mockRejectGate,
    },
  };
});

const theme = createTheme();

const TestWrapper: React.FC<{ children: React.ReactNode }> = ({ children }) => (
  <ThemeProvider theme={theme}>
    {children}
  </ThemeProvider>
);

// Mock data
const createMockApproval = (overrides: Partial<HITLApprovalResponse> = {}): HITLApprovalResponse => ({
  id: 1,
  execution_id: 'exec-123',
  flow_id: 'flow-456',
  gate_node_id: 'gate-node-789',
  crew_sequence: 1,
  status: HITLApprovalStatus.PENDING,
  gate_config: { message: 'Please review and approve this task' },
  previous_crew_name: 'Research Crew',
  previous_crew_output: 'Task completed successfully with results...',
  flow_state_snapshot: {},
  responded_by: null,
  responded_at: null,
  approval_comment: null,
  rejection_reason: null,
  rejection_action: null,
  expires_at: new Date(Date.now() + 24 * 60 * 60 * 1000).toISOString(), // 24 hours from now
  is_expired: false,
  created_at: new Date().toISOString(),
  group_id: 'group-1',
  ...overrides,
});

const createMockExecutionStatus = (
  pendingApproval: HITLApprovalResponse | null = null
): ExecutionHITLStatus => ({
  execution_id: 'exec-123',
  has_pending_approval: pendingApproval !== null,
  pending_approval: pendingApproval,
  approval_history: [],
  total_gates_passed: 0,
});

describe('HITLApprovalDialog', () => {
  const defaultProps = {
    open: true,
    executionId: 'exec-123',
    onClose: vi.fn(),
    onActionComplete: vi.fn(),
  };

  beforeEach(() => {
    vi.clearAllMocks();
    vi.useFakeTimers({ shouldAdvanceTime: true });
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  describe('Loading State', () => {
    it('shows loading indicator while fetching approval', async () => {
      mocks.mockGetExecutionHITLStatus.mockImplementation(
        () => new Promise((resolve) => setTimeout(() => resolve(createMockExecutionStatus()), 1000))
      );

      render(
        <TestWrapper>
          <HITLApprovalDialog {...defaultProps} />
        </TestWrapper>
      );

      expect(screen.getByRole('progressbar')).toBeInTheDocument();
      expect(screen.getByText('Loading approval details...')).toBeInTheDocument();
    });

    it('shows close button during loading', async () => {
      mocks.mockGetExecutionHITLStatus.mockImplementation(
        () => new Promise((resolve) => setTimeout(() => resolve(createMockExecutionStatus()), 1000))
      );

      render(
        <TestWrapper>
          <HITLApprovalDialog {...defaultProps} />
        </TestWrapper>
      );

      expect(screen.getByRole('button', { name: /close/i })).toBeInTheDocument();
    });
  });

  describe('Dialog Title', () => {
    it('shows "Human Approval Required" as default title', async () => {
      const mockApproval = createMockApproval();
      mocks.mockGetExecutionHITLStatus.mockResolvedValue(createMockExecutionStatus(mockApproval));

      render(
        <TestWrapper>
          <HITLApprovalDialog {...defaultProps} />
        </TestWrapper>
      );

      await waitFor(() => {
        expect(screen.getByText('Human Approval Required')).toBeInTheDocument();
      });
    });
  });

  describe('Approval Details Display', () => {
    it('displays approval message from gate config', async () => {
      const mockApproval = createMockApproval({
        gate_config: { message: 'Custom approval message' },
      });
      mocks.mockGetExecutionHITLStatus.mockResolvedValue(createMockExecutionStatus(mockApproval));

      render(
        <TestWrapper>
          <HITLApprovalDialog {...defaultProps} />
        </TestWrapper>
      );

      await waitFor(() => {
        expect(screen.getByText('Custom approval message')).toBeInTheDocument();
      });
    });

    it('displays default message when gate_config.message is not set', async () => {
      const mockApproval = createMockApproval({
        gate_config: {},
      });
      mocks.mockGetExecutionHITLStatus.mockResolvedValue(createMockExecutionStatus(mockApproval));

      render(
        <TestWrapper>
          <HITLApprovalDialog {...defaultProps} />
        </TestWrapper>
      );

      await waitFor(() => {
        expect(screen.getByText('Approval Required')).toBeInTheDocument();
      });
    });

    it('displays previous crew name', async () => {
      const mockApproval = createMockApproval({
        previous_crew_name: 'Data Analysis Crew',
      });
      mocks.mockGetExecutionHITLStatus.mockResolvedValue(createMockExecutionStatus(mockApproval));

      render(
        <TestWrapper>
          <HITLApprovalDialog {...defaultProps} />
        </TestWrapper>
      );

      await waitFor(() => {
        expect(screen.getByText(/Waiting after:/)).toBeInTheDocument();
        expect(screen.getByText('Data Analysis Crew')).toBeInTheDocument();
      });
    });

    it('displays previous crew output', async () => {
      const mockApproval = createMockApproval({
        previous_crew_output: 'The analysis results are complete.',
      });
      mocks.mockGetExecutionHITLStatus.mockResolvedValue(createMockExecutionStatus(mockApproval));

      render(
        <TestWrapper>
          <HITLApprovalDialog {...defaultProps} />
        </TestWrapper>
      );

      await waitFor(() => {
        expect(screen.getByText('Previous Crew Output:')).toBeInTheDocument();
        expect(screen.getByText('The analysis results are complete.')).toBeInTheDocument();
      });
    });

    it('displays status chip', async () => {
      const mockApproval = createMockApproval({
        status: HITLApprovalStatus.PENDING,
      });
      mocks.mockGetExecutionHITLStatus.mockResolvedValue(createMockExecutionStatus(mockApproval));

      render(
        <TestWrapper>
          <HITLApprovalDialog {...defaultProps} />
        </TestWrapper>
      );

      await waitFor(() => {
        expect(screen.getByText('pending')).toBeInTheDocument();
      });
    });

    it('displays time remaining chip', async () => {
      const mockApproval = createMockApproval({
        expires_at: new Date(Date.now() + 2 * 60 * 60 * 1000).toISOString(), // 2 hours
      });
      mocks.mockGetExecutionHITLStatus.mockResolvedValue(createMockExecutionStatus(mockApproval));

      render(
        <TestWrapper>
          <HITLApprovalDialog {...defaultProps} />
        </TestWrapper>
      );

      await waitFor(() => {
        // Check for time remaining pattern
        expect(screen.getByText(/remaining/)).toBeInTheDocument();
      });
    });

    it('shows "Expired" when approval is expired', async () => {
      const mockApproval = createMockApproval({
        expires_at: new Date(Date.now() - 1000).toISOString(), // 1 second ago
        is_expired: true,
      });
      mocks.mockGetExecutionHITLStatus.mockResolvedValue(createMockExecutionStatus(mockApproval));

      render(
        <TestWrapper>
          <HITLApprovalDialog {...defaultProps} />
        </TestWrapper>
      );

      await waitFor(() => {
        expect(screen.getByText('Expired')).toBeInTheDocument();
      });
    });

    it('shows "No expiry" when expires_at is null', async () => {
      const mockApproval = createMockApproval({
        expires_at: null,
      });
      mocks.mockGetExecutionHITLStatus.mockResolvedValue(createMockExecutionStatus(mockApproval));

      render(
        <TestWrapper>
          <HITLApprovalDialog {...defaultProps} />
        </TestWrapper>
      );

      await waitFor(() => {
        expect(screen.getByText('No expiry')).toBeInTheDocument();
      });
    });
  });

  describe('No Pending Approval', () => {
    it('shows error alert when no pending approval exists', async () => {
      mocks.mockGetExecutionHITLStatus.mockResolvedValue(createMockExecutionStatus(null));

      render(
        <TestWrapper>
          <HITLApprovalDialog {...defaultProps} />
        </TestWrapper>
      );

      await waitFor(() => {
        // The component sets error when no pending_approval, so it shows error alert
        expect(screen.getByText('No pending approval found for this execution')).toBeInTheDocument();
      });
    });

    it('shows refresh button when no approval found', async () => {
      mocks.mockGetExecutionHITLStatus.mockResolvedValue(createMockExecutionStatus(null));

      render(
        <TestWrapper>
          <HITLApprovalDialog {...defaultProps} />
        </TestWrapper>
      );

      await waitFor(() => {
        expect(screen.getByRole('button', { name: /refresh/i })).toBeInTheDocument();
      });
    });
  });

  describe('Approve Action', () => {
    it('shows approve and reject buttons when approval exists', async () => {
      const mockApproval = createMockApproval();
      mocks.mockGetExecutionHITLStatus.mockResolvedValue(createMockExecutionStatus(mockApproval));

      render(
        <TestWrapper>
          <HITLApprovalDialog {...defaultProps} />
        </TestWrapper>
      );

      await waitFor(() => {
        expect(screen.getByRole('button', { name: /approve/i })).toBeInTheDocument();
        expect(screen.getByRole('button', { name: /reject/i })).toBeInTheDocument();
      });
    });

    it('switches to approve form when Approve button is clicked', async () => {
      const mockApproval = createMockApproval();
      mocks.mockGetExecutionHITLStatus.mockResolvedValue(createMockExecutionStatus(mockApproval));

      render(
        <TestWrapper>
          <HITLApprovalDialog {...defaultProps} />
        </TestWrapper>
      );

      await waitFor(() => {
        expect(screen.getByRole('button', { name: /approve/i })).toBeInTheDocument();
      });

      fireEvent.click(screen.getByRole('button', { name: /approve/i }));

      await waitFor(() => {
        expect(screen.getByText('Approving this gate will resume the flow execution.')).toBeInTheDocument();
        expect(screen.getByLabelText(/comment/i)).toBeInTheDocument();
      });
    });

    it('shows Approve Gate as title when in approve mode', async () => {
      const mockApproval = createMockApproval();
      mocks.mockGetExecutionHITLStatus.mockResolvedValue(createMockExecutionStatus(mockApproval));

      render(
        <TestWrapper>
          <HITLApprovalDialog {...defaultProps} />
        </TestWrapper>
      );

      await waitFor(() => {
        expect(screen.getByRole('button', { name: /approve/i })).toBeInTheDocument();
      });

      fireEvent.click(screen.getByRole('button', { name: /approve/i }));

      await waitFor(() => {
        expect(screen.getByText('Approve Gate')).toBeInTheDocument();
      });
    });

    it('calls approveGate when confirming approval', async () => {
      const mockApproval = createMockApproval();
      mocks.mockGetExecutionHITLStatus.mockResolvedValue(createMockExecutionStatus(mockApproval));
      mocks.mockApproveGate.mockResolvedValue({
        success: true,
        approval_id: 1,
        status: HITLApprovalStatus.APPROVED,
        message: 'Approved',
        execution_resumed: true,
      });

      render(
        <TestWrapper>
          <HITLApprovalDialog {...defaultProps} />
        </TestWrapper>
      );

      await waitFor(() => {
        expect(screen.getByRole('button', { name: /approve/i })).toBeInTheDocument();
      });

      fireEvent.click(screen.getByRole('button', { name: /approve/i }));

      await waitFor(() => {
        expect(screen.getByRole('button', { name: /confirm approval/i })).toBeInTheDocument();
      });

      fireEvent.click(screen.getByRole('button', { name: /confirm approval/i }));

      await waitFor(() => {
        expect(mocks.mockApproveGate).toHaveBeenCalledWith(1, { comment: undefined });
      });
    });

    it('calls onActionComplete with approve after successful approval', async () => {
      const mockApproval = createMockApproval();
      mocks.mockGetExecutionHITLStatus.mockResolvedValue(createMockExecutionStatus(mockApproval));
      mocks.mockApproveGate.mockResolvedValue({
        success: true,
        approval_id: 1,
        status: HITLApprovalStatus.APPROVED,
        message: 'Approved',
        execution_resumed: true,
      });

      render(
        <TestWrapper>
          <HITLApprovalDialog {...defaultProps} />
        </TestWrapper>
      );

      await waitFor(() => {
        expect(screen.getByRole('button', { name: /approve/i })).toBeInTheDocument();
      });

      fireEvent.click(screen.getByRole('button', { name: /approve/i }));

      await waitFor(() => {
        expect(screen.getByRole('button', { name: /confirm approval/i })).toBeInTheDocument();
      });

      fireEvent.click(screen.getByRole('button', { name: /confirm approval/i }));

      await waitFor(() => {
        expect(defaultProps.onActionComplete).toHaveBeenCalledWith('approve');
        expect(defaultProps.onClose).toHaveBeenCalled();
      });
    });

    it('includes comment when provided during approval', async () => {
      const mockApproval = createMockApproval();
      mocks.mockGetExecutionHITLStatus.mockResolvedValue(createMockExecutionStatus(mockApproval));
      mocks.mockApproveGate.mockResolvedValue({
        success: true,
        approval_id: 1,
        status: HITLApprovalStatus.APPROVED,
        message: 'Approved',
        execution_resumed: true,
      });

      render(
        <TestWrapper>
          <HITLApprovalDialog {...defaultProps} />
        </TestWrapper>
      );

      await waitFor(() => {
        expect(screen.getByRole('button', { name: /approve/i })).toBeInTheDocument();
      });

      fireEvent.click(screen.getByRole('button', { name: /approve/i }));

      await waitFor(() => {
        expect(screen.getByLabelText(/comment/i)).toBeInTheDocument();
      });

      const commentInput = screen.getByLabelText(/comment/i);
      fireEvent.change(commentInput, { target: { value: 'Looks good to proceed' } });

      fireEvent.click(screen.getByRole('button', { name: /confirm approval/i }));

      await waitFor(() => {
        expect(mocks.mockApproveGate).toHaveBeenCalledWith(1, { comment: 'Looks good to proceed' });
      });
    });

    it('shows back button when in approve mode', async () => {
      const mockApproval = createMockApproval();
      mocks.mockGetExecutionHITLStatus.mockResolvedValue(createMockExecutionStatus(mockApproval));

      render(
        <TestWrapper>
          <HITLApprovalDialog {...defaultProps} />
        </TestWrapper>
      );

      await waitFor(() => {
        expect(screen.getByRole('button', { name: /approve/i })).toBeInTheDocument();
      });

      fireEvent.click(screen.getByRole('button', { name: /approve/i }));

      await waitFor(() => {
        expect(screen.getByRole('button', { name: /back/i })).toBeInTheDocument();
      });
    });

    it('returns to default view when back is clicked', async () => {
      const mockApproval = createMockApproval();
      mocks.mockGetExecutionHITLStatus.mockResolvedValue(createMockExecutionStatus(mockApproval));

      render(
        <TestWrapper>
          <HITLApprovalDialog {...defaultProps} />
        </TestWrapper>
      );

      await waitFor(() => {
        expect(screen.getByRole('button', { name: /approve/i })).toBeInTheDocument();
      });

      fireEvent.click(screen.getByRole('button', { name: /approve/i }));

      await waitFor(() => {
        expect(screen.getByRole('button', { name: /back/i })).toBeInTheDocument();
      });

      fireEvent.click(screen.getByRole('button', { name: /back/i }));

      await waitFor(() => {
        expect(screen.getByText('Human Approval Required')).toBeInTheDocument();
      });
    });
  });

  describe('Reject Action', () => {
    it('switches to reject form when Reject button is clicked', async () => {
      const mockApproval = createMockApproval();
      mocks.mockGetExecutionHITLStatus.mockResolvedValue(createMockExecutionStatus(mockApproval));

      render(
        <TestWrapper>
          <HITLApprovalDialog {...defaultProps} />
        </TestWrapper>
      );

      await waitFor(() => {
        expect(screen.getByRole('button', { name: /reject/i })).toBeInTheDocument();
      });

      fireEvent.click(screen.getByRole('button', { name: /reject/i }));

      await waitFor(() => {
        expect(screen.getByText('Please provide a reason for rejection.')).toBeInTheDocument();
        expect(screen.getByLabelText(/rejection reason/i)).toBeInTheDocument();
      });
    });

    it('shows Reject Gate as title when in reject mode', async () => {
      const mockApproval = createMockApproval();
      mocks.mockGetExecutionHITLStatus.mockResolvedValue(createMockExecutionStatus(mockApproval));

      render(
        <TestWrapper>
          <HITLApprovalDialog {...defaultProps} />
        </TestWrapper>
      );

      await waitFor(() => {
        expect(screen.getByRole('button', { name: /reject/i })).toBeInTheDocument();
      });

      fireEvent.click(screen.getByRole('button', { name: /reject/i }));

      await waitFor(() => {
        expect(screen.getByText('Reject Gate')).toBeInTheDocument();
      });
    });

    it('disables Confirm Rejection button when reason is empty', async () => {
      const mockApproval = createMockApproval();
      mocks.mockGetExecutionHITLStatus.mockResolvedValue(createMockExecutionStatus(mockApproval));

      render(
        <TestWrapper>
          <HITLApprovalDialog {...defaultProps} />
        </TestWrapper>
      );

      await waitFor(() => {
        expect(screen.getByRole('button', { name: /reject/i })).toBeInTheDocument();
      });

      fireEvent.click(screen.getByRole('button', { name: /reject/i }));

      await waitFor(() => {
        const confirmButton = screen.getByRole('button', { name: /confirm rejection/i });
        expect(confirmButton).toBeDisabled();
      });
    });

    it('enables Confirm Rejection button when reason is provided', async () => {
      const mockApproval = createMockApproval();
      mocks.mockGetExecutionHITLStatus.mockResolvedValue(createMockExecutionStatus(mockApproval));

      render(
        <TestWrapper>
          <HITLApprovalDialog {...defaultProps} />
        </TestWrapper>
      );

      await waitFor(() => {
        expect(screen.getByRole('button', { name: /reject/i })).toBeInTheDocument();
      });

      fireEvent.click(screen.getByRole('button', { name: /reject/i }));

      await waitFor(() => {
        expect(screen.getByLabelText(/rejection reason/i)).toBeInTheDocument();
      });

      const reasonInput = screen.getByLabelText(/rejection reason/i);
      fireEvent.change(reasonInput, { target: { value: 'Data quality issues' } });

      await waitFor(() => {
        const confirmButton = screen.getByRole('button', { name: /confirm rejection/i });
        expect(confirmButton).not.toBeDisabled();
      });
    });

    it('calls rejectGate when confirming rejection', async () => {
      const mockApproval = createMockApproval();
      mocks.mockGetExecutionHITLStatus.mockResolvedValue(createMockExecutionStatus(mockApproval));
      mocks.mockRejectGate.mockResolvedValue({
        success: true,
        approval_id: 1,
        status: HITLApprovalStatus.REJECTED,
        message: 'Rejected',
        execution_resumed: false,
      });

      render(
        <TestWrapper>
          <HITLApprovalDialog {...defaultProps} />
        </TestWrapper>
      );

      await waitFor(() => {
        expect(screen.getByRole('button', { name: /reject/i })).toBeInTheDocument();
      });

      fireEvent.click(screen.getByRole('button', { name: /reject/i }));

      await waitFor(() => {
        expect(screen.getByLabelText(/rejection reason/i)).toBeInTheDocument();
      });

      fireEvent.change(screen.getByLabelText(/rejection reason/i), {
        target: { value: 'Need more information' },
      });

      fireEvent.click(screen.getByRole('button', { name: /confirm rejection/i }));

      await waitFor(() => {
        expect(mocks.mockRejectGate).toHaveBeenCalledWith(1, {
          reason: 'Need more information',
          action: HITLRejectionAction.REJECT,
        });
      });
    });

    it('calls onActionComplete with reject after successful rejection', async () => {
      const mockApproval = createMockApproval();
      mocks.mockGetExecutionHITLStatus.mockResolvedValue(createMockExecutionStatus(mockApproval));
      mocks.mockRejectGate.mockResolvedValue({
        success: true,
        approval_id: 1,
        status: HITLApprovalStatus.REJECTED,
        message: 'Rejected',
        execution_resumed: false,
      });

      render(
        <TestWrapper>
          <HITLApprovalDialog {...defaultProps} />
        </TestWrapper>
      );

      await waitFor(() => {
        expect(screen.getByRole('button', { name: /reject/i })).toBeInTheDocument();
      });

      fireEvent.click(screen.getByRole('button', { name: /reject/i }));

      await waitFor(() => {
        expect(screen.getByLabelText(/rejection reason/i)).toBeInTheDocument();
      });

      fireEvent.change(screen.getByLabelText(/rejection reason/i), {
        target: { value: 'Rejected' },
      });

      fireEvent.click(screen.getByRole('button', { name: /confirm rejection/i }));

      await waitFor(() => {
        expect(defaultProps.onActionComplete).toHaveBeenCalledWith('reject');
        expect(defaultProps.onClose).toHaveBeenCalled();
      });
    });

    it('shows rejection action dropdown with options', async () => {
      const mockApproval = createMockApproval();
      mocks.mockGetExecutionHITLStatus.mockResolvedValue(createMockExecutionStatus(mockApproval));

      render(
        <TestWrapper>
          <HITLApprovalDialog {...defaultProps} />
        </TestWrapper>
      );

      await waitFor(() => {
        expect(screen.getByRole('button', { name: /reject/i })).toBeInTheDocument();
      });

      fireEvent.click(screen.getByRole('button', { name: /reject/i }));

      await waitFor(() => {
        // The Action label is rendered multiple times by MUI (label + legend)
        const actionLabels = screen.getAllByText('Action');
        expect(actionLabels.length).toBeGreaterThan(0);
      });
    });
  });

  describe('Expired Approval', () => {
    it('disables Approve button when approval is expired', async () => {
      const mockApproval = createMockApproval({
        is_expired: true,
        expires_at: new Date(Date.now() - 1000).toISOString(),
      });
      mocks.mockGetExecutionHITLStatus.mockResolvedValue(createMockExecutionStatus(mockApproval));

      render(
        <TestWrapper>
          <HITLApprovalDialog {...defaultProps} />
        </TestWrapper>
      );

      await waitFor(() => {
        const approveButton = screen.getByRole('button', { name: /approve/i });
        expect(approveButton).toBeDisabled();
      });
    });

    it('disables Reject button when approval is expired', async () => {
      const mockApproval = createMockApproval({
        is_expired: true,
        expires_at: new Date(Date.now() - 1000).toISOString(),
      });
      mocks.mockGetExecutionHITLStatus.mockResolvedValue(createMockExecutionStatus(mockApproval));

      render(
        <TestWrapper>
          <HITLApprovalDialog {...defaultProps} />
        </TestWrapper>
      );

      await waitFor(() => {
        const rejectButton = screen.getByRole('button', { name: /reject/i });
        expect(rejectButton).toBeDisabled();
      });
    });
  });

  describe('Error Handling', () => {
    it('shows error alert when fetching approval fails', async () => {
      mocks.mockGetExecutionHITLStatus.mockRejectedValue(new Error('Network error'));

      render(
        <TestWrapper>
          <HITLApprovalDialog {...defaultProps} />
        </TestWrapper>
      );

      await waitFor(() => {
        expect(screen.getByText('Network error')).toBeInTheDocument();
      });
    });

    it('shows retry button when fetch fails', async () => {
      mocks.mockGetExecutionHITLStatus.mockRejectedValue(new Error('Network error'));

      render(
        <TestWrapper>
          <HITLApprovalDialog {...defaultProps} />
        </TestWrapper>
      );

      await waitFor(() => {
        expect(screen.getByRole('button', { name: /retry/i })).toBeInTheDocument();
      });
    });

    it('retries fetch when retry button is clicked', async () => {
      mocks.mockGetExecutionHITLStatus.mockRejectedValueOnce(new Error('Network error'));
      const mockApproval = createMockApproval();
      mocks.mockGetExecutionHITLStatus.mockResolvedValueOnce(createMockExecutionStatus(mockApproval));

      render(
        <TestWrapper>
          <HITLApprovalDialog {...defaultProps} />
        </TestWrapper>
      );

      await waitFor(() => {
        expect(screen.getByRole('button', { name: /retry/i })).toBeInTheDocument();
      });

      fireEvent.click(screen.getByRole('button', { name: /retry/i }));

      await waitFor(() => {
        expect(mocks.mockGetExecutionHITLStatus).toHaveBeenCalledTimes(2);
      });
    });

    it('shows error when approval action fails', async () => {
      const mockApproval = createMockApproval();
      mocks.mockGetExecutionHITLStatus.mockResolvedValue(createMockExecutionStatus(mockApproval));
      mocks.mockApproveGate.mockRejectedValue(new Error('Approval failed'));

      render(
        <TestWrapper>
          <HITLApprovalDialog {...defaultProps} />
        </TestWrapper>
      );

      await waitFor(() => {
        expect(screen.getByRole('button', { name: /approve/i })).toBeInTheDocument();
      });

      fireEvent.click(screen.getByRole('button', { name: /approve/i }));

      await waitFor(() => {
        expect(screen.getByRole('button', { name: /confirm approval/i })).toBeInTheDocument();
      });

      fireEvent.click(screen.getByRole('button', { name: /confirm approval/i }));

      await waitFor(() => {
        expect(screen.getByText('Approval failed')).toBeInTheDocument();
      });
    });

    it('shows error when rejection action fails', async () => {
      const mockApproval = createMockApproval();
      mocks.mockGetExecutionHITLStatus.mockResolvedValue(createMockExecutionStatus(mockApproval));
      mocks.mockRejectGate.mockRejectedValue(new Error('Rejection failed'));

      render(
        <TestWrapper>
          <HITLApprovalDialog {...defaultProps} />
        </TestWrapper>
      );

      await waitFor(() => {
        expect(screen.getByRole('button', { name: /reject/i })).toBeInTheDocument();
      });

      fireEvent.click(screen.getByRole('button', { name: /reject/i }));

      await waitFor(() => {
        expect(screen.getByLabelText(/rejection reason/i)).toBeInTheDocument();
      });

      fireEvent.change(screen.getByLabelText(/rejection reason/i), {
        target: { value: 'Rejected' },
      });

      fireEvent.click(screen.getByRole('button', { name: /confirm rejection/i }));

      await waitFor(() => {
        expect(screen.getByText('Rejection failed')).toBeInTheDocument();
      });
    });
  });

  describe('Dialog Close', () => {
    it('calls onClose when close button is clicked', async () => {
      const mockApproval = createMockApproval();
      mocks.mockGetExecutionHITLStatus.mockResolvedValue(createMockExecutionStatus(mockApproval));

      render(
        <TestWrapper>
          <HITLApprovalDialog {...defaultProps} />
        </TestWrapper>
      );

      await waitFor(() => {
        expect(screen.getByText('Human Approval Required')).toBeInTheDocument();
      });

      const closeButtons = screen.getAllByRole('button');
      const closeButton = closeButtons.find(
        (btn) => btn.querySelector('svg[data-testid="CloseIcon"]')
      );

      if (closeButton) {
        fireEvent.click(closeButton);
      }

      expect(defaultProps.onClose).toHaveBeenCalled();
    });

    it('calls onClose when Cancel button is clicked', async () => {
      const mockApproval = createMockApproval();
      mocks.mockGetExecutionHITLStatus.mockResolvedValue(createMockExecutionStatus(mockApproval));

      render(
        <TestWrapper>
          <HITLApprovalDialog {...defaultProps} />
        </TestWrapper>
      );

      await waitFor(() => {
        expect(screen.getByRole('button', { name: /cancel/i })).toBeInTheDocument();
      });

      fireEvent.click(screen.getByRole('button', { name: /cancel/i }));

      expect(defaultProps.onClose).toHaveBeenCalled();
    });
  });

  describe('Dialog Not Open', () => {
    it('does not fetch approval when dialog is not open', () => {
      render(
        <TestWrapper>
          <HITLApprovalDialog {...defaultProps} open={false} />
        </TestWrapper>
      );

      expect(mocks.mockGetExecutionHITLStatus).not.toHaveBeenCalled();
    });
  });

  describe('Empty Execution ID', () => {
    it('does not fetch approval when executionId is empty', () => {
      render(
        <TestWrapper>
          <HITLApprovalDialog {...defaultProps} executionId="" />
        </TestWrapper>
      );

      expect(mocks.mockGetExecutionHITLStatus).not.toHaveBeenCalled();
    });
  });
});
