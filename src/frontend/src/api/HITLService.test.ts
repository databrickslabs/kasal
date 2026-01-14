import { vi, Mock, beforeEach, afterEach, describe, it, expect } from 'vitest';
import {
  HITLService,
  HITLApprovalStatus,
  HITLTimeoutAction,
  HITLRejectionAction,
  HITLWebhookEvent,
  HITLApprovalResponse,
  HITLApprovalListResponse,
  HITLActionResponse,
  ExecutionHITLStatus,
  HITLWebhookResponse,
  HITLWebhookListResponse,
  HITLWebhookCreate,
  HITLWebhookUpdate,
  HITLApproveRequest,
  HITLRejectRequest,
} from './HITLService';
import { apiClient } from '../config/api/ApiConfig';

vi.mock('../config/api/ApiConfig', () => ({
  apiClient: {
    get: vi.fn(),
    post: vi.fn(),
    put: vi.fn(),
    delete: vi.fn(),
    patch: vi.fn(),
  },
}));

describe('HITLService', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  afterEach(() => {
    vi.resetAllMocks();
  });

  // ===========================================================================
  // Test Data Factories
  // ===========================================================================

  const createMockApproval = (overrides: Partial<HITLApprovalResponse> = {}): HITLApprovalResponse => ({
    id: 1,
    execution_id: 'exec-123',
    flow_id: 'flow-456',
    gate_node_id: 'gate-789',
    crew_sequence: 1,
    status: HITLApprovalStatus.PENDING,
    gate_config: { message: 'Approve to continue', timeout_seconds: 3600 },
    previous_crew_name: 'Research Crew',
    previous_crew_output: 'Research results here',
    flow_state_snapshot: { currentStep: 1 },
    responded_by: null,
    responded_at: null,
    approval_comment: null,
    rejection_reason: null,
    rejection_action: null,
    expires_at: '2024-12-31T23:59:59Z',
    is_expired: false,
    created_at: '2024-01-01T00:00:00Z',
    group_id: 'group-001',
    ...overrides,
  });

  const createMockWebhook = (overrides: Partial<HITLWebhookResponse> = {}): HITLWebhookResponse => ({
    id: 1,
    name: 'Test Webhook',
    url: 'https://example.com/webhook',
    enabled: true,
    events: [HITLWebhookEvent.GATE_REACHED, HITLWebhookEvent.GATE_APPROVED],
    headers: { 'X-Custom-Header': 'value' },
    group_id: 'group-001',
    created_at: '2024-01-01T00:00:00Z',
    updated_at: null,
    ...overrides,
  });

  // ===========================================================================
  // Approval Endpoints Tests
  // ===========================================================================

  describe('getPendingApprovals', () => {
    it('should fetch pending approvals with default pagination', async () => {
      const mockResponse: HITLApprovalListResponse = {
        items: [createMockApproval()],
        total: 1,
        limit: 50,
        offset: 0,
      };
      (apiClient.get as Mock).mockResolvedValue({ data: mockResponse });

      const result = await HITLService.getPendingApprovals();

      expect(apiClient.get).toHaveBeenCalledWith('/hitl/pending', {
        params: { limit: 50, offset: 0 },
      });
      expect(result).toEqual(mockResponse);
    });

    it('should fetch pending approvals with custom pagination', async () => {
      const mockResponse: HITLApprovalListResponse = {
        items: [createMockApproval(), createMockApproval({ id: 2 })],
        total: 100,
        limit: 10,
        offset: 20,
      };
      (apiClient.get as Mock).mockResolvedValue({ data: mockResponse });

      const result = await HITLService.getPendingApprovals(10, 20);

      expect(apiClient.get).toHaveBeenCalledWith('/hitl/pending', {
        params: { limit: 10, offset: 20 },
      });
      expect(result).toEqual(mockResponse);
      expect(result.items.length).toBe(2);
    });

    it('should handle empty pending approvals list', async () => {
      const mockResponse: HITLApprovalListResponse = {
        items: [],
        total: 0,
        limit: 50,
        offset: 0,
      };
      (apiClient.get as Mock).mockResolvedValue({ data: mockResponse });

      const result = await HITLService.getPendingApprovals();

      expect(result.items).toEqual([]);
      expect(result.total).toBe(0);
    });

    it('should propagate API errors', async () => {
      const error = new Error('Network error');
      (apiClient.get as Mock).mockRejectedValue(error);

      await expect(HITLService.getPendingApprovals()).rejects.toThrow('Network error');
    });
  });

  describe('getApproval', () => {
    it('should fetch a specific approval by ID', async () => {
      const mockApproval = createMockApproval({ id: 42 });
      (apiClient.get as Mock).mockResolvedValue({ data: mockApproval });

      const result = await HITLService.getApproval(42);

      expect(apiClient.get).toHaveBeenCalledWith('/hitl/approvals/42');
      expect(result).toEqual(mockApproval);
      expect(result.id).toBe(42);
    });

    it('should fetch an approved approval', async () => {
      const mockApproval = createMockApproval({
        id: 10,
        status: HITLApprovalStatus.APPROVED,
        responded_by: 'user@example.com',
        responded_at: '2024-01-02T10:00:00Z',
        approval_comment: 'Looks good!',
      });
      (apiClient.get as Mock).mockResolvedValue({ data: mockApproval });

      const result = await HITLService.getApproval(10);

      expect(result.status).toBe(HITLApprovalStatus.APPROVED);
      expect(result.responded_by).toBe('user@example.com');
      expect(result.approval_comment).toBe('Looks good!');
    });

    it('should fetch a rejected approval', async () => {
      const mockApproval = createMockApproval({
        id: 11,
        status: HITLApprovalStatus.REJECTED,
        responded_by: 'admin@example.com',
        responded_at: '2024-01-02T11:00:00Z',
        rejection_reason: 'Data quality issues',
        rejection_action: HITLRejectionAction.REJECT,
      });
      (apiClient.get as Mock).mockResolvedValue({ data: mockApproval });

      const result = await HITLService.getApproval(11);

      expect(result.status).toBe(HITLApprovalStatus.REJECTED);
      expect(result.rejection_reason).toBe('Data quality issues');
      expect(result.rejection_action).toBe(HITLRejectionAction.REJECT);
    });

    it('should handle 404 error for non-existent approval', async () => {
      const error = { response: { status: 404 }, message: 'Not found' };
      (apiClient.get as Mock).mockRejectedValue(error);

      await expect(HITLService.getApproval(9999)).rejects.toEqual(error);
    });
  });

  describe('approveGate', () => {
    it('should approve a gate without comment', async () => {
      const mockResponse: HITLActionResponse = {
        success: true,
        approval_id: 1,
        status: HITLApprovalStatus.APPROVED,
        message: 'Gate approved successfully',
        execution_resumed: true,
      };
      (apiClient.post as Mock).mockResolvedValue({ data: mockResponse });

      const result = await HITLService.approveGate(1);

      expect(apiClient.post).toHaveBeenCalledWith('/hitl/approvals/1/approve', {});
      expect(result).toEqual(mockResponse);
      expect(result.success).toBe(true);
      expect(result.execution_resumed).toBe(true);
    });

    it('should approve a gate with comment', async () => {
      const request: HITLApproveRequest = { comment: 'Reviewed and approved' };
      const mockResponse: HITLActionResponse = {
        success: true,
        approval_id: 2,
        status: HITLApprovalStatus.APPROVED,
        message: 'Gate approved successfully',
        execution_resumed: true,
      };
      (apiClient.post as Mock).mockResolvedValue({ data: mockResponse });

      const result = await HITLService.approveGate(2, request);

      expect(apiClient.post).toHaveBeenCalledWith('/hitl/approvals/2/approve', request);
      expect(result.success).toBe(true);
    });

    it('should handle approval with null comment', async () => {
      const request: HITLApproveRequest = { comment: null };
      const mockResponse: HITLActionResponse = {
        success: true,
        approval_id: 3,
        status: HITLApprovalStatus.APPROVED,
        message: 'Gate approved',
        execution_resumed: true,
      };
      (apiClient.post as Mock).mockResolvedValue({ data: mockResponse });

      const result = await HITLService.approveGate(3, request);

      expect(apiClient.post).toHaveBeenCalledWith('/hitl/approvals/3/approve', request);
      expect(result.success).toBe(true);
    });

    it('should handle approval failure when gate is expired', async () => {
      const error = {
        response: {
          status: 400,
          data: { detail: 'Gate has expired' },
        },
      };
      (apiClient.post as Mock).mockRejectedValue(error);

      await expect(HITLService.approveGate(1)).rejects.toEqual(error);
    });

    it('should handle approval failure when already responded', async () => {
      const error = {
        response: {
          status: 409,
          data: { detail: 'Gate has already been responded to' },
        },
      };
      (apiClient.post as Mock).mockRejectedValue(error);

      await expect(HITLService.approveGate(1)).rejects.toEqual(error);
    });
  });

  describe('rejectGate', () => {
    it('should reject a gate with reason only', async () => {
      const request: HITLRejectRequest = { reason: 'Quality issues detected' };
      const mockResponse: HITLActionResponse = {
        success: true,
        approval_id: 1,
        status: HITLApprovalStatus.REJECTED,
        message: 'Gate rejected',
        execution_resumed: false,
      };
      (apiClient.post as Mock).mockResolvedValue({ data: mockResponse });

      const result = await HITLService.rejectGate(1, request);

      expect(apiClient.post).toHaveBeenCalledWith('/hitl/approvals/1/reject', request);
      expect(result.status).toBe(HITLApprovalStatus.REJECTED);
      expect(result.execution_resumed).toBe(false);
    });

    it('should reject a gate with retry action', async () => {
      const request: HITLRejectRequest = {
        reason: 'Needs more data',
        action: HITLRejectionAction.RETRY,
      };
      const mockResponse: HITLActionResponse = {
        success: true,
        approval_id: 2,
        status: HITLApprovalStatus.RETRY,
        message: 'Gate rejected - retry scheduled',
        execution_resumed: false,
      };
      (apiClient.post as Mock).mockResolvedValue({ data: mockResponse });

      const result = await HITLService.rejectGate(2, request);

      expect(apiClient.post).toHaveBeenCalledWith('/hitl/approvals/2/reject', request);
      expect(result.status).toBe(HITLApprovalStatus.RETRY);
    });

    it('should reject a gate with reject action', async () => {
      const request: HITLRejectRequest = {
        reason: 'Results are invalid',
        action: HITLRejectionAction.REJECT,
      };
      const mockResponse: HITLActionResponse = {
        success: true,
        approval_id: 3,
        status: HITLApprovalStatus.REJECTED,
        message: 'Gate rejected - execution terminated',
        execution_resumed: false,
      };
      (apiClient.post as Mock).mockResolvedValue({ data: mockResponse });

      const result = await HITLService.rejectGate(3, request);

      expect(apiClient.post).toHaveBeenCalledWith('/hitl/approvals/3/reject', request);
      expect(result.status).toBe(HITLApprovalStatus.REJECTED);
    });

    it('should handle rejection failure when gate is expired', async () => {
      const request: HITLRejectRequest = { reason: 'Too late' };
      const error = {
        response: {
          status: 400,
          data: { detail: 'Gate has expired' },
        },
      };
      (apiClient.post as Mock).mockRejectedValue(error);

      await expect(HITLService.rejectGate(1, request)).rejects.toEqual(error);
    });

    it('should handle rejection failure for unauthorized user', async () => {
      const request: HITLRejectRequest = { reason: 'Not allowed' };
      const error = {
        response: {
          status: 403,
          data: { detail: 'User not authorized to reject this gate' },
        },
      };
      (apiClient.post as Mock).mockRejectedValue(error);

      await expect(HITLService.rejectGate(1, request)).rejects.toEqual(error);
    });
  });

  describe('getExecutionHITLStatus', () => {
    it('should fetch execution status with pending approval', async () => {
      const pendingApproval = createMockApproval();
      const mockStatus: ExecutionHITLStatus = {
        execution_id: 'exec-123',
        has_pending_approval: true,
        pending_approval: pendingApproval,
        approval_history: [],
        total_gates_passed: 0,
      };
      (apiClient.get as Mock).mockResolvedValue({ data: mockStatus });

      const result = await HITLService.getExecutionHITLStatus('exec-123');

      expect(apiClient.get).toHaveBeenCalledWith('/hitl/execution/exec-123');
      expect(result.has_pending_approval).toBe(true);
      expect(result.pending_approval).toEqual(pendingApproval);
    });

    it('should fetch execution status without pending approval', async () => {
      const mockStatus: ExecutionHITLStatus = {
        execution_id: 'exec-456',
        has_pending_approval: false,
        pending_approval: null,
        approval_history: [
          createMockApproval({ id: 1, status: HITLApprovalStatus.APPROVED }),
          createMockApproval({ id: 2, status: HITLApprovalStatus.APPROVED }),
        ],
        total_gates_passed: 2,
      };
      (apiClient.get as Mock).mockResolvedValue({ data: mockStatus });

      const result = await HITLService.getExecutionHITLStatus('exec-456');

      expect(result.has_pending_approval).toBe(false);
      expect(result.pending_approval).toBeNull();
      expect(result.approval_history.length).toBe(2);
      expect(result.total_gates_passed).toBe(2);
    });

    it('should fetch execution status with mixed approval history', async () => {
      const mockStatus: ExecutionHITLStatus = {
        execution_id: 'exec-789',
        has_pending_approval: false,
        pending_approval: null,
        approval_history: [
          createMockApproval({ id: 1, status: HITLApprovalStatus.APPROVED }),
          createMockApproval({ id: 2, status: HITLApprovalStatus.REJECTED }),
          createMockApproval({ id: 3, status: HITLApprovalStatus.TIMEOUT }),
        ],
        total_gates_passed: 1,
      };
      (apiClient.get as Mock).mockResolvedValue({ data: mockStatus });

      const result = await HITLService.getExecutionHITLStatus('exec-789');

      expect(result.approval_history.length).toBe(3);
      expect(result.total_gates_passed).toBe(1);
    });

    it('should handle 404 for non-existent execution', async () => {
      const error = { response: { status: 404 }, message: 'Execution not found' };
      (apiClient.get as Mock).mockRejectedValue(error);

      await expect(HITLService.getExecutionHITLStatus('non-existent')).rejects.toEqual(error);
    });
  });

  // ===========================================================================
  // Webhook Endpoints Tests
  // ===========================================================================

  describe('listWebhooks', () => {
    it('should fetch all webhooks', async () => {
      const mockResponse: HITLWebhookListResponse = {
        items: [
          createMockWebhook({ id: 1, name: 'Webhook 1' }),
          createMockWebhook({ id: 2, name: 'Webhook 2' }),
        ],
        total: 2,
      };
      (apiClient.get as Mock).mockResolvedValue({ data: mockResponse });

      const result = await HITLService.listWebhooks();

      expect(apiClient.get).toHaveBeenCalledWith('/hitl/webhooks');
      expect(result).toEqual(mockResponse);
      expect(result.items.length).toBe(2);
    });

    it('should handle empty webhooks list', async () => {
      const mockResponse: HITLWebhookListResponse = {
        items: [],
        total: 0,
      };
      (apiClient.get as Mock).mockResolvedValue({ data: mockResponse });

      const result = await HITLService.listWebhooks();

      expect(result.items).toEqual([]);
      expect(result.total).toBe(0);
    });

    it('should propagate API errors', async () => {
      const error = new Error('Service unavailable');
      (apiClient.get as Mock).mockRejectedValue(error);

      await expect(HITLService.listWebhooks()).rejects.toThrow('Service unavailable');
    });
  });

  describe('createWebhook', () => {
    it('should create a webhook with minimal configuration', async () => {
      const webhookCreate: HITLWebhookCreate = {
        name: 'New Webhook',
        url: 'https://example.com/hook',
      };
      const mockResponse = createMockWebhook({
        id: 5,
        name: 'New Webhook',
        url: 'https://example.com/hook',
        enabled: true,
        events: [],
      });
      (apiClient.post as Mock).mockResolvedValue({ data: mockResponse });

      const result = await HITLService.createWebhook(webhookCreate);

      expect(apiClient.post).toHaveBeenCalledWith('/hitl/webhooks', webhookCreate);
      expect(result.id).toBe(5);
      expect(result.name).toBe('New Webhook');
    });

    it('should create a webhook with full configuration', async () => {
      const webhookCreate: HITLWebhookCreate = {
        name: 'Full Webhook',
        url: 'https://example.com/full-hook',
        enabled: false,
        events: [
          HITLWebhookEvent.GATE_REACHED,
          HITLWebhookEvent.GATE_APPROVED,
          HITLWebhookEvent.GATE_REJECTED,
          HITLWebhookEvent.GATE_TIMEOUT,
        ],
        headers: { Authorization: 'Bearer token123' },
        secret: 'webhook-secret',
      };
      const mockResponse = createMockWebhook({
        id: 6,
        name: 'Full Webhook',
        url: 'https://example.com/full-hook',
        enabled: false,
        events: webhookCreate.events!,
        headers: webhookCreate.headers,
      });
      (apiClient.post as Mock).mockResolvedValue({ data: mockResponse });

      const result = await HITLService.createWebhook(webhookCreate);

      expect(apiClient.post).toHaveBeenCalledWith('/hitl/webhooks', webhookCreate);
      expect(result.enabled).toBe(false);
      expect(result.events).toHaveLength(4);
    });

    it('should handle validation errors', async () => {
      const webhookCreate: HITLWebhookCreate = {
        name: '',
        url: 'invalid-url',
      };
      const error = {
        response: {
          status: 422,
          data: { detail: 'Invalid URL format' },
        },
      };
      (apiClient.post as Mock).mockRejectedValue(error);

      await expect(HITLService.createWebhook(webhookCreate)).rejects.toEqual(error);
    });
  });

  describe('getWebhook', () => {
    it('should fetch a specific webhook by ID', async () => {
      const mockWebhook = createMockWebhook({ id: 10 });
      (apiClient.get as Mock).mockResolvedValue({ data: mockWebhook });

      const result = await HITLService.getWebhook(10);

      expect(apiClient.get).toHaveBeenCalledWith('/hitl/webhooks/10');
      expect(result.id).toBe(10);
    });

    it('should handle 404 for non-existent webhook', async () => {
      const error = { response: { status: 404 }, message: 'Webhook not found' };
      (apiClient.get as Mock).mockRejectedValue(error);

      await expect(HITLService.getWebhook(9999)).rejects.toEqual(error);
    });
  });

  describe('updateWebhook', () => {
    it('should update webhook name only', async () => {
      const webhookUpdate: HITLWebhookUpdate = {
        name: 'Updated Name',
      };
      const mockResponse = createMockWebhook({
        id: 1,
        name: 'Updated Name',
        updated_at: '2024-01-02T00:00:00Z',
      });
      (apiClient.patch as Mock).mockResolvedValue({ data: mockResponse });

      const result = await HITLService.updateWebhook(1, webhookUpdate);

      expect(apiClient.patch).toHaveBeenCalledWith('/hitl/webhooks/1', webhookUpdate);
      expect(result.name).toBe('Updated Name');
      expect(result.updated_at).toBe('2024-01-02T00:00:00Z');
    });

    it('should update webhook URL', async () => {
      const webhookUpdate: HITLWebhookUpdate = {
        url: 'https://new-endpoint.example.com/hook',
      };
      const mockResponse = createMockWebhook({
        id: 2,
        url: 'https://new-endpoint.example.com/hook',
      });
      (apiClient.patch as Mock).mockResolvedValue({ data: mockResponse });

      const result = await HITLService.updateWebhook(2, webhookUpdate);

      expect(apiClient.patch).toHaveBeenCalledWith('/hitl/webhooks/2', webhookUpdate);
      expect(result.url).toBe('https://new-endpoint.example.com/hook');
    });

    it('should toggle webhook enabled status', async () => {
      const webhookUpdate: HITLWebhookUpdate = {
        enabled: false,
      };
      const mockResponse = createMockWebhook({ id: 3, enabled: false });
      (apiClient.patch as Mock).mockResolvedValue({ data: mockResponse });

      const result = await HITLService.updateWebhook(3, webhookUpdate);

      expect(result.enabled).toBe(false);
    });

    it('should update webhook events', async () => {
      const webhookUpdate: HITLWebhookUpdate = {
        events: [HITLWebhookEvent.GATE_TIMEOUT],
      };
      const mockResponse = createMockWebhook({
        id: 4,
        events: [HITLWebhookEvent.GATE_TIMEOUT],
      });
      (apiClient.patch as Mock).mockResolvedValue({ data: mockResponse });

      const result = await HITLService.updateWebhook(4, webhookUpdate);

      expect(result.events).toEqual([HITLWebhookEvent.GATE_TIMEOUT]);
    });

    it('should update webhook headers', async () => {
      const webhookUpdate: HITLWebhookUpdate = {
        headers: { 'X-New-Header': 'new-value' },
      };
      const mockResponse = createMockWebhook({
        id: 5,
        headers: { 'X-New-Header': 'new-value' },
      });
      (apiClient.patch as Mock).mockResolvedValue({ data: mockResponse });

      const result = await HITLService.updateWebhook(5, webhookUpdate);

      expect(result.headers).toEqual({ 'X-New-Header': 'new-value' });
    });

    it('should clear webhook headers by setting to null', async () => {
      const webhookUpdate: HITLWebhookUpdate = {
        headers: null,
      };
      const mockResponse = createMockWebhook({ id: 6, headers: null });
      (apiClient.patch as Mock).mockResolvedValue({ data: mockResponse });

      const result = await HITLService.updateWebhook(6, webhookUpdate);

      expect(result.headers).toBeNull();
    });

    it('should update multiple fields at once', async () => {
      const webhookUpdate: HITLWebhookUpdate = {
        name: 'Fully Updated',
        url: 'https://updated.example.com',
        enabled: true,
        events: [HITLWebhookEvent.GATE_APPROVED, HITLWebhookEvent.GATE_REJECTED],
        headers: { 'Authorization': 'Bearer updated-token' },
        secret: 'new-secret',
      };
      const mockResponse = createMockWebhook({
        id: 7,
        name: 'Fully Updated',
        url: 'https://updated.example.com',
        enabled: true,
        events: webhookUpdate.events!,
        headers: webhookUpdate.headers,
      });
      (apiClient.patch as Mock).mockResolvedValue({ data: mockResponse });

      const result = await HITLService.updateWebhook(7, webhookUpdate);

      expect(apiClient.patch).toHaveBeenCalledWith('/hitl/webhooks/7', webhookUpdate);
      expect(result.name).toBe('Fully Updated');
      expect(result.events).toHaveLength(2);
    });

    it('should handle 404 for non-existent webhook', async () => {
      const webhookUpdate: HITLWebhookUpdate = { name: 'Updated' };
      const error = { response: { status: 404 }, message: 'Webhook not found' };
      (apiClient.patch as Mock).mockRejectedValue(error);

      await expect(HITLService.updateWebhook(9999, webhookUpdate)).rejects.toEqual(error);
    });
  });

  describe('deleteWebhook', () => {
    it('should delete a webhook successfully', async () => {
      (apiClient.delete as Mock).mockResolvedValue({});

      await HITLService.deleteWebhook(1);

      expect(apiClient.delete).toHaveBeenCalledWith('/hitl/webhooks/1');
    });

    it('should handle 404 for non-existent webhook', async () => {
      const error = { response: { status: 404 }, message: 'Webhook not found' };
      (apiClient.delete as Mock).mockRejectedValue(error);

      await expect(HITLService.deleteWebhook(9999)).rejects.toEqual(error);
    });

    it('should propagate server errors', async () => {
      const error = { response: { status: 500 }, message: 'Internal server error' };
      (apiClient.delete as Mock).mockRejectedValue(error);

      await expect(HITLService.deleteWebhook(1)).rejects.toEqual(error);
    });
  });

  // ===========================================================================
  // Type and Enum Tests
  // ===========================================================================

  describe('Enum Values', () => {
    it('should have correct HITLApprovalStatus values', () => {
      expect(HITLApprovalStatus.PENDING).toBe('pending');
      expect(HITLApprovalStatus.APPROVED).toBe('approved');
      expect(HITLApprovalStatus.REJECTED).toBe('rejected');
      expect(HITLApprovalStatus.TIMEOUT).toBe('timeout');
      expect(HITLApprovalStatus.RETRY).toBe('retry');
    });

    it('should have correct HITLTimeoutAction values', () => {
      expect(HITLTimeoutAction.AUTO_REJECT).toBe('auto_reject');
      expect(HITLTimeoutAction.FAIL).toBe('fail');
    });

    it('should have correct HITLRejectionAction values', () => {
      expect(HITLRejectionAction.REJECT).toBe('reject');
      expect(HITLRejectionAction.RETRY).toBe('retry');
    });

    it('should have correct HITLWebhookEvent values', () => {
      expect(HITLWebhookEvent.GATE_REACHED).toBe('gate_reached');
      expect(HITLWebhookEvent.GATE_APPROVED).toBe('gate_approved');
      expect(HITLWebhookEvent.GATE_REJECTED).toBe('gate_rejected');
      expect(HITLWebhookEvent.GATE_TIMEOUT).toBe('gate_timeout');
    });
  });

  // ===========================================================================
  // Response Type Validation Tests
  // ===========================================================================

  describe('Response Type Validation', () => {
    it('should return correctly typed HITLApprovalResponse', async () => {
      const mockApproval = createMockApproval();
      (apiClient.get as Mock).mockResolvedValue({ data: mockApproval });

      const result = await HITLService.getApproval(1);

      // Type assertions - these would fail at compile time if types are wrong
      expect(typeof result.id).toBe('number');
      expect(typeof result.execution_id).toBe('string');
      expect(typeof result.flow_id).toBe('string');
      expect(typeof result.gate_node_id).toBe('string');
      expect(typeof result.crew_sequence).toBe('number');
      expect(typeof result.status).toBe('string');
      expect(typeof result.gate_config).toBe('object');
      expect(typeof result.is_expired).toBe('boolean');
      expect(typeof result.created_at).toBe('string');
      expect(typeof result.group_id).toBe('string');
    });

    it('should return correctly typed HITLActionResponse', async () => {
      const mockResponse: HITLActionResponse = {
        success: true,
        approval_id: 1,
        status: HITLApprovalStatus.APPROVED,
        message: 'Approved',
        execution_resumed: true,
      };
      (apiClient.post as Mock).mockResolvedValue({ data: mockResponse });

      const result = await HITLService.approveGate(1);

      expect(typeof result.success).toBe('boolean');
      expect(typeof result.approval_id).toBe('number');
      expect(typeof result.status).toBe('string');
      expect(typeof result.message).toBe('string');
      expect(typeof result.execution_resumed).toBe('boolean');
    });

    it('should return correctly typed ExecutionHITLStatus', async () => {
      const mockStatus: ExecutionHITLStatus = {
        execution_id: 'exec-123',
        has_pending_approval: true,
        pending_approval: createMockApproval(),
        approval_history: [],
        total_gates_passed: 0,
      };
      (apiClient.get as Mock).mockResolvedValue({ data: mockStatus });

      const result = await HITLService.getExecutionHITLStatus('exec-123');

      expect(typeof result.execution_id).toBe('string');
      expect(typeof result.has_pending_approval).toBe('boolean');
      expect(typeof result.total_gates_passed).toBe('number');
      expect(Array.isArray(result.approval_history)).toBe(true);
    });

    it('should return correctly typed HITLWebhookResponse', async () => {
      const mockWebhook = createMockWebhook();
      (apiClient.get as Mock).mockResolvedValue({ data: mockWebhook });

      const result = await HITLService.getWebhook(1);

      expect(typeof result.id).toBe('number');
      expect(typeof result.name).toBe('string');
      expect(typeof result.url).toBe('string');
      expect(typeof result.enabled).toBe('boolean');
      expect(Array.isArray(result.events)).toBe(true);
      expect(typeof result.group_id).toBe('string');
      expect(typeof result.created_at).toBe('string');
    });

    it('should handle nullable fields in responses', async () => {
      const mockApproval = createMockApproval({
        previous_crew_name: null,
        previous_crew_output: null,
        flow_state_snapshot: null,
        responded_by: null,
        responded_at: null,
        approval_comment: null,
        rejection_reason: null,
        rejection_action: null,
        expires_at: null,
      });
      (apiClient.get as Mock).mockResolvedValue({ data: mockApproval });

      const result = await HITLService.getApproval(1);

      expect(result.previous_crew_name).toBeNull();
      expect(result.previous_crew_output).toBeNull();
      expect(result.flow_state_snapshot).toBeNull();
      expect(result.responded_by).toBeNull();
      expect(result.responded_at).toBeNull();
      expect(result.approval_comment).toBeNull();
      expect(result.rejection_reason).toBeNull();
      expect(result.rejection_action).toBeNull();
      expect(result.expires_at).toBeNull();
    });
  });

  // ===========================================================================
  // Edge Cases and Error Handling
  // ===========================================================================

  describe('Edge Cases', () => {
    it('should handle very large approval IDs', async () => {
      const largeId = 999999999;
      const mockApproval = createMockApproval({ id: largeId });
      (apiClient.get as Mock).mockResolvedValue({ data: mockApproval });

      const result = await HITLService.getApproval(largeId);

      expect(apiClient.get).toHaveBeenCalledWith(`/hitl/approvals/${largeId}`);
      expect(result.id).toBe(largeId);
    });

    it('should handle execution IDs with special characters', async () => {
      const executionId = 'exec-123-abc-456-def';
      const mockStatus: ExecutionHITLStatus = {
        execution_id: executionId,
        has_pending_approval: false,
        pending_approval: null,
        approval_history: [],
        total_gates_passed: 0,
      };
      (apiClient.get as Mock).mockResolvedValue({ data: mockStatus });

      const result = await HITLService.getExecutionHITLStatus(executionId);

      expect(apiClient.get).toHaveBeenCalledWith(`/hitl/execution/${executionId}`);
      expect(result.execution_id).toBe(executionId);
    });

    it('should handle very long webhook names', async () => {
      const longName = 'A'.repeat(255);
      const webhookCreate: HITLWebhookCreate = {
        name: longName,
        url: 'https://example.com/hook',
      };
      const mockResponse = createMockWebhook({ name: longName });
      (apiClient.post as Mock).mockResolvedValue({ data: mockResponse });

      const result = await HITLService.createWebhook(webhookCreate);

      expect(result.name).toBe(longName);
    });

    it('should handle empty gate config', async () => {
      const mockApproval = createMockApproval({ gate_config: {} });
      (apiClient.get as Mock).mockResolvedValue({ data: mockApproval });

      const result = await HITLService.getApproval(1);

      expect(result.gate_config).toEqual({});
    });

    it('should handle complex gate config', async () => {
      const complexConfig = {
        message: 'Review required',
        timeout_seconds: 7200,
        timeout_action: HITLTimeoutAction.AUTO_REJECT,
        require_comment: true,
        allowed_approvers: ['admin@example.com', 'manager@example.com'],
        custom_field: { nested: { deeply: 'value' } },
      };
      const mockApproval = createMockApproval({ gate_config: complexConfig });
      (apiClient.get as Mock).mockResolvedValue({ data: mockApproval });

      const result = await HITLService.getApproval(1);

      expect(result.gate_config).toEqual(complexConfig);
    });
  });

  describe('Network Error Handling', () => {
    it('should propagate network errors from getPendingApprovals', async () => {
      const networkError = new Error('Network error');
      (apiClient.get as Mock).mockRejectedValue(networkError);

      await expect(HITLService.getPendingApprovals()).rejects.toThrow('Network error');
    });

    it('should propagate timeout errors', async () => {
      const timeoutError = new Error('Request timeout');
      (apiClient.get as Mock).mockRejectedValue(timeoutError);

      await expect(HITLService.getApproval(1)).rejects.toThrow('Request timeout');
    });

    it('should handle 401 unauthorized errors', async () => {
      const error = {
        response: {
          status: 401,
          data: { detail: 'Unauthorized' },
        },
      };
      (apiClient.get as Mock).mockRejectedValue(error);

      await expect(HITLService.listWebhooks()).rejects.toEqual(error);
    });

    it('should handle 500 internal server errors', async () => {
      const error = {
        response: {
          status: 500,
          data: { detail: 'Internal server error' },
        },
      };
      (apiClient.post as Mock).mockRejectedValue(error);

      await expect(HITLService.approveGate(1)).rejects.toEqual(error);
    });
  });

  // ===========================================================================
  // Integration-like Tests
  // ===========================================================================

  describe('Workflow Integration Scenarios', () => {
    it('should simulate a complete approval workflow', async () => {
      // Step 1: Get pending approvals
      const pendingResponse: HITLApprovalListResponse = {
        items: [createMockApproval({ id: 1 })],
        total: 1,
        limit: 50,
        offset: 0,
      };
      (apiClient.get as Mock).mockResolvedValueOnce({ data: pendingResponse });

      const pending = await HITLService.getPendingApprovals();
      expect(pending.items).toHaveLength(1);

      // Step 2: Get approval details
      const approvalDetails = createMockApproval({ id: 1 });
      (apiClient.get as Mock).mockResolvedValueOnce({ data: approvalDetails });

      const details = await HITLService.getApproval(1);
      expect(details.status).toBe(HITLApprovalStatus.PENDING);

      // Step 3: Approve the gate
      const approveResponse: HITLActionResponse = {
        success: true,
        approval_id: 1,
        status: HITLApprovalStatus.APPROVED,
        message: 'Gate approved',
        execution_resumed: true,
      };
      (apiClient.post as Mock).mockResolvedValueOnce({ data: approveResponse });

      const approveResult = await HITLService.approveGate(1, { comment: 'Approved!' });
      expect(approveResult.success).toBe(true);
      expect(approveResult.execution_resumed).toBe(true);

      // Verify all API calls were made
      expect(apiClient.get).toHaveBeenCalledTimes(2);
      expect(apiClient.post).toHaveBeenCalledTimes(1);
    });

    it('should simulate a webhook lifecycle', async () => {
      // Step 1: Create webhook
      const webhookCreate: HITLWebhookCreate = {
        name: 'Notification Webhook',
        url: 'https://example.com/notify',
        enabled: true,
        events: [HITLWebhookEvent.GATE_REACHED],
      };
      const createdWebhook = createMockWebhook({
        id: 1,
        name: 'Notification Webhook',
        url: 'https://example.com/notify',
      });
      (apiClient.post as Mock).mockResolvedValueOnce({ data: createdWebhook });

      const created = await HITLService.createWebhook(webhookCreate);
      expect(created.id).toBe(1);

      // Step 2: Update webhook
      const webhookUpdate: HITLWebhookUpdate = {
        events: [HITLWebhookEvent.GATE_REACHED, HITLWebhookEvent.GATE_APPROVED],
      };
      const updatedWebhook = createMockWebhook({
        id: 1,
        events: [HITLWebhookEvent.GATE_REACHED, HITLWebhookEvent.GATE_APPROVED],
      });
      (apiClient.patch as Mock).mockResolvedValueOnce({ data: updatedWebhook });

      const updated = await HITLService.updateWebhook(1, webhookUpdate);
      expect(updated.events).toHaveLength(2);

      // Step 3: Delete webhook
      (apiClient.delete as Mock).mockResolvedValueOnce({});

      await HITLService.deleteWebhook(1);

      // Verify all API calls were made
      expect(apiClient.post).toHaveBeenCalledTimes(1);
      expect(apiClient.patch).toHaveBeenCalledTimes(1);
      expect(apiClient.delete).toHaveBeenCalledTimes(1);
    });

    it('should simulate checking execution status during flow', async () => {
      const executionId = 'exec-workflow-001';

      // Initially has pending approval
      const initialStatus: ExecutionHITLStatus = {
        execution_id: executionId,
        has_pending_approval: true,
        pending_approval: createMockApproval({ status: HITLApprovalStatus.PENDING }),
        approval_history: [],
        total_gates_passed: 0,
      };
      (apiClient.get as Mock).mockResolvedValueOnce({ data: initialStatus });

      const status1 = await HITLService.getExecutionHITLStatus(executionId);
      expect(status1.has_pending_approval).toBe(true);

      // After approval, no more pending
      const afterApprovalStatus: ExecutionHITLStatus = {
        execution_id: executionId,
        has_pending_approval: false,
        pending_approval: null,
        approval_history: [
          createMockApproval({
            status: HITLApprovalStatus.APPROVED,
            responded_at: '2024-01-01T12:00:00Z',
          }),
        ],
        total_gates_passed: 1,
      };
      (apiClient.get as Mock).mockResolvedValueOnce({ data: afterApprovalStatus });

      const status2 = await HITLService.getExecutionHITLStatus(executionId);
      expect(status2.has_pending_approval).toBe(false);
      expect(status2.total_gates_passed).toBe(1);
    });
  });
});
