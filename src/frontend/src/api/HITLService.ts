/**
 * HITL (Human in the Loop) Service
 *
 * This service provides API methods for managing HITL approvals and webhooks.
 */

import { apiClient } from '../config/api/ApiConfig';

// =============================================================================
// Enums
// =============================================================================

export enum HITLApprovalStatus {
  PENDING = 'pending',
  APPROVED = 'approved',
  REJECTED = 'rejected',
  TIMEOUT = 'timeout',
  RETRY = 'retry'
}

export enum HITLTimeoutAction {
  AUTO_REJECT = 'auto_reject',
  FAIL = 'fail'
}

export enum HITLRejectionAction {
  REJECT = 'reject',
  RETRY = 'retry'
}

export enum HITLWebhookEvent {
  GATE_REACHED = 'gate_reached',
  GATE_APPROVED = 'gate_approved',
  GATE_REJECTED = 'gate_rejected',
  GATE_TIMEOUT = 'gate_timeout'
}

// =============================================================================
// Gate Configuration Types
// =============================================================================

export interface HITLGateConfig {
  message: string;
  timeout_seconds: number;
  timeout_action: HITLTimeoutAction;
  require_comment: boolean;
  allowed_approvers?: string[] | null;
}

// =============================================================================
// Approval Types
// =============================================================================

export interface HITLApprovalResponse {
  id: number;
  execution_id: string;
  flow_id: string;
  gate_node_id: string;
  crew_sequence: number;
  status: HITLApprovalStatus;
  gate_config: Record<string, unknown>;
  previous_crew_name?: string | null;
  previous_crew_output?: string | null;
  flow_state_snapshot?: Record<string, unknown> | null;
  responded_by?: string | null;
  responded_at?: string | null;
  approval_comment?: string | null;
  rejection_reason?: string | null;
  rejection_action?: HITLRejectionAction | null;
  expires_at?: string | null;
  is_expired: boolean;
  created_at: string;
  group_id: string;
}

export interface HITLApprovalListResponse {
  items: HITLApprovalResponse[];
  total: number;
  limit: number;
  offset: number;
}

export interface HITLApproveRequest {
  comment?: string | null;
}

export interface HITLRejectRequest {
  reason: string;
  action?: HITLRejectionAction;
}

export interface HITLActionResponse {
  success: boolean;
  approval_id: number;
  status: HITLApprovalStatus;
  message: string;
  execution_resumed: boolean;
}

// =============================================================================
// Execution Status Types
// =============================================================================

export interface ExecutionHITLStatus {
  execution_id: string;
  has_pending_approval: boolean;
  pending_approval?: HITLApprovalResponse | null;
  approval_history: HITLApprovalResponse[];
  total_gates_passed: number;
}

// =============================================================================
// Webhook Types
// =============================================================================

export interface HITLWebhookCreate {
  name: string;
  url: string;
  enabled?: boolean;
  events?: HITLWebhookEvent[];
  headers?: Record<string, string> | null;
  secret?: string | null;
}

export interface HITLWebhookUpdate {
  name?: string;
  url?: string;
  enabled?: boolean;
  events?: HITLWebhookEvent[];
  headers?: Record<string, string> | null;
  secret?: string | null;
}

export interface HITLWebhookResponse {
  id: number;
  name: string;
  url: string;
  enabled: boolean;
  events: HITLWebhookEvent[];
  headers?: Record<string, string> | null;
  group_id: string;
  created_at: string;
  updated_at?: string | null;
}

export interface HITLWebhookListResponse {
  items: HITLWebhookResponse[];
  total: number;
}

// =============================================================================
// Service Class
// =============================================================================

export class HITLService {
  // =========================================================================
  // Approval Endpoints
  // =========================================================================

  /**
   * Get all pending HITL approvals for the current group.
   */
  static async getPendingApprovals(
    limit = 50,
    offset = 0
  ): Promise<HITLApprovalListResponse> {
    const response = await apiClient.get<HITLApprovalListResponse>(
      '/hitl/pending',
      { params: { limit, offset } }
    );
    return response.data;
  }

  /**
   * Get a specific HITL approval by ID.
   */
  static async getApproval(approvalId: number): Promise<HITLApprovalResponse> {
    const response = await apiClient.get<HITLApprovalResponse>(
      `/hitl/approvals/${approvalId}`
    );
    return response.data;
  }

  /**
   * Approve an HITL gate and resume flow execution.
   */
  static async approveGate(
    approvalId: number,
    request: HITLApproveRequest = {}
  ): Promise<HITLActionResponse> {
    const response = await apiClient.post<HITLActionResponse>(
      `/hitl/approvals/${approvalId}/approve`,
      request
    );
    return response.data;
  }

  /**
   * Reject an HITL gate.
   */
  static async rejectGate(
    approvalId: number,
    request: HITLRejectRequest
  ): Promise<HITLActionResponse> {
    const response = await apiClient.post<HITLActionResponse>(
      `/hitl/approvals/${approvalId}/reject`,
      request
    );
    return response.data;
  }

  /**
   * Get HITL status for a specific execution.
   */
  static async getExecutionHITLStatus(
    executionId: string
  ): Promise<ExecutionHITLStatus> {
    const response = await apiClient.get<ExecutionHITLStatus>(
      `/hitl/execution/${executionId}`
    );
    return response.data;
  }

  // =========================================================================
  // Webhook Endpoints
  // =========================================================================

  /**
   * List all HITL webhooks for the current group.
   */
  static async listWebhooks(): Promise<HITLWebhookListResponse> {
    const response = await apiClient.get<HITLWebhookListResponse>(
      '/hitl/webhooks'
    );
    return response.data;
  }

  /**
   * Create a new HITL webhook.
   */
  static async createWebhook(
    webhook: HITLWebhookCreate
  ): Promise<HITLWebhookResponse> {
    const response = await apiClient.post<HITLWebhookResponse>(
      '/hitl/webhooks',
      webhook
    );
    return response.data;
  }

  /**
   * Get a specific HITL webhook by ID.
   */
  static async getWebhook(webhookId: number): Promise<HITLWebhookResponse> {
    const response = await apiClient.get<HITLWebhookResponse>(
      `/hitl/webhooks/${webhookId}`
    );
    return response.data;
  }

  /**
   * Update an existing HITL webhook.
   */
  static async updateWebhook(
    webhookId: number,
    webhook: HITLWebhookUpdate
  ): Promise<HITLWebhookResponse> {
    const response = await apiClient.patch<HITLWebhookResponse>(
      `/hitl/webhooks/${webhookId}`,
      webhook
    );
    return response.data;
  }

  /**
   * Delete an HITL webhook.
   */
  static async deleteWebhook(webhookId: number): Promise<void> {
    await apiClient.delete(`/hitl/webhooks/${webhookId}`);
  }
}

// Export singleton instance for convenience
export const hitlService = new HITLService();
