/**
 * UCMV Correction-Learning Service
 *
 * Uploads customer-corrected/deployed UC Metric View YAMLs and asks the backend
 * to diff them against our original generated output and distil a reusable
 * domain-context README (the free text the generator's "Domain context" field
 * consumes).
 */

import { apiClient } from '../../shared/api/client';

export interface UcmvLearnRequest {
  /** { view_name: corrected_UCMV_yaml } the customer deployed. */
  corrected: Record<string, string>;
  /** Original Kasal run to diff against; omit to use the group's most recent conversion. */
  execution_id?: string;
  /** Model/report name for prompt context. */
  model_hint?: string;
}

export interface UcmvLearnResponse {
  readme?: string | null;
  views_analyzed: number;
  views_with_changes: number;
  matched_views: string[];
  /** Uploads paired to our original by name/measure similarity: view -> {matched_to, how}. */
  fuzzy_matched?: Record<string, { matched_to: string; how: string }>;
  unmatched_views: string[];
  note?: string | null;
  error?: string | null;
}

export class UcmvLearningService {
  // apiClient prepends "/api/v1"; the router self-prefixes with "/api/ucmv-learning",
  // so the full path is "/api/v1/api/ucmv-learning/learn" (same shape as ConverterService).
  private static readonly BASE_PATH = '/api/ucmv-learning';

  static async learn(request: UcmvLearnRequest): Promise<UcmvLearnResponse> {
    const response = await apiClient.post<UcmvLearnResponse>(
      `${this.BASE_PATH}/learn`,
      request
    );
    return response.data;
  }
}
