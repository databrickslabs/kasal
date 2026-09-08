import { apiClient } from '../../shared/api/client';
import type { BillingSummary, ModelRate } from './types';

export class BillingService {
  static async summary(start: string, end: string, executionIds?: string[], signal?: AbortSignal) {
    return (await apiClient.post<BillingSummary>('/billing/summary', {
      start, end, execution_ids: executionIds,
    }, { signal })).data;
  }

  static async saveRate(rate: ModelRate) {
    return (await apiClient.put<ModelRate>('/billing/rates', rate)).data;
  }
}

