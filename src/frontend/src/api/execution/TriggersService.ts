import { apiClient } from '../../config/api/ApiConfig';
import {
  DispatchResult,
  EmitRule,
  EmitRuleCreate,
  EnqueueTrigger,
  Subscription,
  SubscriptionCreate,
  SubscriptionList,
  TriggerEvent,
  TriggerListResponse,
} from '../../types/execution/triggers';

/**
 * Client for the event-trigger queue (`/triggers`). Enqueue an event that
 * triggers a crew/flow run, and inspect/delete queued events. The backend
 * consumer drains the queue and launches the runs.
 */
export class TriggersService {
  static async enqueue(payload: EnqueueTrigger): Promise<TriggerEvent> {
    const response = await apiClient.post<TriggerEvent>('/triggers', payload);
    return response.data;
  }

  static async list(status?: string, limit = 50): Promise<TriggerListResponse> {
    const params: Record<string, unknown> = { limit };
    if (status) {
      params.status = status;
    }
    const response = await apiClient.get<TriggerListResponse>('/triggers', {
      params,
    });
    return response.data;
  }

  static async get(id: number): Promise<TriggerEvent> {
    const response = await apiClient.get<TriggerEvent>(`/triggers/${id}`);
    return response.data;
  }

  static async delete(id: number): Promise<void> {
    await apiClient.delete(`/triggers/${id}`);
  }

  /** Drain the queue on demand: claim due events and launch their runs now. */
  static async dispatch(batch = 10): Promise<DispatchResult> {
    const response = await apiClient.post<DispatchResult>(
      '/triggers/dispatch',
      null,
      { params: { batch } },
    );
    return response.data;
  }

  // --- Choreography config: subscriptions + emit rules ---

  static async listSubscriptions(): Promise<SubscriptionList> {
    const response = await apiClient.get<SubscriptionList>(
      '/triggers/subscriptions',
    );
    return response.data;
  }

  static async createSubscription(
    body: SubscriptionCreate,
  ): Promise<Subscription> {
    const response = await apiClient.post<Subscription>(
      '/triggers/subscriptions',
      body,
    );
    return response.data;
  }

  static async deleteSubscription(id: number): Promise<void> {
    await apiClient.delete(`/triggers/subscriptions/${id}`);
  }

  static async createEmitRule(body: EmitRuleCreate): Promise<EmitRule> {
    const response = await apiClient.post<EmitRule>('/triggers/emit-rules', body);
    return response.data;
  }

  static async deleteEmitRule(id: number): Promise<void> {
    await apiClient.delete(`/triggers/emit-rules/${id}`);
  }
}

export const triggersService = new TriggersService();
