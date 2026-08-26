// Event-driven trigger queue types (mirror src/backend/src/schemas/triggers.py).
// A queued event triggers a crew/flow run; see src/docs/EVENT_TRIGGERS.md.

export type TriggerKind = 'flow' | 'inline' | 'crew';

export type TriggerStatus =
  | 'pending'
  | 'claimed'
  | 'dispatched'
  | 'failed'
  | 'dead';

export interface TriggerTarget {
  /** 'flow' (saved flow by id) | 'inline' (full config) | 'crew' (Phase 2) */
  kind: TriggerKind;
  /** Saved crew/flow id, for kind 'flow'/'crew'. */
  id?: string;
  /** Inline CrewConfig/FlowConfig fields, for kind 'inline'. */
  config?: Record<string, unknown>;
  /** Per-run engine override. */
  harness?: 'kasal' | 'crewai';
}

export interface EnqueueTrigger {
  target: TriggerTarget;
  /** Event body; run inputs are read from payload.inputs. */
  payload?: Record<string, unknown>;
  event_type?: string;
  correlation_id?: string;
  causation_run_id?: string;
  idempotency_key?: string;
}

export interface TriggerEvent {
  id: number;
  group_id?: string | null;
  event_type?: string | null;
  target?: TriggerTarget | null;
  payload?: Record<string, unknown> | null;
  status: TriggerStatus;
  attempts: number;
  available_at?: string | null;
  claimed_at?: string | null;
  last_error?: string | null;
  correlation_id?: string | null;
  causation_run_id?: string | null;
  idempotency_key?: string | null;
  created_at?: string | null;
  updated_at?: string | null;
}

export interface TriggerListResponse {
  events: TriggerEvent[];
  total: number;
}

export interface DispatchResult {
  /** How many due events were claimed and had a run launched. */
  claimed: number;
}

// --- Choreography config: subscriptions + emit rules ---

export interface Subscription {
  id: number;
  group_id?: string | null;
  /** The event name this subscription runs on. */
  event_type: string;
  target?: TriggerTarget | null;
  harness?: string | null;
  input_mapping?: Record<string, unknown> | null;
  /** Object Management schema name the payload is expected to match. */
  schema_ref?: string | null;
  enabled: boolean;
  created_at?: string | null;
  updated_at?: string | null;
}

export interface EmitRule {
  id: number;
  group_id?: string | null;
  /** The crew/flow whose completion emits the event. */
  on_target?: TriggerTarget | null;
  event_type: string;
  schema_ref?: string | null;
  condition?: string | null;
  enabled: boolean;
  created_at?: string | null;
  updated_at?: string | null;
}

export interface SubscriptionCreate {
  event_type: string;
  target: TriggerTarget;
  harness?: string;
  input_mapping?: Record<string, unknown>;
  schema_ref?: string;
  enabled?: boolean;
}

export interface EmitRuleCreate {
  on_target: TriggerTarget;
  event_type: string;
  schema_ref?: string;
  condition?: string;
  enabled?: boolean;
}

export interface SubscriptionList {
  subscriptions: Subscription[];
  emit_rules: EmitRule[];
}
