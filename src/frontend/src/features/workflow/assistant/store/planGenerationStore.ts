import { create } from 'zustand';
import { persist } from 'zustand/middleware';
import DispatcherService, { type DispatcherRequest, type DispatchResult } from '../../../../api/execution/DispatcherService';
import { waitForBuilderGeneration } from '../../../../api/execution/builderGeneration';
import { useGroupStore } from '../../../../store/groups';

export interface PlanGeneration {
  token: string;
  groupId: string | null;
  sessionId: string;
  model?: string;
  jobId?: string;
  status: 'running' | 'complete' | 'failed' | 'paused';
  result?: DispatchResult;
  error?: string;
}
export const planGenerationKey = (groupId: string | null, sessionId: string) => JSON.stringify([groupId, sessionId]);
export const usePlanGenerationStore = create<{
  requests: Record<string, PlanGeneration>;
  activityBySession: Record<string, { jobId: string; createdAt: string }[]>;
}>()(persist(
  () => ({ requests: {}, activityBySession: {} }), { name: 'kasal-plan-generations' },
));
const observers = new Map<string, AbortController>();

function update(key: string, token: string, patch: Partial<PlanGeneration>) {
  usePlanGenerationStore.setState(state => {
    const current = state.requests[key];
    return current?.token === token ? { requests: { ...state.requests, [key]: { ...current, ...patch } } } : state;
  });
}
export function consumePlanGeneration(key: string, token: string) {
  usePlanGenerationStore.setState(state => {
    if (state.requests[key]?.token !== token) return state;
    const requests = { ...state.requests };
    delete requests[key];
    return { requests };
  });
}

async function observe(key: string, request: PlanGeneration, work: (signal: AbortSignal) => Promise<DispatchResult>) {
  const controller = new AbortController();
  observers.set(key, controller);
  // Session navigation is harmless; a teamspace change must stop requests using
  // that team's credentials. Retain the durable job ID for reconnection later.
  const unsubscribe = useGroupStore.subscribe(state => {
    if (state.currentGroupId !== request.groupId) controller.abort();
  });
  try {
    const result = await work(controller.signal);
    update(key, request.token, { status: 'complete', result });
  } catch (error) {
    update(key, request.token, controller.signal.aborted
      ? { status: 'paused' }
      : { status: 'failed', error: error instanceof Error ? error.message : 'Plan generation failed. Please try again.' });
  } finally {
    unsubscribe();
    if (observers.get(key) === controller) observers.delete(key);
  }
}

/** Own the request outside React; even switching before the POST returns is safe. */
export function startPlanGeneration(groupId: string | null, sessionId: string, payload: DispatcherRequest,
  beforeStart: () => Promise<void>, onStarted: (jobId: string) => void) {
  const key = planGenerationKey(groupId, sessionId);
  if (usePlanGenerationStore.getState().requests[key]) return;
  const request: PlanGeneration = { token: crypto.randomUUID(), groupId, sessionId, model: payload.model, status: 'running' };
  usePlanGenerationStore.setState(state => ({ requests: { ...state.requests, [key]: request } }));
  void observe(key, request, async signal => {
    await beforeStart();
    signal.throwIfAborted();
    return DispatcherService.dispatch({ ...payload, session_id: sessionId }, jobId => {
      usePlanGenerationStore.setState(state => ({ activityBySession: {
        ...state.activityBySession,
        [key]: [...(state.activityBySession[key] || []).filter(item => item.jobId !== jobId), { jobId, createdAt: new Date().toISOString() }],
      } }));
      update(key, request.token, { jobId });
      onStarted(jobId);
    }, signal);
  });
}

/** Reconnect after a component remount, refresh, or return to a teamspace. */
export function resumePlanGeneration(key: string) {
  const request = usePlanGenerationStore.getState().requests[key];
  if (!request || observers.has(key) || !['running', 'paused'].includes(request.status)) return;
  if (request.groupId !== useGroupStore.getState().currentGroupId) return;
  if (!request.jobId) {
    update(key, request.token, { status: 'failed', error: 'The generation connection was interrupted before its job ID was received. Please submit the prompt again.' });
    return;
  }
  update(key, request.token, { status: 'running' });
  void observe(key, request, signal => waitForBuilderGeneration<DispatchResult>(request.jobId!, signal));
}
