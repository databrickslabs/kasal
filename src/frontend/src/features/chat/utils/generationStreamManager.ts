import { streamGeneration, StreamEvent } from '../api/streaming';
import { GeneratedAgent, GeneratedTask, GenerationCompleteData } from '../types/dispatcher';

export type { GenerationCompleteData };

export interface GenerationStreamCallbacks {
  onPlanReady: (generationId: string, plan: Record<string, unknown>) => void;
  onAgentDetail: (generationId: string, agent: GeneratedAgent) => void;
  onTaskDetail: (generationId: string, task: GeneratedTask) => void;
  onComplete: (generationId: string, data: GenerationCompleteData) => void;
  /**
   * ChatMode auto-execute: the backend runs the generated crew itself and FOLDS
   * the new execution id INTO generation_complete. The consumer just observes
   * that run (it no longer builds a config and calls createExecution), so a run
   * started before a session switch isn't lost.
   */
  onExecutionStarted?: (generationId: string, executionId: string, runName?: string) => void;
  onFailed: (generationId: string, error: string) => void;
}

// Per-generation stream state. Each generation runs as its OWN concurrent
// stream so starting a new generation (e.g. in another chat session) never
// closes a still-running one — that was dropping backgrounded runs. The dedup
// sets make the stream idempotent against SSE reconnect + full-buffer replay,
// which otherwise re-fired (and cross-routed) trace events.
interface StreamState {
  close: () => void;
  completed: boolean;
  streamedAgents: Record<string, unknown>[];
  streamedTasks: Record<string, unknown>[];
  planSeen: boolean;
  seenAgents: Set<string>;
  seenTasks: Set<string>;
  callbacks: GenerationStreamCallbacks;
}

// Live generation streams keyed by generationId. Module-level (a singleton, like
// the execution side's jobOwners/sessionSnapshots) rather than React state: an
// EventSource isn't serializable so it can't live in a Zustand store, and a
// module Map keeps streams alive across re-renders independent of any component.
const streams = new Map<string, StreamState>();

export function stopGenerationStream(generationId: string): void {
  const st = streams.get(generationId);
  if (st) {
    st.close();
    streams.delete(generationId);
  }
}

export function stopAllGenerationStreams(): void {
  streams.forEach((st) => st.close());
  streams.clear();
}

export function isGenerationStreaming(generationId: string): boolean {
  return streams.has(generationId);
}

/**
 * Begin observing a crew generation. Concurrent-safe: starting another
 * generation never closes this one. Idempotent — a second call for the same
 * generationId just refreshes the callbacks and keeps the single open stream.
 */
export function startGenerationStream(
  generationId: string,
  callbacks: GenerationStreamCallbacks,
): void {
  const existing = streams.get(generationId);
  if (existing) {
    existing.callbacks = callbacks; // already observing — keep the one stream
    return;
  }

  const state: StreamState = {
    close: () => {},
    completed: false,
    streamedAgents: [],
    streamedTasks: [],
    planSeen: false,
    seenAgents: new Set(),
    seenTasks: new Set(),
    callbacks,
  };
  streams.set(generationId, state);

  const close = streamGeneration(
    generationId,
    (event: StreamEvent) => {
      const st = streams.get(generationId);
      if (!st) return; // stream was stopped
      const cb = st.callbacks;
      switch (event.event) {
        case 'plan_ready':
          // Ignore replays (reconnect re-sends the buffer) and anything after
          // this generation already completed.
          if (st.completed || st.planSeen) break;
          st.planSeen = true;
          cb.onPlanReady(generationId, event.data);
          break;
        case 'agent_detail': {
          if (st.completed) break;
          const agent =
            (event.data.agent as unknown as GeneratedAgent) ||
            (event.data as unknown as GeneratedAgent);
          const key = String(
            (agent as { id?: unknown; name?: unknown })?.id ??
              (agent as { name?: unknown })?.name ??
              st.streamedAgents.length,
          );
          if (st.seenAgents.has(key)) break; // dedup replay
          st.seenAgents.add(key);
          st.streamedAgents.push(agent as unknown as Record<string, unknown>);
          cb.onAgentDetail(generationId, agent);
          break;
        }
        case 'task_detail': {
          if (st.completed) break;
          const task =
            (event.data.task as unknown as GeneratedTask) ||
            (event.data as unknown as GeneratedTask);
          const key = String(
            (task as { id?: unknown; name?: unknown })?.id ??
              (task as { name?: unknown })?.name ??
              st.streamedTasks.length,
          );
          if (st.seenTasks.has(key)) break; // dedup replay
          st.seenTasks.add(key);
          st.streamedTasks.push(task as unknown as Record<string, unknown>);
          cb.onTaskDetail(generationId, task);
          break;
        }
        case 'generation_complete': {
          if (st.completed) break; // dedup replay
          st.completed = true;
          const raw = event.data;
          const rawAgents = Array.isArray(raw.agents) ? (raw.agents as Record<string, unknown>[]) : [];
          const rawTasks = Array.isArray(raw.tasks) ? (raw.tasks as Record<string, unknown>[]) : [];

          // Use streamed details as fallback when generation_complete has empty arrays
          const agents = rawAgents.length > 0 ? rawAgents : st.streamedAgents;
          const tasks = rawTasks.length > 0 ? rawTasks : st.streamedTasks;

          cb.onComplete(generationId, { agents, tasks });

          // ChatMode auto-execute folds the execution id into this event:
          // observe the backend-started run, or report a failed launch.
          const executionId =
            (raw.execution_id as string) || (raw.executionId as string) || '';
          if (executionId) {
            cb.onExecutionStarted?.(generationId, executionId, raw.run_name as string | undefined);
          } else if (raw.execution_error) {
            cb.onFailed(generationId, String(raw.execution_error));
          }
          // generation_complete is the single terminal event — close now.
          stopGenerationStream(generationId);
          break;
        }
        case 'generation_failed':
          if (st.completed) break;
          st.completed = true;
          cb.onFailed(
            generationId,
            (event.data.error as string) ||
              (event.data.message as string) ||
              'Generation failed',
          );
          stopGenerationStream(generationId);
          break;
        case 'entity_error':
          // Non-fatal: log but continue
          console.warn('[generationStream] entity_error:', event.data);
          break;
      }
    },
    () => {
      const st = streams.get(generationId);
      if (st && !st.completed) {
        st.callbacks.onFailed(generationId, 'Connection lost during generation');
      }
    },
  );
  state.close = close;
}
