import { useRef, useCallback, useEffect } from 'react';
import { streamGeneration, StreamEvent } from '../api/streaming';
import { GeneratedAgent, GeneratedTask } from '../types/dispatcher';

export interface GenerationCompleteData {
  agents: Record<string, unknown>[];
  tasks: Record<string, unknown>[];
}

interface UseGenerationStreamOptions {
  onPlanReady: (plan: Record<string, unknown>) => void;
  onAgentDetail: (agent: GeneratedAgent) => void;
  onTaskDetail: (task: GeneratedTask) => void;
  onComplete: (data: GenerationCompleteData) => void;
  onFailed: (error: string) => void;
}

export function useGenerationStream(options: UseGenerationStreamOptions) {
  const closeRef = useRef<(() => void) | null>(null);
  const completedRef = useRef(false);
  // Accumulate agent/task details as they stream in — used as fallback
  // when generation_complete arrives with empty arrays (e.g. backend FK errors).
  const streamedAgentsRef = useRef<Record<string, unknown>[]>([]);
  const streamedTasksRef = useRef<Record<string, unknown>[]>([]);
  // Keep a ref to always have the latest options (avoids stale closure)
  const optionsRef = useRef(options);
  useEffect(() => {
    optionsRef.current = options;
  });

  const startStream = useCallback(
    (generationId: string) => {
      if (closeRef.current) {
        closeRef.current();
      }
      completedRef.current = false;
      streamedAgentsRef.current = [];
      streamedTasksRef.current = [];

      closeRef.current = streamGeneration(
        generationId,
        (event: StreamEvent) => {
          const opts = optionsRef.current;
          switch (event.event) {
            case 'plan_ready':
              opts.onPlanReady(event.data);
              break;
            case 'agent_detail': {
              const agent = (event.data.agent as unknown as GeneratedAgent) || event.data as unknown as GeneratedAgent;
              streamedAgentsRef.current.push(agent as unknown as Record<string, unknown>);
              opts.onAgentDetail(agent);
              break;
            }
            case 'task_detail': {
              const task = (event.data.task as unknown as GeneratedTask) || event.data as unknown as GeneratedTask;
              streamedTasksRef.current.push(task as unknown as Record<string, unknown>);
              opts.onTaskDetail(task);
              break;
            }
            case 'generation_complete': {
              completedRef.current = true;
              const raw = event.data;
              const rawAgents = Array.isArray(raw.agents) ? raw.agents as Record<string, unknown>[] : [];
              const rawTasks = Array.isArray(raw.tasks) ? raw.tasks as Record<string, unknown>[] : [];

              // Use streamed details as fallback when generation_complete has empty arrays
              const agents = rawAgents.length > 0 ? rawAgents : streamedAgentsRef.current;
              const tasks = rawTasks.length > 0 ? rawTasks : streamedTasksRef.current;

              opts.onComplete({ agents, tasks });
              stopStream();
              break;
            }
            case 'generation_failed':
              completedRef.current = true;
              opts.onFailed(
                (event.data.error as string) ||
                  (event.data.message as string) ||
                  'Generation failed'
              );
              stopStream();
              break;
            case 'entity_error':
              // Non-fatal: log but continue
              console.warn('[generationStream] entity_error:', event.data);
              break;
          }
        },
        () => {
          if (!completedRef.current) {
            optionsRef.current.onFailed('Connection lost during generation');
          }
        }
      );
    },
    [] // stable — uses optionsRef internally
  );

  const stopStream = useCallback(() => {
    if (closeRef.current) {
      closeRef.current();
      closeRef.current = null;
    }
  }, []);

  return { startStream, stopStream };
}
