/**
 * Custom hook for progressive crew generation SSE streams.
 *
 * Connects to /sse/generations/{generationId}/stream and dispatches
 * typed callbacks as plan_ready, agent_detail, task_detail,
 * entity_error, generation_complete, and generation_failed events arrive.
 */

import { useEffect, useRef, useCallback, useState } from 'react';
import { config } from '../../config/api/ApiConfig';

/* ------------------------------------------------------------------ */
/*  Public types                                                       */
/* ------------------------------------------------------------------ */

export interface PlanAgent {
  name: string;
  role: string;
}

export interface PlanTask {
  name: string;
  assigned_agent: string;
  context?: string[];
}

export interface PlanReadyData {
  type: 'plan_ready';
  agents: PlanAgent[];
  tasks: PlanTask[];
  process_type?: 'sequential' | 'parallel';
  complexity?: 'light' | 'standard' | 'complex';
}

export interface AgentDetailData {
  type: 'agent_detail';
  index: number;
  agent: Record<string, unknown>;
}

export interface TaskDetailData {
  type: 'task_detail';
  index: number;
  task: Record<string, unknown>;
}

export interface EntityErrorData {
  type: 'entity_error';
  index: number;
  entity_type: 'agent' | 'task';
  name: string;
  error: string;
}

export interface GenerationCompleteData {
  type: 'generation_complete';
  agents: Record<string, unknown>[];
  tasks: Record<string, unknown>[];
}

export interface GenerationFailedData {
  type: 'generation_failed';
  error: string;
}

export interface DependenciesResolvedData {
  type: 'dependencies_resolved';
  task_id: string;
  task_name: string;
  context: string[];
}

export interface ToolConfigNeededData {
  type: 'tool_config_needed';
  task_id: string;
  task_name: string;
  tool_name: string;
  config_fields: string[];
  suggested_space: { id: string; name: string; description: string } | null;
}

export interface CrewGenerationSSEHandlers {
  onPlanReady: (data: PlanReadyData) => void;
  onAgentDetail: (data: AgentDetailData) => void;
  onTaskDetail: (data: TaskDetailData) => void;
  onEntityError: (data: EntityErrorData) => void;
  onDependenciesResolved: (data: DependenciesResolvedData) => void;
  onToolConfigNeeded: (data: ToolConfigNeededData) => void;
  onComplete: (data: GenerationCompleteData) => void;
  onFailed: (data: GenerationFailedData) => void;
}

/* ------------------------------------------------------------------ */
/*  Hook                                                               */
/* ------------------------------------------------------------------ */

export function useCrewGenerationSSE(
  generationId: string | null,
  handlers: CrewGenerationSSEHandlers,
) {
  const eventSourceRef = useRef<EventSource | null>(null);
  const [status, setStatus] = useState<
    'idle' | 'connecting' | 'streaming' | 'complete' | 'failed'
  >('idle');

  // Keep handler refs stable so the effect doesn't re-fire
  const handlersRef = useRef(handlers);
  useEffect(() => {
    handlersRef.current = handlers;
  }, [handlers]);

  const disconnect = useCallback(() => {
    if (eventSourceRef.current) {
      eventSourceRef.current.close();
      eventSourceRef.current = null;
    }
  }, []);

  useEffect(() => {
    if (!generationId) {
      setStatus('idle');
      return;
    }

    disconnect();
    setStatus('connecting');

    const url = `${config.apiUrl}/sse/generations/${generationId}/stream`;
    console.log(`[CrewGenSSE] Connecting to ${url}`);

    const es = new EventSource(url);
    eventSourceRef.current = es;

    // Helper to safely parse event data
    const parse = (raw: string) => {
      try {
        return JSON.parse(raw);
      } catch {
        console.error('[CrewGenSSE] Failed to parse:', raw);
        return null;
      }
    };

    es.addEventListener('connected', () => {
      console.log('[CrewGenSSE] Connected');
      setStatus('streaming');
    });

    es.addEventListener('plan_ready', (e: MessageEvent) => {
      const data = parse(e.data);
      if (data) handlersRef.current.onPlanReady(data);
    });

    es.addEventListener('agent_detail', (e: MessageEvent) => {
      const data = parse(e.data);
      if (data) handlersRef.current.onAgentDetail(data);
    });

    es.addEventListener('task_detail', (e: MessageEvent) => {
      const data = parse(e.data);
      if (data) handlersRef.current.onTaskDetail(data);
    });

    es.addEventListener('entity_error', (e: MessageEvent) => {
      const data = parse(e.data);
      if (data) handlersRef.current.onEntityError(data);
    });

    es.addEventListener('dependencies_resolved', (e: MessageEvent) => {
      const data = parse(e.data);
      if (data) handlersRef.current.onDependenciesResolved(data);
    });

    es.addEventListener('tool_config_needed', (e: MessageEvent) => {
      const data = parse(e.data);
      if (data) handlersRef.current.onToolConfigNeeded(data);
    });

    es.addEventListener('generation_complete', (e: MessageEvent) => {
      const data = parse(e.data);
      if (data) handlersRef.current.onComplete(data);
      setStatus('complete');
      es.close();
    });

    es.addEventListener('generation_failed', (e: MessageEvent) => {
      const data = parse(e.data);
      if (data) handlersRef.current.onFailed(data);
      setStatus('failed');
      es.close();
    });

    es.onerror = () => {
      // SSE auto-reconnects on transient errors; only set failed
      // if the EventSource is fully closed.
      if (es.readyState === EventSource.CLOSED) {
        console.error('[CrewGenSSE] Connection closed');
        setStatus('failed');
      }
    };

    return () => {
      es.close();
    };
  }, [generationId, disconnect]);

  return { status, disconnect };
}
