/**
 * Shared utilities for deterministic task ID extraction and mapping.
 *
 * Centralises the logic that was previously duplicated in monitorTraces(),
 * handleTraceUpdate(), and the TaskNode selector (~500 lines → ~60 lines).
 */

import type { TaskStatus } from '../store/taskExecutionStore';

/** Shape of an SSE trace event coming from the backend. */
export interface TraceEvent {
  event_type?: string;
  event_context?: string;
  trace_metadata?: Record<string, unknown>;
  output?: unknown;
  created_at?: string;
}

/** Shape of a single entry in the taskStates Map. */
export interface TaskStateEntry {
  status: TaskStatus;
  task_name: string;
  started_at?: string;
  completed_at?: string;
  failed_at?: string;
}

// ---------------------------------------------------------------------------
// 1. extractTaskId – canonical store key from an SSE trace event
// ---------------------------------------------------------------------------

/**
 * Returns the canonical store key for a trace event.
 *
 * Priority:
 *   1. trace_metadata.frontend_task_id  (original workflow designer ID)
 *   2. trace_metadata.task_id           (CrewAI internal UUID)
 *   3. extracted task name              (fallback)
 *
 * Returns `null` when no usable ID can be derived.
 */
export function extractTaskId(trace: TraceEvent): string | null {
  const meta = trace.trace_metadata;

  if (meta?.frontend_task_id && typeof meta.frontend_task_id === 'string') {
    return meta.frontend_task_id;
  }

  if (meta?.task_id && typeof meta.task_id === 'string') {
    return meta.task_id;
  }

  // Fall back to task name
  const name = extractTaskName(trace);
  return name || null;
}

// ---------------------------------------------------------------------------
// 2. extractTaskName – display-friendly task name
// ---------------------------------------------------------------------------

/**
 * Returns a display-friendly task name from a trace event.
 *
 * Handles generic event_context values like 'starting_task', 'completing_task',
 * 'task_error' by falling back to trace_metadata.task_name.
 */
export function extractTaskName(trace: TraceEvent): string | null {
  const meta = trace.trace_metadata;
  const ctx = trace.event_context;

  const GENERIC_CONTEXTS = new Set([
    'starting_task',
    'completing_task',
    'task_completion',
    'task_error',
  ]);

  // If context is a real task description, use it
  if (ctx && !GENERIC_CONTEXTS.has(ctx) && ctx.length >= 5) {
    return ctx;
  }

  // Otherwise, pull from metadata
  if (meta?.task_name && typeof meta.task_name === 'string') {
    return meta.task_name;
  }

  return null;
}

// ---------------------------------------------------------------------------
// 3. mapEventToStatus – event type → TaskStatus
// ---------------------------------------------------------------------------

/**
 * Maps an SSE event_type (and optional event_context) to a TaskStatus value.
 */
export function mapEventToStatus(
  eventType?: string,
  _eventContext?: string
): TaskStatus {
  switch (eventType) {
    case 'task_failed':
      return 'failed';
    case 'task_completed':
      return 'completed';
    case 'task_started':
    default:
      return 'running';
  }
}

// ---------------------------------------------------------------------------
// 4. findTaskStoreKey – deterministic 2-step lookup for TaskNode
// ---------------------------------------------------------------------------

/**
 * Finds the store key for a TaskNode given its taskId and label.
 *
 * Step 1: Match by nodeTaskId (exact, then strip 'task-' prefix)
 * Step 2: Match by nodeLabel  (exact, then case-insensitive)
 * Step 3: Match by stored task_name field against nodeLabel (case-insensitive)
 * Step 4: Prefix match — key starts with label or label starts with key
 *
 * Returns `null` when no matching key is found.
 */
export function findTaskStoreKey(
  taskStates: Map<string, TaskStateEntry>,
  nodeTaskId?: string,
  nodeLabel?: string
): string | null {
  // Step 1a: exact match on nodeTaskId
  if (nodeTaskId && taskStates.has(nodeTaskId)) {
    return nodeTaskId;
  }

  // Step 1b: strip 'task-' prefix and try again
  if (nodeTaskId?.startsWith('task-')) {
    const stripped = nodeTaskId.substring(5);
    if (taskStates.has(stripped)) {
      return stripped;
    }
  }

  // Step 2a: exact match on label
  if (nodeLabel && taskStates.has(nodeLabel)) {
    return nodeLabel;
  }

  // Step 2b–4: fuzzy matching requires iteration
  if (nodeLabel) {
    const labelLower = nodeLabel.toLowerCase();

    for (const [key, entry] of taskStates.entries()) {
      const keyLower = key.toLowerCase();

      // Step 2b: case-insensitive key match
      if (keyLower === labelLower) {
        return key;
      }

      // Step 3: stored task_name matches the label (case-insensitive)
      if (entry.task_name) {
        const nameLower = entry.task_name.toLowerCase();
        if (nameLower === labelLower) {
          return key;
        }
      }

      // Step 4: prefix match — key starts with label or label starts with key
      // Covers cases where the backend sends a full description but the label
      // is a shorter version, or vice-versa.
      if (labelLower.length >= 5 && (keyLower.startsWith(labelLower) || labelLower.startsWith(keyLower))) {
        return key;
      }
    }
  }

  return null;
}
