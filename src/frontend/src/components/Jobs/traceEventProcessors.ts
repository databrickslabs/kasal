/**
 * Registry-based trace event processing system
 * Replaces 490-line if/else chain with maintainable, extensible registry pattern
 */

import React from 'react';
import CheckCircleIcon from '@mui/icons-material/CheckCircle';
import HistoryIcon from '@mui/icons-material/History';
import SaveIcon from '@mui/icons-material/Save';
import PlayCircleIcon from '@mui/icons-material/PlayCircle';
import PreviewIcon from '@mui/icons-material/Preview';
import TerminalIcon from '@mui/icons-material/Terminal';
import RefreshIcon from '@mui/icons-material/Refresh';
import PlayArrowIcon from '@mui/icons-material/PlayArrow';
import WarningAmberIcon from '@mui/icons-material/WarningAmber';
import ErrorOutlineIcon from '@mui/icons-material/ErrorOutline';
import StorageIcon from '@mui/icons-material/Storage';
import TimelineIcon from '@mui/icons-material/Timeline';
import CompressIcon from '@mui/icons-material/Compress';
import DashboardIcon from '@mui/icons-material/Dashboard';
import ChecklistIcon from '@mui/icons-material/Checklist';

// Import Trace type from the store
import { Trace } from '../../store/runStatus';
import { isRedactedReasoning } from '../Common/ReasoningPanel';
import { extractPlanItems } from './TracePlanView';

// ============================================================================
// Shared Extraction Helpers
// ============================================================================

/**
 * Parse trace_metadata safely from trace
 */
export function parseTraceMetadata(trace: Trace): Record<string, unknown> | null {
  if (!trace.trace_metadata) return null;
  if (typeof trace.trace_metadata === 'string') {
    try {
      return JSON.parse(trace.trace_metadata);
    } catch {
      return null;
    }
  }
  if (typeof trace.trace_metadata === 'object') {
    return trace.trace_metadata as Record<string, unknown>;
  }
  return null;
}

/**
 * Is this LLM row the memory layer labelling the record it is saving?
 *
 * The engine stamps `llm_purpose` on LLM calls raised from inside a memory
 * save. Those run after their task has completed, so without the marker the
 * timeline shows the agent apparently still working past "Task Completed".
 */
export function isMemoryLabelling(metadata: Record<string, unknown> | null): boolean {
  return metadata?.llm_purpose === 'memory_labelling';
}

/**
 * Extract extra_data from output or trace
 */
export function extractExtraData(trace: Trace): Record<string, unknown> | undefined {
  // Check output.extra_data first
  if (trace.output && typeof trace.output === 'object' && 'extra_data' in trace.output) {
    return (trace.output as Record<string, unknown>).extra_data as Record<string, unknown>;
  }
  // Then trace-level extra_data
  if (trace.extra_data && typeof trace.extra_data === 'object') {
    return trace.extra_data as Record<string, unknown>;
  }
  return undefined;
}

/**
 * Extract output as a string
 */
export function extractOutputStr(trace: Trace): string {
  if (!trace.output) return '';
  if (typeof trace.output === 'string') return trace.output;
  if (typeof trace.output === 'object' && 'content' in trace.output) {
    return String((trace.output as Record<string, unknown>).content || '');
  }
  return '';
}

/**
 * Extract content for display (unwrap output.content)
 */
export function extractOutputForDisplay(output: any): string | Record<string, unknown> | undefined {
  if (!output) return undefined;
  if (typeof output === 'object' && 'content' in output) {
    const content = (output as Record<string, unknown>).content;
    if (typeof content === 'string' || (typeof content === 'object' && content !== null)) {
      return content as string | Record<string, unknown>;
    }
  }
  return output;
}

/**
 * Extract tool name from event_context
 */
export function extractToolName(trace: Trace): string {
  if (trace.event_context && trace.event_context.startsWith('tool:')) {
    return trace.event_context.substring(5);
  }
  // Check output.extra_data.tool_name (OTel bridge stores it there)
  const toolName = getField(trace, 'tool_name') as string | undefined;
  if (toolName) {
    return toolName;
  }
  return 'Tool';
}

/**
 * Was this call answered from an earlier run's recording?
 *
 * Three places to look, because the two paths write the row differently: the
 * crew and flow paths go through the OTel bridge, which lands the flag in
 * `trace_metadata` and `extra_data`; the chat path builds its own row and puts
 * it at the top level of `output`. Missing that last case is why a chat call
 * that WAS replayed — `from_cache: true`, 0 ms, no API call — still rendered
 * as an ordinary tool row.
 */
export function isFromCache(trace: Trace): boolean {
  if (getField(trace, 'from_cache')) return true;
  const output = trace.output;
  return Boolean(
    output &&
      typeof output === 'object' &&
      (output as Record<string, unknown>).from_cache,
  );
}

/**
 * Get a field from trace_metadata, falling back to extra_data
 */
export function getField(trace: Trace, field: string): unknown {
  const metadata = parseTraceMetadata(trace);
  if (metadata && metadata[field] !== undefined) return metadata[field];
  const extra = extractExtraData(trace);
  if (extra && extra[field] !== undefined) return extra[field];
  return undefined;
}

/**
 * Extract memory type from trace
 */
export function extractMemoryType(trace: Trace): string {
  const metadata = parseTraceMetadata(trace);
  if (metadata?.memory_type && metadata.memory_type !== 'memory') {
    return metadata.memory_type as string;
  }
  const extra = extractExtraData(trace);
  if (extra?.memory_type && extra.memory_type !== 'memory') {
    return extra.memory_type as string;
  }
  if (trace.event_context) {
    const match = trace.event_context.match(/(?:saved_|saving_|retrieved_|memory_query\[)(\w+)/);
    if (match) return match[1];
  }
  return 'memory';
}

/**
 * Format memory type for display
 */
export function formatMemoryType(type: string): string {
  if (type === 'short_term') return 'Short-Term Memory';
  if (type === 'long_term') return 'Long-Term Memory';
  if (type === 'entity') return 'Entity Memory';
  return type;
}

// ============================================================================
// ProcessedEvent Interface
// ============================================================================

export interface ProcessedEvent {
  type: string;
  description: string;
  /** Intrinsic measured duration in ms (memory query/save time, MCP call time,
   *  ...). Kept out of the label — the timeline renders it in the shared
   *  duration column; rows without one get a derived gap-to-next instead. */
  durationMs?: number;
}

// ============================================================================
// Event Processor Type
// ============================================================================

type EventProcessor = (trace: Trace) => ProcessedEvent | null;

// ============================================================================
// Event Processors Registry
// ============================================================================

export const EVENT_PROCESSORS: Record<string, EventProcessor> = {
  // LLM Call (prompt sent to LLM)
  llm_call: (trace: Trace): ProcessedEvent => {
    const metadata = parseTraceMetadata(trace);

    const modelName = (metadata?.model as string) || '';
    const messageCount = metadata?.message_count as number | undefined;
    // `prompt_chars` is the TRUE length, recorded when the list response trimmed
    // the copy it shipped. Reading length off the trimmed string would report
    // the preview size — "(2,000 chars)" for a 34,000-char prompt is not a
    // smaller truth, it is a wrong one.
    const promptLen = (metadata?.prompt_chars as number)
      ?? ((metadata?.prompt as string)?.length || 0);

    // Whose call this is. Both of these fire AFTER the task finished, so an
    // unlabelled row reads as the agent still working: the memory layer tagging
    // the record it just saved, and A2UI composing the answer into a surface.
    const attempt = metadata?.attempt as number | undefined;
    const label = metadata?.llm_purpose === 'a2ui_compose'
      ? `A2UI Compose Request${attempt ? ` #${attempt}` : ''}`
      : isMemoryLabelling(metadata)
        ? 'Memory Labelling'
        : 'LLM Request';

    let description = label;
    if (modelName) {
      const modelParts = modelName.split('/');
      const shortModel = modelParts[modelParts.length - 1];
      description = `${label} — ${shortModel}`;
    }
    if (promptLen > 0) {
      description += ` (${promptLen.toLocaleString()} chars)`;
    } else if (messageCount) {
      description += ` (${messageCount} messages)`;
    }

    return { type: 'llm', description };
  },

  // LLM Call Failed
  llm_call_failed: (trace: Trace): ProcessedEvent => {
    const metadata = parseTraceMetadata(trace);
    const error = (metadata?.error as string) || 'LLM call failed';
    const description = error.length > 100 ? error.substring(0, 97) + '...' : error;
    return { type: 'error', description: `LLM Error: ${description}` };
  },

  // Tool Usage
  tool_usage: (trace: Trace): ProcessedEvent => {
    const toolName = extractToolName(trace);
    const metadata = parseTraceMetadata(trace);
    const operation = metadata?.operation as string | undefined;
    const cacheSuffix = isFromCache(trace) ? ' [cached]' : '';

    if (operation === 'tool_started') {
      return { type: 'tool', description: `${toolName} (input)` };
    } else if (operation === 'tool_finished') {
      return { type: 'tool_result', description: `${toolName} (output)${cacheSuffix}` };
    } else {
      return { type: 'tool', description: `${toolName}${cacheSuffix}` };
    }
  },

  // LLM Response
  llm_response: (trace: Trace): ProcessedEvent => {
    let outputLen = 0;
    const metadata = parseTraceMetadata(trace);

    // Check metadata for the true size (output_length from the backend, or
    // content_chars recorded when the list response trimmed this row).
    if (metadata?.content_chars) {
      outputLen = metadata.content_chars as number;
    } else if (metadata?.output_length) {
      outputLen = metadata.output_length as number;
    } else {
      // Calculate from output
      if (trace.output) {
        if (typeof trace.output === 'string') {
          outputLen = trace.output.length;
        } else if (typeof trace.output === 'object' && 'content' in trace.output) {
          outputLen = String((trace.output as Record<string, unknown>).content || '').length;
        }
      }
    }

    const attempt = metadata?.attempt as number | undefined;
    const label = metadata?.llm_purpose === 'a2ui_compose'
      ? `A2UI Compose Response${attempt ? ` #${attempt}` : ''}`
      : isMemoryLabelling(metadata)
        ? 'Memory Labels'
        : 'LLM Response';
    // Flag reasoning in the row itself so it is discoverable without opening
    // the detail pane — otherwise the only hint that a model exposed its
    // thinking is a collapsed section one click away. The REDACTED sentinel gets
    // its own wording: the model did think, the provider just withheld it, and
    // saying plain "reasoning" would promise text the pane cannot show.
    const rawReasoning = typeof metadata?.reasoning === 'string'
      ? (metadata.reasoning as string).trim()
      : '';
    const suffix = !rawReasoning
      ? ''
      : isRedactedReasoning(rawReasoning)
        ? ' · reasoning hidden'
        : ' · reasoning';
    const description = outputLen > 0
      ? `${label} (${outputLen.toLocaleString()} chars)${suffix}`
      : `${label}${suffix}`;

    return { type: 'llm_response', description };
  },

  // Agent Execution — all are instrumentor container spans; real events use specific types
  agent_execution: (): ProcessedEvent | null => null,

  // Agent Step — same as agent_execution, instrumentor container
  agent_step: (): ProcessedEvent | null => null,

  // Task Started - skip.
  // It rendered "Starting: <task description>", which is the task header's own
  // text repeated one line below it, and it carries no output to open. The header
  // already names the task and shows its start offset, so the row was pure
  // duplication. (Same treatment as memory_write_started below.)
  task_started: (): ProcessedEvent | null => null,

  // Task Completed. Labelled without echoing the description — the row sits
  // under the task header that already shows it. Kept (unlike task_started)
  // because this row carries the task OUTPUT and is clickable.
  task_completed: (): ProcessedEvent => {
    return { type: 'task_complete', description: 'Task Completed' };
  },

  // Memory Write Started - skip
  memory_write_started: (): ProcessedEvent | null => null,

  // Memory Retrieval Started - skip
  memory_retrieval_started: (): ProcessedEvent | null => null,

  // Memory Write
  memory_write: (trace: Trace): ProcessedEvent => {
    const memoryType = extractMemoryType(trace);
    const formattedType = memoryType !== 'memory' ? formatMemoryType(memoryType) : '';
    const metadata = parseTraceMetadata(trace);
    const saveTime = metadata?.save_time_ms as number | undefined;
    const description = formattedType
      ? `Memory Write (${formattedType})`
      : 'Memory Write';
    return { type: 'memory_write', description, durationMs: saveTime };
  },

  // Memory Retrieval
  memory_retrieval: (trace: Trace): ProcessedEvent => {
    const memoryType = extractMemoryType(trace);
    const formattedType = memoryType !== 'memory' ? formatMemoryType(memoryType) : '';

    const metadata = parseTraceMetadata(trace);
    const extra = extractExtraData(trace);
    const resultsCount = (metadata?.results_count as number) || (extra?.results_count as number) || 0;
    const queryTime = (metadata?.query_time_ms as number) || (metadata?.retrieval_time_ms as number);

    let description = formattedType ? `Memory Read (${formattedType})` : 'Memory Read';
    if (resultsCount > 0) {
      description += ` — ${resultsCount} results`;
    }

    return { type: 'memory_retrieval', description, durationMs: queryTime };
  },

  // Memory Retrieval Completed - aggregated memory context
  memory_retrieval_completed: (trace: Trace): ProcessedEvent => {
    const metadata = parseTraceMetadata(trace);
    const extra = extractExtraData(trace);
    const retrievalTime = (metadata?.retrieval_time_ms as number) || (extra?.retrieval_time_ms as number);
    return { type: 'memory_context', description: 'Memory Context Retrieved', durationMs: retrievalTime };
  },

  // Memory Context Retrieved
  memory_context_retrieved: (trace: Trace): ProcessedEvent => {
    const extra = extractExtraData(trace);
    const contentLength = (extra?.content_length as number) || 0;

    const description = contentLength > 0
      ? `Memory Context Retrieved (${contentLength} chars)`
      : 'Memory Context Retrieved';

    return { type: 'memory_context', description };
  },

  // Memory Operation
  memory_operation: (trace: Trace): ProcessedEvent => {
    const metadata = parseTraceMetadata(trace);
    const extra = extractExtraData(trace);

    const memoryType = (metadata?.memory_type as string) || (extra?.memory_type as string) || '';
    const operation = (metadata?.operation as string) || (extra?.operation as string) || '';

    // Build description from available info
    if (operation && memoryType) {
      const opLabel = operation.includes('query') || operation.includes('retriev') ? 'Read' : 'Write';
      return { type: 'memory_operation', description: `Memory ${opLabel} (${memoryType})` };
    } else if (trace.event_context) {
      if (trace.event_context.includes('query')) {
        const desc = memoryType ? `Memory Query (${memoryType})` : 'Memory Query';
        return { type: 'memory_operation', description: desc };
      } else if (trace.event_context.includes('sav')) {
        const desc = memoryType ? `Memory Save (${memoryType})` : 'Memory Save';
        return { type: 'memory_operation', description: desc };
      } else {
        const desc = memoryType ? `Memory Operation (${memoryType})` : 'Memory Operation';
        return { type: 'memory_operation', description: desc };
      }
    } else {
      const desc = memoryType ? `Memory Operation (${memoryType})` : 'Memory Operation';
      return { type: 'memory_operation', description: desc };
    }
  },

  // Memory Backend Error
  memory_backend_error: (trace: Trace): ProcessedEvent => {
    const metadata = parseTraceMetadata(trace);
    const extra = extractExtraData(trace);

    let title = 'Memory Backend Error';
    const errorType = (metadata?.error_type as string) || (extra?.error_type as string) || '';

    if (metadata?.title) title = metadata.title as string;
    if (!title && extra?.title) title = extra.title as string;

    // Provide descriptive message based on error type
    let description = title;
    if (errorType === 'missing_indexes') {
      description = '⚠️ Databricks Indexes Not Found';
    } else if (errorType === 'provisioning_indexes') {
      description = '⏳ Databricks Indexes Still Provisioning';
    }

    return { type: 'memory_backend_error', description };
  },

  // Knowledge Operation
  knowledge_operation: (): ProcessedEvent => {
    return { type: 'knowledge_operation', description: 'Knowledge Operation' };
  },

  // LLM Guardrail
  llm_guardrail: (trace: Trace): ProcessedEvent => {
    const metadata = parseTraceMetadata(trace);
    const extra = extractExtraData(trace);
    const success = metadata?.success ?? extra?.success;
    const guardrailName = (metadata?.guardrail as string) || (extra?.guardrail as string) || '';

    let description = guardrailName ? `Guardrail: ${guardrailName}` : 'LLM Guardrail Check';
    if (success === true) {
      description = guardrailName ? `Guardrail Passed: ${guardrailName}` : 'Guardrail Passed';
    } else if (success === false) {
      description = guardrailName ? `Guardrail Failed: ${guardrailName}` : 'Guardrail Failed';
    }

    return { type: 'guardrail', description };
  },

  // Rate Limit
  rate_limit: (trace: Trace): ProcessedEvent => {
    const metadata = parseTraceMetadata(trace);

    const model = (metadata?.model as string) || '';
    const attempt = metadata?.attempt ? `(attempt ${metadata.attempt})` : '';

    const description = model
      ? `Rate Limit: ${model} ${attempt}`.trim()
      : `Rate Limit ${attempt}`.trim();

    return { type: 'rate_limit', description };
  },

  // Task Failed
  task_failed: (trace: Trace): ProcessedEvent => {
    const metadata = parseTraceMetadata(trace);
    const extra = extractExtraData(trace);

    let errorMsg = 'Task Failed';
    const error = (metadata?.error as string) || (extra?.error as string);
    if (error) {
      errorMsg = error.length > 120 ? error.substring(0, 117) + '...' : error;
    } else if (trace.output) {
      const outputStr = extractOutputStr(trace);
      if (outputStr && outputStr.includes('failed:')) {
        const failedPart = outputStr.split('failed:')[1]?.trim();
        if (failedPart) {
          errorMsg = failedPart;
        }
      }
    }

    return { type: 'task_failed', description: errorMsg };
  },

  // LLM Request
  llm_request: (trace: Trace): ProcessedEvent => {
    const extra = extractExtraData(trace);

    let promptLength = 0;
    if (typeof extra?.prompt_length === 'number') {
      promptLength = extra.prompt_length;
    } else if (trace.output) {
      const outputStr = typeof trace.output === 'string'
        ? trace.output
        : JSON.stringify(trace.output);
      promptLength = outputStr.length;
    }

    return { type: 'llm_request', description: `LLM Request (${promptLength.toLocaleString()} chars)` };
  },

  // Knowledge Retrieval Started - skip
  knowledge_retrieval_started: (): ProcessedEvent | null => null,

  // Guardrail Started - skip
  guardrail_started: (): ProcessedEvent | null => null,

  // Tool Error
  tool_error: (trace: Trace): ProcessedEvent => {
    let toolName = '';
    if (trace.event_context && trace.event_context.startsWith('tool:')) {
      toolName = trace.event_context.substring(5);
    }

    const errorMsg = trace.output && typeof trace.output === 'object'
      ? ((trace.output as Record<string, unknown>).content as string) || ''
      : '';

    const description = toolName
      ? `Tool Error: ${toolName}${errorMsg ? ' — ' + errorMsg.substring(0, 100) : ''}`
      : `Tool Error${errorMsg ? ': ' + errorMsg.substring(0, 100) : ''}`;

    return { type: 'tool_error', description };
  },

  // Flow Created
  flow_created: (): ProcessedEvent => {
    return { type: 'flow_created', description: 'Flow Created' };
  },

  // MCP Connection Started
  mcp_connection_started: (trace: Trace): ProcessedEvent => {
    const metadata = parseTraceMetadata(trace);
    const serverName = metadata?.server_name as string | undefined;
    const description = serverName ? `MCP Connecting: ${serverName}...` : 'MCP Connecting...';
    return { type: 'mcp_connection', description };
  },

  // MCP Connection Completed
  mcp_connection_completed: (trace: Trace): ProcessedEvent => {
    const metadata = parseTraceMetadata(trace);
    const serverName = metadata?.server_name as string | undefined;
    const duration = metadata?.connection_duration_ms as number | undefined;
    const description = serverName ? `MCP Connected: ${serverName}` : 'MCP Connected';
    return { type: 'mcp_connection', description, durationMs: duration };
  },

  // MCP Tool Started
  mcp_tool_started: (trace: Trace): ProcessedEvent => {
    const mcpToolName = trace.event_context && trace.event_context.startsWith('tool:')
      ? trace.event_context.substring(5)
      : 'MCP Tool';
    return { type: 'mcp_tool', description: `MCP: ${mcpToolName} (calling)` };
  },

  // MCP Tool Completed
  mcp_tool_completed: (trace: Trace): ProcessedEvent => {
    const metadata = parseTraceMetadata(trace);
    const mcpToolName = trace.event_context && trace.event_context.startsWith('tool:')
      ? trace.event_context.substring(5)
      : 'MCP Tool';
    const execTime = metadata?.execution_duration_ms as number | undefined;
    return { type: 'mcp_tool_result', description: `MCP: ${mcpToolName} (result)`, durationMs: execTime };
  },

  // HITL Feedback Requested
  hitl_feedback_requested: (trace: Trace): ProcessedEvent => {
    const metadata = parseTraceMetadata(trace);
    const message = metadata?.message as string | undefined;
    const description = message
      ? `Human Input: ${message.length > 60 ? message.substring(0, 57) + '...' : message}`
      : 'Human Feedback Requested';
    return { type: 'hitl_request', description };
  },

  // HITL Feedback Received
  hitl_feedback_received: (trace: Trace): ProcessedEvent => {
    const metadata = parseTraceMetadata(trace);
    const outcome = metadata?.outcome as string | undefined;
    const description = outcome
      ? `Human Feedback: ${outcome}`
      : 'Human Feedback Received';
    return { type: 'hitl_response', description };
  },

  // Flow Execution Failed — flow-level error (timeout, configuration, etc.)
  flow_execution_failed: (trace: Trace): ProcessedEvent => {
    const metadata = parseTraceMetadata(trace);
    const extra = extractExtraData(trace);
    const output = trace.output && typeof trace.output === 'object'
      ? (trace.output as Record<string, unknown>)
      : null;
    let errorMsg = 'Flow execution failed';
    const error = (metadata?.error as string) || (extra?.error as string) || (output?.content as string);
    if (error) {
      errorMsg = error.length > 200 ? error.substring(0, 197) + '...' : error;
    }
    return { type: 'error', description: errorMsg };
  },

  // Context Compaction — the conversation was trimmed to fit the model window.
  // Lossy, so the row leads with WHAT was dropped and against which budget: a
  // run that compacts repeatedly is losing tool results it still needs, which
  // is what drives an agent to re-query and burn its round budget.
  context_compaction: (trace: Trace): ProcessedEvent => {
    const metadata = parseTraceMetadata(trace);
    const extra = extractExtraData(trace);
    const num = (field: string): number | undefined => {
      const value = (metadata?.[field] ?? extra?.[field]) as unknown;
      return typeof value === 'number' ? value : undefined;
    };
    const compact = (tokens: number): string =>
      tokens >= 1000 ? `${(tokens / 1000).toFixed(1)}k` : String(tokens);

    const before = num('tokens_before');
    const after = num('tokens_after');
    const dropped = num('messages_compacted');
    const window = num('window');

    let description = 'Context Compacted';
    if (before !== undefined && after !== undefined) {
      description += ` — ${compact(before)} → ${compact(after)} tokens`;
    }
    if (window !== undefined) {
      description += ` (budget ${compact(window)})`;
    }
    if (dropped) {
      description += `, ${dropped} message${dropped === 1 ? '' : 's'} dropped`;
    }

    return { type: 'context_compaction', description };
  },

  // A2UI surface composition. Recorded for EVERY outcome, so the row must say
  // which one: a skipped surface is the case people actually ask about ("why did
  // I not get a presentation?"), and every skip used to be silent.
  a2ui_surface: (trace: Trace): ProcessedEvent => {
    const metadata = parseTraceMetadata(trace);
    const extra = extractExtraData(trace);
    const field = (name: string): unknown => metadata?.[name] ?? extra?.[name];

    const outcome = String(field('outcome') || 'composed');
    const kind = field('surface_kind');
    const components = field('component_count');

    // Composition is a real LLM round-trip (often several) — carry its measured
    // time so the row does not read as 0 ms, which is what a single-row group
    // derives from timestamps alone.
    const durationMs = typeof field('duration_ms') === 'number'
      ? (field('duration_ms') as number)
      : undefined;

    if (outcome === 'composed') {
      let description = kind ? `A2UI Surface — ${kind}` : 'A2UI Surface';
      if (typeof components === 'number') {
        description += ` (${components} component${components === 1 ? '' : 's'})`;
      }
      return { type: 'a2ui_surface', description, durationMs };
    }

    // Named so the gate is readable at a glance in the timeline; the full
    // sentence is on the row's `reason`, in the output dialog.
    const SKIP_LABEL: Record<string, string> = {
      disabled: 'A2UI disabled for this workspace',
      no_text: 'no answer text to render',
      no_rich_intent: 'no rich surface implied',
      composer_unavailable: 'composer LLM unavailable',
      compose_failed: 'composer failed',
      conversation_fallback: 'composer returned prose',
      no_data_component: 'surface had no data component',
    };
    return {
      type: 'a2ui_skipped',
      description: `A2UI Skipped — ${SKIP_LABEL[outcome] || outcome}`,
      durationMs,
    };
  },

  // The agent's plan for the task. The engine emits one of these beside every
  // `todo` tool call, so the row has to earn its place: "Plan Updated" (the
  // generic Title-Case fallback) says nothing the `todo` row above it does not,
  // which is why it read as noise. Progress is the one thing only this row
  // knows — how far along the plan is, and which step is running now.
  plan_updated: (trace: Trace): ProcessedEvent => {
    const extra = extractExtraData(trace);
    const num = (name: string): number | undefined =>
      typeof extra?.[name] === 'number' ? (extra[name] as number) : undefined;

    // Counts come off the event when the bridge stamped them; deriving from the
    // items keeps the row honest for a plan that arrived as JSON only.
    const items = extractPlanItems(trace.output) ?? [];
    const total = num('plan_total') ?? items.length;
    const completed =
      num('plan_completed') ?? items.filter((i) => i.status === 'completed').length;

    if (!total) return { type: 'plan_updated', description: 'Plan Updated' };

    let description = `Plan — ${completed}/${total} done`;
    const current = items.find((i) => i.status === 'in_progress');
    if (current) {
      const step = current.label || current.content;
      description += ` · now: ${step.length > 60 ? `${step.slice(0, 59)}…` : step}`;
    }
    return { type: 'plan_updated', description };
  },

  // Crew Execution (instrumentor root span) — skip, bridge handles crew_started/completed
  crew_execution: (): ProcessedEvent | null => {
    return null;
  },
};

// ============================================================================
// Process Trace Event Function
// ============================================================================

/**
 * Process a trace event using the registry
 * Returns null if event should be filtered out
 */
export function processTraceEvent(trace: Trace): ProcessedEvent | null {
  const processor = EVENT_PROCESSORS[trace.event_type];
  if (processor) {
    return processor(trace);
  }

  // Light-agent (chat) path emits tool results as `<tool>_run` and the agent's
  // final answer as `response_run` (the crew/OTel path instead uses
  // tool_usage+operation, which the registry above handles). Map these to
  // clickable result rows so their output — the tool's answer / final response —
  // is viewable, instead of falling to the generic non-clickable Title-Case row.
  if (trace.event_type.endsWith('_run')) {
    if (trace.event_type === 'response_run') {
      // Dropped, not renamed. It is the agent-completion echo of the answer the
      // llm_response row above already carries IN FULL — this copy is a 280-char
      // preview (light_agent_service._on_agent_completed) — and it was labelled
      // "Final Response" while A2UI composition still runs after it, so it was
      // neither final nor the whole answer. The OTel bridge drops the crew
      // path's equivalent (AgentExecutionCompletedEvent) for the same reason,
      // which is why a crew trace ends at "LLM Response" and chat did not.
      //
      // The row stays in the DB: ChatMode's own step list renders `_run` rows
      // live and on refresh, and that is where a one-line preview belongs.
      return null;
    }
    const toolName = extractToolName(trace);
    // The badge belongs here too. These are the CHAT path's tool rows, and
    // chat is where replay pays off most (re-asking the same question), so a
    // branch that could not render "[cached]" made a working feature look
    // broken — the row said nothing while the record behind it said
    // from_cache: true, 0 ms.
    const cacheSuffix = isFromCache(trace) ? ' [cached]' : '';
    return { type: 'tool_result', description: `${toolName} (output)${cacheSuffix}` };
  }

  // Default: convert event_type to Title Case
  const readableDesc = trace.event_type
    .replace(/_/g, ' ')
    .replace(/\b\w/g, (l) => l.toUpperCase());

  return { type: trace.event_type, description: readableDesc };
}

// ============================================================================
// Icon Configuration
// ============================================================================

interface IconConfig {
  Component: React.ElementType;
  color: 'primary' | 'success' | 'error' | 'warning' | 'info' | 'action' | 'inherit';
}

export const ICON_CONFIG: Record<string, IconConfig> = {
  tool: { Component: TerminalIcon, color: 'primary' },
  tool_result: { Component: TerminalIcon, color: 'success' },
  tool_usage: { Component: TerminalIcon, color: 'action' },
  llm: { Component: PlayCircleIcon, color: 'primary' },
  llm_response: { Component: PlayCircleIcon, color: 'success' },
  agent_start: { Component: PlayArrowIcon, color: 'primary' },
  task_start: { Component: PlayArrowIcon, color: 'primary' },
  started: { Component: PlayArrowIcon, color: 'primary' },
  agent_complete: { Component: CheckCircleIcon, color: 'success' },
  task_complete: { Component: CheckCircleIcon, color: 'success' },
  completed: { Component: CheckCircleIcon, color: 'success' },
  agent_output: { Component: PreviewIcon, color: 'action' },
  agent_execution: { Component: PreviewIcon, color: 'action' },
  agent_processing: { Component: RefreshIcon, color: 'action' },
  memory_write: { Component: StorageIcon, color: 'primary' },
  memory_retrieval: { Component: StorageIcon, color: 'success' },
  memory_context: { Component: StorageIcon, color: 'info' },
  memory_operation: { Component: StorageIcon, color: 'action' },
  memory_backend_error: { Component: ErrorOutlineIcon, color: 'error' },
  knowledge_operation: { Component: TimelineIcon, color: 'action' },
  crew_started: { Component: PlayCircleIcon, color: 'primary' },
  crew_completed: { Component: CheckCircleIcon, color: 'success' },
  // A crew a resume RESTORED from a checkpoint rather than executed. Its own
  // icon and colour on purpose: the timeline shows the whole flow, but restored
  // work must not read as work that just ran.
  crew_checkpoint_restored: { Component: HistoryIcon, color: 'info' },
  task_checkpoint_restored: { Component: HistoryIcon, color: 'info' },
  // A checkpoint being WRITTEN. Muted on purpose — it is the bookkeeping a
  // resume depends on, not work the user asked for — but present, because
  // "nothing was written" and "it was written and ignored" were previously
  // indistinguishable without querying the database.
  flow_checkpoint_saved: { Component: SaveIcon, color: 'action' },
  // A completed unit written to the checkpoint — the CREW path's writes,
  // which had no icon at all and so never appeared in a crew's timeline.
  checkpoint_unit_saved: { Component: SaveIcon, color: 'action' },
  flow_started: { Component: PlayCircleIcon, color: 'primary' },
  flow_created: { Component: PlayCircleIcon, color: 'primary' },
  flow_completed: { Component: CheckCircleIcon, color: 'success' },
  mcp_connection: { Component: TerminalIcon, color: 'info' },
  mcp_tool: { Component: TerminalIcon, color: 'primary' },
  mcp_tool_result: { Component: TerminalIcon, color: 'success' },
  hitl_request: { Component: WarningAmberIcon, color: 'warning' },
  hitl_response: { Component: CheckCircleIcon, color: 'info' },
  tool_error: { Component: ErrorOutlineIcon, color: 'error' },
  rate_limit: { Component: WarningAmberIcon, color: 'warning' },
  task_failed: { Component: ErrorOutlineIcon, color: 'error' },
  flow_execution_failed: { Component: ErrorOutlineIcon, color: 'error' },
  error: { Component: ErrorOutlineIcon, color: 'error' },
  guardrail: { Component: CheckCircleIcon, color: 'warning' },
  llm_request: { Component: PlayCircleIcon, color: 'primary' },
  // Compaction is LOSSY — warning-coloured on purpose. A run that
  // compacts repeatedly is losing tool results it may still need.
  context_compaction: { Component: CompressIcon, color: 'warning' },
  // A composed surface is a deliverable; a skipped one is information, not a
  // fault — muted rather than warning-coloured.
  a2ui_surface: { Component: DashboardIcon, color: 'success' },
  a2ui_skipped: { Component: DashboardIcon, color: 'action' },
  // Progress, not an outcome — muted, so a plan row never reads as a step that
  // succeeded or failed.
  plan_updated: { Component: ChecklistIcon, color: 'action' },
};

/**
 * Get icon configuration for an event type
 */
export function getEventIcon(type: string): { Component: React.ElementType | null; color: string } {
  const config = ICON_CONFIG[type];
  if (config) {
    return { Component: config.Component, color: config.color };
  }
  return { Component: null, color: 'inherit' };
}

// ============================================================================
// Clickable Types
// ============================================================================

export const CLICKABLE_TYPES = new Set([
  'llm',
  'llm_request',
  'llm_response',
  'agent_complete',
  'agent_output',
  'tool_result',
  'task_complete',
  'memory_operation',
  'memory_write',
  'memory_retrieval',
  'tool_usage',
  'knowledge_operation',
  'agent_execution',
  'guardrail',
  'mcp_tool',
  'mcp_tool_result',
  'hitl_request',
  'hitl_response',
  'tool_error',
  'task_failed',
  'flow_execution_failed',
  'memory_context',
  'memory_backend_error',
  'context_compaction',
  'a2ui_surface',
  'a2ui_skipped',
  // Opens the checklist (TracePlanView). It was NOT clickable before, so the
  // full plan was reachable only through the `todo` tool row's Python-repr
  // arguments — the one place the plan is already clean JSON was the one place
  // you could not open.
  'plan_updated',
]);

/**
 * Determine if an event is clickable
 */
export function isEventClickable(eventType: string, hasOutput: boolean): boolean {
  if (!hasOutput) return false;
  if (CLICKABLE_TYPES.has(eventType)) return true;

  // Also check partial matches for extensibility
  return (
    eventType.includes('memory') ||
    eventType.includes('tool') ||
    eventType.includes('knowledge') ||
    eventType.includes('guardrail')
  );
}
