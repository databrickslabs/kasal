/**
 * Registry-based trace event processing system
 * Replaces 490-line if/else chain with maintainable, extensible registry pattern
 */

import React from 'react';
import CheckCircleIcon from '@mui/icons-material/CheckCircle';
import PlayCircleIcon from '@mui/icons-material/PlayCircle';
import PreviewIcon from '@mui/icons-material/Preview';
import TerminalIcon from '@mui/icons-material/Terminal';
import RefreshIcon from '@mui/icons-material/Refresh';
import PlayArrowIcon from '@mui/icons-material/PlayArrow';
import WarningAmberIcon from '@mui/icons-material/WarningAmber';
import ErrorOutlineIcon from '@mui/icons-material/ErrorOutline';
import StorageIcon from '@mui/icons-material/Storage';
import TimelineIcon from '@mui/icons-material/Timeline';

// Import Trace type from the store
import { Trace } from '../../store/runStatus';

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
    const promptLen = (metadata?.prompt as string)?.length || 0;

    let description = 'LLM Request';
    if (modelName) {
      const modelParts = modelName.split('/');
      const shortModel = modelParts[modelParts.length - 1];
      description = `LLM Request — ${shortModel}`;
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
    const fromCache = metadata?.from_cache as boolean | undefined;
    const cacheSuffix = fromCache ? ' [cached]' : '';

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

    // Check metadata for output_length from backend
    if (metadata?.output_length) {
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

    const description = outputLen > 0
      ? `LLM Response (${outputLen.toLocaleString()} chars)`
      : 'LLM Response';

    return { type: 'llm_response', description };
  },

  // Agent Reasoning
  agent_reasoning: (trace: Trace): ProcessedEvent | null => {
    const metadata = parseTraceMetadata(trace);
    const extra = extractExtraData(trace);
    const operation = metadata?.operation || extra?.operation;

    // Skip "reasoning_started" events
    if (operation === 'reasoning_started') {
      return null;
    }

    // Get the actual content to determine description
    const outputStr = extractOutputStr(trace);

    let description = 'Agent Reasoning';
    // Check if it's a planning event
    if (outputStr && outputStr.toLowerCase().includes('plan')) {
      description = 'Agent Planning';
    } else if (outputStr && outputStr.length > 100) {
      description = 'Agent Reasoning';
    }

    return { type: 'agent_reasoning', description };
  },

  // Agent Reasoning Error
  agent_reasoning_error: (trace: Trace): ProcessedEvent => {
    return { type: 'agent_reasoning', description: 'Reasoning Failed' };
  },

  // Agent Execution — all are instrumentor container spans; real events use specific types
  agent_execution: (): ProcessedEvent | null => null,

  // Agent Step — same as agent_execution, instrumentor container
  agent_step: (): ProcessedEvent | null => null,

  // Task Started
  task_started: (trace: Trace): ProcessedEvent => {
    const metadata = parseTraceMetadata(trace);
    const extra = extractExtraData(trace);

    let taskName = 'Task Started';
    const name = (metadata?.task_name as string) || (extra?.task_name as string);
    if (name) {
      // Truncate long task names for display
      taskName = name.length > 50 ? name.substring(0, 47) + '...' : name;
    }

    return { type: 'task_start', description: `Starting: ${taskName}` };
  },

  // Task Completed
  task_completed: (trace: Trace): ProcessedEvent => {
    const metadata = parseTraceMetadata(trace);
    const extra = extractExtraData(trace);

    let taskName = 'Task Completed';
    const name = (metadata?.task_name as string) || (extra?.task_name as string);
    if (name) {
      // Truncate long task names for display
      taskName = name.length > 50 ? name.substring(0, 47) + '...' : name;
    }

    return { type: 'task_complete', description: `Completed: ${taskName}` };
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
    const timeSuffix = saveTime !== undefined ? ` — ${saveTime}ms` : '';
    const description = formattedType
      ? `Memory Write (${formattedType})${timeSuffix}`
      : `Memory Write${timeSuffix}`;
    return { type: 'memory_write', description };
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
      description += ` - ${resultsCount} results`;
    }
    if (queryTime !== undefined) {
      description += ` — ${queryTime}ms`;
    }

    return { type: 'memory_retrieval', description };
  },

  // Memory Retrieval Completed - skip
  memory_retrieval_completed: (): ProcessedEvent | null => null,

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

  // Reasoning Started - skip
  reasoning_started: (): ProcessedEvent | null => null,

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
    let description = serverName ? `MCP Connected: ${serverName}` : 'MCP Connected';
    if (duration !== undefined) description += ` — ${duration}ms`;
    return { type: 'mcp_connection', description };
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
    const timeSuffix = execTime !== undefined ? ` — ${execTime}ms` : '';
    return { type: 'mcp_tool_result', description: `MCP: ${mcpToolName} (result)${timeSuffix}` };
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
  error: { Component: ErrorOutlineIcon, color: 'error' },
  agent_reasoning: { Component: PreviewIcon, color: 'info' },
  guardrail: { Component: CheckCircleIcon, color: 'warning' },
  llm_request: { Component: PlayCircleIcon, color: 'primary' },
  crew_planning: { Component: PlayCircleIcon, color: 'info' },
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
  'agent_reasoning',
  'mcp_tool',
  'mcp_tool_result',
  'hitl_request',
  'hitl_response',
  'tool_error',
  'task_failed',
  'memory_context',
  'memory_backend_error',
  'crew_planning',
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
    eventType.includes('guardrail') ||
    eventType.includes('reasoning')
  );
}
