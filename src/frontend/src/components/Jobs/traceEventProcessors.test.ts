/**
 * Unit tests for traceEventProcessors.ts
 *
 * Covers event processor registry, helper functions, icon config,
 * and clickable-type logic with focus on memory event handling.
 */
import { describe, it, expect } from 'vitest';
import {
  parseTraceMetadata,
  extractExtraData,
  extractOutputForDisplay,
  processTraceEvent,
  getEventIcon,
  isEventClickable,
  CLICKABLE_TYPES,
  ICON_CONFIG,
  EVENT_PROCESSORS,
} from './traceEventProcessors';
import type { Trace } from '../../store/runStatus';

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/** Minimal Trace factory — only required fields filled. */
function makeTrace(overrides: Partial<Trace> = {}): Trace {
  return {
    id: 1,
    event_source: 'crewai',
    event_context: '',
    event_type: 'unknown_event',
    output: null,
    created_at: '2025-01-01T00:00:00Z',
    ...overrides,
  };
}

// ============================================================================
// parseTraceMetadata
// ============================================================================

describe('parseTraceMetadata', () => {
  it('returns null when trace_metadata is absent', () => {
    expect(parseTraceMetadata(makeTrace())).toBeNull();
  });

  it('parses JSON string metadata', () => {
    const trace = makeTrace({ trace_metadata: '{"key": "val"}' });
    expect(parseTraceMetadata(trace)).toEqual({ key: 'val' });
  });

  it('returns object metadata directly', () => {
    const trace = makeTrace({ trace_metadata: { key: 'val' } });
    expect(parseTraceMetadata(trace)).toEqual({ key: 'val' });
  });

  it('returns null for invalid JSON string', () => {
    const trace = makeTrace({ trace_metadata: '{invalid json' });
    expect(parseTraceMetadata(trace)).toBeNull();
  });
});

// ============================================================================
// extractExtraData
// ============================================================================

describe('extractExtraData', () => {
  it('returns undefined when extra_data is absent', () => {
    expect(extractExtraData(makeTrace())).toBeUndefined();
  });

  it('returns trace-level extra_data object', () => {
    const trace = makeTrace({ extra_data: { foo: 'bar' } });
    expect(extractExtraData(trace)).toEqual({ foo: 'bar' });
  });

  it('returns output.extra_data when present', () => {
    const trace = makeTrace({ output: { extra_data: { nested: true } } });
    expect(extractExtraData(trace)).toEqual({ nested: true });
  });
});

// ============================================================================
// extractOutputForDisplay
// ============================================================================

describe('extractOutputForDisplay', () => {
  it('returns undefined for null', () => {
    expect(extractOutputForDisplay(null)).toBeUndefined();
  });

  it('returns undefined for undefined', () => {
    expect(extractOutputForDisplay(undefined)).toBeUndefined();
  });

  it('returns string output directly', () => {
    expect(extractOutputForDisplay('hello')).toBe('hello');
  });

  it('returns content string from object', () => {
    expect(extractOutputForDisplay({ content: 'msg' })).toBe('msg');
  });

  it('returns full object when content is not a string', () => {
    const obj = { data: 123 };
    expect(extractOutputForDisplay(obj)).toEqual(obj);
  });
});

// ============================================================================
// memory_retrieval_completed processor
// ============================================================================

describe('memory_retrieval_completed processor', () => {
  it('returns a ProcessedEvent (not null)', () => {
    const trace = makeTrace({
      event_type: 'memory_retrieval_completed',
      output: { content: 'memory data' },
    });
    const result = EVENT_PROCESSORS['memory_retrieval_completed'](trace);
    expect(result).not.toBeNull();
    expect(result!.type).toBe('memory_context');
    expect(result!.description).toContain('Memory Context Retrieved');
  });

  it('exposes retrieval time from metadata as durationMs (not in the label)', () => {
    const trace = makeTrace({
      event_type: 'memory_retrieval_completed',
      output: { content: 'data' },
      trace_metadata: JSON.stringify({ retrieval_time_ms: 42.7 }),
    });
    const result = EVENT_PROCESSORS['memory_retrieval_completed'](trace);
    expect(result).not.toBeNull();
    expect(result!.description).toBe('Memory Context Retrieved');
    expect(result!.durationMs).toBeCloseTo(42.7);
  });

  it('exposes retrieval time from extra_data fallback as durationMs', () => {
    const trace = makeTrace({
      event_type: 'memory_retrieval_completed',
      output: { content: 'data' },
      extra_data: { retrieval_time_ms: 100 },
    });
    const result = EVENT_PROCESSORS['memory_retrieval_completed'](trace);
    expect(result).not.toBeNull();
    expect(result!.description).toBe('Memory Context Retrieved');
    expect(result!.durationMs).toBe(100);
  });

  it('omits timing when not available', () => {
    const trace = makeTrace({
      event_type: 'memory_retrieval_completed',
      output: { content: 'data' },
    });
    const result = EVENT_PROCESSORS['memory_retrieval_completed'](trace);
    expect(result).not.toBeNull();
    expect(result!.description).toBe('Memory Context Retrieved');
    expect(result!.durationMs).toBeUndefined();
  });
});

// ============================================================================
// memory_retrieval processor
// ============================================================================

describe('memory_retrieval processor', () => {
  it('shows results count when present in extra_data', () => {
    const trace = makeTrace({
      event_type: 'memory_retrieval',
      output: { content: 'query results' },
      extra_data: { results_count: 5 },
    });
    const result = EVENT_PROCESSORS['memory_retrieval'](trace);
    expect(result).not.toBeNull();
    expect(result!.description).toContain('5 results');
  });

  it('shows memory type when present in metadata', () => {
    const trace = makeTrace({
      event_type: 'memory_retrieval',
      output: { content: 'results' },
      trace_metadata: JSON.stringify({ memory_type: 'short_term' }),
    });
    const result = EVENT_PROCESSORS['memory_retrieval'](trace);
    expect(result).not.toBeNull();
    expect(result!.type).toBe('memory_retrieval');
  });

  it('handles missing results count gracefully', () => {
    const trace = makeTrace({
      event_type: 'memory_retrieval',
      output: { content: 'data' },
    });
    const result = EVENT_PROCESSORS['memory_retrieval'](trace);
    expect(result).not.toBeNull();
    expect(result!.description).toContain('Memory Read');
  });
});

// ============================================================================
// memory_write processor
// ============================================================================

describe('memory_write processor', () => {
  it('returns memory_write type', () => {
    const trace = makeTrace({
      event_type: 'memory_write',
      output: { content: 'saved value' },
    });
    const result = EVENT_PROCESSORS['memory_write'](trace);
    expect(result).not.toBeNull();
    expect(result!.type).toBe('memory_write');
    expect(result!.description).toContain('Memory Write');
  });
});

// ============================================================================
// memory_context_retrieved processor
// ============================================================================

describe('memory_context_retrieved processor', () => {
  it('shows content length when available', () => {
    const trace = makeTrace({
      event_type: 'memory_context_retrieved',
      output: { content: 'context' },
      extra_data: { content_length: 1500 },
    });
    const result = EVENT_PROCESSORS['memory_context_retrieved'](trace);
    expect(result).not.toBeNull();
    expect(result!.type).toBe('memory_context');
    expect(result!.description).toContain('1500 chars');
  });

  it('handles missing content length', () => {
    const trace = makeTrace({
      event_type: 'memory_context_retrieved',
      output: { content: 'context' },
    });
    const result = EVENT_PROCESSORS['memory_context_retrieved'](trace);
    expect(result).not.toBeNull();
    expect(result!.description).toBe('Memory Context Retrieved');
  });
});

// ============================================================================
// processTraceEvent integration
// ============================================================================

describe('processTraceEvent', () => {
  it('dispatches to correct processor by event_type', () => {
    const trace = makeTrace({
      event_type: 'memory_retrieval_completed',
      output: { content: 'data' },
    });
    const result = processTraceEvent(trace);
    expect(result).not.toBeNull();
    expect(result!.type).toBe('memory_context');
  });

  it('returns Title Case description for unknown events', () => {
    const trace = makeTrace({ event_type: 'custom_event_type' });
    const result = processTraceEvent(trace);
    expect(result).not.toBeNull();
    expect(result!.description).toBe('Custom Event Type');
  });

  it('returns null for explicitly skipped events', () => {
    const trace = makeTrace({ event_type: 'guardrail_started' });
    const result = processTraceEvent(trace);
    expect(result).toBeNull();
  });

  // Light-agent (chat) tool results are emitted as `<tool>_run` and the final
  // answer as `response_run` (the crew/OTel path uses tool_usage+operation).
  // processTraceEvent maps these to clickable result rows so their output shows.
  it('maps a <tool>_run event to a clickable tool_result with "(output)"', () => {
    // Real light-agent _run traces carry tool_name in trace_metadata (and output);
    // extractToolName reads trace_metadata/extra_data.
    const trace = makeTrace({
      event_type: 'databrickssqlexecutesql_run',
      trace_metadata: { tool_name: 'databricks_sql_execute_sql', agent_role: 'Researcher' },
      output: { tool_name: 'databricks_sql_execute_sql', content: 'rows...' },
    });
    const result = processTraceEvent(trace);
    expect(result).not.toBeNull();
    expect(result!.type).toBe('tool_result');
    expect(result!.description).toBe('databricks_sql_execute_sql (output)');
    // tool_result is clickable when there's output → the result is viewable.
    expect(isEventClickable(result!.type, !!trace.output)).toBe(true);
  });

  it('drops response_run — the llm_response row already carries that answer', () => {
    // It used to render as "Final Response": a 280-char preview of the answer
    // the llm_response row holds in full, and not final at all (A2UI
    // composition runs after it). See the light-agent answer row tests below.
    const trace = makeTrace({
      event_type: 'response_run',
      output: { tool_name: 'Response', content: 'the answer' },
    });

    expect(processTraceEvent(trace)).toBeNull();
  });
});

// ============================================================================
// plan_updated
// ============================================================================

describe('plan_updated', () => {
  const planTrace = (extra: Record<string, unknown>): Trace =>
    makeTrace({ event_type: 'plan_updated', output: { extra_data: extra } });

  it('reports progress instead of the generic Title-Case label', () => {
    const event = processTraceEvent(
      planTrace({ plan_total: 5, plan_completed: 2 }),
    );

    expect(event?.type).toBe('plan_updated');
    expect(event?.description).toBe('Plan — 2/5 done');
  });

  it('names the step running now', () => {
    const event = processTraceEvent(
      planTrace({
        plan_total: 3,
        plan_completed: 1,
        plan_items: JSON.stringify([
          { id: '1', content: 'Read the schema', status: 'completed' },
          { id: '2', content: 'Scrape the articles', status: 'in_progress' },
          { id: '3', content: 'Write the rows', status: 'pending' },
        ]),
      }),
    );

    expect(event?.description).toBe('Plan — 1/3 done · now: Scrape the articles');
  });

  it('derives the counts when only the items arrived', () => {
    const event = processTraceEvent(
      planTrace({
        plan_items: [
          { id: '1', content: 'a', status: 'completed' },
          { id: '2', content: 'b', status: 'pending' },
        ],
      }),
    );

    expect(event?.description).toBe('Plan — 1/2 done');
  });

  it('falls back to the plain label when the event carries no plan', () => {
    expect(processTraceEvent(planTrace({}))?.description).toBe('Plan Updated');
  });

  it('opens the checklist — the row is clickable and has its own icon', () => {
    expect(isEventClickable('plan_updated', true)).toBe(true);
    expect(getEventIcon('plan_updated').Component).not.toBeNull();
  });
});

// ============================================================================
// getEventIcon
// ============================================================================

describe('getEventIcon', () => {
  it('returns correct icon for memory_context', () => {
    const icon = getEventIcon('memory_context');
    expect(icon.Component).not.toBeNull();
    expect(icon.color).toBe('info');
  });

  it('returns correct icon for memory_write', () => {
    const icon = getEventIcon('memory_write');
    expect(icon.Component).not.toBeNull();
    expect(icon.color).toBe('primary');
  });

  it('returns correct icon for memory_retrieval', () => {
    const icon = getEventIcon('memory_retrieval');
    expect(icon.Component).not.toBeNull();
    expect(icon.color).toBe('success');
  });

  it('returns null Component for unknown type', () => {
    const icon = getEventIcon('unknown_type_xyz');
    expect(icon.Component).toBeNull();
    expect(icon.color).toBe('inherit');
  });
});

// ============================================================================
// isEventClickable
// ============================================================================

describe('isEventClickable', () => {
  it('memory_context is clickable with output', () => {
    expect(isEventClickable('memory_context', true)).toBe(true);
  });

  it('memory_retrieval is clickable with output', () => {
    expect(isEventClickable('memory_retrieval', true)).toBe(true);
  });

  it('memory_write is clickable with output', () => {
    expect(isEventClickable('memory_write', true)).toBe(true);
  });

  it('not clickable without output', () => {
    expect(isEventClickable('memory_context', false)).toBe(false);
  });

  it('partial match for memory_ prefix', () => {
    expect(isEventClickable('memory_something_custom', true)).toBe(true);
  });
});

// ============================================================================
// CLICKABLE_TYPES set
// ============================================================================

describe('CLICKABLE_TYPES', () => {
  it('includes memory_context', () => {
    expect(CLICKABLE_TYPES.has('memory_context')).toBe(true);
  });

  it('includes memory_write', () => {
    expect(CLICKABLE_TYPES.has('memory_write')).toBe(true);
  });

  it('includes memory_retrieval', () => {
    expect(CLICKABLE_TYPES.has('memory_retrieval')).toBe(true);
  });

  it('includes memory_operation', () => {
    expect(CLICKABLE_TYPES.has('memory_operation')).toBe(true);
  });
});

// ============================================================================
// ICON_CONFIG
// ============================================================================

describe('ICON_CONFIG', () => {
  it('has memory_context entry', () => {
    expect(ICON_CONFIG['memory_context']).toBeDefined();
    expect(ICON_CONFIG['memory_context'].color).toBe('info');
  });

  it('has memory_write entry', () => {
    expect(ICON_CONFIG['memory_write']).toBeDefined();
  });

  it('has memory_retrieval entry', () => {
    expect(ICON_CONFIG['memory_retrieval']).toBeDefined();
  });

  it('has memory_backend_error entry', () => {
    expect(ICON_CONFIG['memory_backend_error']).toBeDefined();
    expect(ICON_CONFIG['memory_backend_error'].color).toBe('error');
  });
});

// ============================================================================
// LLM rows raised by the memory layer
// ============================================================================

describe('memory-labelling LLM rows', () => {
  it('names the request for what it is', () => {
    const result = EVENT_PROCESSORS.llm_call(makeTrace({
      event_type: 'llm_call',
      trace_metadata: {
        model: 'databricks/some-model',
        prompt: 'x'.repeat(1564),
        llm_purpose: 'memory_labelling',
      },
    }));

    expect(result?.description).toBe('Memory Labelling — some-model (1,564 chars)');
  });

  it('names the response for what it is', () => {
    const result = EVENT_PROCESSORS.llm_response(makeTrace({
      event_type: 'llm_response',
      output: { content: 'y'.repeat(318) },
      trace_metadata: { llm_purpose: 'memory_labelling' },
    }));

    expect(result?.description).toBe('Memory Labels (318 chars)');
  });

  it('leaves the agent\'s own LLM rows alone', () => {
    const request = EVENT_PROCESSORS.llm_call(makeTrace({
      event_type: 'llm_call',
      trace_metadata: { model: 'some-model', prompt: 'x'.repeat(2549) },
    }));
    const response = EVENT_PROCESSORS.llm_response(makeTrace({
      event_type: 'llm_response',
      output: { content: 'y'.repeat(1398) },
    }));

    expect(request?.description).toBe('LLM Request — some-model (2,549 chars)');
    expect(response?.description).toBe('LLM Response (1,398 chars)');
  });
});

// ============================================================================
// A2UI composition rows
// ============================================================================

describe('a2ui_surface rows', () => {
  it('names the surface and carries its measured time', () => {
    // The row is its own group in the timeline, so a duration derived from
    // timestamps is always 0 ms — composition's real cost is on the row.
    const result = EVENT_PROCESSORS.a2ui_surface(makeTrace({
      event_type: 'a2ui_surface',
      trace_metadata: {
        outcome: 'composed',
        surface_kind: 'presentation',
        component_count: 48,
        duration_ms: 50881.39,
      },
    }));

    expect(result?.description).toBe('A2UI Surface — presentation (48 components)');
    expect(result?.durationMs).toBe(50881.39);
  });

  it('says which gate declined, in words', () => {
    const result = EVENT_PROCESSORS.a2ui_surface(makeTrace({
      event_type: 'a2ui_surface',
      trace_metadata: { outcome: 'no_data_component', surface_kind: 'dashboard' },
    }));

    expect(result?.type).toBe('a2ui_skipped');
    expect(result?.description).toBe('A2UI Skipped — surface had no data component');
  });

  it("pairs each composer call as a request and a response", () => {
    // One row per call could only report one length, so "A2UI Compose (2,474
    // chars)" left the reader guessing whether that was sent or received — and
    // with three calls in a row, which was which.
    const request = EVENT_PROCESSORS.llm_call(makeTrace({
      event_type: 'llm_call',
      trace_metadata: {
        llm_purpose: 'a2ui_compose',
        attempt: 2,
        model: 'some-model',
        prompt: 'p'.repeat(10560),
      },
    }));
    const response = EVENT_PROCESSORS.llm_response(makeTrace({
      event_type: 'llm_response',
      output: { content: 'x'.repeat(2474) },
      trace_metadata: { llm_purpose: 'a2ui_compose', attempt: 2 },
    }));

    expect(request?.description).toBe('A2UI Compose Request #2 — some-model (10,560 chars)');
    expect(response?.description).toBe('A2UI Compose Response #2 (2,474 chars)');
  });
});

describe('the light-agent answer row', () => {
  it('is not repeated as a truncated "Final Response"', () => {
    // response_run is the agent-completion echo of the same answer: a 280-char
    // preview of what llm_response already carries in full, labelled "Final"
    // while A2UI composition still runs after it.
    const result = processTraceEvent(makeTrace({
      event_type: 'response_run',
      output: { tool_name: 'Response', content: 'a 280-char preview…' },
    }));

    expect(result).toBeNull();
  });

  it('leaves the real answer row alone', () => {
    const result = processTraceEvent(makeTrace({
      event_type: 'llm_response',
      output: { content: 'x'.repeat(4578) },
    }));

    expect(result?.description).toBe('LLM Response (4,578 chars)');
  });
});
