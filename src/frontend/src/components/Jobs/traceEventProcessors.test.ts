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

  it('includes retrieval time from metadata', () => {
    const trace = makeTrace({
      event_type: 'memory_retrieval_completed',
      output: { content: 'data' },
      trace_metadata: JSON.stringify({ retrieval_time_ms: 42.7 }),
    });
    const result = EVENT_PROCESSORS['memory_retrieval_completed'](trace);
    expect(result).not.toBeNull();
    expect(result!.description).toContain('43ms');
  });

  it('includes retrieval time from extra_data fallback', () => {
    const trace = makeTrace({
      event_type: 'memory_retrieval_completed',
      output: { content: 'data' },
      extra_data: { retrieval_time_ms: 100 },
    });
    const result = EVENT_PROCESSORS['memory_retrieval_completed'](trace);
    expect(result).not.toBeNull();
    expect(result!.description).toContain('100ms');
  });

  it('omits timing when not available', () => {
    const trace = makeTrace({
      event_type: 'memory_retrieval_completed',
      output: { content: 'data' },
    });
    const result = EVENT_PROCESSORS['memory_retrieval_completed'](trace);
    expect(result).not.toBeNull();
    expect(result!.description).toBe('Memory Context Retrieved');
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
    const trace = makeTrace({ event_type: 'reasoning_started' });
    const result = processTraceEvent(trace);
    expect(result).toBeNull();
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
