/**
 * The "[cached]" badge, across both row shapes.
 *
 * A replayed call is only useful if you can SEE it was replayed. The two
 * execution paths write the row differently, and the chat one — where replay
 * pays off most — had no branch that could render the badge at all.
 */
import { describe, it, expect } from 'vitest';
import { processTraceEvent, isFromCache } from './traceEventProcessors';
import type { Trace } from '../../store/runStatus';

function makeTrace(overrides: Partial<Trace> = {}): Trace {
  return {
    id: 1,
    event_source: 'Assistant',
    event_context: '',
    event_type: 'unknown_event',
    output: null,
    created_at: '2026-08-12T13:08:46Z',
    ...overrides,
  };
}

describe('a replayed chat call', () => {
  /** Exactly what services/chat/service.py writes. */
  const chatRow = (fromCache: boolean) =>
    makeTrace({
      event_type: 'perplexitytool_run',
      trace_metadata: { agent_role: 'Assistant', tool_name: 'PerplexityTool' },
      output: {
        tool_name: 'PerplexityTool',
        input: '{"query": "lebanon news"}',
        content: 'the recorded answer',
        from_cache: fromCache,
        duration_ms: 0,
      },
    });

  it('is badged in the timeline', () => {
    expect(processTraceEvent(chatRow(true))?.description).toBe(
      'PerplexityTool (output) [cached]',
    );
  });

  it('a live one is not', () => {
    expect(processTraceEvent(chatRow(false))?.description).toBe(
      'PerplexityTool (output)',
    );
  });

  it('reads the flag from output, where chat puts it', () => {
    expect(isFromCache(chatRow(true))).toBe(true);
  });
});

describe('a replayed crew call', () => {
  /** The OTel bridge shape: the flag rides in extra_data / trace_metadata. */
  const crewRow = (fromCache: boolean) =>
    makeTrace({
      event_type: 'tool_usage',
      trace_metadata: { operation: 'tool_finished', tool_name: 'PerplexityTool' },
      output: {
        content: 'the recorded answer',
        extra_data: { tool_name: 'PerplexityTool', from_cache: fromCache },
      },
    });

  it('is badged in the timeline', () => {
    expect(processTraceEvent(crewRow(true))?.description).toBe(
      'PerplexityTool (output) [cached]',
    );
  });

  it('a live one is not', () => {
    expect(processTraceEvent(crewRow(false))?.description).toBe(
      'PerplexityTool (output)',
    );
  });
});

describe('isFromCache', () => {
  it('is false when nothing says otherwise', () => {
    expect(isFromCache(makeTrace())).toBe(false);
  });

  it('does not read the final answer row as a tool result', () => {
    const answer = makeTrace({
      event_type: 'response_run',
      output: { tool_name: 'Response', content: 'hi', duration_ms: 5 },
    });
    expect(processTraceEvent(answer)).toBeNull();
  });
});
