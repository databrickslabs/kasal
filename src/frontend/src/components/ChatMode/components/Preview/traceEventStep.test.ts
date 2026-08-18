import { describe, it, expect } from 'vitest';
import { traceEventToRunStep } from './traceEventStep';
import { makeEvent } from './testTraceFixture';

describe('traceEventToRunStep', () => {
  it('unwraps a single-string envelope so the markdown inside is what shows', () => {
    // A page-reading tool returns {"result": "…## Sources…"}. Shown as JSON that
    // is one enormous line with literal \n escapes, and the wrapper says
    // nothing the reader needs.
    const step = traceEventToRunStep(
      makeEvent({ output: { result: 'Research for X\n\n## Sources\n\n[1] a source' } }),
    );
    expect(step.detail).toContain('## Sources');
    expect(step.detail).not.toContain('"result"');
    expect(step.code).toBeUndefined();
  });

  it('unwraps an envelope that arrived as a JSON string (the polling transport)', () => {
    const step = traceEventToRunStep(
      makeEvent({ output: JSON.stringify({ output: 'plain body text' }) }),
    );
    expect(step.detail).toBe('plain body text');
  });

  it('lifts a single code argument out of its JSON envelope', () => {
    const step = traceEventToRunStep(
      makeEvent({ extraData: { tool_args: '{"sql": "CREATE TABLE t (id SERIAL);"}' } }),
    );
    expect(step.code).toEqual({ language: 'sql', text: 'CREATE TABLE t (id SERIAL);' });
  });

  it('keeps a search query as JSON — a `query` is not SQL', () => {
    // `query` used to be mapped to SQL, which labelled a web search's phrase as
    // a SQL statement and highlighted it as one.
    const step = traceEventToRunStep(
      makeEvent({ extraData: { tool_args: '{"query": "Swiss fintech companies"}' } }),
    );
    expect(step.code?.language).toBe('json');
    expect(step.code?.text).toContain('"query"');
  });

  it('formats a multi-field argument object as readable JSON', () => {
    const step = traceEventToRunStep(
      makeEvent({ extraData: { tool_args: { table: 't', limit: 10 } } }),
    );
    expect(step.code?.language).toBe('json');
    expect(step.code?.text).toBe('{\n  "table": "t",\n  "limit": 10\n}');
  });
});

describe('a plan step carries its checklist however the plan was written', () => {
  it('parses the todo tool\'s rendered result', () => {
    const step = traceEventToRunStep(
      makeEvent({
        type: 'plan_updated',
        description: 'Plan — 1/2 done',
        output: 'Plan (1/2 completed):\n[x] 1. Create the table\n[>] 2. Gather the companies',
      }),
    );
    expect(step.plan).toHaveLength(2);
    expect(step.plan?.[1]).toMatchObject({ content: 'Gather the companies', status: 'in_progress' });
    // A plan is a checklist, never a code block.
    expect(step.code).toBeUndefined();
  });
})
