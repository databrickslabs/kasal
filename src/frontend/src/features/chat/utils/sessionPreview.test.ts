import { describe, it, expect, beforeEach, vi } from 'vitest';
import { deriveSessionPreviews } from './sessionPreview';
import { getExecution } from '../api/executions';
import type { ChatMessage } from '../types/chat';

vi.mock('../api/executions', () => ({ getExecution: vi.fn() }));
const mockedGet = vi.mocked(getExecution);

// Minimal valid A2UI surface (the new shared shape).
const doc = {
  surfaceKind: 'document',
  root: 'root',
  components: [{ id: 'root', component: 'Text', text: 'Hi' }],
  dataModel: {},
};

const msg = (id: string, executionId?: string): ChatMessage => ({
  id,
  role: 'assistant',
  content: '',
  timestamp: new Date('2020-01-01'),
  ...(executionId ? { executionId } : {}),
});

beforeEach(() => {
  mockedGet.mockReset();
});

describe('deriveSessionPreviews', () => {
  it('derives the deliverable from a run by walking its execution result', async () => {
    // The backend stores the {text, a2ui} envelope — derive must extract the surface.
    mockedGet.mockResolvedValue({ id: 'job-1', result: { text: 'Hi', a2ui: doc } } as never);

    const { history, current } = await deriveSessionPreviews([msg('m1', 'job-1')]);

    expect(history).toHaveLength(1);
    expect(history[0].type).toBe('ui');
    // The stored data is the EXTRACTED surface, not the envelope.
    expect(JSON.parse(history[0].data)).toEqual(doc);
    expect(current).toEqual(history[0]);
  });

  it('caches results — repeated derivation does not refetch', async () => {
    mockedGet.mockResolvedValue({ id: 'job-cache', result: doc } as never);
    await deriveSessionPreviews([msg('m1', 'job-cache')]);
    await deriveSessionPreviews([msg('m1', 'job-cache')]);
    expect(mockedGet).toHaveBeenCalledTimes(1);
  });

  it('skips runs whose result has no A2UI surface', async () => {
    mockedGet.mockResolvedValue({ id: 'job-plain', result: { output: 'just text' } } as never);
    const { history, current } = await deriveSessionPreviews([msg('m1', 'job-plain')]);
    expect(history).toHaveLength(0);
    expect(current).toBeNull();
  });

  it('dedupes executionIds and ignores messages without one', async () => {
    mockedGet.mockResolvedValue({ id: 'job-d', result: doc } as never);
    await deriveSessionPreviews([msg('u', 'job-d'), msg('a'), msg('b', 'job-d')]);
    expect(mockedGet).toHaveBeenCalledTimes(1);
  });

  it('returns the latest run as current across multiple runs (history order)', async () => {
    const doc2 = {
      surfaceKind: 'document',
      root: 'root',
      components: [{ id: 'root', component: 'Text', text: 'Second' }],
      dataModel: {},
    };
    mockedGet.mockImplementation(
      async (id: string) => ({ id, result: id === 'j2' ? doc2 : doc } as never),
    );

    const { history, current } = await deriveSessionPreviews([
      msg('m1', 'j1'),
      msg('m2', 'j2'),
    ]);

    expect(history).toHaveLength(2);
    expect(current).toEqual(history[1]);
    expect(JSON.parse(current!.data)).toEqual(doc2);
  });

  it('tolerates a fetch failure (no preview, no throw)', async () => {
    mockedGet.mockRejectedValue(new Error('boom'));
    const { history, current } = await deriveSessionPreviews([msg('m1', 'job-err')]);
    expect(history).toHaveLength(0);
    expect(current).toBeNull();
  });

  it('prefers the surface persisted on the message (restyle theme survives) over execution.result', async () => {
    // Regression: a "Customize → Look" restyle is persisted on message.resultData
    // (with `theme`); the pristine execution.result has no theme. Session restore
    // must render the RESTYLED copy — and must not even need the fetch.
    const restyled = { ...doc, theme: { accent: '#ff0000', _pinned: true } };
    mockedGet.mockResolvedValue({ id: 'job-t', result: { text: 'Hi', a2ui: doc } } as never);

    const { history } = await deriveSessionPreviews([
      { ...msg('m1', 'job-t'), resultType: 'a2ui', resultData: restyled },
    ]);

    expect(history).toHaveLength(1);
    expect(JSON.parse(history[0].data).theme).toEqual({ accent: '#ff0000', _pinned: true });
    expect(mockedGet).not.toHaveBeenCalled();
  });

  it('keeps an a2ui card with NO run anchor (envelope clobbered by an old partial update)', async () => {
    // Regression (HAR-confirmed): a restyle PUT used to strip executionId from
    // the message envelope — the restyled deck must still surface as a preview.
    const restyled = { ...doc, theme: { accent: '#FF3621' } };
    const { history } = await deriveSessionPreviews([
      { ...msg('m1'), resultType: 'a2ui', resultData: restyled },
    ]);
    expect(history).toHaveLength(1);
    expect(JSON.parse(history[0].data).theme).toEqual({ accent: '#FF3621' });
    expect(history[0].sourceMessageId).toBe('m1');
    expect(mockedGet).not.toHaveBeenCalled();
  });

  it('never mistakes non-a2ui resultData (crew cards, traces) for a deliverable', async () => {
    mockedGet.mockResolvedValue({ id: 'job-c', result: doc } as never);
    const { history } = await deriveSessionPreviews([
      // crew card WITH a run anchor → derive from execution.result, not the card
      { ...msg('m1', 'job-c'), resultType: 'crew_actions', resultData: { agents: [] } },
      // trace payload with NO anchor → contributes nothing
      { ...msg('m2'), resultType: 'trace', resultData: { kind: 'tool_result', label: 'Memory' } },
    ]);
    expect(history).toHaveLength(1);
    expect(JSON.parse(history[0].data)).toEqual(doc);
    expect(mockedGet).toHaveBeenCalledTimes(1);
  });

  it('stamps sourceMessageId so a pane restyle can round-trip to the message', async () => {
    mockedGet.mockResolvedValue({ id: 'job-s', result: doc } as never);
    const { history } = await deriveSessionPreviews([msg('m9', 'job-s')]);
    expect(history[0].sourceMessageId).toBe('m9');
  });

  it('falls back to execution.result when the message resultData carries no surface', async () => {
    mockedGet.mockResolvedValue({ id: 'job-f', result: doc } as never);
    const { history } = await deriveSessionPreviews([
      { ...msg('m1', 'job-f'), resultData: { just: 'metadata' } },
    ]);
    expect(history).toHaveLength(1);
    expect(JSON.parse(history[0].data)).toEqual(doc);
    expect(mockedGet).toHaveBeenCalledTimes(1);
  });
});

describe('deriveSessionPreviews — parallel result prefetch (perf W4.2)', () => {
  it('fetches all runs concurrently instead of one serial round-trip per run', async () => {
    // Regression: the derivation loop awaited each run's full-result GET one at
    // a time, so a 5-run session paid 5 sequential round-trips on every switch.
    const resolvers: Array<(v: unknown) => void> = [];
    mockedGet.mockImplementation(
      () =>
        new Promise((resolve) => {
          resolvers.push(resolve as (v: unknown) => void);
        }) as never,
    );

    const derivation = deriveSessionPreviews([
      msg('m1', 'job-parallel-1'),
      msg('m2', 'job-parallel-2'),
      msg('m3', 'job-parallel-3'),
    ]);

    // All three fetches must be IN FLIGHT before any of them resolves.
    await vi.waitFor(() => expect(resolvers).toHaveLength(3));

    resolvers.forEach((resolve, i) =>
      resolve({ id: `job-parallel-${i + 1}`, result: doc }),
    );
    const { history } = await derivation;
    expect(history).toHaveLength(3);
  });
});
