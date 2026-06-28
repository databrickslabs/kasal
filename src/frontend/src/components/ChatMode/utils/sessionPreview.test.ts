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
});
