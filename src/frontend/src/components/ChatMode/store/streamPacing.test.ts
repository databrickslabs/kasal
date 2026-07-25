/**
 * Streamed text must arrive at a readable, steady rate.
 *
 * Chunks were painted the instant an SSE frame landed. The backend coalesces
 * tokens, so what reached the eye was a burst — a paragraph at once, a pause,
 * another burst — and on a fast model the whole answer could appear in a single
 * frame. The buffer only ever DELAYS text: nothing may be dropped, and every
 * terminal path has to flush what is still queued.
 */
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';

// Mirror executionStore.test.ts: the session store is mocked so a message list
// can be observed without IndexedDB. The array lives INSIDE the factory —
// vi.mock is hoisted above any top-level const.
interface FakeMessage { id: string; content: string }

vi.mock('./sessionStore', () => {
  const painted: FakeMessage[] = [];
  const state = {
    currentSessionId: 'session-1' as string | null,
    messages: painted,
    addMessage: vi.fn((_role: string, content: string, extra?: { id?: string }) => {
      const id = extra?.id ?? `m-${painted.length}`;
      painted.push({ id, content });
      return id;
    }),
    addMessageToTargetSession: vi.fn(),
    updateMessageInTargetSession: vi.fn(),
    updateMessage: vi.fn(),
    appendToMessage: vi.fn((id: string, chunk: string) => {
      const msg = painted.find((m) => m.id === id);
      if (msg) msg.content += chunk;
    }),
  };
  return { useSessionStore: { getState: vi.fn(() => state) } };
});

vi.mock('../db/sessionApi', () => ({
  saveSessionPreview: vi.fn(),
  getSessionPreview: vi.fn(() => Promise.resolve(undefined)),
  getSessionMessages: vi.fn(() => Promise.resolve([])),
  setSessionRunningJob: vi.fn(() => Promise.resolve()),
  getSessionRunningJob: vi.fn(() => Promise.resolve(null)),
  clearSessionRunningJob: vi.fn(() => Promise.resolve()),
}));

vi.mock('../components/Preview/PreviewPanel', () => ({ parsePreviewContent: vi.fn() }));
vi.mock('../utils/sessionPreview', () => ({
  deriveSessionPreviews: vi.fn(() => Promise.resolve({ history: [], current: null })),
}));

import { useExecutionStore } from './executionStore';
import { useSessionStore } from './sessionStore';

/** The mocked session store's message list. */
const paintedMessages = (): FakeMessage[] =>
  (useSessionStore as unknown as { getState: () => { messages: FakeMessage[] } }).getState()
    .messages;

/** Run every queued animation frame until the queue drains. */
function drainFrames(maxTicks = 200): void {
  for (let i = 0; i < maxTicks && frameQueue.length; i++) {
    const callbacks = frameQueue.splice(0, frameQueue.length);
    callbacks.forEach((cb) => cb(performance.now()));
  }
}

let frameQueue: FrameRequestCallback[] = [];

beforeEach(() => {
  paintedMessages().length = 0;
  frameQueue = [];
  vi.stubGlobal('requestAnimationFrame', (cb: FrameRequestCallback) => {
    frameQueue.push(cb);
    return frameQueue.length;
  });
  vi.stubGlobal('cancelAnimationFrame', () => {
    /* the queue is rebuilt per test */
  });
});

afterEach(() => {
  vi.unstubAllGlobals();
});

/** A session that owns a running job, which is what appendStreamChunk requires. */
function startOwnedJob(jobId = 'job-1') {
  useExecutionStore.getState().startExecution(jobId, 'session-1');
  return { jobId, sessionId: 'session-1' };
}

const streamBubblesPainted = () => paintedMessages().filter((m) => m.id.startsWith('stream-'));
const streamedText = (): string => streamBubblesPainted().map((m) => m.content).join('');

describe('stream pacing', () => {
  it('does not paint the whole answer in one frame', () => {
    const { jobId } = startOwnedJob();
    const answer = 'word '.repeat(200); // ~1000 chars in a single SSE frame

    useExecutionStore.getState().appendStreamChunk(jobId, answer);

    // One frame's worth only — the rest stays queued.
    const firstTick = frameQueue.splice(0, frameQueue.length);
    firstTick.forEach((cb) => cb(performance.now()));
    const afterOneFrame = streamedText().length;

    expect(afterOneFrame).toBeGreaterThan(0);
    expect(afterOneFrame).toBeLessThan(answer.length);
  });

  it('eventually paints every character, in order', () => {
    const { jobId } = startOwnedJob('job-2');
    const answer = 'The quick brown fox jumps over the lazy dog. '.repeat(20);

    useExecutionStore.getState().appendStreamChunk(jobId, answer);
    drainFrames();

    expect(streamedText()).toBe(answer);
  });

  it('does not slice words mid-token', () => {
    const { jobId } = startOwnedJob('job-3');
    useExecutionStore.getState().appendStreamChunk(jobId, 'alpha bravo charlie '.repeat(20));

    const firstTick = frameQueue.splice(0, frameQueue.length);
    firstTick.forEach((cb) => cb(performance.now()));

    const text = streamedText();
    // Whatever landed ends at a word boundary, not inside "brav|o".
    expect(/\s$/.test(text) || text.length === 0).toBe(true);
  });

  it('flushes what is queued when the bubble closes at a task boundary', () => {
    const { jobId } = startOwnedJob('job-4');
    const answer = 'x'.repeat(500);

    useExecutionStore.getState().appendStreamChunk(jobId, answer);
    // Close BEFORE the frames drain — the tail must not be lost.
    useExecutionStore.getState().closeStreamBubble(jobId);

    expect(streamedText()).toBe(answer);
  });

  it('starts a NEW bubble after a close, so each task keeps its own', () => {
    const { jobId } = startOwnedJob('job-5');

    useExecutionStore.getState().appendStreamChunk(jobId, 'first task output');
    drainFrames();
    useExecutionStore.getState().closeStreamBubble(jobId);
    useExecutionStore.getState().appendStreamChunk(jobId, 'second task output');
    drainFrames();

    const bubbles = streamBubblesPainted();
    expect(bubbles.length).toBe(2);
    expect(bubbles[0].content).toContain('first task output');
    expect(bubbles[1].content).toContain('second task output');
  });

  it('is a no-op for an untracked job', () => {
    expect(() =>
      useExecutionStore.getState().appendStreamChunk('never-started', 'text'),
    ).not.toThrow();
    expect(() => useExecutionStore.getState().closeStreamBubble('never-started')).not.toThrow();
  });
});
