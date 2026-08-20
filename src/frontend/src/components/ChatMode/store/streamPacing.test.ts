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
    // Implemented, not stubbed: the store routes a run's bubble by its OWNER
    // session so switching away mid-run cannot lose it. A no-op here swallows
    // every message the code under test writes — which is exactly how the
    // wrong-session bug this routing fixes stayed invisible.
    addMessageToTargetSession: vi.fn(
      (_session: string, _role: string, content: string, extra?: Record<string, unknown>) => {
        const id = (extra?.id as string) ?? `m-${painted.length}`;
        painted.push({ id, content, ...(extra as object) });
        return id;
      },
    ),
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

  // Persistence regression: addMessage writes to IndexedDB fire-and-forget and
  // awaits ensureSession() first, while appendToMessage's update fires
  // immediately. An empty insert can therefore land AFTER the update that
  // carried the text and overwrite the row with ''. Leaving the session and
  // returning then showed the task headers but no streamed answer — and took the
  // A2UI surface with it, because completeExecution writes the surface into this
  // same bubble.
  it('never creates an empty bubble — the row must be inserted WITH content', () => {
    const { jobId } = startOwnedJob('job-6');

    useExecutionStore.getState().appendStreamChunk(jobId, 'the answer text');
    drainFrames();

    // A run's bubble is created through the OWNER-targeted variant so switching
    // sessions mid-run cannot lose it, and through `addMessage` when the job has
    // no owner. Both are checked: watching only one let this invariant slip the
    // moment the routing changed.
    const state = (
      useSessionStore as unknown as {
        getState: () => Record<string, { mock: { calls: unknown[][] } }>;
      }
    ).getState();
    const bubbleInserts = [
      // addMessage(role, content, extra)
      ...state.addMessage.mock.calls.map((c) => ({ content: c[1], extra: c[2] })),
      // addMessageToTargetSession(session, role, content, extra)
      ...state.addMessageToTargetSession.mock.calls.map((c) => ({ content: c[2], extra: c[3] })),
    ].filter((i) => String((i.extra as { id?: string } | undefined)?.id ?? '').startsWith('stream-'));

    expect(bubbleInserts.length).toBeGreaterThan(0);
    bubbleInserts.forEach((i) => expect(i.content).not.toBe(''));
  });

  it('is a no-op for an untracked job', () => {
    expect(() =>
      useExecutionStore.getState().appendStreamChunk('never-started', 'text'),
    ).not.toThrow();
    expect(() => useExecutionStore.getState().closeStreamBubble('never-started')).not.toThrow();
  });
});

/**
 * A crew announces each task's answer TWICE: live as `llm_chunk` tokens, then
 * again in the `task_completed` trace carrying the finished text. While the
 * subprocess event pipe was broken only the second arrived, so rendering both
 * looked right — once streaming worked, every research answer printed twice.
 *
 * `hasStreamedTaskText` is the signal useChatRunStream uses to drop the trace
 * body when the tokens are already on screen.
 */
describe('hasStreamedTaskText — the duplicate-answer guard', () => {
  it('is false before anything streams', () => {
    const { jobId } = startOwnedJob('job-dup-1');

    expect(useExecutionStore.getState().hasStreamedTaskText(jobId)).toBe(false);
  });

  it('is true while the current task is streaming', () => {
    const { jobId } = startOwnedJob('job-dup-2');

    useExecutionStore.getState().appendStreamChunk(jobId, 'the answer');
    drainFrames();

    expect(useExecutionStore.getState().hasStreamedTaskText(jobId)).toBe(true);
  });

  it('is true for text still queued in the pacer, before it is painted', () => {
    // The trace can land before the pacer has flushed. Reporting false there
    // would post the body and THEN paint the same text under it.
    const { jobId } = startOwnedJob('job-dup-3');

    useExecutionStore.getState().appendStreamChunk(jobId, 'word '.repeat(200));

    expect(useExecutionStore.getState().hasStreamedTaskText(jobId)).toBe(true);
  });

  it('goes false again once the bubble closes at a task boundary', () => {
    // The NEXT task has not streamed yet, so its trace body must still post.
    const { jobId } = startOwnedJob('job-dup-4');
    useExecutionStore.getState().appendStreamChunk(jobId, 'task one output');
    drainFrames();

    useExecutionStore.getState().closeStreamBubble(jobId);

    expect(useExecutionStore.getState().hasStreamedTaskText(jobId)).toBe(false);
  });

  it('is false for a job that never started', () => {
    expect(useExecutionStore.getState().hasStreamedTaskText('never-started')).toBe(false);
  });

  it('marks the run finalized so a late task_completed is dropped', () => {
    // `_relay_task_events` broadcasts task_completed — carrying the task's full
    // output — from its own queue-driven relay with no DB id, so the frontend's
    // trace de-dupe cannot collapse it, and it routinely lands after the run has
    // completed. Completion clears both bubble maps, so a guard reading only
    // those said "nothing streamed" and posted the answer a SECOND time under
    // the copy the reader had been watching.
    const { jobId } = startOwnedJob('job-late-1');
    useExecutionStore.getState().appendStreamChunk(jobId, 'the answer');
    drainFrames();
    useExecutionStore.getState().completeExecution('the answer', jobId);
    drainFrames();

    // Not hasStreamedTaskText — that is per-TASK and correctly goes false at a
    // boundary, so a later non-streaming task still gets its body while live.
    expect(useExecutionStore.getState().isRunFinalized(jobId)).toBe(true);
  });

  it('is not finalized while the run is still going', () => {
    const { jobId } = startOwnedJob('job-late-3');
    useExecutionStore.getState().appendStreamChunk(jobId, 'partial');
    drainFrames();

    expect(useExecutionStore.getState().isRunFinalized(jobId)).toBe(false);
  });

  it('forgets a job that was abandoned', () => {
    const { jobId } = startOwnedJob('job-late-2');
    useExecutionStore.getState().appendStreamChunk(jobId, 'text');
    drainFrames();

    useExecutionStore.getState().abandonExecution(jobId);

    expect(useExecutionStore.getState().isRunFinalized(jobId)).toBe(false);
  });
});

/**
 * The answer must appear ONCE.
 *
 * completeExecution finalizes the live stream bubble in place — but
 * `closeStreamBubble` empties `streamBubbles` at every task boundary, so a run
 * whose last task had already closed its bubble arrived at completion with none
 * to finalize and posted the answer as a NEW message, directly beneath the
 * streamed copy the reader had been watching.
 *
 * `supersedeTruncatedTail` could not cover it: that scan looks for a CAPPED
 * tail, and a streamed bubble holds the full text. Hence `lastStreamBubble`,
 * which survives boundary closes the way `streamBubbleSeq` already did.
 */
describe('completion folds into the streamed bubble', () => {
  const ANSWER = "I'm doing well, thanks for asking!";
  const copiesOf = (jobId: string) =>
    paintedMessages().filter(
      (m) => m.content.includes(ANSWER) && (m.id.includes(jobId) || !m.id.startsWith('stream-')),
    );

  it('prints once when the bubble is still open', () => {
    const { jobId } = startOwnedJob('job-fold-1');
    useExecutionStore.getState().appendStreamChunk(jobId, ANSWER);
    drainFrames();

    useExecutionStore.getState().completeExecution(ANSWER, jobId);
    drainFrames();

    expect(copiesOf(jobId)).toHaveLength(1);
  });

  it('prints once when a task boundary already closed the bubble', () => {
    // The regression: this printed the answer twice.
    const { jobId } = startOwnedJob('job-fold-2');
    useExecutionStore.getState().appendStreamChunk(jobId, ANSWER);
    drainFrames();
    useExecutionStore.getState().closeStreamBubble(jobId);

    useExecutionStore.getState().completeExecution(ANSWER, jobId);
    drainFrames();

    expect(copiesOf(jobId)).toHaveLength(1);
  });

  it('still posts the answer when nothing streamed at all', () => {
    // Streaming off / non-streaming model: the terminal text is the ONLY copy,
    // so the fold must not swallow it. Asserted on the target-session mock,
    // which is where a message with an owner lands in this harness.
    const { jobId } = startOwnedJob('job-fold-3');
    const posted = (
      useSessionStore as unknown as {
        getState: () => { addMessageToTargetSession: { mock: { calls: unknown[][] } } };
      }
    ).getState().addMessageToTargetSession;
    const before = posted.mock.calls.length;

    useExecutionStore.getState().completeExecution(ANSWER, jobId);
    drainFrames();

    const added = posted.mock.calls.slice(before);
    expect(added.some((c) => String(c[2] ?? '').includes(ANSWER))).toBe(true);
  });
});
