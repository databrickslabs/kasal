/**
 * When a surface gets to the screen first, the prose stops being the answer.
 *
 * The instant deck shell ships before the agent writes a token, so on a
 * presentation turn the reader would otherwise watch a wall of markdown type
 * itself out *beside* the deck being composed from it — the same content twice,
 * in two forms, racing each other. There the prose is the deck's source
 * material, so it is not painted.
 *
 * Only a surface that beats every token does this. Anything arriving mid-run (a
 * dashboard's surface, a late skeleton) leaves the streamed text alone — taking
 * away what the reader is already reading is the failure this codebase has
 * repeatedly guarded against.
 */
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';

interface FakeMessage {
  id: string;
  content: string;
  resultType?: string;
  resultData?: unknown;
}

// Same shape as streamPacing.test.ts — the array lives INSIDE the factory
// because vi.mock is hoisted above any top-level const.
vi.mock('./sessionStore', () => {
  const painted: FakeMessage[] = [];
  const state = {
    currentSessionId: 'session-1' as string | null,
    messages: painted,
    addMessage: vi.fn((_role: string, content: string, extra?: Record<string, unknown>) => {
      const id = (extra?.id as string) ?? `m-${painted.length}`;
      painted.push({ id, content, ...extra });
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
    // A job with an owner routes through the *TargetSession variants, which is
    // the path a real run always takes — stubbing them as no-ops would make this
    // suite pass while asserting nothing about where the surface lands.
    updateMessageInTargetSession: vi.fn(
      (_session: string, id: string, updates: Partial<FakeMessage>) => {
        const msg = painted.find((m) => m.id === id);
        if (msg) Object.assign(msg, updates);
      },
    ),
    updateMessage: vi.fn((id: string, updates: Partial<FakeMessage>) => {
      const msg = painted.find((m) => m.id === id);
      if (msg) Object.assign(msg, updates);
    }),
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
import type { A2uiMessage } from '../../../shared/a2ui/stream';

const paintedMessages = (): FakeMessage[] =>
  (useSessionStore as unknown as { getState: () => { messages: FakeMessage[] } }).getState()
    .messages;

const paintedText = (): string => paintedMessages().map((m) => m.content).join('');

let frameQueue: FrameRequestCallback[] = [];

function drainFrames(maxTicks = 200): void {
  for (let i = 0; i < maxTicks && frameQueue.length; i++) {
    frameQueue.splice(0, frameQueue.length).forEach((cb) => cb(performance.now()));
  }
}

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

afterEach(() => vi.unstubAllGlobals());

const SHELL: A2uiMessage = {
  createSurface: {
    surfaceId: 'a2ui',
    surfaceKind: 'presentation',
    root: 'deck',
    components: [
      { id: 'deck', component: 'SlideDeck', children: ['slide_1'] },
      { id: 'slide_1', component: 'Slide', title: 'How LLM Works', pending: true },
    ],
  },
};

const RETRACT: A2uiMessage = { deleteSurface: { surfaceId: 'a2ui' } };

function startOwnedJob(jobId: string) {
  useExecutionStore.getState().startExecution(jobId, 'session-1');
  return jobId;
}

describe('a surface that leads the run', () => {
  it('suppresses the prose stream once the shell is on screen', () => {
    const jobId = startOwnedJob('job-led-1');
    const store = useExecutionStore.getState();

    store.applySurfaceDelta(jobId, SHELL);
    store.appendStreamChunk(jobId, 'Large language models work by predicting tokens.');
    drainFrames();

    expect(paintedText()).not.toContain('Large language models');
  });

  it('still renders the surface it suppressed the text for', () => {
    const jobId = startOwnedJob('job-led-2');
    useExecutionStore.getState().applySurfaceDelta(jobId, SHELL);

    const msg = paintedMessages().find((m) => m.resultType === 'a2ui');
    expect(msg).toBeTruthy();
    expect((msg!.resultData as { root: string }).root).toBe('deck');
  });

  it('leaves prose alone when the text got there first', () => {
    const jobId = startOwnedJob('job-led-3');
    const store = useExecutionStore.getState();

    store.appendStreamChunk(jobId, 'Here are the numbers you asked for. ');
    drainFrames();
    store.applySurfaceDelta(jobId, SHELL);
    store.appendStreamChunk(jobId, 'And some more detail.');
    drainFrames();

    expect(paintedText()).toContain('Here are the numbers');
    expect(paintedText()).toContain('And some more detail');
  });

  it('hands the run back to prose when the surface is retracted', () => {
    // A presentation request that ends up answering in text: the shell goes, and
    // the words have to start flowing again.
    const jobId = startOwnedJob('job-led-4');
    const store = useExecutionStore.getState();

    store.applySurfaceDelta(jobId, SHELL);
    store.appendStreamChunk(jobId, 'dropped while the shell was up');
    store.applySurfaceDelta(jobId, RETRACT);
    store.appendStreamChunk(jobId, 'this must appear');
    drainFrames();

    expect(paintedText()).toContain('this must appear');
  });

  it('does not treat a surface as leading when only a later one arrives', () => {
    // The dashboard case: the composer's own createSurface lands mid-run, well
    // after the answer has been streaming.
    const jobId = startOwnedJob('job-led-5');
    const store = useExecutionStore.getState();

    store.appendStreamChunk(jobId, 'Sales rose 12% in Q3. ');
    drainFrames();
    store.applySurfaceDelta(jobId, {
      createSurface: {
        surfaceId: 'a2ui',
        surfaceKind: 'dashboard',
        root: 'col',
        components: [{ id: 'col', component: 'Column', children: [] }],
      },
    });
    store.appendStreamChunk(jobId, 'Margins held flat.');
    drainFrames();

    expect(paintedText()).toContain('Margins held flat');
  });
});

describe('a shell that can still be taken back', () => {
  // dashboard/document surfaces are dropped to plain text when the answer turns
  // out to carry no real data. Silencing the prose under one risks the frame
  // vanishing with nothing left in its place.
  const RETRACTABLE_SHELL: A2uiMessage = {
    createSurface: {
      surfaceId: 'a2ui',
      surfaceKind: 'dashboard',
      root: 'shell',
      components: [{ id: 'shell', component: 'Skeleton', variant: 'kanban', pending: true }],
    },
  };

  it('keeps streaming the prose underneath', () => {
    const jobId = startOwnedJob('job-retractable-1');
    const store = useExecutionStore.getState();

    store.applySurfaceDelta(jobId, RETRACTABLE_SHELL);
    store.appendStreamChunk(jobId, 'The sprint has three columns of work.');
    drainFrames();

    expect(paintedText()).toContain('three columns of work');
  });

  it('still shows the frame while it waits', () => {
    const jobId = startOwnedJob('job-retractable-2');
    useExecutionStore.getState().applySurfaceDelta(jobId, RETRACTABLE_SHELL);

    const msg = paintedMessages().find((m) => m.resultType === 'a2ui');
    expect((msg!.resultData as { surfaceKind: string }).surfaceKind).toBe('dashboard');
  });

  it('leaves nothing behind if it is retracted', () => {
    const jobId = startOwnedJob('job-retractable-3');
    const store = useExecutionStore.getState();

    store.applySurfaceDelta(jobId, RETRACTABLE_SHELL);
    store.appendStreamChunk(jobId, 'Actually here is a prose answer.');
    store.applySurfaceDelta(jobId, RETRACT);
    drainFrames();

    const msg = paintedMessages().find((m) => m.id.startsWith('stream-'));
    expect(msg?.resultType).toBeUndefined();
    expect(paintedText()).toContain('prose answer');
  });
});

describe('switching sessions mid-run', () => {
  it('creates the surface in the session that ASKED for it, not the one on screen', () => {
    // The bug: the bubble was opened with `addMessage`, which writes to whatever
    // session is currently being viewed. Switch away while a deck is composing
    // and it was created in the wrong conversation and persisted there — the
    // session that asked for it came back to nothing.
    const jobId = 'job-switch-1';
    useExecutionStore.getState().startExecution(jobId, 'session-1');

    // The reader moves to another conversation while the deck composes.
    const session = useSessionStore.getState() as unknown as {
      currentSessionId: string;
      addMessageToTargetSession: { mock: { calls: unknown[][] } };
    };
    session.currentSessionId = 'session-2';

    useExecutionStore.getState().applySurfaceDelta(jobId, SHELL);

    const targeted = session.addMessageToTargetSession.mock.calls.filter((c) =>
      String((c[3] as { id?: string } | undefined)?.id ?? '').startsWith('stream-'),
    );
    expect(targeted.length).toBeGreaterThan(0);
    expect(targeted[0][0]).toBe('session-1');

    session.currentSessionId = 'session-1'; // restore for the next test
  });
});

describe('reconnecting after a session switch', () => {
  // A run's SSE stream belongs to whichever session is on screen, so switching
  // away closes it and switching back reconnects. The reconnect REPLAYS the
  // snapshots — that is what lets a late joiner see the deck at all — but a
  // snapshot replaces the whole surface, so replaying the shell at a client that
  // had already accumulated slides threw every one of them away.
  const slide = (id: string, seq: number): [A2uiMessage, number] => [
    { updateComponents: { surfaceId: 'a2ui', components: [{ id, component: 'Slide', title: id }] } },
    seq,
  ];

  it('does not let a replayed shell wipe the slides already built', () => {
    const jobId = 'job-reconnect-1';
    startOwnedJob(jobId);
    const store = useExecutionStore.getState();

    store.applySurfaceDelta(jobId, SHELL, 0);
    store.applySurfaceDelta(jobId, ...slide('slide_2', 1));
    store.applySurfaceDelta(jobId, ...slide('slide_3', 2));

    // Switch away, switch back: the stream reconnects and replays seq 0.
    store.applySurfaceDelta(jobId, SHELL, 0);

    const msg = paintedMessages().find((m) => m.resultType === 'a2ui')!;
    const ids = (msg.resultData as { components: { id: string }[] }).components.map((c) => c.id);
    expect(ids).toContain('slide_2');
    expect(ids).toContain('slide_3');
  });

  it('still applies genuinely new deltas after the replay', () => {
    const jobId = 'job-reconnect-2';
    startOwnedJob(jobId);
    const store = useExecutionStore.getState();

    store.applySurfaceDelta(jobId, SHELL, 0);
    store.applySurfaceDelta(jobId, ...slide('slide_2', 1));
    store.applySurfaceDelta(jobId, SHELL, 0); // replayed
    store.applySurfaceDelta(jobId, ...slide('slide_9', 7)); // new, post-reconnect

    const msg = paintedMessages().find((m) => m.resultType === 'a2ui')!;
    const ids = (msg.resultData as { components: { id: string }[] }).components.map((c) => c.id);
    expect(ids).toContain('slide_9');
  });

  it('still accepts a first delta from a client that joined late', () => {
    // No seq seen yet, so the replayed shell is exactly what this client needs.
    const jobId = 'job-reconnect-3';
    startOwnedJob(jobId);
    useExecutionStore.getState().applySurfaceDelta(jobId, SHELL, 4);

    expect(paintedMessages().find((m) => m.resultType === 'a2ui')).toBeTruthy();
  });
});

describe('a surface belongs to exactly one session and one message', () => {
  it('is dropped rather than leaked when the run has no owner', () => {
    // Opening a bubble for an unattributable run writes it into whatever
    // conversation is on screen — which is how a deck appeared above an
    // unrelated prompt in an older session.
    const before = paintedMessages().length;
    useExecutionStore.getState().applySurfaceDelta('job-with-no-owner', SHELL, 0);
    expect(paintedMessages().length).toBe(before);
  });

  it('does not copy the deck onto a second message at completion', () => {
    // The bubble persists its streamed surface now, so applying the run's
    // surface to a superseded message too rendered the deck twice, one under
    // the other.
    const jobId = 'job-single-home';
    startOwnedJob(jobId);
    const store = useExecutionStore.getState();
    store.applySurfaceDelta(jobId, SHELL, 0);

    store.completeExecution('the full prose answer', jobId, {
      surfaceKind: 'presentation',
      root: 'deck',
      components: [{ id: 'deck', component: 'SlideDeck', children: [] }],
      dataModel: {},
    } as never);

    const withDecks = paintedMessages().filter((m) => m.resultType === 'a2ui');
    expect(withDecks).toHaveLength(1);
  });
});
