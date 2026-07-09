import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import ChatContainer, { liveStepLine } from './ChatContainer';
import type { ChatMessage as ChatMessageType } from '../../types/chat';

// Stub the heavy children so we can isolate ChatContainer logic. The run/
// generation status + Stop control now live INSIDE ChatInput, so the mock
// surfaces the forwarded props for assertions.
vi.mock('./ChatInput', () => ({
  default: (props: {
    onSend: (m: string) => void;
    disabled?: boolean;
    isExecuting?: boolean;
    isGenerating?: boolean;
    onStopExecution?: () => void;
  }) => (
    <div>
      <button data-testid="chat-input" disabled={props.disabled} onClick={() => props.onSend('typed')}>
        input
      </button>
      {props.isExecuting && <span data-testid="input-executing">running</span>}
      {props.isGenerating && <span data-testid="input-generating">generating</span>}
      {props.isExecuting && props.onStopExecution && (
        <button data-testid="input-stop" onClick={props.onStopExecution}>
          stop
        </button>
      )}
    </div>
  ),
}));

vi.mock('./ChatMessage', () => ({
  default: (props: { message: ChatMessageType; onCommand: (c: string) => void }) => (
    <button data-testid={`msg-${props.message.id}`} onClick={() => props.onCommand('cmd')}>
      {props.message.content}
    </button>
  ),
  // RunProgress's timeline uses formatDurationMs for each step.
  formatDurationMs: (ms?: number) => (typeof ms === 'number' ? `${ms}ms` : null),
  // The crew structure card is mounted inside the run-activity container.
  GenerationCompleteCard: (props: { messageId: string }) => (
    <div data-testid={`crew-card-${props.messageId}`}>crew card</div>
  ),
}));

// jsdom lacks scrollIntoView
beforeEach(() => {
  Element.prototype.scrollIntoView = vi.fn();
});

const msg = (id: string, content = 'hi'): ChatMessageType =>
  ({ id, role: 'assistant', content, timestamp: new Date() } as ChatMessageType);

const baseProps = {
  onSend: vi.fn(),
  isLoading: false,
  models: [],
  selectedModel: '',
  onModelChange: vi.fn(),
};

describe('ChatContainer', () => {
  it('renders the empty state (greeting + input) when no messages and not executing', () => {
    render(<ChatContainer {...baseProps} messages={[]} />);
    expect(screen.getByText('What can I help you with?')).toBeInTheDocument();
    expect(screen.getByTestId('chat-input')).toBeInTheDocument();
  });

  it('holds the greeting while hydrating (restoring a session on refresh)', () => {
    // With no messages YET but a session being restored, the empty greeting must
    // not flash — the input still renders, but not the "new chat" greeting.
    render(<ChatContainer {...baseProps} messages={[]} hydrating />);
    expect(screen.queryByText('What can I help you with?')).not.toBeInTheDocument();
    expect(screen.getByTestId('chat-input')).toBeInTheDocument();
  });

  it('renders the conversation list when messages exist', () => {
    render(<ChatContainer {...baseProps} messages={[msg('a'), msg('b')]} />);
    expect(screen.getByTestId('msg-a')).toBeInTheDocument();
    expect(screen.getByTestId('msg-b')).toBeInTheDocument();
    // not the empty greeting
    expect(screen.queryByText('What can I help you with?')).not.toBeInTheDocument();
  });

  it('forwards executing state + Stop handler to the input (no top banner)', () => {
    const onStopExecution = vi.fn();
    render(
      <ChatContainer
        {...baseProps}
        messages={[]}
        isExecuting
        onStopExecution={onStopExecution}
      />,
    );
    // The old top-of-screen banner is gone; status lives in the input.
    expect(screen.queryByText('Running crew...')).not.toBeInTheDocument();
    expect(screen.getByTestId('input-executing')).toBeInTheDocument();
    fireEvent.click(screen.getByTestId('input-stop'));
    expect(onStopExecution).toHaveBeenCalled();
  });

  it('does not surface executing state in the input when not executing', () => {
    render(<ChatContainer {...baseProps} messages={[msg('a')]} />);
    expect(screen.queryByTestId('input-executing')).not.toBeInTheDocument();
    expect(screen.queryByTestId('input-stop')).not.toBeInTheDocument();
  });

  it('forwards generating state to the input', () => {
    render(<ChatContainer {...baseProps} messages={[msg('a')]} isGenerating />);
    expect(screen.queryByText('Generating crew...')).not.toBeInTheDocument();
    expect(screen.getByTestId('input-generating')).toBeInTheDocument();
  });

  it('handleCommand uses onCommand when provided', () => {
    const onCommand = vi.fn();
    render(<ChatContainer {...baseProps} messages={[msg('a')]} onCommand={onCommand} />);
    fireEvent.click(screen.getByTestId('msg-a'));
    expect(onCommand).toHaveBeenCalledWith('cmd');
  });

  it('handleCommand falls back to onSend when onCommand is not provided', () => {
    const onSend = vi.fn();
    render(<ChatContainer {...baseProps} onSend={onSend} messages={[msg('a')]} />);
    fireEvent.click(screen.getByTestId('msg-a'));
    expect(onSend).toHaveBeenCalledWith('cmd');
  });

  it('renders the reopen-preview pill above the composer (conversation + empty state) and wires the click', () => {
    const onReopenPreview = vi.fn();
    // Conversation state.
    const { unmount } = render(
      <ChatContainer {...baseProps} messages={[msg('a')]} showReopenPreview onReopenPreview={onReopenPreview} />,
    );
    fireEvent.click(screen.getByText('Show preview'));
    expect(onReopenPreview).toHaveBeenCalledTimes(1);
    unmount();
    // Empty state shows it too (e.g. a restored session whose deliverable persisted).
    render(<ChatContainer {...baseProps} messages={[]} showReopenPreview onReopenPreview={onReopenPreview} />);
    expect(screen.getByText('Show preview')).toBeInTheDocument();
  });

  it('hides the reopen-preview pill when not armed', () => {
    render(<ChatContainer {...baseProps} messages={[msg('a')]} />);
    expect(screen.queryByText('Show preview')).not.toBeInTheDocument();
  });

  it('disables the input while loading', () => {
    render(<ChatContainer {...baseProps} messages={[]} isLoading />);
    expect(screen.getByTestId('chat-input')).toBeDisabled();
  });
});

describe('liveStepLine — the live header one-liner', () => {
  const step = (over: Record<string, unknown>) =>
    ({ label: 'Tool', kind: 'tool_call', ...over } as never);

  it('Memory steps surface the retrieved context, falling back to the sublabel', () => {
    expect(liveStepLine(step({ label: 'Memory', detail: 'ctx line\nmore' }))).toEqual({
      name: 'Memory',
      line: 'ctx line',
    });
    expect(liveStepLine(step({ label: 'Memory', sublabel: 'context retrieved' }))).toEqual({
      name: 'Memory',
      line: 'context retrieved',
    });
  });

  it('other steps prefer the sublabel and fall back to the detail', () => {
    expect(liveStepLine(step({ sublabel: 'query', detail: 'full' })).line).toBe('query');
    expect(liveStepLine(step({ detail: 'full output' })).line).toBe('full output');
  });

  it('returns an empty line when the step has no text at all (or only blanks)', () => {
    expect(liveStepLine(step({})).line).toBe('');
    expect(liveStepLine(step({ sublabel: '\n  \n' })).line).toBe('');
  });

  it('truncates the line to 100 characters', () => {
    const { line } = liveStepLine(step({ sublabel: 'y'.repeat(140) }));
    expect(line).toBe(`${'y'.repeat(100)}…`);
  });
});

describe('ChatContainer — run-activity container (RunProgress)', () => {
  const traceMsg = (id: string, label: string, extra: Record<string, unknown> = {}): ChatMessageType =>
    ({
      id,
      role: 'assistant',
      content: '',
      timestamp: new Date(),
      resultType: 'trace',
      resultData: { label, kind: 'tool_result', durationMs: 100, ...extra },
    } as unknown as ChatMessageType);

  it('shows a live "Thinking…" indicator with animated dots while generating', () => {
    render(<ChatContainer {...baseProps} messages={[msg('u', 'q')]} isGenerating />);
    const label = screen.getByText(/Thinking/);
    expect(label).toBeInTheDocument();
    expect(label.querySelector('.kasal-thinking-dots')).not.toBeNull();
    expect(screen.queryByLabelText('Stop execution')).toBeNull();
    expect(screen.queryByTestId('trace-group')).toBeNull();
  });

  it('while executing with traces: live header, Stop, and an expandable thinking stream (friendly phases)', () => {
    const onStop = vi.fn();
    render(
      <ChatContainer
        {...baseProps}
        messages={[
          msg('u', 'q'),
          traceMsg('t1', 'PerplexityTool', { sublabel: 'find stock photos' }),
          traceMsg('t2', 'Memory', { detail: 'recalled 3 items' }),
          traceMsg('t3', 'ScrapeTool'),
        ]}
        isExecuting
        onStopExecution={onStop}
      />,
    );
    // The header tracks the LATEST step (no static "Working…" once traces exist).
    expect(screen.queryByText('Working…')).toBeNull();
    fireEvent.click(screen.getByLabelText('Stop execution'));
    expect(onStop).toHaveBeenCalledTimes(1);
    // collapsed by default → the stream is hidden.
    expect(screen.queryByText('Searching the web')).toBeNull();
    // expand → the thinking stream's friendly phase headings (not raw tool names).
    fireEvent.click(screen.getByLabelText('Expand run activity'));
    expect(screen.getByText('Searching the web')).toBeInTheDocument();
    expect(screen.getByText('Recalling context')).toBeInTheDocument();
    expect(screen.getByText('Reading sources')).toBeInTheDocument();
    // the search query is woven into the first-person narrative.
    expect(screen.getByText(/find stock photos/)).toBeInTheDocument();
    // toggling closed again (Collapse label) hides the stream.
    fireEvent.click(screen.getByLabelText('Collapse run activity'));
    expect(screen.queryByText('Searching the web')).toBeNull();
  });

  it('live header shows the latest step name + first line of its query (one-liner)', () => {
    render(
      <ChatContainer
        {...baseProps}
        messages={[
          msg('u', 'q'),
          traceMsg('t1', 'Search memory', { sublabel: 'oil price benchmarks\nsecond line ignored' }),
        ]}
        isExecuting
      />,
    );
    expect(screen.getByText('Search memory')).toBeInTheDocument();
    // Only the FIRST line of the sublabel, prefixed with an em dash.
    expect(screen.getByText(/—\s*oil price benchmarks/)).toBeInTheDocument();
    expect(screen.queryByText(/second line ignored/)).toBeNull();
  });

  it('live header shows the first line of the retrieved Memory context (not the generic sublabel)', () => {
    render(
      <ChatContainer
        {...baseProps}
        messages={[
          msg('u', 'q'),
          traceMsg('m1', 'Memory', {
            sublabel: 'context retrieved',
            detail: 'WTI closed at $78.12 yesterday\nBrent at $82.40',
          }),
        ]}
        isExecuting
      />,
    );
    expect(screen.getByText('Memory')).toBeInTheDocument();
    expect(screen.getByText(/—\s*WTI closed at \$78\.12 yesterday/)).toBeInTheDocument();
    expect(screen.queryByText(/Brent at/)).toBeNull();
  });

  it('expands into the thinking stream (no per-step "Show context" toggle)', () => {
    // Not executing → no live header one-liner, so the content appears once.
    render(
      <ChatContainer
        {...baseProps}
        messages={[msg('u'), traceMsg('e1', 'EchoTool', { sublabel: 'same text', detail: 'extra context' })]}
      />,
    );
    fireEvent.click(screen.getByLabelText('Expand run activity'));
    // Unknown tool → its name is the heading; the legacy "Show context" toggle is gone.
    expect(screen.getByText('EchoTool')).toBeInTheDocument();
    expect(screen.queryByLabelText('Show context for EchoTool')).toBeNull();
  });

  it('renders each tool call as its own step in the thinking stream', () => {
    render(
      <ChatContainer
        {...baseProps}
        messages={[msg('u'), traceMsg('p1', 'PerplexityTool', { sublabel: 'q1' }), traceMsg('p2', 'PerplexityTool', { sublabel: 'q2' })]}
      />,
    );
    fireEvent.click(screen.getByLabelText('Expand run activity'));
    // Two Perplexity calls → two "Searching the web" steps; each query in its narrative.
    expect(screen.getAllByText('Searching the web')).toHaveLength(2);
    expect(screen.getByText(/q1/)).toBeInTheDocument();
    expect(screen.getByText(/q2/)).toBeInTheDocument();
  });

  it('keeps non-tool_result steps (pending calls, events) visible after the run completes', () => {
    // Regression: anything shown while the run streamed must not vanish when it
    // ends — the timeline previously filtered to kind==='tool_result' only.
    render(
      <ChatContainer
        {...baseProps}
        messages={[
          msg('u', 'q'),
          traceMsg('c1', 'ScrapeTool', { kind: 'tool_call', sublabel: 'fetching the page' }),
          traceMsg('e1', "⚠ MCP server 'x': HTTP 403 - Forbidden", { kind: 'event' }),
          traceMsg('r1', 'Memory', { detail: 'recalled 3 items' }),
        ]}
      />,
    );
    fireEvent.click(screen.getByLabelText('Expand run activity'));
    expect(screen.getByText('Reading sources')).toBeInTheDocument(); // the pending call
    expect(screen.getByText(/HTTP 403/)).toBeInTheDocument(); // the event
    expect(screen.getByText('Recalling context')).toBeInTheDocument(); // the result
  });

  it('clicking a step ROW opens that step in the preview pane (focusStep passed through)', () => {
    const onShowRunInPane = vi.fn();
    render(
      <ChatContainer
        {...baseProps}
        messages={[msg('u'), traceMsg('m1', 'Memory', { sublabel: 'context retrieved', detail: 'WTI closed at $78.12' })]}
        onShowRunInPane={onShowRunInPane}
      />,
    );
    fireEvent.click(screen.getByLabelText('Expand run activity'));
    fireEvent.click(screen.getByLabelText('Open the full context for Recalling context'));
    expect(onShowRunInPane).toHaveBeenCalledTimes(1);
    const [, , focusStep] = onShowRunInPane.mock.calls[0];
    expect(focusStep).toMatchObject({ label: 'Memory', detail: 'WTI closed at $78.12' });
  });

  it('after the run (not executing) shows "Run activity" and no Stop', () => {
    render(<ChatContainer {...baseProps} messages={[msg('u', 'q'), traceMsg('t1', 'PerplexityTool')]} />);
    expect(screen.getByText('Run activity')).toBeInTheDocument();
    expect(screen.queryByLabelText('Stop execution')).toBeNull();
  });

  it('executing without a stop handler shows no Stop button', () => {
    render(<ChatContainer {...baseProps} messages={[msg('u', 'q'), traceMsg('t1', 'PerplexityTool')]} isExecuting />);
    // Live header shows the latest step instead of a static "Working…".
    expect(screen.getByText('PerplexityTool')).toBeInTheDocument();
    expect(screen.queryByLabelText('Stop execution')).toBeNull();
  });

  it('shows a transient "Stopping…" state when Stop is pressed, then clears it when the run ends', () => {
    const onStop = vi.fn();
    const stillRunning = [msg('u', 'q'), traceMsg('t1', 'PerplexityTool')];
    const { rerender } = render(
      <ChatContainer {...baseProps} messages={stillRunning} isExecuting onStopExecution={onStop} />,
    );
    expect(screen.getByText('PerplexityTool')).toBeInTheDocument();
    fireEvent.click(screen.getByLabelText('Stop execution'));
    expect(onStop).toHaveBeenCalledTimes(1);
    // immediate feedback: "Stopping…" replaces the live line + the control
    // becomes a disabled spinner
    expect(screen.getByText('Stopping…')).toBeInTheDocument();
    expect(screen.queryByText('Working…')).toBeNull();
    expect(screen.queryByText('PerplexityTool')).toBeNull();
    expect(screen.getByLabelText('Stopping…')).toBeDisabled();
    // run actually ends → state clears, container settles into the done view
    rerender(<ChatContainer {...baseProps} messages={stillRunning} />); // isExecuting now false
    expect(screen.queryByText('Stopping…')).toBeNull();
    expect(screen.getByText('Run activity')).toBeInTheDocument();
  });

  const genMsg = (id = 'gen1'): ChatMessageType =>
    ({
      id,
      role: 'assistant',
      content: '',
      timestamp: new Date(),
      resultType: 'generation_complete',
      resultData: { agents: [{ name: 'A' }], tasks: [{ name: 'T' }] },
    } as unknown as ChatMessageType);

  it('no longer mounts the crew card inside the run container', () => {
    render(<ChatContainer {...baseProps} messages={[msg('u', 'q'), genMsg('gen1')]} />);
    // The run container no longer hosts a crew card (new generations never
    // produce these messages; legacy ones flow through ChatMessage as a
    // normal bubble instead).
    expect(screen.queryByTestId('crew-card-gen1')).toBeNull();
    expect(screen.getByTestId('msg-gen1')).toBeInTheDocument();
    expect(screen.queryByText('Crew ready')).toBeNull();
    // the user's prompt still renders normally
    expect(screen.getByTestId('msg-u')).toBeInTheDocument();
  });

  it('folds activity into the collapsible while executing — no crew card anywhere', () => {
    const onStop = vi.fn();
    render(
      <ChatContainer
        {...baseProps}
        messages={[msg('u', 'q'), genMsg('gen1'), traceMsg('t1', 'PerplexityTool', { sublabel: 'find stock photos' })]}
        isExecuting
        onStopExecution={onStop}
      />,
    );
    // Live header shows the latest step name (the timeline itself stays collapsed).
    expect(screen.getByText('PerplexityTool')).toBeInTheDocument();
    expect(screen.queryByTestId('crew-card-gen1')).toBeNull();
    expect(screen.getByLabelText('Stop execution')).toBeInTheDocument();
    // stream collapsed by default; expands to the friendly phase + narrative.
    expect(screen.queryByText('Searching the web')).toBeNull();
    fireEvent.click(screen.getByLabelText('Expand run activity'));
    expect(screen.getByText('Searching the web')).toBeInTheDocument();
    // The query shows in the live header AND the stream narrative while executing.
    expect(screen.getAllByText(/find stock photos/).length).toBeGreaterThan(0);
  });

  it('keeps an inline-rendered Genie answer in the chat (not inside the container)', () => {
    const genie = {
      id: 'g1',
      role: 'assistant',
      content: '',
      timestamp: new Date(),
      resultType: 'trace',
      resultData: { label: 'GenieTool', kind: 'tool_result', detail: 'SQL Query:\nSELECT 1' },
    } as unknown as ChatMessageType;
    render(<ChatContainer {...baseProps} messages={[msg('u', 'q'), genie]} />);
    // The Genie trace renders as a normal chat message (stubbed), not folded away.
    expect(screen.getByTestId('msg-g1')).toBeInTheDocument();
  });

  it('renders a completed run as plain conversation — no card, no leftover container', () => {
    // Regression: a completed run with an inline Genie answer + a result message
    // but NO trace groups. The run container (crew card) must stay anchored above
    // the result it produced, not pin to the bottom below it.
    const genie = {
      id: 'g1', role: 'assistant', content: '', timestamp: new Date(),
      resultType: 'trace',
      resultData: { label: 'GenieTool', kind: 'tool_result', detail: 'SQL Query:\nSELECT 1' },
    } as unknown as ChatMessageType;
    const result = msg('res', 'Generated an app. View it in the preview pane.');
    render(
      <ChatContainer {...baseProps} messages={[msg('u', 'q'), genMsg('gen1'), genie, result]} />,
    );
    // No crew card and no leftover container chrome: the inline Genie answer
    // and the result render as normal conversation, nothing pinned below.
    expect(screen.queryByTestId('crew-card-gen1')).toBeNull();
    expect(screen.queryByText('Crew ready')).toBeNull();
    expect(screen.getByTestId('msg-res')).toBeInTheDocument();
  });
});

describe('ChatContainer — one run-activity section per prompt', () => {
  const userMsg = (id: string, content: string) =>
    ({ id, role: 'user', content, timestamp: new Date() } as unknown as ChatMessageType);
  const trace = (id: string, label: string, sublabel?: string) =>
    ({
      id, role: 'assistant', content: '', timestamp: new Date(),
      resultType: 'trace',
      resultData: { label, ...(sublabel ? { sublabel } : {}), kind: 'tool_result', timestamp: Date.now() },
    } as unknown as ChatMessageType);

  it('a second prompt gets its OWN activity section under it (not merged into the first)', () => {
    render(
      <ChatContainer
        {...baseProps}
        messages={[
          userMsg('u1', 'first prompt'),
          trace('t1', 'PerplexityTool', 'alpha query'),
          userMsg('u2', 'second prompt'),
          trace('t2', 'GenieTool', 'beta query'),
        ]}
      />,
    );
    // Two separate containers, both idle → both labelled "Run activity"
    const containers = screen.getAllByText('Run activity');
    expect(containers).toHaveLength(2);

    // First section sits between prompt 1 and prompt 2; second sits after prompt 2
    const [first, second] = containers;
    const p2 = screen.getByText('second prompt');
    expect(first.compareDocumentPosition(p2) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();
    expect(p2.compareDocumentPosition(second) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();

    // Each section's stream contains only its OWN step (friendly phase heading).
    fireEvent.click(screen.getAllByLabelText('Expand run activity')[0]);
    expect(screen.getByText('Searching the web')).toBeInTheDocument();
    expect(screen.queryByText('Querying your data')).toBeNull();
    // The first toggle now reads "Collapse…" — the remaining Expand is section 2
    fireEvent.click(screen.getAllByLabelText('Expand run activity')[0]);
    expect(screen.getByText('Querying your data')).toBeInTheDocument();
  });

  it('only the LATEST prompt section is live while running (Stop only there)', () => {
    const onStop = vi.fn();
    render(
      <ChatContainer
        {...baseProps}
        messages={[
          userMsg('u1', 'first prompt'),
          trace('t1', 'Crew planned'),
          userMsg('u2', 'second prompt'),
          trace('t2', 'Crew planned'),
        ]}
        isExecuting
        onStopExecution={onStop}
      />,
    );
    // First container is finished; the second is live and shows its latest step
    expect(screen.getByText('Run activity')).toBeInTheDocument();
    expect(screen.getByText('Crew planned')).toBeInTheDocument();
    expect(screen.getAllByLabelText('Stop execution')).toHaveLength(1);
  });

  it('a follow-up prompt with no traces yet shows a fresh Thinking section at the end', () => {
    render(
      <ChatContainer
        {...baseProps}
        messages={[
          userMsg('u1', 'first prompt'),
          trace('t1', 'Crew planned'),
          userMsg('u2', 'second prompt'),
        ]}
        isGenerating
      />,
    );
    // Previous run keeps its own finished section; the new one thinks fresh
    expect(screen.getByText('Run activity')).toBeInTheDocument();
    expect(screen.getByText(/Thinking/)).toBeInTheDocument();
  });
});

describe('ChatContainer — run activity in the chat ("chat" placement)', () => {
  const steps = [
    { id: '1', label: 'PerplexityTool', sublabel: 'Switzerland news today', detail: 'Title: SwissInfo\nUrl: https://www.swissinfo.ch/eng/x' },
  ];

  it('shows the Working bar with a "Show in panel" icon that opens the run in the pane, expandable to the thinking stream', () => {
    const onShow = vi.fn();
    render(
      <ChatContainer
        {...baseProps}
        messages={[{ ...msg('u', 'q'), role: 'user' }]}
        isExecuting
        runSteps={steps}
        onShowRunInPane={onShow}
      />,
    );
    expect(screen.getByText('Working…')).toBeInTheDocument();
    fireEvent.click(screen.getByLabelText('Show in panel'));
    // The latest run is left unpinned ([] steps) so the pane tracks the live feed;
    // it has no deliverable yet (no answer message), so the deliverable is undefined.
    expect(onShow).toHaveBeenCalledWith(undefined, []);
    // Expanding the bar reveals the thinking stream (friendly phase heading).
    fireEvent.click(screen.getByLabelText('Expand run activity'));
    expect(screen.getByText('Searching the web')).toBeInTheDocument();
  });

  it('opens a HISTORICAL run\'s plain-text answer in the pane as a text deliverable, pinned to its own steps', () => {
    const onShow = vi.fn();
    render(
      <ChatContainer
        {...baseProps}
        messages={[
          // Run 1 (historical): user prompt, a tool-result trace, the text answer.
          { ...msg('u1', 'q1'), role: 'user' },
          { ...msg('t', ''), resultType: 'trace', resultData: { kind: 'tool_result', label: 'PerplexityTool', sublabel: 'q1', detail: 'x' } },
          { ...msg('a', 'Here is your answer.') },
          // Run 2 makes run 1 historical (so it isn't the latest segment).
          { ...msg('u2', 'q2'), role: 'user' },
        ]}
        onShowRunInPane={onShow}
      />,
    );
    fireEvent.click(screen.getByLabelText('Show in panel'));
    expect(onShow).toHaveBeenCalledWith(
      { type: 'text', data: 'Here is your answer.' },
      expect.arrayContaining([expect.objectContaining({ label: 'PerplexityTool' })]),
    );
  });
});
