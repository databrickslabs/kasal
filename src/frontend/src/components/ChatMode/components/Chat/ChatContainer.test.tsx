import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import ChatContainer from './ChatContainer';
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

  it('disables the input while loading', () => {
    render(<ChatContainer {...baseProps} messages={[]} isLoading />);
    expect(screen.getByTestId('chat-input')).toBeDisabled();
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
      resultData: { label, kind: 'tool_call', durationMs: 100, ...extra },
    } as unknown as ChatMessageType);

  it('shows a live "Generating crew…" indicator while generating (no traces, no Stop, nothing to expand)', () => {
    render(<ChatContainer {...baseProps} messages={[msg('u', 'q')]} isGenerating />);
    expect(screen.getByText('Generating crew…')).toBeInTheDocument();
    expect(screen.queryByLabelText('Stop execution')).toBeNull();
    expect(screen.queryByTestId('trace-group')).toBeNull();
  });

  it('while executing with traces: "Working…", Stop on the right, expandable timeline (dots + bold names + text)', () => {
    const onStop = vi.fn();
    render(
      <ChatContainer
        {...baseProps}
        messages={[
          msg('u', 'q'),
          traceMsg('t1', 'PerplexityTool', { sublabel: 'find stock photos' }),
          traceMsg('t2', 'Memory', { durationMs: undefined, detail: 'recalled 3 items' }),
          traceMsg('t3', 'ScrapeTool', { durationMs: undefined }),
        ]}
        isExecuting
        onStopExecution={onStop}
      />,
    );
    expect(screen.getByText('Working…')).toBeInTheDocument();
    fireEvent.click(screen.getByLabelText('Stop execution'));
    expect(onStop).toHaveBeenCalledTimes(1);
    // collapsed by default → no steps visible yet
    expect(screen.queryByText('PerplexityTool')).toBeNull();
    // expand → chronological steps, bold names + their summary + duration
    fireEvent.click(screen.getByLabelText('Expand run activity'));
    expect(screen.getByText('PerplexityTool')).toBeInTheDocument();
    expect(screen.getByText('find stock photos')).toBeInTheDocument(); // summary (sublabel), always shown
    expect(screen.getByText('Memory')).toBeInTheDocument();
    // the retrieved context (detail) stays hidden until that step is expanded
    expect(screen.queryByText('recalled 3 items')).toBeNull();
    fireEvent.click(screen.getByLabelText('Show context for Memory'));
    expect(screen.getByText('recalled 3 items')).toBeInTheDocument();
    fireEvent.click(screen.getByLabelText('Hide context for Memory'));
    expect(screen.queryByText('recalled 3 items')).toBeNull();
    expect(screen.getByText('ScrapeTool')).toBeInTheDocument(); // no summary, no duration, no context
    expect(screen.getByText('100ms')).toBeInTheDocument(); // first step's duration
    // toggling closed again (Collapse label)
    fireEvent.click(screen.getByLabelText('Collapse run activity'));
    expect(screen.queryByText('PerplexityTool')).toBeNull();
  });

  it('shows no "Show context" toggle when a step\'s detail just duplicates its summary', () => {
    render(
      <ChatContainer
        {...baseProps}
        messages={[msg('u'), traceMsg('e1', 'EchoTool', { sublabel: 'same text', detail: 'same text' })]}
        isExecuting
      />,
    );
    fireEvent.click(screen.getByLabelText('Expand run activity'));
    expect(screen.getByText('same text')).toBeInTheDocument(); // summary
    expect(screen.queryByLabelText('Show context for EchoTool')).toBeNull(); // detail === summary → no toggle
  });

  it('collapses consecutive same-tool traces into one chronological run of steps', () => {
    render(
      <ChatContainer
        {...baseProps}
        messages={[msg('u'), traceMsg('p1', 'PerplexityTool', { sublabel: 'q1' }), traceMsg('p2', 'PerplexityTool', { sublabel: 'q2' })]}
        isExecuting
      />,
    );
    fireEvent.click(screen.getByLabelText('Expand run activity'));
    // two consecutive Perplexity calls → one group → two timeline steps
    expect(screen.getByText('q1')).toBeInTheDocument();
    expect(screen.getByText('q2')).toBeInTheDocument();
  });

  it('after the run (not executing) shows "Run activity" and no Stop', () => {
    render(<ChatContainer {...baseProps} messages={[msg('u', 'q'), traceMsg('t1', 'PerplexityTool')]} />);
    expect(screen.getByText('Run activity')).toBeInTheDocument();
    expect(screen.queryByLabelText('Stop execution')).toBeNull();
  });

  it('executing without a stop handler shows no Stop button', () => {
    render(<ChatContainer {...baseProps} messages={[msg('u', 'q'), traceMsg('t1', 'PerplexityTool')]} isExecuting />);
    expect(screen.getByText('Working…')).toBeInTheDocument();
    expect(screen.queryByLabelText('Stop execution')).toBeNull();
  });

  it('shows a transient "Stopping…" state when Stop is pressed, then clears it when the run ends', () => {
    const onStop = vi.fn();
    const stillRunning = [msg('u', 'q'), traceMsg('t1', 'PerplexityTool')];
    const { rerender } = render(
      <ChatContainer {...baseProps} messages={stillRunning} isExecuting onStopExecution={onStop} />,
    );
    expect(screen.getByText('Working…')).toBeInTheDocument();
    fireEvent.click(screen.getByLabelText('Stop execution'));
    expect(onStop).toHaveBeenCalledTimes(1);
    // immediate feedback: "Stopping…" + the control becomes a disabled spinner
    expect(screen.getByText('Stopping…')).toBeInTheDocument();
    expect(screen.queryByText('Working…')).toBeNull();
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

  it('mounts the generated crew card inside the container (not as a separate bubble) and labels it "Crew ready"', () => {
    render(<ChatContainer {...baseProps} messages={[msg('u', 'q'), genMsg('gen1')]} />);
    // crew card lives in the run-activity container...
    expect(screen.getByTestId('crew-card-gen1')).toBeInTheDocument();
    // ...and is NOT also rendered as its own chat bubble
    expect(screen.queryByTestId('msg-gen1')).toBeNull();
    // generated but not yet running, no traces → "Crew ready", no Stop, nothing to expand
    expect(screen.getByText('Crew ready')).toBeInTheDocument();
    expect(screen.queryByLabelText('Stop execution')).toBeNull();
    expect(screen.queryByLabelText('Expand run activity')).toBeNull();
    // the user's prompt still renders normally
    expect(screen.getByTestId('msg-u')).toBeInTheDocument();
  });

  it('keeps the crew card visible inside the container while executing, with the timeline collapsible below it', () => {
    const onStop = vi.fn();
    render(
      <ChatContainer
        {...baseProps}
        messages={[msg('u', 'q'), genMsg('gen1'), traceMsg('t1', 'PerplexityTool', { sublabel: 'find stock photos' })]}
        isExecuting
        onStopExecution={onStop}
      />,
    );
    expect(screen.getByText('Working…')).toBeInTheDocument();
    expect(screen.getByTestId('crew-card-gen1')).toBeInTheDocument();
    expect(screen.getByLabelText('Stop execution')).toBeInTheDocument();
    // timeline collapsed by default — but the crew card stays visible
    expect(screen.queryByText('PerplexityTool')).toBeNull();
    fireEvent.click(screen.getByLabelText('Expand run activity'));
    expect(screen.getByText('PerplexityTool')).toBeInTheDocument();
    expect(screen.getByText('find stock photos')).toBeInTheDocument();
    expect(screen.getByTestId('crew-card-gen1')).toBeInTheDocument(); // still there alongside the timeline
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

  it('keeps the crew card ABOVE the result after the run completes (does not drop below it)', () => {
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
    const card = screen.getByTestId('crew-card-gen1');
    const resultBubble = screen.getByTestId('msg-res');
    // The result follows the crew card in document order (card is above it).
    expect(
      card.compareDocumentPosition(resultBubble) & Node.DOCUMENT_POSITION_FOLLOWING,
    ).toBeTruthy();
  });
});
