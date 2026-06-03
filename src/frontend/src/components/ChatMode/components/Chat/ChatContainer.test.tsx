import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import ChatContainer from './ChatContainer';
import type { ChatMessage as ChatMessageType } from '../../types/chat';

// Stub the heavy children so we can isolate ChatContainer logic.
vi.mock('./ChatInput', () => ({
  default: (props: { onSend: (m: string) => void; disabled?: boolean }) => (
    <button data-testid="chat-input" disabled={props.disabled} onClick={() => props.onSend('typed')}>
      input
    </button>
  ),
}));

vi.mock('./ChatMessage', () => ({
  default: (props: { message: ChatMessageType; onCommand: (c: string) => void }) => (
    <button data-testid={`msg-${props.message.id}`} onClick={() => props.onCommand('cmd')}>
      {props.message.content}
    </button>
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

  it('renders the executing banner with crew name, agents, tasks and a Stop button', () => {
    const onStopExecution = vi.fn();
    render(
      <ChatContainer
        {...baseProps}
        messages={[]}
        isExecuting
        executionContext={{
          crewName: 'News Crew',
          agents: [{ name: 'Researcher' }, { name: 'Writer' }],
          tasks: [{ name: 'Gather' }],
        }}
        onStopExecution={onStopExecution}
      />,
    );
    expect(screen.getByText('Running crew...')).toBeInTheDocument();
    expect(screen.getByText('News Crew')).toBeInTheDocument();
    expect(screen.getByText(/Researcher, Writer/)).toBeInTheDocument();
    expect(screen.getByText(/Gather/)).toBeInTheDocument();
    fireEvent.click(screen.getByTitle('Stop execution'));
    expect(onStopExecution).toHaveBeenCalled();
  });

  it('renders the executing banner without optional bits (no crewName/agents/tasks, no stop)', () => {
    render(
      <ChatContainer
        {...baseProps}
        messages={[]}
        isExecuting
        executionContext={{ crewName: '', agents: [], tasks: [] }}
      />,
    );
    expect(screen.getByText('Running crew...')).toBeInTheDocument();
    expect(screen.queryByTitle('Stop execution')).not.toBeInTheDocument();
  });

  it('does not render the executing banner when executionContext is null', () => {
    render(<ChatContainer {...baseProps} messages={[msg('a')]} isExecuting executionContext={null} />);
    expect(screen.queryByText('Running crew...')).not.toBeInTheDocument();
  });

  it('renders the generating banner when generating and not executing', () => {
    // Banner lives in the conversation layout, so include a message (otherwise
    // the empty state renders instead).
    render(<ChatContainer {...baseProps} messages={[msg('a')]} isGenerating />);
    expect(screen.getByText('Generating crew...')).toBeInTheDocument();
  });

  it('does not render the generating banner while executing', () => {
    render(
      <ChatContainer
        {...baseProps}
        messages={[]}
        isExecuting
        isGenerating
        executionContext={{ crewName: 'C', agents: [], tasks: [] }}
      />,
    );
    expect(screen.queryByText('Generating crew...')).not.toBeInTheDocument();
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
