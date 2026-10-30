/**
 * A message typed while the previous one is still being sent stays in the
 * composer.
 *
 * The dispatcher handles one message at a time. A second send during a
 * dispatch used to be cleared from the composer and then dropped by the
 * dispatcher's guard, so the text was gone with nothing on screen. Now the
 * composer keeps it, says why, and sends it once the first has gone.
 */
import React from 'react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { fireEvent, render, screen } from '@testing-library/react';
import ChatInput from './ChatInput';
import ChatContainer from './ChatContainer';
import type { ModelConfigResponse } from '../../types/dispatcher';
import type { ChatMessage } from '../../types/chat';

vi.mock('../../api/knowledge', () => ({
  uploadKnowledgeFile: vi.fn(),
  forgetKnowledgeFile: vi.fn().mockResolvedValue(true),
}));
vi.mock('../../hooks/useRunTimeline', () => ({
  useRunTimeline: () => ({ processed: null, loading: false }),
}));
vi.mock('./ChatMessage', () => ({
  default: (props: { message: ChatMessage }) => <div>{props.message.content}</div>,
  formatDurationMs: () => null,
  GenerationCompleteCard: () => null,
}));

const MODELS: ModelConfigResponse[] = [{ key: 'm1', name: 'Model One' } as ModelConfigResponse];
const props = { models: MODELS, selectedModel: 'm1', onModelChange: vi.fn() };

// The landing composer rotates its placeholder, so find it by role.
const composer = () => screen.getByRole('textbox') as HTMLTextAreaElement;
const typeAndEnter = (text: string) => {
  fireEvent.change(composer(), { target: { value: text } });
  fireEvent.keyDown(composer(), { key: 'Enter' });
};

beforeEach(() => {
  Element.prototype.scrollIntoView = vi.fn();
});

describe('ChatInput while the previous message is still sending', () => {
  it('keeps the new message, says why, and sends it once the first has gone', () => {
    const onSend = vi.fn();
    const { rerender } = render(<ChatInput {...props} onSend={onSend} sending />);

    typeAndEnter('and the second question');

    expect(onSend).not.toHaveBeenCalled();
    expect(composer()).toHaveValue('and the second question');
    expect(screen.getByRole('status')).toHaveTextContent(/still sending your last message/i);
    expect(screen.getByRole('button', { name: 'Send message' })).toBeDisabled();
    // The user can keep writing: the composer is not locked.
    expect(composer()).not.toHaveAttribute('readonly');

    rerender(<ChatInput {...props} onSend={onSend} />);
    expect(screen.queryByRole('status')).toBeNull();
    fireEvent.keyDown(composer(), { key: 'Enter' });

    expect(onSend).toHaveBeenCalledWith('and the second question');
    expect(composer()).toHaveValue('');
  });
});

describe('ChatInput: stop waiting for a message that does not come back', () => {
  it('offers Cancel while a message is sending, even with nothing typed', () => {
    const onCancelSending = vi.fn();
    render(<ChatInput {...props} onSend={vi.fn()} sending onCancelSending={onCancelSending} />);

    expect(screen.getByRole('status')).toHaveTextContent(/sending your last message/i);
    fireEvent.click(screen.getByRole('button', { name: 'Cancel' }));
    expect(onCancelSending).toHaveBeenCalledOnce();
  });
});

describe('ChatContainer forwards the sending state to its composer', () => {
  const message = { id: 'm1', role: 'user', content: 'first question', timestamp: new Date() } as ChatMessage;

  it.each([
    ['the new-chat composer', [] as ChatMessage[]],
    ['the conversation composer', [message]],
  ])('%s keeps a message sent mid-dispatch', (_label, messages) => {
    const onSend = vi.fn();
    render(<ChatContainer {...props} messages={messages} onSend={onSend} isLoading={false} isSending />);

    typeAndEnter('second question');

    expect(onSend).not.toHaveBeenCalled();
    expect(composer()).toHaveValue('second question');
  });

  it('keeps what was typed during the first message of a new chat when the composer is replaced', () => {
    // The first message shows only once its conversation exists; then the
    // landing composer is swapped for the conversation one.
    const { rerender } = render(<ChatContainer {...props} messages={[]} onSend={vi.fn()} isLoading={false} isSending />);
    fireEvent.change(composer(), { target: { value: 'typed while it was sending' } });

    rerender(<ChatContainer {...props} messages={[message]} onSend={vi.fn()} isLoading={false} isSending />);

    expect(composer()).toHaveValue('typed while it was sending');
  });

  it('starts the conversation composer empty after a send that posts its message at once', () => {
    // A local command posts the user message in the same render that clears
    // the composer: the shared draft must already be empty when it is read.
    function Chat() {
      const [messages, setMessages] = React.useState<ChatMessage[]>([]);
      return (
        <ChatContainer {...props} messages={messages} isLoading={false}
          onSend={(text) => setMessages([{ ...message, content: text }])} />
      );
    }
    render(<Chat />);

    typeAndEnter('/jobs');

    expect(screen.getByText('/jobs')).toBeInTheDocument();
    expect(composer()).toHaveValue('');
  });

  it('shows text the workspace put back into the draft', () => {
    const draftRef = { current: '' };
    const { rerender } = render(
      <ChatContainer {...props} messages={[message]} onSend={vi.fn()} isLoading={false} draftRef={draftRef} draftVersion={0} />);

    draftRef.current = 'first question';
    rerender(<ChatContainer {...props} messages={[message]} onSend={vi.fn()} isLoading={false} draftRef={draftRef} draftVersion={1} />);

    expect(composer()).toHaveValue('first question');
  });
});
