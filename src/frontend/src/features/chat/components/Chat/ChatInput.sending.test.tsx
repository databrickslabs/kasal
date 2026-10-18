/**
 * A message typed while the previous one is still being sent stays in the
 * composer.
 *
 * The dispatcher handles one message at a time. A second send during a
 * dispatch used to be cleared from the composer and then dropped by the
 * dispatcher's guard, so the text was gone with nothing on screen. Now the
 * composer keeps it, says why, and sends it once the first has gone.
 */
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
});
