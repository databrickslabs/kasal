import { beforeEach, expect, it, vi } from 'vitest';
import { replaceBuilderDeck, saveBuilderDeck } from './builderDeckEditing';
import { useChatMessagesStore } from '../store/chatMessagesStore';
import { ChatHistoryServiceEnhanced } from '../../../../api/chat/ChatHistoryServiceEnhanced';

vi.mock('../../../../api/chat/ChatHistoryServiceEnhanced', () => ({
  ChatHistoryServiceEnhanced: { updateMessageContent: vi.fn() },
}));
const deck = '<section class="slide"><h1>Original</h1></section>';
const edited = '<section class="slide"><h1>Edited</h1></section>';
const other = '<section class="slide"><h1>Other deck</h1></section>';
beforeEach(() => {
  vi.resetAllMocks();
  const message = { id: 'm', backendId: 'stored', type: 'result' as const, content: deck, timestamp: new Date() };
  useChatMessagesStore.setState({ currentSessionId: 'owner', messagesBySession: {
    owner: [message], other: [{ ...message, backendId: 'other-stored', content: other }],
  } });
});

it.each([deck, `\`\`\`\n${deck}\n\`\`\``, `\`\`\`html\n${deck}\n\`\`\``])(
  'edits raw and fenced builder decks', content => {
    expect(replaceBuilderDeck(content, edited, deck)).toContain(edited);
    expect(replaceBuilderDeck(content, edited, deck)).not.toContain(deck);
  },
);
it('preserves prose, another deck, nested envelopes and UI siblings', () => {
  const text = `Introduction\n\`\`\`html\n${other}\n\`\`\`\nSeparate presentation\n\`\`\`html\n${deck}\n\`\`\`\nEnd`;
  const content = JSON.stringify({ result: JSON.stringify({ text, a2ui: { title: 'Keep me' } }), count: 2 });
  const result = JSON.parse(replaceBuilderDeck(content, edited, deck));
  const answer = JSON.parse(result.result);
  expect(result.count).toBe(2);
  expect(answer.a2ui).toEqual({ title: 'Keep me' });
  expect(answer.text).toContain(other);
  expect(answer.text).toContain(edited);
  expect(answer.text).toContain('Introduction');
  expect(answer.text).toContain('End');
});
it('refuses to overwrite a deck that has changed since it was opened', () => {
  expect(() => replaceBuilderDeck(other, edited, deck)).toThrow('This deck changed');
});
it('persists to the original session and teamspace before publishing the edit', async () => {
  let finish!: () => void;
  vi.mocked(ChatHistoryServiceEnhanced.updateMessageContent).mockImplementationOnce(() => new Promise(resolve => { finish = resolve; }));
  const saving = saveBuilderDeck('owner', 'team-a', 'm', edited, deck);
  useChatMessagesStore.getState().setCurrentSession('other');
  localStorage.setItem('selectedGroupId', 'team-b');
  expect(useChatMessagesStore.getState().messagesBySession.owner[0].content).toBe(deck);
  finish();
  await saving;
  expect(ChatHistoryServiceEnhanced.updateMessageContent).toHaveBeenCalledWith('stored', expect.stringContaining(edited), 'team-a');
  expect(useChatMessagesStore.getState().messagesBySession.owner[0].content).toContain(edited);
  expect(useChatMessagesStore.getState().messagesBySession.other[0].content).toBe(other);
});
it('retains the previous content when saving fails', async () => {
  vi.mocked(ChatHistoryServiceEnhanced.updateMessageContent).mockRejectedValueOnce(new Error('offline'));
  await expect(saveBuilderDeck('owner', 'team-a', 'm', edited, deck)).rejects.toThrow('offline');
  expect(useChatMessagesStore.getState().messagesBySession.owner[0].content).toBe(deck);
});
