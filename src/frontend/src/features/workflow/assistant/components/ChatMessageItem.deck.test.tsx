import React from 'react';
import { act, fireEvent, render, screen, waitFor, within } from '@testing-library/react';
import { beforeEach, expect, it, vi } from 'vitest';
import { ChatMessageItem } from './ChatMessageItem';
import { useChatMessagesStore } from '../store/chatMessagesStore';
import { useSessionStore } from '../../../../app/sessions/sessionStore';
import { useUILayoutStore } from '../../../../store/uiLayout';
import { DeckService } from '../../../../api/chat/DeckService';
import { ChatHistoryServiceEnhanced } from '../../../../api/chat/ChatHistoryServiceEnhanced';

vi.mock('../../../../api/chat/DeckService', () => ({ DeckService: { refineSlide: vi.fn() } }));
vi.mock('../../../../api/chat/ChatHistoryServiceEnhanced', () => ({
  ChatHistoryServiceEnhanced: { updateMessageContent: vi.fn() },
}));
vi.mock('../../../chat/utils/deckExport', () => ({ downloadDeckPdf: vi.fn(), downloadDeckPptx: vi.fn() }));
const slide = (title: string) => `<section class="slide"><h1>${title}</h1></section>`;
const deck = slide('Original') + '\n' + slide('Second');
const owner = 'builder-session';
function Transcript() {
  const message = useChatMessagesStore(state => state.messagesBySession[owner][0]);
  return <ChatMessageItem message={message} sessionId={owner} groupId="team-a" model="builder-model" />;
}
beforeEach(() => {
  vi.resetAllMocks();
  vi.mocked(ChatHistoryServiceEnhanced.updateMessageContent).mockResolvedValue(undefined);
  vi.mocked(DeckService.refineSlide).mockResolvedValue({ section: slide('Refined') });
  useChatMessagesStore.setState({ currentSessionId: owner, messagesBySession: { [owner]: [{
    id: 'same-id', backendId: 'backend-message', type: 'result', content: deck, timestamp: new Date(),
  }] } });
  useSessionStore.setState({ messages: [{ id: 'same-id', role: 'assistant', content: 'Unrelated chat', timestamp: new Date() }] });
});
it.each(['crew', 'flow'] as const)('edits, persists, reopens and undoes a deck in %s mode', async mode => {
  useUILayoutStore.setState({ appMode: mode });
  const view = render(<Transcript />);
  fireEvent.click(screen.getByTitle('Edit deck'));
  fireEvent.change(screen.getByLabelText('Slide instruction'), { target: { value: 'Improve title' } });
  fireEvent.click(screen.getByText('Apply'));
  await waitFor(() => expect(screen.getByText('2 slides · 1 edit')).toBeInTheDocument());
  expect(DeckService.refineSlide).toHaveBeenCalledWith(expect.objectContaining({ model: 'builder-model' }));
  expect(ChatHistoryServiceEnhanced.updateMessageContent).toHaveBeenCalledWith('backend-message', expect.stringContaining('Refined'), 'team-a');
  expect(useSessionStore.getState().messages[0].content).toBe('Unrelated chat');
  fireEvent.click(screen.getByTitle('Undo: Refined slide 1'));
  await waitFor(() => expect(useChatMessagesStore.getState().messagesBySession[owner][0].content).not.toContain('Refined'));
  expect(ChatHistoryServiceEnhanced.updateMessageContent).toHaveBeenCalledTimes(2);
  fireEvent.click(screen.getByLabelText('Duplicate slide 1'));
  await waitFor(() => expect(screen.getByText('3 slides · 1 edit')).toBeInTheDocument());
  const stored = vi.mocked(ChatHistoryServiceEnhanced.updateMessageContent).mock.calls.at(-1)![1];
  view.unmount();
  // Reload the persisted message under its server ID, as useChatSession does.
  useChatMessagesStore.setState({ messagesBySession: { [owner]: [{ id: 'backend-message', backendId: 'backend-message', type: 'result', content: stored, timestamp: new Date() }] } });
  render(<Transcript />);
  fireEvent.click(screen.getByTitle('Edit deck'));
  expect(within(screen.getByRole('dialog', { name: 'Deck studio' })).getAllByRole('listitem')).toHaveLength(3);
});
it('shows a failed save without changing the deck and allows retry', async () => {
  vi.mocked(ChatHistoryServiceEnhanced.updateMessageContent).mockRejectedValueOnce(new Error('offline'));
  render(<Transcript />);
  fireEvent.click(screen.getByTitle('Edit deck'));
  fireEvent.click(screen.getByLabelText('Duplicate slide 1'));
  await waitFor(() => expect(screen.getByRole('alert')).toHaveTextContent('could not be saved'));
  expect(within(screen.getByRole('dialog', { name: 'Deck studio' })).getAllByRole('listitem')).toHaveLength(2);
  expect(useChatMessagesStore.getState().messagesBySession[owner][0].content).toBe(deck);
  fireEvent.click(screen.getByLabelText('Duplicate slide 1'));
  await waitFor(() => expect(within(screen.getByRole('dialog', { name: 'Deck studio' })).getAllByRole('listitem')).toHaveLength(3));
});
it('prevents overlapping structural saves', async () => {
  let finish!: () => void;
  vi.mocked(ChatHistoryServiceEnhanced.updateMessageContent).mockImplementationOnce(() => new Promise(resolve => { finish = resolve; }));
  render(<Transcript />);
  fireEvent.click(screen.getByTitle('Edit deck'));
  fireEvent.click(screen.getByLabelText('Duplicate slide 1'));
  fireEvent.click(screen.getByLabelText('Delete slide 2'));
  expect(ChatHistoryServiceEnhanced.updateMessageContent).toHaveBeenCalledTimes(1);
  await act(async () => finish());
  expect(within(screen.getByRole('dialog', { name: 'Deck studio' })).getAllByRole('listitem')).toHaveLength(3);
});

it('saves a refinement to its original owner after navigation', async () => {
  let finish!: (value: { section: string }) => void;
  vi.mocked(DeckService.refineSlide).mockImplementationOnce(() => new Promise(resolve => { finish = resolve; }));
  const view = render(<Transcript />);
  fireEvent.click(screen.getByTitle('Edit deck'));
  fireEvent.change(screen.getByLabelText('Slide instruction'), { target: { value: 'Improve title' } });
  fireEvent.click(screen.getByText('Apply'));
  view.unmount();
  useChatMessagesStore.getState().setCurrentSession('another-builder');
  localStorage.setItem('selectedGroupId', 'team-b');
  await act(async () => finish({ section: slide('Finished after navigation') }));
  expect(ChatHistoryServiceEnhanced.updateMessageContent).toHaveBeenCalledWith('backend-message', expect.stringContaining('Finished after navigation'), 'team-a');
  expect(useChatMessagesStore.getState().messagesBySession[owner][0].content).toContain('Finished after navigation');
  expect(useChatMessagesStore.getState().currentSessionId).toBe('another-builder');
  expect(useSessionStore.getState().messages[0].content).toBe('Unrelated chat');
});
