import { act, renderHook } from '@testing-library/react';
import { beforeEach, expect, it, vi } from 'vitest';
import { useSessionRunIds } from './useSessionRunIds';
import { useTabManagerStore } from '../../store/tabManager';
import { useUILayoutStore } from '../../store/uiLayout';
import { useSessionStore } from '../../features/chat/store/sessionStore';
import { useChatMessagesStore } from '../../features/workflow/assistant/store/chatMessagesStore';
vi.mock('../../features/chat/persistence/sessionApi', () => ({}));
beforeEach(() => {
  localStorage.setItem('selectedGroupId', 'a');
  useTabManagerStore.setState({ tabs: [], activeTabId: null });
  useChatMessagesStore.setState({ messagesBySession: {} });
  useSessionStore.setState({ currentSessionId: 'chat', sessions: [], messages: [] });
});
it('keeps builder runs isolated, deduplicates persisted results, and preserves links after canvas edits', () => {
  const first = useTabManagerStore.getState().createTab('Crew', 'crew');
  const second = useTabManagerStore.getState().createTab('Flow', 'flow');
  useTabManagerStore.setState(state => ({ tabs: state.tabs.map(tab => ({ ...tab, executionJobIds: tab.id === first ? ['run-a'] : ['run-b'] })), activeTabId: first }));
  const sid = useTabManagerStore.getState().getActiveTab()!.chatSessionId!;
  useChatMessagesStore.getState().addMessage(sid, { id: 'result', type: 'result', jobId: 'run-a', content: 'Done', timestamp: new Date() });
  useUILayoutStore.setState({ appMode: 'crew' });
  const { result } = renderHook(() => useSessionRunIds());
  expect(result.current.ids).toEqual(['run-a']);
  act(() => useTabManagerStore.getState().updateTabNodes(first, [{ id: 'node', data: {}, position: { x: 0, y: 0 } }]));
  expect(result.current.ids).toEqual(['run-a']);
  act(() => useTabManagerStore.getState().setActiveTab(second));
  expect(result.current.ids).toEqual(['run-b']);
});
it('uses Chat execution links and does not include the inactive builder', () => {
  useUILayoutStore.setState({ appMode: 'chat' });
  useSessionStore.setState({ messages: [{ id: 'm', role: 'assistant', content: 'Done', timestamp: new Date(), executionId: 'chat-run' }] });
  const { result } = renderHook(() => useSessionRunIds());
  expect(result.current.ids).toEqual(['chat-run']);
});
