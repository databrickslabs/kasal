import { beforeEach, describe, expect, it } from 'vitest';
import { openConversationCanvas } from './conversationCanvas';
import { useTabManagerStore } from '../../../../store/tabManager';
import { useWorkflowStore } from '../../../../store/workflow';

beforeEach(() => {
  localStorage.setItem('selectedGroupId', 'workspace-1');
  useTabManagerStore.setState({ tabs: [], activeTabId: null });
  useWorkflowStore.setState({ nodes: [], edges: [] });
});
describe('Conversations and canvas tabs', () => {
  it('starts a fresh canvas and conversation while preserving the old canvas', () => {
    const first = useTabManagerStore.getState().createTab('Original', 'crew');
    const originalSession = useTabManagerStore.getState().getTab(first)?.chatSessionId;
    const nodes = [{ id: 'agent-original', type: 'agentNode', position: { x: 50, y: 70 }, data: { label: 'Researcher' } }];
    useWorkflowStore.setState({ nodes });
    const next = openConversationCanvas();
    expect(next).not.toBe(first);
    expect(useTabManagerStore.getState().getTab(first)?.nodes).toEqual(nodes);
    expect(useTabManagerStore.getState().getTab(first)?.chatSessionId).toBe(originalSession);
    expect(useTabManagerStore.getState().getTab(next)?.nodes).toEqual([]);
    expect(useTabManagerStore.getState().getTab(next)?.chatSessionId).not.toBe(originalSession);
  });
  it('returns to an existing conversation canvas without creating another tab', () => {
    const first = useTabManagerStore.getState().createTab('Original', 'crew');
    const session = useTabManagerStore.getState().getTab(first)?.chatSessionId;
    useTabManagerStore.getState().createTab('Second', 'crew');
    expect(openConversationCanvas(session)).toBe(first);
    expect(useTabManagerStore.getState().activeTabId).toBe(first);
    expect(useTabManagerStore.getState().tabs).toHaveLength(2);
  });
  it('opens older history on a fresh canvas and never selects a different workspace tab', () => {
    localStorage.setItem('selectedGroupId', 'workspace-other');
    const foreign = useTabManagerStore.getState().createTab('Other workspace', 'crew');
    const session = useTabManagerStore.getState().getTab(foreign)?.chatSessionId;
    localStorage.setItem('selectedGroupId', 'workspace-1');
    useTabManagerStore.getState().createTab('Current workspace', 'crew');
    const next = openConversationCanvas(session);
    expect(next).not.toBe(foreign);
    expect(useTabManagerStore.getState().getTab(next)?.group_id).toBe('workspace-1');
    expect(useTabManagerStore.getState().getTab(next)?.chatSessionId).toBe(session);
    expect(useTabManagerStore.getState().getTab(next)?.nodes).toEqual([]);
  });
});
