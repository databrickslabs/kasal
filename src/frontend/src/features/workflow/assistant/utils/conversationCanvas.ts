import { useTabManagerStore } from '../../../../store/tabManager';
import { useWorkflowStore } from '../../../../store/workflow';

/** Open a conversation's canvas within the current workspace, preserving the current draft canvas. */
export function openConversationCanvas(sessionId?: string): string {
  const tabs = useTabManagerStore.getState();
  const current = tabs.getActiveTab();
  if (current) {
    const workflow = useWorkflowStore.getState();
    tabs.updateTabNodes(current.id, workflow.nodes);
    tabs.updateTabEdges(current.id, workflow.edges);
  }
  const existing = sessionId ? tabs.getTabsForCurrentGroup().find(tab => tab.chatSessionId === sessionId) : undefined;
  if (existing) {
    tabs.setActiveTab(existing.id);
    return existing.id;
  }
  const id = tabs.createTab(undefined, 'crew');
  if (sessionId) {
    // Historical conversations without a local canvas begin on a fresh tab.
    useTabManagerStore.setState(state => ({ tabs: state.tabs.map(tab => tab.id === id ? { ...tab, chatSessionId: sessionId } : tab) }));
  }
  return id;
}
