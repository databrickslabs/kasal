import { useBuilderCanvasStore } from '../../../../app/sessions/builderCanvasStore';
import { useWorkflowStore } from '../../../../store/workflow';

/** Open a conversation's canvas within the current workspace, preserving the current draft canvas. */
export function openConversationCanvas(sessionId?: string, viewMode: 'crew' | 'flow' = 'crew'): string {
  const tabs = useBuilderCanvasStore.getState();
  const current = tabs.getActiveCanvas();
  if (current) {
    const workflow = useWorkflowStore.getState();
    tabs.updateCanvasNodes(current.id, workflow.nodes);
    tabs.updateCanvasEdges(current.id, workflow.edges);
  }
  const existing = sessionId ? tabs.getCanvasesForCurrentGroup().find(tab => tab.chatSessionId === sessionId) : undefined;
  if (existing) {
    tabs.setActiveCanvas(existing.id);
    return existing.id;
  }
  const id = tabs.createCanvas(undefined, viewMode);
  if (sessionId) {
    // Historical conversations without a local canvas begin on a fresh tab.
    useBuilderCanvasStore.setState(state => ({ canvases: state.canvases.map(tab => tab.id === id ? { ...tab, chatSessionId: sessionId } : tab) }));
  }
  return id;
}
