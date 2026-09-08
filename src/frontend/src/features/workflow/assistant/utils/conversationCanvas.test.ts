import { beforeEach, describe, expect, it } from 'vitest';
import { openConversationCanvas } from './conversationCanvas';
import { useBuilderCanvasStore } from '../../../../app/sessions/builderCanvasStore';
import { useWorkflowStore } from '../../../../store/workflow';

beforeEach(() => {
  localStorage.setItem('selectedGroupId', 'workspace-1');
  useBuilderCanvasStore.setState({ canvases: [], activeCanvasId: null });
  useWorkflowStore.setState({ nodes: [], edges: [] });
});
describe('Conversations and canvas tabs', () => {
  it('starts a fresh canvas and conversation while preserving the old canvas', () => {
    const first = useBuilderCanvasStore.getState().createCanvas('Original', 'crew');
    const originalSession = useBuilderCanvasStore.getState().getCanvas(first)?.chatSessionId;
    const nodes = [{ id: 'agent-original', type: 'agentNode', position: { x: 50, y: 70 }, data: { label: 'Researcher' } }];
    useWorkflowStore.setState({ nodes });
    const next = openConversationCanvas();
    expect(next).not.toBe(first);
    expect(useBuilderCanvasStore.getState().getCanvas(first)?.nodes).toEqual(nodes);
    expect(useBuilderCanvasStore.getState().getCanvas(first)?.chatSessionId).toBe(originalSession);
    expect(useBuilderCanvasStore.getState().getCanvas(next)?.nodes).toEqual([]);
    expect(useBuilderCanvasStore.getState().getCanvas(next)?.chatSessionId).not.toBe(originalSession);
  });
  it('returns to an existing conversation canvas without creating another tab', () => {
    const first = useBuilderCanvasStore.getState().createCanvas('Original', 'crew');
    const session = useBuilderCanvasStore.getState().getCanvas(first)?.chatSessionId;
    useBuilderCanvasStore.getState().createCanvas('Second', 'crew');
    expect(openConversationCanvas(session)).toBe(first);
    expect(useBuilderCanvasStore.getState().activeCanvasId).toBe(first);
    expect(useBuilderCanvasStore.getState().canvases).toHaveLength(2);
  });
  it('opens older history on a fresh canvas and never selects a different workspace tab', () => {
    localStorage.setItem('selectedGroupId', 'workspace-other');
    const foreign = useBuilderCanvasStore.getState().createCanvas('Other workspace', 'crew');
    const session = useBuilderCanvasStore.getState().getCanvas(foreign)?.chatSessionId;
    localStorage.setItem('selectedGroupId', 'workspace-1');
    useBuilderCanvasStore.getState().createCanvas('Current workspace', 'crew');
    const next = openConversationCanvas(session);
    expect(next).not.toBe(foreign);
    expect(useBuilderCanvasStore.getState().getCanvas(next)?.group_id).toBe('workspace-1');
    expect(useBuilderCanvasStore.getState().getCanvas(next)?.chatSessionId).toBe(session);
    expect(useBuilderCanvasStore.getState().getCanvas(next)?.nodes).toEqual([]);
  });
});
