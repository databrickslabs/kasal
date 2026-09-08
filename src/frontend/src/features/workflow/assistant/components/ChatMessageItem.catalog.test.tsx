import { act, fireEvent, render, screen, waitFor } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { usePermissionStore } from '../../../../store/permissions';
import { ChatMessageItem } from './ChatMessageItem';
import { useBuilderCanvasStore } from '../../../../app/sessions/builderCanvasStore';
import type { ChatMessage } from '../types';

const save = vi.hoisted(() => vi.fn(async () => ({ name: 'Dutch news' })));
vi.mock('../utils/saveCanvasToCatalog', () => ({ saveCanvasToCatalog: save }));
vi.mock('../../../chat/store/appStore', () => ({ useAppStore: { getState: () => ({ loadCatalog: vi.fn() }) } }));
beforeEach(() => { vi.clearAllMocks(); useBuilderCanvasStore.setState({ canvases: [], activeCanvasId: null }); usePermissionStore.setState({ allowAgentBuilder: true, allowFlowBuilder: true, userRole: 'admin' }); });
describe('Generated plan catalog action', () => {
  it.each(['crew', 'flow'])('saves a generated %s plan immediately using its name', async kind => {
    const message: ChatMessage = { id: 'plan', type: 'assistant', content: 'Your plan is ready.', timestamp: new Date(), metadata: { catalogKind: kind, catalogName: 'Dutch news' } };
    render(<ChatMessageItem message={message} appearance="assistant-panel" />);
    fireEvent.click(screen.getByRole('button', { name: 'Save to catalog', exact: true }));
    await waitFor(() => expect(save).toHaveBeenCalledWith(kind === 'flow', 'Dutch news'));
    expect(await screen.findByRole('button', { name: 'Saved to catalog' })).toBeDisabled();
    expect(screen.queryByRole('dialog')).not.toBeInTheDocument();
  });
  it.each(['agentNode', 'taskNode', 'crewNode'])('allows saving again after editing a %s', async type => {
    const flow = type === 'crewNode';
    const store = useBuilderCanvasStore.getState();
    const id = store.createCanvas('News', flow ? 'flow' : 'crew');
    const nodes = [{ id: 'node', type, position: { x: 0, y: 0 }, data: { name: 'Before' } }];
    const update = flow ? store.updateCanvasFlowNodes : store.updateCanvasNodes;
    update(id, nodes);
    render(<ChatMessageItem message={{ id: 'plan', type: 'assistant', content: 'Ready', timestamp: new Date(),
      metadata: { catalogKind: flow ? 'flow' : 'crew', catalogName: 'News' } }} appearance="assistant-panel" />);
    fireEvent.click(screen.getByRole('button', { name: 'Save to catalog', exact: true }));
    expect(await screen.findByRole('button', { name: 'Saved to catalog' })).toBeDisabled();
    act(() => update(id, [{ ...nodes[0], selected: true, position: { x: 100, y: 100 } }]));
    expect(screen.getByRole('button', { name: 'Saved to catalog' })).toBeDisabled();
    act(() => update(id, [{ ...nodes[0], data: { name: 'Edited instructions' } }]));
    fireEvent.click(screen.getByRole('button', { name: 'Save to catalog', exact: true }));
    await waitFor(() => expect(save).toHaveBeenCalledTimes(2));
  });
  it('offers saving for previously generated crew plans without new metadata', () => {
    render(<ChatMessageItem message={{ id: 'old-plan', type: 'assistant', content: 'Crew Plan\n✓ Crew generated successfully', timestamp: new Date() }} appearance="assistant-panel" />);
    expect(screen.getByRole('button', { name: 'Save to catalog', exact: true })).toBeVisible();
  });
  it('does not offer saving on an unfinished plan', () => {
    render(<ChatMessageItem message={{ id: 'pending', type: 'assistant', content: 'Crew Plan', timestamp: new Date(), isIntermediate: true }} appearance="assistant-panel" />);
    expect(screen.queryByRole('button', { name: 'Save to catalog', exact: true })).not.toBeInTheDocument();
  });
  it('shows one save action after the full plan, not on the generation acknowledgement', () => {
    render(<>
      <ChatMessageItem message={{ id: 'intro', type: 'assistant', content: "I've created a crew with:\nClick Play to run the crew.", intent: 'generate_crew', result: { agents: [], tasks: [] }, timestamp: new Date() }} appearance="assistant-panel" />
      <ChatMessageItem message={{ id: 'plan', type: 'assistant', content: 'Crew Plan\n✓ Crew generated successfully', timestamp: new Date() }} appearance="assistant-panel" />
    </>);
    expect(screen.getAllByRole('button', { name: 'Save to catalog', exact: true })).toHaveLength(1);
  });
});
