import { beforeEach, describe, expect, it, vi } from 'vitest';
import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import CrewFlowSelectionDialog from './CrewFlowDialog';
import { FlowService } from '../../../../../api/workflow/FlowService';
import { useFlowConfigStore } from '../../../../../store/flowConfig';
import { usePublicationStore } from '../../../../../store/publication';
import { useBuilderCanvasStore } from '../../../../../app/sessions/builderCanvasStore';
import type { FlowResponse } from '../../../../../types/workflow/flow';

const permissions = vi.hoisted(() => ({ canEdit: true, canDelete: true }));
vi.mock('../../../../../hooks/usePermissions', () => ({ usePermissions: () => permissions }));
vi.mock('../../../../../hooks/global/useMLflowEnabled', () => ({ useMLflowEnabled: () => false }));
vi.mock('../../../../../api/workflow/FlowService', () => ({ FlowService: { getFlows: vi.fn(), getFlow: vi.fn() } }));
// Test the real Catalog action and loading path without requiring a workspace host.
vi.mock('./CatalogSurface', () => ({ default: ({ children }: { children: React.ReactNode }) => <div>{children}</div> }));
vi.mock('./PublishButton', () => ({ default: () => null }));
vi.mock('../CrewOptimizeDialog', () => ({ default: () => null }));

const config = {
  id: 'flow-config', name: 'Conditional briefing', type: 'default', listeners: [], actions: [], startingPoints: [],
  routers: [{ id: 'saved-router', name: 'Decide', routes: [{ name: 'green', condition: 'score > 100' }] }],
  state: { conversational: true, channels: { findings: { type: 'string', reducer: 'append' } } },
  outcomes: { Research: 'A sourced briefing' }, persistence: { enabled: true },
};
const saved = {
  id: 'saved-flow', name: 'Conditional briefing', crew_id: 'saved-crew', created_at: '2026-09-08T10:00:00Z', updated_at: '2026-09-08T10:00:00Z',
  nodes: [{ id: 'research', type: 'crewNode', position: { x: 10, y: 20 }, data: { crewId: 'saved-crew', crewName: 'Research', allTasks: [] } }],
  edges: [], flowConfig: config,
} as unknown as FlowResponse;
const updateInfo = vi.fn();
beforeEach(() => {
  vi.clearAllMocks();
  permissions.canEdit = true;
  useFlowConfigStore.setState({ kasalFlowEnabled: true });
  usePublicationStore.setState({ refresh: vi.fn(async () => undefined) });
  useBuilderCanvasStore.setState({ activeCanvasId: 'flow-canvas', updateCanvasFlowInfo: updateInfo });
  vi.mocked(FlowService.getFlows).mockResolvedValue([saved]);
  vi.mocked(FlowService.getFlow).mockResolvedValue(saved);
});
function setup() {
  const onClose = vi.fn();
  const onFlowSelect = vi.fn();
  render(<CrewFlowSelectionDialog open embedded initialTab={3} showOnlyTab={3} onClose={onClose} onCrewSelect={vi.fn()} onFlowSelect={onFlowSelect} />);
  return { onClose, onFlowSelect };
}
describe('Catalog flow editing', () => {
  it.each(['flowConfig', 'flow_config'] as const)('opens the saved flow on the canvas with its complete %s and identity', async key => {
    vi.mocked(FlowService.getFlow).mockResolvedValue({ ...saved, flowConfig: undefined, [key]: config } as unknown as FlowResponse);
    const { onClose, onFlowSelect } = setup();
    fireEvent.click(await screen.findByRole('button', { name: 'Edit Conditional briefing on canvas' }));
    await waitFor(() => expect(onFlowSelect).toHaveBeenCalledWith(saved.nodes, saved.edges, config));
    expect(FlowService.getFlow).toHaveBeenCalledExactlyOnceWith('saved-flow');
    expect(updateInfo).toHaveBeenCalledWith('flow-canvas', 'saved-flow', saved.name);
    expect(onClose).toHaveBeenCalledOnce();
    expect(screen.queryByRole('dialog')).toBeNull();
    expect(screen.queryByRole('textbox', { name: 'Flow Name' })).toBeNull();
  });
  it('supports the keyboard edit action without also activating the surrounding card', async () => {
    const { onFlowSelect } = setup();
    const button = await screen.findByRole('button', { name: 'Edit Conditional briefing on canvas' });
    button.focus();
    await userEvent.keyboard('{Enter}');
    await waitFor(() => expect(onFlowSelect).toHaveBeenCalledOnce());
    expect(FlowService.getFlow).toHaveBeenCalledOnce();
  });
  it('keeps Catalog open and reports a load failure without changing the canvas', async () => {
    vi.mocked(FlowService.getFlow).mockRejectedValue(new Error('Flow unavailable'));
    const { onClose, onFlowSelect } = setup();
    fireEvent.click(await screen.findByRole('button', { name: 'Edit Conditional briefing on canvas' }));
    expect(await screen.findByText('Flow unavailable')).toBeVisible();
    expect(onClose).not.toHaveBeenCalled();
    expect(onFlowSelect).not.toHaveBeenCalled();
    expect(updateInfo).not.toHaveBeenCalled();
  });
  it('keeps editing hidden from users without edit permission', async () => {
    permissions.canEdit = false;
    setup();
    await screen.findByRole('heading', { name: saved.name });
    expect(screen.queryByRole('button', { name: 'Edit Conditional briefing on canvas' })).toBeNull();
  });
});
