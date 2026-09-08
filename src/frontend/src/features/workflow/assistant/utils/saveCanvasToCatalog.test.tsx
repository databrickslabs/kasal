import { act, render, screen, waitFor } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import SaveCrew from '../../crews/components/SaveCrew';
import SaveFlow from '../../flows/components/SaveFlow';
import { useBuilderCanvasStore } from '../../../../app/sessions/builderCanvasStore';
import { saveCanvasToCatalog } from './saveCanvasToCatalog';

const api = vi.hoisted(() => ({ saveCrew: vi.fn(), updateCrew: vi.fn(), saveFlow: vi.fn(), updateFlow: vi.fn() }));
vi.mock('../../../../api/workflow/CrewService', () => ({ CrewService: api }));
vi.mock('../../../../api/workflow/FlowService', () => ({ FlowService: api }));
vi.mock('../../../chat/store/appStore', () => ({ useAppStore: { getState: () => ({ loadCatalog: vi.fn() }) } }));
beforeEach(() => {
  vi.clearAllMocks();
  useBuilderCanvasStore.setState({ canvases: [], activeCanvasId: null });
  Object.values(api).forEach(fn => fn.mockResolvedValue({ id: 'saved-id', name: 'Swiss news' }));
});
describe('Save from Conversation through the existing sidebar handlers', () => {
  it.each([false, true])('saves a new canvas directly without opening a naming dialog (flow=%s)', async flow => {
    const tabId = useBuilderCanvasStore.getState().createCanvas('Swiss news', flow ? 'flow' : 'crew');
    const nodes = [{ id: 'node-1', type: flow ? 'crewNode' : 'agentNode', position: { x: 0, y: 0 }, data: { crewId: 'crew-1', label: 'Researcher' } }];
    const tabs = useBuilderCanvasStore.getState();
    if (flow) tabs.updateCanvasFlowNodes(tabId, nodes); else tabs.updateCanvasNodes(tabId, nodes);
    const Component = flow ? SaveFlow : SaveCrew;
    render(<Component nodes={nodes} edges={[]} trigger={<button>Sidebar save</button>} />);
    let saving!: Promise<{ name: string }>;
    act(() => { saving = saveCanvasToCatalog(flow, 'Swiss news'); });
    await waitFor(() => expect(flow ? api.saveFlow : api.saveCrew).toHaveBeenCalledWith(expect.objectContaining({ name: 'Swiss news', nodes: expect.arrayContaining([expect.objectContaining({ id: 'node-1' })]) })));
    await expect(saving).resolves.toEqual({ name: 'Swiss news' });
    expect(screen.queryByRole('dialog')).not.toBeInTheDocument();
    expect(flow ? useBuilderCanvasStore.getState().getCanvas(tabId)?.savedFlowId : useBuilderCanvasStore.getState().getCanvas(tabId)?.savedCrewId).toBe('saved-id');
  });
  it.each([false, true])('reuses the existing catalog identity on save (flow=%s)', async flow => {
    const tabs = useBuilderCanvasStore.getState();
    const tabId = tabs.createCanvas('Swiss news', flow ? 'flow' : 'crew');
    const nodes = [{ id: 'node-1', type: flow ? 'crewNode' : 'agentNode', position: { x: 0, y: 0 }, data: { crewId: 'crew-1' } }];
    if (flow) { tabs.updateCanvasFlowNodes(tabId, nodes); tabs.updateCanvasFlowInfo(tabId, 'existing', 'Swiss news'); }
    else { tabs.updateCanvasNodes(tabId, nodes); tabs.updateCanvasCrewInfo(tabId, 'existing', 'Swiss news'); }
    const Component = flow ? SaveFlow : SaveCrew;
    render(<Component nodes={nodes} edges={[]} trigger={<button>Sidebar save</button>} />);
    await act(async () => { await saveCanvasToCatalog(flow, 'Run name'); });
    expect(flow ? api.updateFlow : api.updateCrew).toHaveBeenCalledWith('existing', expect.objectContaining({ name: 'Swiss news' }));
    expect(flow ? api.saveFlow : api.saveCrew).not.toHaveBeenCalled();
    expect(screen.queryByRole('dialog')).not.toBeInTheDocument();
  });
});
