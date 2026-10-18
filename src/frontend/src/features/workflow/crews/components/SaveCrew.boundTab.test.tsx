/**
 * Saving a canvas that is bound to a catalog crew never creates a second crew.
 *
 * A crew opened in place holds that crew's own agent and task rows. "Save
 * crew" always POSTed a new crew built from those rows, so the two crews then
 * shared them and a task or agent edit silently changed both. Under the bound
 * name the save updates the bound crew; under another name it is refused.
 */
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { act, render, screen, waitFor } from '@testing-library/react';
import SaveCrew from './SaveCrew';
import { CrewService } from '../../../../api/workflow/CrewService';
import { useBuilderCanvasStore } from '../../../../app/sessions/builderCanvasStore';

vi.mock('../../../../api/workflow/CrewService', () => ({
  CrewService: { saveCrew: vi.fn(), updateCrew: vi.fn(), getCrews: vi.fn(), getCrew: vi.fn() },
}));
vi.mock('../../../chat/store/appStore', () => ({
  useAppStore: { getState: () => ({ loadCatalog: vi.fn(async () => undefined) }) },
}));

const position = { x: 0, y: 0 };
const nodes = [
  { id: 'agent-a1', type: 'agentNode', position, data: { label: 'Researcher', agentId: 'a1' } },
  { id: 'task-t1', type: 'taskNode', position, data: { label: 'research', taskId: 't1' } },
];
const edges = [{ id: 'e1', source: 'agent-a1', target: 'task-t1' }];

let tabId: string;
beforeEach(() => {
  vi.clearAllMocks();
  const store = useBuilderCanvasStore.getState();
  tabId = store.createCanvas('Research crew', 'crew');
  store.updateCanvasNodes(tabId, nodes);
  store.updateCanvasEdges(tabId, edges);
  store.setActiveCanvas(tabId);
  store.updateCanvasCrewInfo(tabId, 'crew-1', 'Research crew');
  vi.mocked(CrewService.updateCrew).mockResolvedValue({ id: 'crew-1', name: 'Research crew' } as never);
  vi.mocked(CrewService.saveCrew).mockResolvedValue({ id: 'crew-2', name: 'Other' } as never);
  vi.mocked(CrewService.getCrew).mockResolvedValue({ id: 'crew-1', name: 'Research crew' } as never);
});

function saveAs(name: string) {
  render(<SaveCrew nodes={nodes} edges={edges} trigger={<button>Save</button>} />);
  act(() => {
    window.dispatchEvent(new CustomEvent('openSaveCrewDialog', { detail: { suggestedName: name } }));
  });
}

describe('Saving a canvas bound to a catalog crew', () => {
  it('refuses a new name: no new crew, and the user is told why', async () => {
    const shown = vi.fn();
    window.addEventListener('saveError', shown);

    saveAs('Research crew v2');

    await waitFor(() => expect(shown).toHaveBeenCalledOnce());
    const { message } = (shown.mock.calls[0][0] as CustomEvent).detail;
    expect(message).toContain('is open from the catalog');
    expect(message).toContain('Update catalog');
    expect(message).toContain('Open as copy');
    expect(CrewService.saveCrew).not.toHaveBeenCalled();
    expect(CrewService.updateCrew).not.toHaveBeenCalled();
    expect(useBuilderCanvasStore.getState().getCanvas(tabId)?.savedCrewId).toBe('crew-1');
    window.removeEventListener('saveError', shown);
  });

  it('a refused /save crew leaves no stale name or error for the next dialog', async () => {
    const shown = vi.fn();
    window.addEventListener('saveError', shown);
    saveAs('Research crew v2');
    await waitFor(() => expect(shown).toHaveBeenCalledOnce());
    window.removeEventListener('saveError', shown);

    act(() => {
      window.dispatchEvent(new CustomEvent('openSaveCrewDialog'));
    });

    await waitFor(() => expect(screen.getByRole('dialog')).toBeTruthy());
    expect(screen.queryByText(/is open from the catalog/)).toBeNull();
    expect(screen.queryByDisplayValue('Research crew v2')).toBeNull();
  });

  it('under the bound name it updates the bound crew instead of creating one', async () => {
    saveAs('Research crew');

    await waitFor(() => expect(CrewService.updateCrew).toHaveBeenCalledOnce());
    expect(CrewService.updateCrew).toHaveBeenCalledWith('crew-1', expect.objectContaining({ name: 'Research crew' }));
    expect(CrewService.saveCrew).not.toHaveBeenCalled();
  });

  it('an unbound canvas still saves as a new crew', async () => {
    useBuilderCanvasStore.getState().clearCanvasCrewInfo(tabId);

    saveAs('Fresh crew');

    await waitFor(() => expect(CrewService.saveCrew).toHaveBeenCalledOnce());
    expect(CrewService.saveCrew).toHaveBeenCalledWith(expect.objectContaining({ name: 'Fresh crew' }));
    expect(CrewService.updateCrew).not.toHaveBeenCalled();
  });

  it('when the bound crew has been deleted, a new name creates a new crew', async () => {
    vi.mocked(CrewService.getCrew).mockRejectedValue({ response: { status: 404 } });
    vi.mocked(CrewService.saveCrew).mockResolvedValue({ id: 'crew-2', name: 'Research crew v2' } as never);

    saveAs('Research crew v2');

    await waitFor(() => expect(CrewService.saveCrew).toHaveBeenCalledOnce());
    expect(CrewService.getCrew).toHaveBeenCalledWith('crew-1');
    expect(CrewService.saveCrew).toHaveBeenCalledWith(expect.objectContaining({ name: 'Research crew v2' }));
    expect(CrewService.updateCrew).not.toHaveBeenCalled();
    // The canvas now belongs to the crew it was saved as.
    await waitFor(() => expect(useBuilderCanvasStore.getState().getCanvas(tabId)?.savedCrewId).toBe('crew-2'));
  });

  it('still refuses a new name when the bound crew cannot be checked', async () => {
    const shown = vi.fn();
    window.addEventListener('saveError', shown);
    vi.mocked(CrewService.getCrew).mockRejectedValue({ response: { status: 500 } });

    saveAs('Research crew v2');

    await waitFor(() => expect(shown).toHaveBeenCalledOnce());
    expect((shown.mock.calls[0][0] as CustomEvent).detail.message).toContain('is open from the catalog');
    expect(CrewService.saveCrew).not.toHaveBeenCalled();
    window.removeEventListener('saveError', shown);
  });
});
