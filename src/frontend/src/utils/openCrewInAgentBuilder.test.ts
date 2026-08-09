import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { openCrewInAgentBuilder } from './openCrewInAgentBuilder';
import { CrewService } from '../api/workflow/CrewService';
import { useCrewExecutionStore } from '../store/crewExecution';

vi.mock('../api/workflow/CrewService', () => ({
  CrewService: { getCrew: vi.fn() },
}));

describe('openCrewInAgentBuilder', () => {
  let events: CustomEvent[];
  const listener = (e: Event) => events.push(e as CustomEvent);

  beforeEach(() => {
    events = [];
    vi.clearAllMocks();
    window.addEventListener('catalogLoadCrew', listener);
    useCrewExecutionStore.getState().setShowError(false);
  });

  afterEach(() => window.removeEventListener('catalogLoadCrew', listener));

  it('hands the crew to the same event the chat /load command uses', async () => {
    vi.mocked(CrewService.getCrew).mockResolvedValue({
      id: 42,
      name: 'Gather News',
      nodes: [{ id: 'agent-1', type: 'agentNode' }],
      edges: [{ id: 'e1', source: 'agent-1', target: 'task-1' }],
    } as never);

    await openCrewInAgentBuilder('42');

    expect(CrewService.getCrew).toHaveBeenCalledWith('42');
    expect(events).toHaveLength(1);
    expect(events[0].detail).toEqual({
      nodes: [{ id: 'agent-1', type: 'agentNode' }],
      edges: [{ id: 'e1', source: 'agent-1', target: 'task-1' }],
      name: 'Gather News',
      id: '42',
    });
  });

  it('accepts a numeric crew id', async () => {
    vi.mocked(CrewService.getCrew).mockResolvedValue({
      id: 7,
      name: 'C',
      nodes: [{ id: 'a' }],
      edges: [],
    } as never);

    await openCrewInAgentBuilder(7);

    expect(CrewService.getCrew).toHaveBeenCalledWith('7');
  });

  it('reports a crew that cannot be fetched instead of doing nothing', async () => {
    vi.mocked(CrewService.getCrew).mockRejectedValue(new Error('404 not found'));

    await openCrewInAgentBuilder('42', 'Gather News');

    expect(events).toHaveLength(0);
    const state = useCrewExecutionStore.getState();
    expect(state.showError).toBe(true);
    expect(state.errorMessage).toContain('Gather News');
    expect(state.errorMessage).toContain('404 not found');
  });

  it('reports an empty crew rather than opening a blank canvas', async () => {
    vi.mocked(CrewService.getCrew).mockResolvedValue({
      id: 42,
      name: 'Empty',
      nodes: [],
      edges: [],
    } as never);

    await openCrewInAgentBuilder('42', 'Empty');

    expect(events).toHaveLength(0);
    expect(useCrewExecutionStore.getState().showError).toBe(true);
  });
});
