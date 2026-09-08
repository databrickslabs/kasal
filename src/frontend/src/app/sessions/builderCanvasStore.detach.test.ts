/**
 * Tab/crew detachment (chat generation overwrite bug).
 *
 * When chat generates a NEW crew into a tab that was previously associated
 * with a saved crew, the tab must be detached from that crew — otherwise the
 * next Save silently overwrites the old crew record (new content, old name).
 */
import { describe, it, expect, beforeEach, vi } from 'vitest';
import { useBuilderCanvasStore } from './builderCanvasStore';
import { applyCrewDispatchResult } from '../../features/workflow/assistant/utils/applyCrewDispatchResult';

describe('builderCanvasStore - clearCanvasCrewInfo', () => {
  beforeEach(() => {
    useBuilderCanvasStore.setState({ canvases: [], activeCanvasId: null });
  });

  it('detaches the tab from its saved crew so the next save creates a new one', () => {
    const store = useBuilderCanvasStore.getState();
    const tabId = store.createCanvas('Send Email');
    store.updateCanvasCrewInfo(tabId, 'crew-old-id', 'Send Email');

    let tab = useBuilderCanvasStore.getState().getCanvas(tabId);
    expect(tab?.savedCrewId).toBe('crew-old-id');
    expect(tab?.savedCrewName).toBe('Send Email');

    useBuilderCanvasStore.getState().clearCanvasCrewInfo(tabId);

    tab = useBuilderCanvasStore.getState().getCanvas(tabId);
    expect(tab?.savedCrewId).toBeUndefined();
    expect(tab?.savedCrewName).toBeUndefined();
    expect(tab?.lastSavedAt).toBeUndefined();
    // Tab itself survives — only the crew association is dropped
    expect(tab?.name).toBe('Send Email');
  });
});


describe('generated plans detach an existing catalog association', () => {
  it.each(['legacy', 'recovered stream'] as const)('%s clears the old crew before applying the new plan', async (path) => {
    useBuilderCanvasStore.setState({ canvases: [], activeCanvasId: null });
    const store = useBuilderCanvasStore.getState();
    const id = store.createCanvas('Existing crew');
    store.updateCanvasCrewInfo(id, 'saved-crew', 'Existing crew');
    const crew = {
      agents: [{ id: 'agent-new', name: 'Researcher', role: 'Research', goal: 'Find news', backstory: 'Reporter', tools: [] }],
      tasks: [{ id: 'task-new', name: 'Find news', description: 'Research current news', expected_output: 'Report', agent_id: 'agent-new', tools: [] }],
    };
    const apply = vi.fn(() => {
      expect(useBuilderCanvasStore.getState().getCanvas(id)?.savedCrewId).toBeUndefined();
    });
    await applyCrewDispatchResult({
      dispatcher: { intent: 'generate_crew', confidence: 1, extracted_info: {} },
      service_called: null,
      generation_result: path === 'legacy' ? crew : { type: 'streaming', generation_id: 'generation-1', completed: true, generated_crew: crew },
    }, {
      generationCompletedRef: { current: false },
      detachTabFromSavedCrew: () => useBuilderCanvasStore.getState().clearCanvasCrewInfo(id),
      handleCrewGenerated: apply,
      handleAgentGenerated: vi.fn().mockResolvedValue(undefined),
      handleTaskGenerated: vi.fn().mockResolvedValue(undefined),
      setMessages: vi.fn(),
      saveMessageToBackend: vi.fn().mockResolvedValue(undefined),
      setGenerationId: vi.fn(),
      inputRef: { current: null },
      nodes: [],
    });
    expect(apply).toHaveBeenCalledWith(crew);
    expect(useBuilderCanvasStore.getState().getCanvas(id)?.name).toBe('Existing crew');
  });
});
