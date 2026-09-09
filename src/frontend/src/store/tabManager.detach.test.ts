/**
 * Tab/crew detachment (chat generation overwrite bug).
 *
 * When chat generates a NEW crew into a tab that was previously associated
 * with a saved crew, the tab must be detached from that crew — otherwise the
 * next Save silently overwrites the old crew record (new content, old name).
 */
import { describe, it, expect, beforeEach, vi } from 'vitest';
import { useTabManagerStore } from './tabManager';
import { applyCrewDispatchResult } from '../features/workflow/assistant/utils/applyCrewDispatchResult';

describe('tabManager - clearTabCrewInfo', () => {
  beforeEach(() => {
    useTabManagerStore.setState({ tabs: [], activeTabId: null });
  });

  it('detaches the tab from its saved crew so the next save creates a new one', () => {
    const store = useTabManagerStore.getState();
    const tabId = store.createTab('Send Email');
    store.updateTabCrewInfo(tabId, 'crew-old-id', 'Send Email');

    let tab = useTabManagerStore.getState().getTab(tabId);
    expect(tab?.savedCrewId).toBe('crew-old-id');
    expect(tab?.savedCrewName).toBe('Send Email');

    useTabManagerStore.getState().clearTabCrewInfo(tabId);

    tab = useTabManagerStore.getState().getTab(tabId);
    expect(tab?.savedCrewId).toBeUndefined();
    expect(tab?.savedCrewName).toBeUndefined();
    expect(tab?.lastSavedAt).toBeUndefined();
    // Tab itself survives — only the crew association is dropped
    expect(tab?.name).toBe('Send Email');
  });
});


describe('generated plans detach an existing catalog association', () => {
  it.each(['legacy', 'recovered stream'] as const)('%s clears the old crew before applying the new plan', async (path) => {
    useTabManagerStore.setState({ tabs: [], activeTabId: null });
    const store = useTabManagerStore.getState();
    const id = store.createTab('Existing crew');
    store.updateTabCrewInfo(id, 'saved-crew', 'Existing crew');
    const crew = {
      agents: [{ id: 'agent-new', name: 'Researcher', role: 'Research', goal: 'Find news', backstory: 'Reporter', tools: [] }],
      tasks: [{ id: 'task-new', name: 'Find news', description: 'Research current news', expected_output: 'Report', agent_id: 'agent-new', tools: [] }],
    };
    const apply = vi.fn(() => {
      expect(useTabManagerStore.getState().getTab(id)?.savedCrewId).toBeUndefined();
    });
    await applyCrewDispatchResult({
      dispatcher: { intent: 'generate_crew', confidence: 1, extracted_info: {} },
      service_called: null,
      generation_result: path === 'legacy' ? crew : { type: 'streaming', generation_id: 'generation-1', completed: true, generated_crew: crew },
    }, {
      generationCompletedRef: { current: false },
      detachTabFromSavedCrew: () => useTabManagerStore.getState().clearTabCrewInfo(id),
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
    expect(useTabManagerStore.getState().getTab(id)?.name).toBe('Existing crew');
  });
});
