/**
 * Saving a chat result back into the workspace: the crew itself, and the
 * answer as a catalog entry.
 *
 * Both read only from the stores, so this hook takes nothing.
 */
import { useCallback } from 'react';
import { saveGeneratedCrew, synthesizeCrewFromConversation, CrewNameConflictError } from '../api/crews';
import { useSessionStore } from '../store/sessionStore';
import { useAppStore } from '../store/appStore';
import { useExecutionStore } from '../store/executionStore';
import { GenerationCompleteData } from '../types/dispatcher';

export function useChatLibraryActions() {

  const addMessage = useSessionStore((s) => s.addMessage);
  const updateMessage = useSessionStore((s) => s.updateMessage);
  const refreshLibrary = useAppStore((s) => s.loadCatalog);
  const selectedModel = useAppStore((s) => s.selectedModel);

  // --- Save a generated crew's plan to the catalog ---
  // Used by the bookmark on each crew card (it owns its own saved-state UI), so
  // this just performs the save and resolves to the created crew.
  const handleSaveCrew = useCallback(
    (data: GenerationCompleteData, opts?: { overwrite?: boolean; spaceId?: string }) => {
      // Capture the chat's current memory choice so the saved crew matches what
      // the user sees here (no-memory mode → saved crew has memory disabled).
      // opts carries overwrite + the picked Genie space from the crew card.
      // Answer mode → persist reasoning so a Research/Deep crew reloads with the
      // same behaviour.
      const mode = useExecutionStore.getState().chatModeType;
      return saveGeneratedCrew(data, undefined, {
        ...opts,
        memoryEnabled: useExecutionStore.getState().memoryEnabled,
        // Persist the MCP servers selected for the run so the saved crew keeps them.
        mcpServers: useExecutionStore.getState().selectedMcpServers,
        // Persist the Agent Bricks endpoint picked in the "+" so the saved crew
        // reloads with the agent assigned and runs against it.
        agentBricksEndpoints: useExecutionStore.getState().selectedAgentBricksEndpoints,
        reasoning: mode === 'research' || mode === 'deep',
      }).then((r) => {
        // Surface the freshly saved crew in the rail library.
        void refreshLibrary();
        return r;
      });
    },
    [refreshLibrary],
  );

  // --- Answer mode: distill a reusable crew from the conversation and SAVE it ---
  // ChatMode 'chat' turns run a generic single assistant, so bookmarking that
  // saves nothing specific. Instead we ask the backend to read the conversation
  // and synthesize an agent + task, then save it to the catalog in one shot and
  // show what was saved (read-only) — no second confirmation click.
  const handleSaveAnswerToCatalog = useCallback(
    async (sessionId?: string) => {
      const sid = sessionId || useSessionStore.getState().currentSessionId;
      if (!sid) {
        addMessage('assistant', 'There is no active chat session to build a crew from.');
        return;
      }
      const thinkingId = addMessage(
        'assistant',
        'Distilling a reusable crew from this conversation and saving it…',
        { isStreaming: true },
      );
      try {
        const data = await synthesizeCrewFromConversation(sid, selectedModel || undefined);
        if (data.agents.length === 0 && data.tasks.length === 0) {
          updateMessage(thinkingId, {
            content:
              'I could not distill a reusable crew from this conversation yet — try again after a few more messages.',
            isStreaming: false,
          });
          return;
        }
        // Save automatically (no second click). On a name clash, overwrite the
        // same-named crew rather than dead-ending — this one-shot save re-derives
        // the same distilled crew, so overwrite is the intended outcome.
        const saved = await handleSaveCrew(data).catch((e) => {
          if (e instanceof CrewNameConflictError) return handleSaveCrew(data, { overwrite: true });
          throw e;
        });
        updateMessage(thinkingId, {
          content: `✓ Saved **${saved.name}** to the catalog — find it in the **Crews** library on the left.`,
          isStreaming: false,
          // Read-only display of exactly what was saved (no save bookmark, no Run).
          // Carry the saved crew id/name so the card can offer "Open in Agent/Flow
          // Builder" without re-saving.
          resultType: 'saved_crew',
          resultData: { ...data, savedCrewId: saved.id, savedName: saved.name },
        });
      } catch (error) {
        const errMsg = error instanceof Error ? error.message : 'Failed to save a crew';
        updateMessage(thinkingId, {
          content: `I couldn't save a crew from this conversation: ${errMsg}`,
          isStreaming: false,
        });
      }
    },
    [addMessage, updateMessage, selectedModel, handleSaveCrew],
  );

  return {
    handleSaveCrew,
    handleSaveAnswerToCatalog,
  };
}
