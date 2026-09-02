/**
 * What the composer does with what the user typed.
 *
 * `handleLocalCommand` is the slash-command layer that runs BEFORE the
 * dispatcher sees a message (/clear, /refine, /memory, …); anything it does not
 * claim falls through to the dispatcher as a normal turn.
 */
import React, { useCallback } from 'react';
import { SkillService } from '../../../api/tools/SkillService';
import {
  buildTranscript,
  draftFailedStep,
  draftMessage,
  draftedStep,
  draftingStep,
  parseSkillCommand,
} from '../utils/skillCommand';
import { stopExecution, listExecutions } from '../api/executions';
import { latestDeck, parseSlideEdit } from '../utils/slideRefine';
import { splitSlides } from '../utils/htmlDeck';
import { saveGeneratedCrew, CrewNameConflictError } from '../api/crews';
import { GenerationCompleteData } from '../types/dispatcher';
import { useSessionStore } from '../store/sessionStore';
import { useExecutionStore } from '../store/executionStore';
import { useAppStore } from '../store/appStore';

interface UseChatCommandsArgs {
  dispatcher: ReturnType<typeof import('../hooks/useDispatcher').useDispatcher>;
  executionStream: { stopStream: () => void };
  handleRefine: (instruction: string) => void;
  lastGeneratedRef: React.MutableRefObject<GenerationCompleteData | null>;
  lastUserPromptRef: React.MutableRefObject<string>;
  setPendingRun: (v: { sessionId: string | null; label: string; run: () => void } | null) => void;
}

export function useChatCommands({ dispatcher, executionStream, handleRefine, lastGeneratedRef, lastUserPromptRef, setPendingRun }: UseChatCommandsArgs) {

  const addMessage = useSessionStore((s) => s.addMessage);
  const clearMessages = useSessionStore((s) => s.clearMessages);
  const currentSessionId = useSessionStore((s) => s.currentSessionId);
  const refreshLibrary = useAppStore((s) => s.loadCatalog);
  const selectedModel = useAppStore((s) => s.selectedModel);

  // --- Local command handling ---
  const handleLocalCommand = useCallback(
    async (message: string): Promise<boolean> => {
      const lower = message.toLowerCase().trim();
      const execStore = useExecutionStore.getState();

      if (lower === '/clear') {
        clearMessages();
        execStore.resetForSession();
        return true;
      }

      if (lower === '/jobs' || lower === '/list jobs' || lower === '/list executions') {
        addMessage('user', message);
        execStore.setIsLoading(true);
        try {
          const executions = await listExecutions(20);
          if (executions.length === 0) {
            addMessage('assistant', 'No recent executions found.');
          } else {
            let msg = `**Recent Executions** (${executions.length})\n\n`;
            msg += '| # | Job ID | Status | Created |\n';
            msg += '|---|--------|--------|---------|\n';
            executions.forEach((exec, i) => {
              const shortId = exec.job_id?.slice(0, 8) || exec.id?.slice(0, 8) || '\u2014';
              const status = exec.status || 'unknown';
              const created = exec.created_at
                ? new Date(exec.created_at).toLocaleString()
                : '\u2014';
              msg += `| ${i + 1} | \`${shortId}\` | ${status} | ${created} |\n`;
            });
            msg += '\nUse `/stop <job_id>` to stop a running execution.';
            addMessage('assistant', msg);
          }
        } catch (error) {
          const errMsg = error instanceof Error ? error.message : 'Failed to list executions';
          addMessage('assistant', `Failed to list executions: ${errMsg}`);
        }
        execStore.setIsLoading(false);
        return true;
      }

      if (lower === '/stop' || lower.startsWith('/stop ')) {
        const jobId = message.trim().slice(5).trim();
        if (!jobId) {
          addMessage('assistant', 'Usage: `/stop <job_id>`');
          return true;
        }
        addMessage('user', message);
        try {
          await stopExecution(jobId);
          addMessage('assistant', `Execution \`${jobId.slice(0, 8)}...\` stop requested.`);
          const currentExec = execStore.activeExecution;
          if (currentExec?.jobId === jobId || currentExec?.jobId.startsWith(jobId)) {
            executionStream.stopStream();
            execStore.updateExecutionStatus('stopped');
          }
        } catch (error) {
          const errMsg = error instanceof Error ? error.message : 'Failed to stop execution';
          addMessage('assistant', `Failed to stop: ${errMsg}`);
        }
        return true;
      }

      // Create a skill by chatting: "/skill <what it should cover>", a bare
      // "/skill" to capture this conversation, or plain language ("create a
      // skill for…", "save what we learned as a skill"). A dedicated generation
      // call — not a meta-skill the agent has to load — validated before it
      // returns; the draft renders as a card whose Save is the human gate.
      const skillCmd = parseSkillCommand(message);
      if (skillCmd) {
        addMessage('user', message);
        // The draft is a generation, and shows as one: the "Thinking" activity
        // container under the prompt, with the drafting call as its live step.
        // Everything routes to the session that asked, so switching sessions
        // mid-draft neither hides the work nor posts it into the wrong chat.
        const sessionStore = useSessionStore.getState();
        const owner = sessionStore.currentSessionId || undefined;
        const post = (content: string, extra?: Parameters<typeof addMessage>[2]) =>
          owner
            ? sessionStore.addMessageToTargetSession(owner, 'assistant', content, extra)
            : sessionStore.addMessage('assistant', content, extra);
        const transcript =
          skillCmd.mode === 'capture' ? buildTranscript(sessionStore.messages) : undefined;
        const model = selectedModel || undefined;
        const startedAt = Date.now();
        execStore.startGeneration(owner);
        const stepId = post('', {
          resultType: 'trace',
          resultData: draftingStep(skillCmd, transcript?.length ?? 0, model),
        });
        // The draft is recorded as a run; carrying its id on the step is what
        // lets the activity open the run's trace (the LLM calls) and the pane.
        const setStep = (resultData: unknown, executionId?: string) => {
          const updates = { resultType: 'trace', resultData, ...(executionId ? { executionId } : {}) };
          if (owner) sessionStore.updateMessageInTargetSession(owner, stepId, updates);
          else sessionStore.updateMessage(stepId, updates);
        };
        try {
          const draft = await SkillService.draft(skillCmd.request, transcript, model);
          setStep(draftedStep(draft, startedAt), draft.job_id || undefined);
          post(draftMessage(draft));
        } catch (error) {
          const detail = (error as { response?: { data?: { detail?: unknown } } })?.response
            ?.data?.detail;
          const errMsg =
            typeof detail === 'string'
              ? detail
              : error instanceof Error
                ? error.message
                : 'Failed to draft the skill';
          setStep(draftFailedStep(errMsg, startedAt));
          post(`Could not draft the skill: ${errMsg}`);
        } finally {
          execStore.completeGeneration(owner);
        }
        return true;
      }

      // A slide edit in plain language — "make the chart bigger on slide 3",
      // "delete slide 5", "add a slide after 2 about pricing" — refines the
      // latest deck in this conversation rather than starting a new turn.
      // Only when there IS a deck: without one, "slide" is just a word.
      const deck = latestDeck(useSessionStore.getState().messages);
      if (deck && parseSlideEdit(message, splitSlides(deck.code).length)) {
        handleRefine(message);
        return true;
      }

      if (lower === '/dismiss' || lower === '/close') {
        execStore.resetForSession();
        return true;
      }

      if (lower === '/refine' || lower.startsWith('/refine ')) {
        const instruction = message.trim().slice(7).trim();
        if (!instruction) {
          addMessage('assistant', 'Usage: `/refine <how to improve the current result>`');
          return true;
        }
        handleRefine(instruction);
        return true;
      }

      if (lower === '/save' || lower.startsWith('/save ')) {
        const data = lastGeneratedRef.current;
        if (!data) {
          addMessage(
            'assistant',
            'There is no generated crew to save yet. Generate a crew first, then `/save` it or use the bookmark on the crew card.',
          );
          return true;
        }
        addMessage('user', message);
        // Allow "/save overwrite [name]" to replace an existing same-named crew.
        let arg = message.trim().slice(5).trim();
        let overwrite = false;
        if (arg.toLowerCase() === 'overwrite' || arg.toLowerCase().startsWith('overwrite ')) {
          overwrite = true;
          arg = arg.slice('overwrite'.length).trim();
        }
        const name = arg;
        const memoryEnabled = useExecutionStore.getState().memoryEnabled;
        const mcpServers = useExecutionStore.getState().selectedMcpServers;
        const saveMode = useExecutionStore.getState().chatModeType;
        try {
          const agentBricksEndpoints = useExecutionStore.getState().selectedAgentBricksEndpoints;
          const saved = await saveGeneratedCrew(data, name || undefined, {
            overwrite, memoryEnabled, mcpServers, agentBricksEndpoints,
            reasoning: saveMode === 'research' || saveMode === 'deep',
          });
          void refreshLibrary();
          addMessage(
            'assistant',
            `✓ ${overwrite ? 'Updated' : 'Saved'} **${saved.name}** ${overwrite ? 'in' : 'to'} the catalog — find it in the **Crews** library on the left.`,
          );
        } catch (error) {
          if (error instanceof CrewNameConflictError) {
            addMessage(
              'assistant',
              `**${error.crewName}** is already in the catalog. Type \`/save overwrite\` to replace it, or \`/save <a different name>\`.`,
            );
          } else {
            const errMsg = error instanceof Error ? error.message : 'Failed to save crew';
            addMessage('assistant', `Failed to save crew: ${errMsg}`);
          }
        }
        return true;
      }

      return false;
    },
    [addMessage, clearMessages, executionStream, handleRefine, selectedModel],
  );

  const handleSend = useCallback(
    async (
      message: string,
      meta?: { tools?: string[]; dispatchSuffix?: string; attachments?: string[]; displayAs?: string; knowledgeFilePaths?: string[] },
    ) => {
      // A genuine user message supersedes any pending loaded-crew run (the rail's
      // own "/load …" send is exempt — it's what arms the pending run).
      if (!message.startsWith('/load ')) setPendingRun(null);

      const handled = await handleLocalCommand(message);
      if (handled) return;

      // Remember the prompt: if this message triggers a crew generation, the
      // executed run is grounded with it (see onComplete / doExecuteGenerated).
      lastUserPromptRef.current = message;

      useExecutionStore.getState().setIsLoading(true);
      try {
        // Send the picker selection; when none is set the backend falls back to a
        // working default (gpt-5.3-codex), so we don't force a model here.
        await dispatcher.sendMessage(
          message,
          selectedModel || undefined,
          meta?.tools,
          meta?.dispatchSuffix,
          meta?.attachments,
          meta?.displayAs,
          meta?.knowledgeFilePaths,
        );
      } finally {
        useExecutionStore.getState().setIsLoading(false);
      }
    },
    [dispatcher, handleLocalCommand, selectedModel],
  );

  // Load a saved crew/flow from the rail library into a FRESH chat session (so it
  // never clobbers the current conversation). Reuses the deterministic /load
  // command under the hood but shows a friendly label in the transcript.
  const handleLoadFromLibrary = useCallback(
    async (kind: 'crew' | 'flow', name: string) => {
      if (currentSessionId) {
        useExecutionStore.getState().saveSessionState(currentSessionId);
      }
      const newId = await useSessionStore.getState().createNewSession();
      useExecutionStore.getState().restoreSessionState(newId);
      void handleSend(`/load ${kind} ${name}`, { displayAs: `Open ${kind}: ${name}` });
    },
    [handleSend, currentSessionId],
  );

  const handleStopExecution = useCallback(async () => {
    const execStore = useExecutionStore.getState();
    const activeExec = execStore.activeExecution;
    if (!activeExec?.jobId) return;
    try {
      await stopExecution(activeExec.jobId);
      executionStream.stopStream();
      addMessage('assistant', 'Execution stopped.');
      execStore.failExecution('Stopped by user', activeExec.jobId);
    } catch (error) {
      const errMsg = error instanceof Error ? error.message : 'Failed to stop execution';
      addMessage('assistant', `Failed to stop: ${errMsg}`);
    }
  }, [addMessage, executionStream]);

  return {
    handleSend,
    handleLoadFromLibrary,
    handleStopExecution,
  };
}
