import { useCallback, useEffect, useMemo, useRef } from 'react';
import { dispatch } from '../api/dispatcher';
import { useExecutionStore } from '../store/executionStore';
import {
  DispatchResult,
  GeneratedCrew,
  StreamingGenerationResult,
  CatalogListResult,
  CatalogLoadResult,
  FlowListResult,
  FlowLoadResult,
  ExecuteCrewResult,
  ExecuteFlowResult,
} from '../types/dispatcher';
import { ChatMessage } from '../types/chat';
import { generateId } from '../utils/markdown';
import { GenerationCompleteData } from '../types/dispatcher';

export type PlanData = NonNullable<CatalogLoadResult['plan']>;
export type FlowData = NonNullable<FlowLoadResult['flow']>;

interface UseDispatcherOptions {
  addMessage: (
    role: ChatMessage['role'],
    content: string,
    extra?: Partial<ChatMessage>
  ) => string;
  addMessageToTargetSession: (
    targetSessionId: string,
    role: ChatMessage['role'],
    content: string,
    extra?: Partial<ChatMessage>
  ) => string;
  updateMessage: (id: string, updates: Partial<ChatMessage>) => void;
  updateMessageInTargetSession: (
    targetSessionId: string,
    id: string,
    updates: Partial<ChatMessage>,
  ) => void;
  onStartGenerationStream: (generationId: string, sessionId: string) => void;
  onStartExecutionStream: (jobId: string, sessionId?: string) => void;
  onExecuteCrew?: (plan: PlanData) => void;
  onExecuteFlow?: (flow: FlowData) => void;
  onExecuteGenerated?: (data: GenerationCompleteData, spaceId?: string) => void;
  /** Fired when a crew/flow is LOADED (not run) so the host can make the chat
   *  submit button run it. sessionId is the session the plan was loaded into. */
  onCrewLoaded?: (plan: PlanData, sessionId: string | null) => void;
  onFlowLoaded?: (flow: FlowData, sessionId: string | null) => void;
  getCurrentSessionId: () => string | null;
  /** Resolve the current session id, lazily creating one if none exists yet.
   *  Awaited before capturing the run's origin session so a fresh chat's first
   *  turn doesn't register the run under an empty owner (the "first prompt
   *  renders empty" bug, which only surfaces under remote-DB latency). */
  ensureSession: () => Promise<string>;
}

function getAssistantResponse(result: DispatchResult): string {
  const { dispatcher, generation_result } = result;

  if (dispatcher.intent === 'unknown' || dispatcher.intent === 'conversation') {
    if (generation_result && typeof generation_result === 'object' && 'message' in (generation_result as Record<string, unknown>)) {
      return (generation_result as Record<string, unknown>).message as string;
    }
    return "I'm not sure what you want to do. Try `/help` for available commands.";
  }

  if (!generation_result) {
    return 'I understood your request but could not generate a result. Please try again.';
  }

  switch (dispatcher.intent) {
    case 'generate_agent':
    case 'generate_task':
    case 'generate_crew':
    case 'generate_plan': {
      const genResult = generation_result as
        | StreamingGenerationResult
        | GeneratedCrew;
      if (
        genResult &&
        typeof genResult === 'object' &&
        'type' in genResult &&
        genResult.type === 'streaming'
      ) {
        // No status text — the run-activity container shows "Generating crew…".
        return '';
      }
      // Non-streaming result — use the robust converter to detect shape
      const genData = crewToGenerationData(generation_result);
      if (genData.agents.length > 0 || genData.tasks.length > 0) {
        let response = 'Crew generation complete!\n\n';
        genData.agents.forEach((agent, i) => {
          const name = (agent.name as string) || (agent.role as string) || `Agent ${i + 1}`;
          const role = (agent.role as string) || '';
          response += `${i + 1}. **${name}**${role && role !== name ? ` (${role})` : ''}\n`;
        });
        genData.tasks.forEach((task, i) => {
          const name = (task.name as string) || `Task ${i + 1}`;
          const desc = (task.description as string) || '';
          response += `${genData.agents.length + i + 1}. Task: **${name}**${desc ? `: ${desc.slice(0, 80)}` : ''}\n`;
        });
        return response;
      }
      return 'Crew generation complete!';
    }
    case 'catalog_list': {
      const listResult = generation_result as CatalogListResult;
      return listResult.message;
    }
    case 'catalog_load': {
      const genResult = generation_result as Record<string, unknown>;
      if (
        genResult.type === 'catalog_list' &&
        Array.isArray(genResult.plans)
      ) {
        return (genResult.message as string) || 'Multiple matches found.';
      }
      return (genResult.message as string) || 'Plan loaded.';
    }
    case 'flow_list': {
      const flowListResult = generation_result as FlowListResult;
      return flowListResult.message;
    }
    case 'flow_load': {
      const genResult = generation_result as Record<string, unknown>;
      if (genResult.type === 'flow_list' && Array.isArray(genResult.flows)) {
        return (genResult.message as string) || 'Multiple flows found.';
      }
      return (genResult.message as string) || 'Flow loaded.';
    }
    case 'execute_crew':
    case 'execute_flow':
    case 'catalog_save':
    case 'catalog_schedule':
    case 'catalog_help':
    case 'catalog_delete':
    case 'flow_delete':
    case 'flow_save':
    case 'configure_crew': {
      const msg = (generation_result as Record<string, unknown>).message;
      if (typeof msg === 'string') return msg;
      if (msg && typeof msg === 'object') return JSON.stringify(msg);
      return 'Your request has been processed.';
    }
    default:
      return 'Your request has been processed.';
  }
}

function getResultType(
  result: DispatchResult
): string | undefined {
  const { dispatcher, generation_result } = result;
  if (!generation_result) return undefined;

  switch (dispatcher.intent) {
    case 'generate_agent':
    case 'generate_task':
    case 'generate_crew':
    case 'generate_plan': {
      const genResult = generation_result as Record<string, unknown>;
      if (genResult.type === 'streaming') return 'streaming';
      // Non-streaming crew → show "Run crew" button
      return 'generation_complete';
    }
    case 'catalog_list':
      return 'catalog_list';
    case 'catalog_load': {
      const genResult = generation_result as Record<string, unknown>;
      if (genResult.type === 'catalog_list') return 'catalog_list';
      return 'catalog_load';
    }
    case 'flow_list':
      return 'flow_list';
    case 'flow_load': {
      const genResult = generation_result as Record<string, unknown>;
      if (genResult.type === 'flow_list') return 'flow_list';
      return 'flow_load';
    }
    case 'execute_crew':
      return 'execute_crew';
    case 'execute_flow':
      return 'execute_flow';
    case 'catalog_help':
      return 'help';
    default:
      return undefined;
  }
}

/**
 * Convert a non-streaming generation result into GenerationCompleteData.
 * Handles multiple shapes: full crew, single agent, single task, or wrapped.
 */
function crewToGenerationData(result: unknown): GenerationCompleteData {
  if (!result || typeof result !== 'object') return { agents: [], tasks: [] };
  const obj = result as Record<string, unknown>;

  // Full crew result with arrays
  if (Array.isArray(obj.agents) || Array.isArray(obj.tasks)) {
    return {
      agents: (Array.isArray(obj.agents) ? obj.agents : []).map((a: unknown) => ({ ...(a as Record<string, unknown>) })),
      tasks: (Array.isArray(obj.tasks) ? obj.tasks : []).map((t: unknown) => ({ ...(t as Record<string, unknown>) })),
    };
  }

  // Nested under common wrapper keys (e.g. { result: { agents, tasks } })
  for (const key of ['result', 'data', 'crew', 'generation_result']) {
    if (obj[key] && typeof obj[key] === 'object' && !Array.isArray(obj[key])) {
      const nested = crewToGenerationData(obj[key]);
      if (nested.agents.length > 0 || nested.tasks.length > 0) return nested;
    }
  }

  // Single agent result (has role + goal + backstory)
  const isAgent = typeof obj.role === 'string' && typeof obj.goal === 'string';
  // Single task result (has description + expected_output)
  const isTask = typeof obj.description === 'string' && typeof obj.expected_output === 'string';

  // Could also have a nested single agent/task under an "agent" or "task" key
  const wrappedAgent = obj.agent && typeof obj.agent === 'object' ? obj.agent as Record<string, unknown> : null;
  const wrappedTask = obj.task && typeof obj.task === 'object' ? obj.task as Record<string, unknown> : null;

  const agents: Record<string, unknown>[] = [];
  const tasks: Record<string, unknown>[] = [];

  if (isAgent) agents.push({ ...obj });
  if (wrappedAgent) agents.push({ ...wrappedAgent });
  if (isTask) tasks.push({ ...obj });
  if (wrappedTask) tasks.push({ ...wrappedTask });

  return { agents, tasks };
}

export function useDispatcher(options: UseDispatcherOptions) {
  const isDispatchingRef = useRef(false);
  // Store last generated crew for "ec"/"execute crew" command
  const lastGeneratedRef = useRef<GenerationCompleteData | null>(null);
  // Hold options in a ref (updated each render) so sendMessage can be a STABLE
  // callback. ChatWorkspace passes a fresh options literal every render, and a
  // sendMessage that depended on it churned the whole dispatcher object — which
  // churned ChatWorkspace's handleSend → ChatContainer's handleCommand, making
  // the memoized ChatMessage bubbles re-render on every tick anyway. Mirrors
  // the optionsRef pattern in useExecutionStream.
  const optionsRef = useRef(options);
  useEffect(() => {
    optionsRef.current = options;
  });

  const sendMessage = useCallback(
    async (
      message: string,
      model?: string,
      tools?: string[],
      dispatchSuffix?: string,
      attachments?: string[],
      displayAs?: string,
      knowledgeFilePaths?: string[],
    ) => {
      const options = optionsRef.current;
      if (isDispatchingRef.current) return;
      isDispatchingRef.current = true;

      // Capture the session ID NOW, before any async work, so all operations
      // target the correct session even if the user switches sessions during the
      // dispatch API call. On the FIRST turn of a fresh chat the session is
      // created lazily and `currentSessionId` stays null until the create POST
      // returns — a window that's effectively zero locally (SQLite) but wide on
      // Databricks Apps' remote Postgres. Capturing it too early registered the
      // run under an empty owner, so its completed result never rendered ("first
      // prompt empty, second works"). Await the (deduped) session-ensure so we
      // hold the REAL id before dispatch + onStartGenerationStream.
      const originSessionId = await options.ensureSession();

      // Intercept "ec" / "execute crew" locally
      const lower = message.toLowerCase().trim();
      if (
        (lower === 'ec' || lower === 'execute crew' || lower === 'run crew' || lower === 'start crew') &&
        lastGeneratedRef.current &&
        options.onExecuteGenerated
      ) {
        options.addMessage('user', message);
        options.onExecuteGenerated(lastGeneratedRef.current);
        isDispatchingRef.current = false;
        return;
      }

      options.addMessage(
        'user',
        // Show a friendly label (e.g. "Open crew: X" from the library rail)
        // while still dispatching the real command/message below.
        displayAs || message,
        attachments && attachments.length > 0 ? { attachments } : undefined,
      );

      const assistantId = generateId();
      options.addMessage('assistant', 'Thinking...', {
        id: assistantId,
        isStreaming: true,
      });

      // Augment the prompt to steer intent detection toward generate_crew
      // when the message looks like a generation request but doesn't already
      // contain crew/plan keywords.  The original message is shown to the user;
      // only the dispatch payload is modified.
      let dispatchMessage = message;
      const lowerMsg = message.toLowerCase();
      const isSlashCommand = lowerMsg.startsWith('/');
      const alreadyHasCrewHint = /\b(crew|plan|create crew|create plan|generate crew)\b/i.test(message);
      // Chat (light agent) mode answers the literal question with a single agent —
      // don't steer it into "create a crew plan with agents and tasks". Research /
      // Deep modes still build a crew, so they keep the prefix.
      const chatModeType = useExecutionStore.getState().chatModeType;
      if (chatModeType !== 'chat' && !isSlashCommand && !alreadyHasCrewHint) {
        dispatchMessage = `create a crew plan with agents and tasks: ${message}`;
      }
      // Hidden steering text (e.g. attached-knowledge note): sent to the crew
      // but never shown in the chat (addMessage above used the clean message).
      if (dispatchSuffix) {
        dispatchMessage += dispatchSuffix;
      }

      try {
        // ChatMode run settings: the backend auto-executes the generated crew
        // with the chat's own memory scope + attached MCP data sources, so the
        // run survives a session switch before the plan finishes. Read at
        // dispatch time so the values reflect the user's current choices.
        const execState = useExecutionStore.getState();
        const result = await dispatch(dispatchMessage, model, tools, {
          // ChatMode runs the generated crew on the backend (the crew canvas
          // doesn't — it runs via Play, so it omits this and defaults false).
          auto_execute: true,
          session_id: originSessionId || undefined,
          memory_workspace_scope: execState.workspaceMemory,
          disable_memory: !execState.memoryEnabled,
          mcp_servers: execState.selectedMcpServers,
          // Agent Bricks endpoints picked in the chat "+" — the backend equips +
          // configures the AgentBricksTool on the auto-executed crew with these.
          agentbricks_endpoints: execState.selectedAgentBricksEndpoints,
          // Files attached in THIS turn — scopes the knowledge search tool to the
          // just-uploaded document (otherwise group-wide search picks a wrong file).
          knowledge_file_paths: knowledgeFilePaths,
          // Answer mode → backend sets reasoning/planning/execution_type:
          // chat = single light agent, research = crew+reasoning, deep = +planning.
          chat_mode_type: execState.chatModeType,
        }, message);

        const content = getAssistantResponse(result);
        const resultType = getResultType(result);

        // Keys only — pretty-printing the ENTIRE generation_result (multi-100KB
        // A2UI payloads) just to slice a log sample was a per-dispatch CPU tax.
        console.log('[dispatcher] intent:', result.dispatcher.intent, 'resultType:', resultType,
          'generation_result keys:', result.generation_result ? Object.keys(result.generation_result as Record<string, unknown>) : 'null');

        // For non-streaming crew results, convert to GenerationCompleteData format
        let resultData = result.generation_result;
        if (resultType === 'generation_complete' && result.generation_result) {
          const genData = crewToGenerationData(result.generation_result);
          resultData = genData;
          console.log('[dispatcher] crewToGenerationData — agents:', genData.agents.length, 'tasks:', genData.tasks.length);
          // Store for "ec" command
          if (genData.agents.length > 0 || genData.tasks.length > 0) {
            lastGeneratedRef.current = genData;
          }
        }

        // Use session-targeted update so the message goes to the correct
        // session even if the user switched sessions during the API call.
        if (originSessionId) {
          options.updateMessageInTargetSession(originSessionId, assistantId, {
            content,
            isStreaming: false,
            intent: result.dispatcher.intent,
            resultType,
            resultData,
          });
        } else {
          options.updateMessage(assistantId, {
            content,
            isStreaming: false,
            intent: result.dispatcher.intent,
            resultType,
            resultData,
          });
        }

        if (result.generation_result) {
          const intent = result.dispatcher.intent;

          if (
            intent === 'generate_crew' ||
            intent === 'generate_plan' ||
            intent === 'generate_agent' ||
            intent === 'generate_task'
          ) {
            const genResult = result.generation_result as Record<string, unknown>;
            if (genResult.type === 'streaming') {
              const streamResult =
                genResult as unknown as StreamingGenerationResult;
              options.onStartGenerationStream(
                streamResult.generation_id,
                originSessionId || '',
              );
            }
          }

          if (intent === 'execute_crew') {
            const execResult = result.generation_result as ExecuteCrewResult;
            if (execResult.plan && options.onExecuteCrew) {
              const plan = execResult.plan;
              setTimeout(() => {
                options.onExecuteCrew!(plan);
              }, 300);
            }
          }

          // A loaded (not run) crew/flow becomes the session's "pending run" so
          // the chat submit button executes it (no separate Run button).
          if (resultType === 'catalog_load') {
            const loaded = result.generation_result as { plan?: PlanData };
            if (loaded.plan && options.onCrewLoaded) options.onCrewLoaded(loaded.plan, originSessionId);
          }
          if (resultType === 'flow_load') {
            const loaded = result.generation_result as { flow?: FlowData };
            if (loaded.flow && options.onFlowLoaded) options.onFlowLoaded(loaded.flow, originSessionId);
          }

          if (intent === 'execute_flow') {
            const execResult = result.generation_result as ExecuteFlowResult;
            if (execResult.flow && options.onExecuteFlow) {
              const flow = execResult.flow;
              setTimeout(() => {
                options.onExecuteFlow!(flow);
              }, 300);
            }
          }
        }
      } catch (error) {
        const errMsg =
          error instanceof Error ? error.message : 'Unknown error';
        if (originSessionId) {
          options.updateMessageInTargetSession(originSessionId, assistantId, {
            content: `Failed to process your request: ${errMsg}`,
            isStreaming: false,
          });
        } else {
          options.updateMessage(assistantId, {
            content: `Failed to process your request: ${errMsg}`,
            isStreaming: false,
          });
        }
      } finally {
        isDispatchingRef.current = false;
      }
    },
    []
  );

  const setLastGenerated = useCallback((data: GenerationCompleteData) => {
    lastGeneratedRef.current = data;
  }, []);

  // Stable object identity (all members are stable): keeps ChatWorkspace's
  // handleSend — which depends on the whole dispatcher — from churning per render.
  return useMemo(
    () => ({ sendMessage, isDispatching: isDispatchingRef, setLastGenerated }),
    [sendMessage, setLastGenerated],
  );
}
