import React, { useState, useCallback, useEffect, useRef } from 'react';
import { ExecutionStatus } from './types/execution';
import { createExecution, stopExecution, listExecutions } from './api/executions';
import { useSessionStore } from './store/sessionStore';
import { useExecutionStore } from './store/executionStore';
import { useAppStore } from './store/appStore';
import { useDispatcher, PlanData, FlowData } from './hooks/useDispatcher';
import { useExecutionStream } from './hooks/useExecutionStream';
import { useGenerationStream, GenerationCompleteData } from './hooks/useGenerationStream';
import { generateId } from './utils/markdown';
import { buildCrewConfig, buildFlowConfig, buildCrewConfigFromGenerated } from './utils/crewConfigBuilder';
import { detectVariablesFromNodes, detectVariablesFromGenerated, DetectedVariable } from './utils/variableDetector';
import ChatContainer from './components/Chat/ChatContainer';
import PreviewPanel, { parsePreviewContent, PreviewContent } from './components/Preview/PreviewPanel';
import InputVariablesDialog from './components/InputVariablesDialog';
import { saveSessionPreview, getSessionPreview } from './db/sessionDb';
import { useThemeStore } from '../../store/theme';
import './chat.css';

export interface TraceEntry {
  label: string;
  sublabel?: string;
  durationMs?: number;
  source?: string;
  kind: 'tool_call' | 'tool_result' | 'event';
  detail?: string;
  timestamp: number;
  matchKey?: string;
}

/**
 * Build a stable key that pairs a tool_usage (start) event with its matching
 * `<tool>_run` (result) event, so they can be rendered as a single pill.
 */
export function toolMatchKey(name: unknown, args: unknown): string {
  const n = String(name || '').toLowerCase().replace(/[_\s]/g, '');
  let parsed: Record<string, unknown> = {};
  try {
    if (typeof args === 'string' && args.trim()) {
      parsed = JSON.parse(args.replace(/'/g, '"'));
    } else if (args && typeof args === 'object') {
      parsed = args as Record<string, unknown>;
    }
  } catch { /* ignore */ }
  const vals = Object.values(parsed)
    .filter((v) => typeof v === 'string' || typeof v === 'number')
    .join('|');
  return `${n}::${vals}`;
}

export function summarizeArgs(args: unknown): string | undefined {
  if (!args) return undefined;
  let parsed: Record<string, unknown> = {};
  try {
    if (typeof args === 'string') {
      parsed = JSON.parse(args.replace(/'/g, '"'));
    } else if (typeof args === 'object') {
      parsed = args as Record<string, unknown>;
    }
  } catch {
    return typeof args === 'string' ? args : undefined;
  }
  const vals = Object.values(parsed).filter((v) => typeof v === 'string');
  let s = vals.join(', ');
  if (!s) return undefined;
  if (s.length > 80) s = s.slice(0, 80) + '…';
  return s;
}

/**
 * Turn a raw SSE trace event into a clean entry for the chat. Returns null
 * when the event is pure noise (LLM retries, token fragments, internal IDs).
 */
export function buildTraceEntry(
  message: string,
  data?: Record<string, unknown>,
): TraceEntry | null {
  const eventType = (data?.event_type as string) || '';
  const eventSource = (data?.event_source as string) || '';
  const output = (data?.output as Record<string, unknown>) || {};
  const extra = (output.extra_data as Record<string, unknown>) || {};
  const duration = typeof output.duration_ms === 'number' ? (output.duration_ms as number) : undefined;
  const now = Date.now();

  // Hard-filter known noise events.
  if (eventType === 'llm_retry' || eventType === 'task_started') return null;

  // Tool invocation (start of a call).
  if (eventType === 'tool_usage') {
    const toolName = (extra.tool_name as string) || 'tool';
    const rawArgs = (extra.tool_args as string) || '';
    return {
      kind: 'tool_call',
      label: toolName,
      sublabel: summarizeArgs(rawArgs),
      source: eventSource || undefined,
      detail: rawArgs || undefined,
      timestamp: now,
      matchKey: toolMatchKey(toolName, rawArgs),
    };
  }

  // Tool result event — backend names them like `perplexitytool_run`,
  // `scrapewebsitetool_run`, etc.
  if (eventType.endsWith('_run')) {
    const toolName = (output.tool_name as string) || eventType.replace(/_run$/, '');
    const content = typeof output.content === 'string' ? (output.content as string) : '';
    const input = output.input;
    return {
      kind: 'tool_result',
      label: toolName,
      sublabel: summarizeArgs(input),
      durationMs: duration,
      source: eventSource || undefined,
      detail: content || undefined,
      timestamp: now,
      matchKey: toolMatchKey(toolName, input),
    };
  }

  // Strip raw JSON dumps and single-token fragments that have no useful label.
  const trimmed = message.trim();
  if (!trimmed) return null;
  // Raw JSON event payload — useless without parsing.
  if (trimmed.startsWith('{') && /"id":\s*\d+/.test(trimmed)) return null;
  // Single-word / sub-token fragments from agent streaming ("_usage", "s.", "Read").
  if (trimmed.length < 12 && !/\s/.test(trimmed)) return null;
  // Generic "Calling tools." status pings.
  if (/^calling tool/i.test(trimmed) && trimmed.length <= 30) return null;

  return {
    kind: 'event',
    label: trimmed.length > 80 ? trimmed.slice(0, 80) + '…' : trimmed,
    detail: trimmed.length > 80 ? trimmed : undefined,
    timestamp: now,
  };
}

/**
 * Distill a task output into a concise chat-message body. Returns null when
 * the output is pure progress-noise that shouldn't appear in chat.
 */
/**
 * Produce a concise, human-friendly label for a task-output chat message.
 *
 * Traces report the task *name* as the full (interpolated) task description, so
 * a refine surfaces the entire "Improve the artifact below… CURRENT ARTIFACT:
 * <!DOCTYPE html>…" prompt — which dumps the whole artifact into the chat. We
 * special-case the refine prompt to "Refined artifact" and otherwise collapse
 * any over-long description to its first line, truncated.
 */
export function cleanTaskLabel(taskName: string): string {
  const name = (taskName || '').trim();
  if (!name) return 'Task';
  // The refine editor task description always starts with this sentinel.
  if (/^Improve the artifact below based on this instruction/i.test(name)) {
    return 'Refined artifact';
  }
  const firstLine = name.split('\n')[0].trim();
  return firstLine.length > 80 ? `${firstLine.slice(0, 80).trim()}…` : firstLine;
}

export function summarizeTaskOutput(
  raw: string,
  preview: PreviewContent | null,
): string | null {
  const trimmed = raw.trim();
  if (!trimmed) return null;

  // Status noise like "Calling tools.", "Thinking...", "Using tool X" — skip.
  if (
    trimmed.length <= 120 &&
    /^(calling tool|thinking|processing|searching|using tool|executing|agent (started|thinking|finished)|tool call)/i.test(trimmed)
  ) {
    return null;
  }

  if (preview) {
    const kind = preview.type === 'html' ? 'HTML output' : preview.type === 'markdown' ? 'report' : 'result';
    return `Generated ${kind}. View it in the preview pane.`;
  }

  // Long plain-text outputs get collapsed too, otherwise they take over the chat.
  if (trimmed.length > 400) {
    return `${trimmed.slice(0, 300).trim()}…`;
  }

  return trimmed;
}

const ChatWorkspace: React.FC = () => {
  // --- Zustand Stores ---
  const sessions = useSessionStore((s) => s.sessions);
  const currentSessionId = useSessionStore((s) => s.currentSessionId);
  const messages = useSessionStore((s) => s.messages);
  const addMessage = useSessionStore((s) => s.addMessage);
  const addMessageToTargetSession = useSessionStore((s) => s.addMessageToTargetSession);
  const updateMessage = useSessionStore((s) => s.updateMessage);
  const updateMessageInTargetSession = useSessionStore((s) => s.updateMessageInTargetSession);
  const clearMessages = useSessionStore((s) => s.clearMessages);

  const isExecuting = useExecutionStore((s) => s.isExecuting);
  const isGenerating = useExecutionStore((s) => s.isGenerating);
  const isLoading = useExecutionStore((s) => s.isLoading);
  const executionContext = useExecutionStore((s) => s.executionContext);
  const rawPreviewContent = useExecutionStore((s) => s.previewContent);
  const previewOwnerSessionId = useExecutionStore((s) => s.previewOwnerSessionId);
  const previewHistory = useExecutionStore((s) => s.previewHistory);
  const previewIndex = useExecutionStore((s) => s.previewIndex);
  const navigatePreview = useExecutionStore((s) => s.navigatePreview);
  const chatCollapsed = useExecutionStore((s) => s.chatCollapsed);

  // Render-time isolation guard: only show a preview that belongs to the
  // session currently on screen. This is the backstop that prevents a preview
  // produced by a job in another session (e.g. a late SSE completion after the
  // user switched chats) from leaking into the session being viewed.
  const previewContent =
    rawPreviewContent && previewOwnerSessionId === currentSessionId
      ? rawPreviewContent
      : null;

  const models = useAppStore((s) => s.models);
  const selectedModel = useAppStore((s) => s.selectedModel);
  const sidebarOpen = useAppStore((s) => s.sidebarOpen);

  // --- Local UI state (sidebar-only concerns) ---
  const [contextMenu, setContextMenu] = useState<{ sessionId: string; x: number; y: number } | null>(null);
  const [renamingSessionId, setRenamingSessionId] = useState<string | null>(null);
  const [renameValue, setRenameValue] = useState('');
  // Track whether a persisted preview exists that could be reopened
  const [hasHiddenPreview, setHasHiddenPreview] = useState(false);

  // Input variables dialog state
  const [variablesDialogOpen, setVariablesDialogOpen] = useState(false);
  const [detectedVariables, setDetectedVariables] = useState<DetectedVariable[]>([]);
  const [pendingExecution, setPendingExecution] = useState<{
    type: 'crew' | 'generated';
    plan?: PlanData;
    data?: GenerationCompleteData;
    spaceId?: string;
  } | null>(null);

  // Check for hidden preview when session changes or preview is closed.
  // For old sessions that never had a preview saved, scan messages for
  // embedded HTML and backfill the preview into IndexedDB.
  useEffect(() => {
    if (previewContent) {
      setHasHiddenPreview(false);
      return;
    }
    if (!currentSessionId) {
      setHasHiddenPreview(false);
      return;
    }
    getSessionPreview(currentSessionId).then((stored) => {
      if (stored) {
        setHasHiddenPreview(true);
        return;
      }
      // No stored preview — scan messages for previewable content (backfill)
      const sessionMessages = useSessionStore.getState().messages;
      for (let i = sessionMessages.length - 1; i >= 0; i--) {
        const msg = sessionMessages[i];
        if (msg.role !== 'assistant' || !msg.content) continue;
        const preview = parsePreviewContent(msg.content);
        if (preview) {
          // Save to IndexedDB so it persists and reopenPreview() works
          saveSessionPreview(currentSessionId, preview);
          setHasHiddenPreview(true);
          return;
        }
      }
      setHasHiddenPreview(false);
    });
  }, [currentSessionId, previewContent, messages]);

  // Sync chat theme from Kasal's theme store (dark-mode toggle).
  const kasalIsDarkMode = useThemeStore((s) => s.isDarkMode);
  useEffect(() => {
    useAppStore.getState().setTheme(kasalIsDarkMode ? 'dark' : 'light');
  }, [kasalIsDarkMode]);

  // --- Initialize stores on mount ---
  useEffect(() => {
    useAppStore.getState().init();
    // Apply Kasal's current theme to the chat container immediately on mount.
    useAppStore.getState().setTheme(useThemeStore.getState().isDarkMode ? 'dark' : 'light');
    useAppStore.getState().loadModels();
    useAppStore.getState().loadTools();
    useSessionStore.getState().init().then(() => {
      const sessionId = useSessionStore.getState().currentSessionId;
      if (sessionId) {
        useExecutionStore.getState().restoreSessionState(sessionId);
      }
    });
  }, []);

  // Collapse paired tool events (tool_usage + matching *_run) into a single
  // chat pill keyed by matchKey, regardless of which order they arrive in.
  const traceMessageIdsRef = useRef<Map<string, { messageId: string; resolved: boolean }>>(
    new Map(),
  );

  // --- Execution Stream ---
  const executionStream = useExecutionStream({
    onTrace: (message, data) => {
      const trace = buildTraceEntry(message, data);
      if (!trace) return;
      const ownerSession = useExecutionStore.getState().executionOwnerSessionId;
      const sessionStore = useSessionStore.getState();

      if (trace.matchKey) {
        const existing = traceMessageIdsRef.current.get(trace.matchKey);
        if (existing) {
          // Already have a pill for this key. The tool_result is the richer
          // event (has duration + content), so promote the pill to it; drop
          // any later tool_call for an already-resolved key.
          if (trace.kind === 'tool_result' && !existing.resolved) {
            const updates = { resultData: trace };
            if (ownerSession) {
              sessionStore.updateMessageInTargetSession(ownerSession, existing.messageId, updates);
            } else {
              sessionStore.updateMessage(existing.messageId, updates);
            }
            traceMessageIdsRef.current.set(trace.matchKey, {
              messageId: existing.messageId,
              resolved: true,
            });
          }
          return;
        }
      }

      const extra = { resultType: 'trace', resultData: trace };
      const id = ownerSession
        ? sessionStore.addMessageToTargetSession(ownerSession, 'assistant', '', extra)
        : sessionStore.addMessage('assistant', '', extra);

      if (trace.matchKey) {
        traceMessageIdsRef.current.set(trace.matchKey, {
          messageId: id,
          resolved: trace.kind === 'tool_result',
        });
      }
    },
    onTaskOutput: (taskName, output) => {
      const execState = useExecutionStore.getState();
      const ownerSession = execState.executionOwnerSessionId;
      const sessionStore = useSessionStore.getState();

      // Try to extract renderable content from various result shapes
      let displayContent = output;
      try {
        // The output may be JSON with a "content" field wrapping the actual result
        const parsed = typeof output === 'string' && output.trim().startsWith('{')
          ? JSON.parse(output) as Record<string, unknown>
          : null;
        if (parsed?.content && typeof parsed.content === 'string') {
          displayContent = parsed.content;
        }
      } catch { /* not JSON, use raw */ }

      // Check if the content is previewable (HTML, structured markdown, etc.)
      const preview = parsePreviewContent(displayContent);
      const currentSession = sessionStore.currentSessionId;
      console.log(
        '[Preview] onTaskOutput — preview:', preview ? `${preview.type} (${preview.data.length} chars)` : 'null',
        '| currentSession:', currentSession,
        '| ownerSession:', ownerSession,
        '| match:', currentSession === ownerSession,
        '| displayContent preview:', displayContent.slice(0, 120),
      );
      if (preview) {
        if (currentSession === ownerSession) {
          execState.setPreviewContent(preview);
        }
        // Always persist to IndexedDB so it's available when switching back
        if (ownerSession) {
          saveSessionPreview(ownerSession, preview);
        }
      }

      // Build a concise chat-message body. Raw task output (HTML dumps, status
      // pings like "Calling tools.", or echoed task descriptions) clutters the
      // chat; the real content lives in the preview pane.
      const chatBody = summarizeTaskOutput(displayContent, preview);
      if (chatBody !== null) {
        const msg = `**${cleanTaskLabel(taskName)}** — ${chatBody}`;
        if (ownerSession) {
          sessionStore.addMessageToTargetSession(ownerSession, 'assistant', msg);
        } else {
          sessionStore.addMessage('assistant', msg);
        }
      }

      // NOTE: we intentionally do NOT auto-complete the execution on a timer.
      // The "Running crew..." banner stays until the real SSE completion/error
      // event, so a multi-task crew keeps running in the UI (and keeps appending
      // each task's output to the preview) instead of being cut short after an
      // intermediate task. If a completion event is genuinely never received the
      // user can dismiss with /dismiss; the crew-mode job history remains the
      // source of truth for run status.
    },
    onStatusChange: (status) => {
      useExecutionStore.getState().updateExecutionStatus(status as ExecutionStatus);
    },
    onComplete: (data) => {
      // Extract result text from potentially nested structures.
      // The backend may return the result in various shapes:
      //   { result: "text" }
      //   { result: { result: "text" } }
      //   { result: { content: "text" } }
      //   { content: "text" }
      //   { output: "text" }
      let resultText = '';
      try {
        const rawResult = data.result;
        if (typeof rawResult === 'string') {
          try {
            const parsed = JSON.parse(rawResult);
            if (parsed && typeof parsed === 'object') {
              resultText = (typeof parsed.result === 'string' ? parsed.result : '')
                || (typeof parsed.content === 'string' ? parsed.content : '')
                || rawResult;
            } else {
              resultText = rawResult;
            }
          } catch {
            resultText = rawResult;
          }
        } else if (rawResult && typeof rawResult === 'object') {
          const nested = rawResult as Record<string, unknown>;
          const inner = nested.result ?? nested.content ?? nested.raw;
          if (typeof inner === 'string') {
            resultText = inner;
          } else if (inner && typeof inner === 'object') {
            // Check one more level for content field
            const deepContent = (inner as Record<string, unknown>).content;
            if (typeof deepContent === 'string') {
              resultText = deepContent;
            } else {
              resultText = JSON.stringify(inner);
            }
          } else {
            resultText = JSON.stringify(nested);
          }
        }
        // Fallback: check top-level content or output
        if (!resultText && typeof data.content === 'string') {
          resultText = data.content;
        }
        if (!resultText) {
          const output = data.output;
          resultText = typeof output === 'string' ? output : '';
        }
      } catch {
        resultText = '';
      }

      useExecutionStore.getState().completeExecution(resultText);
    },
    onError: (error) => {
      useExecutionStore.getState().failExecution(error);
    },
  });

  // --- Generation Stream ---
  const generationStream = useGenerationStream({
    onPlanReady: () => {},
    onAgentDetail: () => {},
    onTaskDetail: () => {},
    onComplete: (data: GenerationCompleteData) => {
      console.log('[App onComplete] SSE generation data — agents:', data?.agents?.length, 'tasks:', data?.tasks?.length,
        'keys:', data ? Object.keys(data) : 'null');
      const ownerSession = useExecutionStore.getState().executionOwnerSessionId;
      const sessionStore = useSessionStore.getState();
      const id = generateId();
      const msgContent = 'Crew generated. Starting run...';

      if (ownerSession) {
        sessionStore.addMessageToTargetSession(ownerSession, 'assistant', msgContent, {
          id,
          resultType: 'generation_complete',
          resultData: data,
        });
      } else {
        sessionStore.addMessage('assistant', msgContent, {
          id,
          resultType: 'generation_complete',
          resultData: data,
        });
      }

      if (dispatcher.setLastGenerated) {
        dispatcher.setLastGenerated(data);
      }

      useExecutionStore.getState().completeGeneration();

      // Auto-run the generated crew so the user doesn't have to click "Run crew".
      // handleExecuteGenerated handles variable detection and will open the
      // variables dialog if any inputs are required.
      handleExecuteGenerated(data);
    },
    onFailed: (error) => {
      useExecutionStore.getState().failGeneration(error);
    },
  });

  // --- Execution handlers ---
  const handleStartGenerationStream = useCallback(
    (generationId: string, sessionId: string) => {
      const origin = sessionId || useSessionStore.getState().currentSessionId;
      useExecutionStore.getState().startGeneration(origin || undefined);
      generationStream.startStream(generationId);
    },
    [generationStream],
  );

  const handleStartExecutionStream = useCallback(
    (jobId: string, sessionId?: string, opts?: { preservePreview?: boolean }) => {
      const origin = sessionId || useSessionStore.getState().currentSessionId;
      useExecutionStore.getState().startExecution(jobId, origin || undefined, opts);
      traceMessageIdsRef.current.clear();
      executionStream.startStream(jobId);
    },
    [executionStream],
  );

  const doExecuteCrew = useCallback(
    async (plan: PlanData, inputs?: Record<string, string>) => {
      // Capture the session ID NOW, before the async createExecution call.
      // If the user switches sessions during the API call, currentSessionId
      // will have changed, but originSessionId preserves the correct owner.
      const originSessionId = useSessionStore.getState().currentSessionId;

      const execStore = useExecutionStore.getState();
      execStore.setIsLoading(true);
      try {
        const nodes = (plan.nodes || []) as { type: string; data: Record<string, unknown> }[];
        const agentNames = nodes
          .filter((n) => n.type === 'agentNode' || n.type === 'agent')
          .map((n) => ({ name: (n.data.role as string) || (n.data.name as string) || 'Agent' }));
        const taskNames = nodes
          .filter((n) => n.type === 'taskNode' || n.type === 'task')
          .map((n) => ({ name: (n.data.name as string) || (n.data.description as string)?.slice(0, 40) || 'Task' }));
        execStore.setExecutionContext({
          crewName: plan.name || 'Crew',
          agents: agentNames,
          tasks: taskNames,
        });

        const crewConfig = buildCrewConfig(plan, selectedModel || undefined, inputs);
        const execution = await createExecution(crewConfig);
        const jobId = execution.job_id || execution.execution_id;
        if (jobId) {
          handleStartExecutionStream(jobId, originSessionId || undefined);
        } else {
          addMessage('assistant', 'Execution started but no job ID was returned.');
          execStore.setExecutionContext(null);
          execStore.setIsLoading(false);
        }
      } catch (error) {
        const errMsg = error instanceof Error ? error.message : 'Failed to start execution';
        addMessage('assistant', `Execution failed: ${errMsg}`);
        execStore.setExecutionContext(null);
        execStore.setIsLoading(false);
      }
    },
    [addMessage, handleStartExecutionStream, selectedModel],
  );

  const handleExecuteCrew = useCallback(
    async (plan: PlanData) => {
      const vars = detectVariablesFromNodes(plan.nodes || []);
      if (vars.length > 0) {
        setDetectedVariables(vars);
        setPendingExecution({ type: 'crew', plan });
        setVariablesDialogOpen(true);
        return;
      }
      doExecuteCrew(plan);
    },
    [doExecuteCrew],
  );

  const doExecuteGenerated = useCallback(
    async (
      data: GenerationCompleteData,
      spaceId?: string,
      inputs?: Record<string, string>,
      opts?: { preservePreview?: boolean },
    ) => {
      // Capture the session ID NOW, before the async createExecution call.
      const originSessionId = useSessionStore.getState().currentSessionId;

      const execStore = useExecutionStore.getState();
      execStore.setIsLoading(true);
      try {
        const agentNames = (data.agents || []).map((a) => ({
          name: (a.name as string) || (a.role as string) || 'Agent',
          role: (a.role as string) || undefined,
        }));
        const taskNames = (data.tasks || []).map((t) => ({
          name: (t.name as string) || (t.description as string)?.slice(0, 40) || 'Task',
        }));
        execStore.setExecutionContext({
          crewName: 'Generated Crew',
          agents: agentNames,
          tasks: taskNames,
        });

        // If a Genie space was selected, pass tool_configs with the spaceId
        // (the selector only shows when GenieTool is already in the crew's tools)
        const agents = data.agents;
        const taskList = data.tasks;
        const toolConfigs = spaceId
          ? { GenieTool: { spaceId } }
          : undefined;

        const crewConfig = buildCrewConfigFromGenerated(
          agents,
          taskList,
          selectedModel || undefined,
          toolConfigs,
          inputs,
        );
        const execution = await createExecution(crewConfig);
        const jobId = execution.job_id || execution.execution_id;
        if (jobId) {
          handleStartExecutionStream(jobId, originSessionId || undefined, opts);
        } else {
          addMessage('assistant', 'Execution started but no job ID was returned.');
          execStore.setExecutionContext(null);
          execStore.setIsLoading(false);
        }
      } catch (error) {
        const errMsg = error instanceof Error ? error.message : 'Failed to start execution';
        addMessage('assistant', `Execution failed: ${errMsg}`);
        execStore.setExecutionContext(null);
        execStore.setIsLoading(false);
      }
    },
    [addMessage, handleStartExecutionStream, selectedModel],
  );

  const handleExecuteGenerated = useCallback(
    async (data: GenerationCompleteData, spaceId?: string) => {
      const vars = detectVariablesFromGenerated(data.agents || [], data.tasks || []);
      if (vars.length > 0) {
        setDetectedVariables(vars);
        setPendingExecution({ type: 'generated', data, spaceId });
        setVariablesDialogOpen(true);
        return;
      }
      doExecuteGenerated(data, spaceId);
    },
    [doExecuteGenerated],
  );

  // --- Refine the current artifact instead of generating a brand-new crew ---
  // Builds a single "editor" agent + task whose input is the previous artifact
  // plus the user's instruction, then runs it through the normal execution path
  // so the revised artifact streams straight back into the preview pane.
  const handleRefine = useCallback(
    async (instruction: string) => {
      const trimmed = instruction.trim();
      if (!trimmed) return;

      // Resolve the artifact currently shown (or last persisted) for this session.
      let artifact = useExecutionStore.getState().previewContent?.data || '';
      if (!artifact) {
        const sid = useSessionStore.getState().currentSessionId;
        if (sid) {
          const stored = await getSessionPreview(sid);
          artifact = stored?.data || '';
        }
      }
      if (!artifact) {
        addMessage(
          'assistant',
          'There is no result to refine yet. Run a crew first, then use the Refine button or `/refine <instruction>`.',
        );
        return;
      }

      addMessage('user', `Refine: ${trimmed}`);

      const editorAgents = [
        {
          id: 'refiner',
          role: 'Content Editor',
          goal: 'Revise the provided artifact according to the user instruction, preserving correctness and returning the complete updated artifact.',
          backstory:
            'You are an expert editor and front-end developer who refines documents and HTML, keeping the output valid, self-contained and ready to render.',
          tools: [],
          // Pin the editor to the user-selected model. Without an explicit llm
          // the backend defaults this hand-built agent to gpt-4o, which fails in
          // Databricks environments with no OpenAI key.
          ...(selectedModel ? { llm: selectedModel } : {}),
          // A refine is a single-shot edit, not a research crew. Disabling memory
          // (the only agent → disables crew memory entirely) skips the cognitive
          // memory search/save flow; no delegation keeps it to one LLM pass.
          memory: false,
          allow_delegation: false,
        },
      ];
      const editorTasks = [
        {
          id: 'refine_task',
          name: 'Refine artifact',
          agent_id: 'refiner',
          // The instruction and artifact are passed as crew inputs (below) and
          // referenced via {instruction}/{artifact} placeholders. CrewAI runs a
          // single-pass {var} interpolation over the description, so the artifact
          // must NOT be inlined: an HTML/CSS/JS artifact routinely contains brace
          // tokens (e.g. a JS template literal `${spread}` → `{spread}`) that
          // CrewAI would otherwise read as missing template variables and fail
          // ("Template variable 'spread' not found in inputs dictionary"). The
          // substituted input values are not re-scanned, so their braces are safe.
          description:
            `Improve the artifact below based on this instruction.\n\n` +
            `INSTRUCTION:\n{instruction}\n\n` +
            `CURRENT ARTIFACT:\n{artifact}\n\n` +
            `Return ONLY the complete revised artifact (e.g. the full HTML document) with no commentary and no markdown code fences.`,
          expected_output: 'The complete revised artifact, ready to render.',
          tools: [],
        },
      ];

      // doExecuteGenerated runs immediately (no variable-detection dialog).
      // preservePreview keeps the current artifact + history visible so the
      // refined version is appended (scroll back to compare), not wiped.
      doExecuteGenerated(
        { agents: editorAgents, tasks: editorTasks },
        undefined,
        { instruction: trimmed, artifact },
        { preservePreview: true },
      );
    },
    [addMessage, doExecuteGenerated, selectedModel],
  );

  const handleExecuteFlow = useCallback(
    async (flow: FlowData) => {
      // Capture the session ID NOW, before the async createExecution call.
      const originSessionId = useSessionStore.getState().currentSessionId;

      const execStore = useExecutionStore.getState();
      execStore.setIsLoading(true);
      try {
        execStore.setExecutionContext({
          crewName: flow.name || 'Flow',
          agents: [],
          tasks: [],
        });

        const flowConfig = buildFlowConfig(flow, selectedModel || undefined);
        const execution = await createExecution(flowConfig);
        const jobId = execution.job_id || execution.execution_id;
        if (jobId) {
          handleStartExecutionStream(jobId, originSessionId || undefined);
        } else {
          addMessage('assistant', 'Execution started but no job ID was returned.');
          execStore.setExecutionContext(null);
          execStore.setIsLoading(false);
        }
      } catch (error) {
        const errMsg = error instanceof Error ? error.message : 'Failed to start execution';
        addMessage('assistant', `Execution failed: ${errMsg}`);
        execStore.setExecutionContext(null);
        execStore.setIsLoading(false);
      }
    },
    [addMessage, handleStartExecutionStream, selectedModel],
  );

  // --- Variables dialog handlers ---
  const handleVariablesConfirm = useCallback(
    (inputs: Record<string, string>) => {
      setVariablesDialogOpen(false);
      const pending = pendingExecution;
      setPendingExecution(null);
      setDetectedVariables([]);

      if (!pending) return;
      if (pending.type === 'crew' && pending.plan) {
        doExecuteCrew(pending.plan, inputs);
      } else if (pending.type === 'generated' && pending.data) {
        doExecuteGenerated(pending.data, pending.spaceId, inputs);
      }
    },
    [pendingExecution, doExecuteCrew, doExecuteGenerated],
  );

  const handleVariablesCancel = useCallback(() => {
    setVariablesDialogOpen(false);
    setPendingExecution(null);
    setDetectedVariables([]);
  }, []);

  // --- Dispatcher ---
  const dispatcher = useDispatcher({
    addMessage,
    addMessageToTargetSession,
    updateMessage,
    updateMessageInTargetSession,
    onStartGenerationStream: handleStartGenerationStream,
    onStartExecutionStream: handleStartExecutionStream,
    onExecuteCrew: handleExecuteCrew,
    onExecuteFlow: handleExecuteFlow,
    onExecuteGenerated: handleExecuteGenerated,
    getCurrentSessionId: () => useSessionStore.getState().currentSessionId,
  });

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

      return false;
    },
    [addMessage, clearMessages, executionStream, handleRefine],
  );

  const handleSend = useCallback(
    async (message: string) => {
      const handled = await handleLocalCommand(message);
      if (handled) return;

      useExecutionStore.getState().setIsLoading(true);
      try {
        await dispatcher.sendMessage(message, selectedModel || undefined);
      } finally {
        useExecutionStore.getState().setIsLoading(false);
      }
    },
    [dispatcher, handleLocalCommand, selectedModel],
  );

  const handleStopExecution = useCallback(async () => {
    const execStore = useExecutionStore.getState();
    const activeExec = execStore.activeExecution;
    if (!activeExec?.jobId) return;
    try {
      await stopExecution(activeExec.jobId);
      executionStream.stopStream();
      addMessage('assistant', 'Execution stopped.');
      execStore.failExecution('Stopped by user');
    } catch (error) {
      const errMsg = error instanceof Error ? error.message : 'Failed to stop execution';
      addMessage('assistant', `Failed to stop: ${errMsg}`);
    }
  }, [addMessage, executionStream]);

  // --- Session switching ---
  const handleNewChat = useCallback(async () => {
    if (currentSessionId) {
      useExecutionStore.getState().saveSessionState(currentSessionId);
    }
    const newId = await useSessionStore.getState().createNewSession();
    useExecutionStore.getState().restoreSessionState(newId);
  }, [currentSessionId]);

  const handleSwitchSession = useCallback(async (sessionId: string) => {
    if (currentSessionId) {
      useExecutionStore.getState().saveSessionState(currentSessionId);
    }
    await useSessionStore.getState().switchSession(sessionId);
    useExecutionStore.getState().restoreSessionState(sessionId);
  }, [currentSessionId]);

  const handleDeleteSession = useCallback(async (sessionId: string) => {
    await useSessionStore.getState().deleteSession(sessionId);
    setContextMenu(null);
    useExecutionStore.getState().resetForSession();
  }, []);

  const handleStartRename = useCallback((sessionId: string, currentTitle: string) => {
    setRenamingSessionId(sessionId);
    setRenameValue(currentTitle);
    setContextMenu(null);
  }, []);

  const handleFinishRename = useCallback(async () => {
    if (renamingSessionId && renameValue.trim()) {
      await useSessionStore.getState().renameSession(renamingSessionId, renameValue.trim());
    }
    setRenamingSessionId(null);
    setRenameValue('');
  }, [renamingSessionId, renameValue]);

  return (
    <div id="kasal-chat-root" className="h-full w-full flex" style={{ backgroundColor: 'var(--bg-primary)' }}>
      {/* Sidebar */}
      {sidebarOpen && (
        <aside
          className="w-64 flex flex-col flex-shrink-0"
          style={{ backgroundColor: 'var(--bg-rail)' }}
        >
          {/* New Chat button — soft filled chip */}
          <div className="px-3 pt-3 pb-1">
            <button
              onClick={handleNewChat}
              className="kasal-newchat w-full flex items-center gap-2.5 px-3 py-2.5 rounded-xl text-sm font-medium"
              style={{
                backgroundColor: 'var(--bg-secondary)',
                color: 'var(--text-primary)',
              }}
            >
              <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
                <path strokeLinecap="round" strokeLinejoin="round" d="M12 4.5v15m7.5-7.5h-15" />
              </svg>
              New Chat
            </button>
          </div>

          {/* Section label */}
          {sessions.length > 0 && (
            <div className="px-3 pt-4 pb-1">
              <span
                className="text-[10px] font-semibold uppercase tracking-wider"
                style={{ color: 'var(--text-muted)' }}
              >
                Recent
              </span>
            </div>
          )}

          {/* Session list */}
          <div className="flex-1 overflow-y-auto px-2 pb-2">
            {sessions.map((s) => {
              const isActive = s.id === currentSessionId;
              return (
              <div key={s.id} className="relative">
                {renamingSessionId === s.id ? (
                  <input
                    autoFocus
                    value={renameValue}
                    onChange={(e) => setRenameValue(e.target.value)}
                    onBlur={handleFinishRename}
                    onKeyDown={(e) => {
                      if (e.key === 'Enter') handleFinishRename();
                      if (e.key === 'Escape') { setRenamingSessionId(null); setRenameValue(''); }
                    }}
                    className="kasal-rename-input w-full px-3 py-2 my-0.5 rounded-lg text-sm"
                    style={{
                      backgroundColor: 'var(--bg-input)',
                      color: 'var(--text-primary)',
                      border: '1px solid var(--border-color)',
                    }}
                  />
                ) : (
                  <div
                    className="kasal-session flex items-center gap-1 rounded-lg my-0.5 group"
                    style={{
                      backgroundColor: isActive ? 'var(--bg-active-chip)' : 'transparent',
                    }}
                  >
                    <button
                      onClick={() => handleSwitchSession(s.id)}
                      onContextMenu={(e) => {
                        e.preventDefault();
                        setContextMenu({ sessionId: s.id, x: e.clientX, y: e.clientY });
                      }}
                      className="flex-1 text-left pl-3 pr-1 py-2 text-sm truncate min-w-0"
                      style={{
                        color: isActive ? 'var(--text-primary)' : 'var(--text-secondary)',
                      }}
                      title={s.title}
                    >
                      <div className="flex items-center gap-1.5">
                        <SessionSpinner sessionId={s.id} />
                        <div className={`kasal-session-title truncate text-[13px] ${isActive ? 'font-medium' : ''}`}>{s.title}</div>
                      </div>
                    </button>
                    {/* Kebab menu button */}
                    <button
                      onClick={(e) => {
                        e.stopPropagation();
                        const rect = (e.target as HTMLElement).getBoundingClientRect();
                        setContextMenu({ sessionId: s.id, x: rect.right, y: rect.bottom });
                      }}
                      className="flex-shrink-0 w-7 h-7 mr-1.5 rounded-md flex items-center justify-center opacity-0 group-hover:opacity-100 transition-opacity hover:bg-[var(--bg-rail-hover)]"
                      style={{ color: 'var(--text-muted)' }}
                      title="Options"
                    >
                      <svg className="w-4 h-4" fill="currentColor" viewBox="0 0 24 24">
                        <circle cx="12" cy="6" r="1.5" />
                        <circle cx="12" cy="12" r="1.5" />
                        <circle cx="12" cy="18" r="1.5" />
                      </svg>
                    </button>
                  </div>
                )}
              </div>
              );
            })}
          </div>

          {/* Context menu */}
          {contextMenu && (
            <>
              <div className="fixed inset-0 z-40" onClick={() => setContextMenu(null)} />
              <div
                className="kasal-popover fixed z-50 rounded-xl overflow-hidden py-1"
                style={{
                  left: contextMenu.x,
                  top: contextMenu.y,
                  backgroundColor: 'var(--bg-input)',
                  border: '1px solid var(--border-color)',
                }}
              >
                <button
                  onClick={() => {
                    const session = sessions.find((s) => s.id === contextMenu.sessionId);
                    if (session) handleStartRename(session.id, session.title);
                  }}
                  className="w-full text-left px-4 py-2 text-sm transition-colors hover:opacity-80"
                  style={{ color: 'var(--text-primary)' }}
                >
                  Rename
                </button>
                <button
                  onClick={() => handleDeleteSession(contextMenu.sessionId)}
                  className="w-full text-left px-4 py-2 text-sm transition-colors hover:opacity-80"
                  style={{ color: '#ef4444' }}
                >
                  Delete
                </button>
              </div>
            </>
          )}
        </aside>
      )}

      {/* Main content — chat panel */}
      {!(chatCollapsed && previewContent) && (
        <main className="flex-1 flex flex-col overflow-hidden relative" style={{ flex: previewContent ? '1 1 50%' : '1 1 100%' }}>
          {/* The sidebar toggle + Databricks wordmark now live in the app top bar
              (ChatModeHeaderSlot), so the main area no longer renders its own
              header — this keeps it vertically stable when the sidebar toggles. */}

          {/* Reopen preview button — shown when a preview was closed but data persists */}
          {hasHiddenPreview && !previewContent && (
            <div className="absolute bottom-20 right-4 z-10">
              <button
                onClick={() => useExecutionStore.getState().reopenPreview()}
                className="flex items-center gap-1.5 px-3 py-2 rounded-xl text-xs font-medium shadow-lg transition-all hover:scale-[1.02] active:scale-[0.98]"
                style={{
                  backgroundColor: 'var(--bg-secondary)',
                  color: 'var(--text-primary)',
                  border: '1px solid var(--border-color)',
                }}
                title="Reopen preview panel"
              >
                <svg className="w-3.5 h-3.5" style={{ color: 'var(--accent)' }} fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
                  <path strokeLinecap="round" strokeLinejoin="round" d="M17.25 6.75L22.5 12l-5.25 5.25m-10.5 0L1.5 12l5.25-5.25m7.5-3l-4.5 16.5" />
                </svg>
                Show preview
              </button>
            </div>
          )}

          {/* Chat container */}
          <div className="flex-1 overflow-hidden">
            <ChatContainer
              messages={messages}
              onSend={handleSend}
              onCommand={handleSend}
              onExecuteCrew={handleExecuteCrew}
              onExecuteFlow={handleExecuteFlow}
              onExecuteGenerated={handleExecuteGenerated}
              onStopExecution={handleStopExecution}
              isLoading={isLoading}
              isExecuting={isExecuting}
              isGenerating={isGenerating}
              executionContext={executionContext}
              models={models}
              selectedModel={selectedModel}
              onModelChange={(m) => useAppStore.getState().setSelectedModel(m)}
            />
          </div>
        </main>
      )}

      {/* Preview panel — right side */}
      {previewContent && (
        <PreviewPanel
          key={currentSessionId}
          content={previewContent}
          onClose={() => useExecutionStore.getState().clearPreview()}
          chatCollapsed={chatCollapsed}
          onToggleChat={() => useExecutionStore.getState().toggleChatCollapsed()}
          onRefine={handleRefine}
          history={previewHistory}
          index={previewIndex}
          onNavigate={navigatePreview}
        />
      )}

      {/* Input variables dialog */}
      <InputVariablesDialog
        open={variablesDialogOpen}
        variables={detectedVariables}
        onConfirm={handleVariablesConfirm}
        onCancel={handleVariablesCancel}
      />
    </div>
  );
};

/** Tiny component to show spinner for sessions with active executions */
const SessionSpinner: React.FC<{ sessionId: string }> = ({ sessionId }) => {
  const hasActive = useExecutionStore((s) => s.hasActiveExecution(sessionId));
  if (!hasActive) return null;
  return (
    <div
      className="w-2 h-2 rounded-full border border-t-transparent animate-spin flex-shrink-0"
      style={{ borderColor: 'var(--accent)', borderTopColor: 'transparent' }}
    />
  );
};

export default ChatWorkspace;
