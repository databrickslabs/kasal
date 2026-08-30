import React, { useState, useCallback, useEffect, useRef } from 'react';
import { useChatRunStream } from './hooks/useChatRunStream';
import { useRunActivity } from './hooks/useRunActivity';
import { useChatCommands } from './hooks/useChatCommands';
import { useChatSessionActions } from './hooks/useChatSessionActions';
import { useChatLibraryActions } from './hooks/useChatLibraryActions';
import { useChatExecutionActions } from './hooks/useChatExecutionActions';
import { useSessionStore } from './store/sessionStore';
import { useExecutionStore } from './store/executionStore';
import { useAppStore } from './store/appStore';
import { useDispatcher } from './hooks/useDispatcher';
import { startGenerationStream } from './utils/generationStreamManager';
import { GenerationCompleteData } from './types/dispatcher';
import ChatContainer from './components/Chat/ChatContainer';
import CatalogLibrary from './components/CatalogLibrary';
import ScheduleLibrary from './components/ScheduleLibrary';
import CollapsedRail from './components/CollapsedRail';
import PreviewPanel from './components/Preview/PreviewPanel';
import PreviewSkeleton, { shouldShowPreviewSkeleton } from './components/Preview/PreviewSkeleton';
import { useThemeStore } from '../../store/theme';
import ChatMcpDialog from './components/Chat/ChatMcpDialog';
import './chat.css';



const ChatWorkspace: React.FC = () => {
  // --- Zustand Stores ---
  const sessions = useSessionStore((s) => s.sessions);
  const currentSessionId = useSessionStore((s) => s.currentSessionId);
  const messages = useSessionStore((s) => s.messages);
  // True until init() finishes restoring a persisted session — holds the empty
  // "new chat" greeting so a refresh doesn't flash it before the chat loads.
  const hydrating = useSessionStore((s) => s.hydrating);
  const addMessage = useSessionStore((s) => s.addMessage);
  const addMessageToTargetSession = useSessionStore((s) => s.addMessageToTargetSession);
  const updateMessage = useSessionStore((s) => s.updateMessage);
  const updateMessageInTargetSession = useSessionStore((s) => s.updateMessageInTargetSession);

  const isExecuting = useExecutionStore((s) => s.isExecuting);
  const isGenerating = useExecutionStore((s) => s.isGenerating);
  const isLoading = useExecutionStore((s) => s.isLoading);
  const executionContext = useExecutionStore((s) => s.executionContext);
  const activeExecution = useExecutionStore((s) => s.activeExecution);
  const rawPreviewContent = useExecutionStore((s) => s.previewContent);
  const previewOwnerSessionId = useExecutionStore((s) => s.previewOwnerSessionId);
  const previewHistory = useExecutionStore((s) => s.previewHistory);
  const previewIndex = useExecutionStore((s) => s.previewIndex);
  const navigatePreview = useExecutionStore((s) => s.navigatePreview);
  // The side preview pane is opt-in: closed by default even when a deliverable
  // exists (it renders inline in the chat), opened via a surface's "expand"
  // control or the reopen pill.
  const previewPaneOpen = useExecutionStore((s) => s.previewPaneOpen);
  const chatCollapsed = useExecutionStore((s) => s.chatCollapsed);
  // Where the run activity ("thinking" stream) is shown: the preview pane (default)
  // or collapsed into the chat's "Working…" bar (expandable). A persisted choice.
  const activityPlacement = useExecutionStore((s) => s.activityPlacement);
  const activityInChat = activityPlacement === 'chat';
  // Memory mode (workspace vs session) is owned by the store so it persists
  // across the empty→conversation input swap (local state would reset to ON).
  const memoryEnabled = useExecutionStore((s) => s.memoryEnabled);
  const setMemoryEnabled = useExecutionStore((s) => s.setMemoryEnabled);

  // Render-time isolation guard: only show a preview that belongs to the
  // session currently on screen. This is the backstop that prevents a preview
  // produced by a job in another session (e.g. a late SSE completion after the
  // user switched chats) from leaking into the session being viewed.
  const previewContent =
    rawPreviewContent && previewOwnerSessionId === currentSessionId
      ? rawPreviewContent
      : null;

  // Execution UI (the "Running crew…" banner, generation spinner, loading
  // state) belongs to the session that OWNS the run. A run started in one
  // session must never surface in whatever session is on screen now — e.g. you
  // submit in chat A, switch to B, and A's crew starts: it must stay in A.
  // Strict equality so a run owned by another session never leaks here.
  const executionOwnerSessionId = useExecutionStore((s) => s.executionOwnerSessionId);
  const ownsExecution = executionOwnerSessionId === currentSessionId;
  const viewIsExecuting = isExecuting && ownsExecution;
  const viewIsGenerating = isGenerating && ownsExecution;
  const viewIsLoading = isLoading && ownsExecution;
  const viewExecutionContext = ownsExecution ? executionContext : null;


  // Run activity timeline + focus state. See hooks/useRunActivity.ts.
  const {
    handleShowRunInPane,
    latestRunJobId,
    focusedRunJobId,
    setFocusedRunJobId,
    focusedRunStep,
    setFocusedRunStep,
  } = useRunActivity({ viewIsExecuting });


  // When the user routes activity to the pane ('preview' placement), the pane
  // shows the run-activity surface. It appears immediately during a live run
  // (shouldShowPreviewSkeleton) instead of staying blank, AND it survives the
  // prompt ending: once the run finishes we still have its steps, so the
  // expanded activity keeps showing rather than vanishing. In 'chat' placement
  // the activity lives in the chat's Working bar, so the pane stays out until a
  // real deliverable exists. A finished deliverable (previewContent) always wins
  // the pane — the skeleton never competes with it.
  // The pane is OPT-IN: it expands only when the user has opened it
  // (previewPaneOpen). A live run no longer force-expands the pane — run
  // activity stays in the chat's Working bar until the user opens the pane.
  // The run the pane shows: a pinned one wins; while a run is in flight the live
  // one wins over the last id seen in the transcript (which still points at the
  // PREVIOUS run until this run's id-carrying message arrives).
  const paneRunJobId =
    focusedRunJobId ?? (viewIsExecuting ? (activeExecution?.jobId ?? latestRunJobId) : latestRunJobId);

  const showPreviewSkeleton =
    previewPaneOpen &&
    !activityInChat &&
    !previewContent &&
    (shouldShowPreviewSkeleton({ runActive: viewIsExecuting, hasPreview: !!previewContent }) ||
      Boolean(paneRunJobId));
  // Opt-in: the deliverable pane shows only when the user opened it (a deliverable
  // alone no longer forces it open). The run skeleton still shows when activity is
  // routed to the pane.
  const previewPaneVisible = (previewPaneOpen && !!previewContent) || showPreviewSkeleton;

  const models = useAppStore((s) => s.models);
  const selectedModel = useAppStore((s) => s.selectedModel);

  const sidebarOpen = useAppStore((s) => s.sidebarOpen);

  // Saved-catalog library shown in the rail (replaces /list crews & /list flows).
  // Lives in the Zustand appStore so it's shared + refreshed consistently.
  const libraryCrews = useAppStore((s) => s.savedCrews);
  const libraryFlows = useAppStore((s) => s.savedFlows);
  const refreshLibrary = useAppStore((s) => s.loadCatalog);
  // A crew/flow loaded from the catalog that the chat submit button will run.
  // Session-scoped so it only applies to the session it was loaded into.
  const [pendingRun, setPendingRun] = useState<{ sessionId: string | null; label: string; run: () => void } | null>(null);
  // MCP config dialog opened from the composer's "+" picker ("Connect a tool").
  const [mcpConfigOpen, setMcpConfigOpen] = useState(false);

  // Sync chat theme from Kasal's theme store (dark-mode toggle).
  const kasalIsDarkMode = useThemeStore((s) => s.isDarkMode);
  // The chat-scoped theme (drives the sidebar dark-mode toggle — flips instantly,
  // no page reload, persisted to localStorage by appStore).
  const chatThemeIsDark = useAppStore((s) => s.theme) === 'dark';
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

  // Chat sessions are per workspace. When the user switches workspace (the
  // group store fires 'group-changed'), re-list sessions for the new group and
  // rehydrate that group's active session — so the sidebar + chat only ever
  // show the current workspace's conversations.
  useEffect(() => {
    const onGroupChange = () => {
      void useSessionStore.getState().reloadForGroup().then(() => {
        const sid = useSessionStore.getState().currentSessionId;
        if (sid) {
          useExecutionStore.getState().restoreSessionState(sid);
        } else {
          useExecutionStore.getState().resetForSession();
        }
      });
      void refreshLibrary();
    };
    window.addEventListener('group-changed', onGroupChange);
    return () => window.removeEventListener('group-changed', onGroupChange);
  }, [refreshLibrary]);

  // Populate the catalog library (rail) on mount. It's refreshed on workspace
  // change (above), after each chat save (handleSaveCrew / /save), and after
  // agent-builder saves (SaveCrew calls useAppStore.getState().loadCatalog()).
  useEffect(() => {
    void refreshLibrary();
  }, [refreshLibrary]);

  // Origin session per in-flight generation, keyed by generationId. Generations
  // run as concurrent streams, so every trace / completion / execution-start
  // routes by the generation's OWN origin — never a single global owner, which
  // cross-contaminated run-activity traces between parallel sessions.
  const genOriginRef = useRef<Map<string, string>>(new Map());
  // The most recent generated crew in this session — the target for `/save`.
  // (The bookmark on each crew card saves its own specific crew directly.)
  const lastGeneratedRef = useRef<GenerationCompleteData | null>(null);
  // The chat prompt that triggered the in-flight generation — attached to the
  // generation result so the executed run answers the user's actual request.
  const lastUserPromptRef = useRef<string>('');


  // The bookmark/feedback actions row for the latest generated crew, parked
  // until that crew's run finishes — feedback only makes sense once the
  // result is visible. Cleared on post; a refine run never sets it.
  const pendingActionsRef = useRef<{ data: GenerationCompleteData; ownerSession: string | null; mode?: string; usedWorkspaceMemory?: boolean; capability?: string } | null>(null);

  // The run event stream (SSE wiring, trace -> messages, completion and
  // reconnect handling) lives in its own hook — the JSX never touched any of
  // it, and it owns its own bookkeeping refs. See hooks/useChatRunStream.ts.
  const { executionStream, handleStartExecutionStream } = useChatRunStream({
    pendingActionsRef,
  });

  // Run entry points (crew / generated / flow / refine) and the
  // variable-detection gate in front of them. See hooks/useChatExecutionActions.ts.
  const {
    handleExecuteCrew,
    handleExecuteGenerated,
    handleRefine,
    handleExecuteFlow,
    handleVariablesSubmit,
  } = useChatExecutionActions({ handleStartExecutionStream });


  // --- Generation Stream ---
  // Generation steps fold into the SAME collapsible run-activity element as
  // tool calls (no crew card in the conversation): each step posts a trace
  // entry, and the only interactive remnant is the Genie-space prompt when a
  // crew needs one. Final output renders in the preview pane as usual.
  const addGenerationTrace = useCallback((ownerSession: string | undefined, label: string, sublabel?: string) => {
    const sessionStore = useSessionStore.getState();
    const extra = {
      resultType: 'trace',
      resultData: {
        label,
        ...(sublabel ? { sublabel } : {}),
        source: 'generation',
        kind: 'event',
        timestamp: Date.now(),
      },
    };
    if (ownerSession) sessionStore.addMessageToTargetSession(ownerSession, 'assistant', '', extra);
    else sessionStore.addMessage('assistant', '', extra);
  }, []);

  // Post a rich crew-detail card into the chat as each agent/task is generated,
  // so the chatbox shows the FULL details (agent goal + backstory, task
  // description + expected output) — not just a terse "ready" tick. ChatMessage
  // renders resultType 'agent'/'task' as AgentCard/TaskCard. Routes to the
  // generating session like addGenerationTrace.
  const addGenerationCard = useCallback(
    (ownerSession: string | undefined, resultType: 'agent' | 'task', resultData: unknown) => {
      if (!resultData) return;
      const sessionStore = useSessionStore.getState();
      const extra = { resultType, resultData };
      if (ownerSession) sessionStore.addMessageToTargetSession(ownerSession, 'assistant', '', extra);
      else sessionStore.addMessage('assistant', '', extra);
    },
    [],
  );

  // The origin session of a generation. handleStartGenerationStream always
  // registers it before any event arrives, so the map is the source of truth;
  // the global-owner fallback is a safety net only (it never fires in the real
  // flow, where genId is always registered).
  const ownerForGen = useCallback(
    (generationId: string) =>
      genOriginRef.current.get(generationId)
      ?? useExecutionStore.getState().executionOwnerSessionId
      ?? undefined,
    [],
  );

  // The plan produced by each in-flight generation, keyed by generationId, so
  // execution-start can show the right crew even when several runs overlap.
  const genDataRef = useRef<Map<string, GenerationCompleteData>>(new Map());


  const handleStartGenerationStream = useCallback(
    (generationId: string, sessionId: string) => {
      const origin = sessionId || useSessionStore.getState().currentSessionId;
      // Tie this generation to its origin so all its events route there, even if
      // the user switches sessions (or starts other generations) before it ends.
      if (origin) genOriginRef.current.set(generationId, origin);
      useExecutionStore.getState().startGeneration(origin || undefined);
      // Observe via the module-level manager (not a React hook): concurrent-safe
      // and independent of this component's render lifecycle, like the execution
      // side. Callbacks are passed per-call and route by the generation's origin.
      startGenerationStream(generationId, {
        onPlanReady: (genId, plan) => {
          const owner = ownerForGen(genId);
          const agents = Array.isArray(plan?.agents) ? (plan.agents as unknown[]).length : 0;
          const tasks = Array.isArray(plan?.tasks) ? (plan.tasks as unknown[]).length : 0;
          addGenerationTrace(owner, 'Crew planned', `${agents} agent${agents === 1 ? '' : 's'} · ${tasks} task${tasks === 1 ? '' : 's'}`);
        },
        onAgentDetail: (genId, agent) => {
          // Render the full agent card (role · goal · backstory · tools) in chat.
          addGenerationCard(ownerForGen(genId), 'agent', agent);
        },
        onTaskDetail: (genId, task) => {
          // Render the full task card (description · expected output · tools) in chat.
          addGenerationCard(ownerForGen(genId), 'task', task);
        },
        onComplete: (genId, raw: GenerationCompleteData) => {
          // Route by THIS generation's own origin — never a global owner, which a
          // parallel session's run may hold. The crew is generated AND run on the
          // BACKEND now (auto-execute); the frontend just records the plan and the
          // backend folds the execution id into this event (see onExecutionStarted).
          const ownerSession = ownerForGen(genId);
          const data = raw;
          genDataRef.current.set(genId, data);
          // Park the actions row (bookmark + thumbs feedback) — it posts only
          // AFTER the run's result comes back, so users rate what they've seen.
          pendingActionsRef.current = {
            data,
            ownerSession: ownerSession ?? null,
            mode: useExecutionStore.getState().chatModeType,
            // memoryEnabled === true means the run used Workspace memory (false =
            // session-only). Snapshot it now so a later toggle can't change it.
            usedWorkspaceMemory: useExecutionStore.getState().memoryEnabled,
            // Which published capability answered, when this was a routed run.
            // Persisted on the message so the BACKEND router can see, next
            // turn, that a capability is mid-conversation.
            capability: useExecutionStore.getState().routedCapability ?? undefined,
          };
          dispatcher.setLastGenerated(data);
          lastGeneratedRef.current = data; // /save target
          useExecutionStore.getState().completeGeneration(ownerSession ?? undefined);
        },
        onExecutionStarted: (genId, executionId) => {
          // The backend launched the run; observe it under the session that asked
          // for it (origin), even if the user has since switched sessions.
          const ownerSession = ownerForGen(genId);
          const data = genDataRef.current.get(genId);
          // Only drive the live crew display when the owner is on screen — a
          // backgrounded run must not overwrite the viewed session's context.
          const viewingOwner = !ownerSession
            || ownerSession === useSessionStore.getState().currentSessionId;
          if (data && viewingOwner) {
            useExecutionStore.getState().setExecutionContext({
              crewName: 'Generated Crew',
              agents: (data.agents || []).map((a) => ({
                name: (a.name as string) || (a.role as string) || 'Agent',
                role: (a.role as string) || undefined,
              })),
              tasks: (data.tasks || []).map((t) => ({
                name: (t.name as string) || (t.description as string)?.slice(0, 40) || 'Task',
              })),
            });
          }
          handleStartExecutionStream(executionId, ownerSession ?? undefined);
          genOriginRef.current.delete(genId);
          genDataRef.current.delete(genId);
        },
        onFailed: (genId, error) => {
          useExecutionStore.getState().failGeneration(error, ownerForGen(genId));
          genOriginRef.current.delete(genId);
          genDataRef.current.delete(genId);
        },
      });
    },
    // `dispatcher` is intentionally not a dep: useDispatcher consumes this
    // callback (onStartGenerationStream), so depending on it here would be a
    // declaration cycle. Its methods (setLastGenerated) are stable useCallbacks,
    // and this runs only after dispatcher is initialized.
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [ownerForGen, addGenerationTrace, addGenerationCard, handleStartExecutionStream],
  );


  // Saving a crew / answer back to the workspace. See hooks/useChatLibraryActions.ts.
  const { handleSaveCrew, handleSaveAnswerToCatalog } = useChatLibraryActions();


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
    onCrewLoaded: (plan, sessionId) =>
      setPendingRun({ sessionId, label: plan.name || 'crew', run: () => handleExecuteCrew(plan) }),
    onFlowLoaded: (flow, sessionId) =>
      setPendingRun({ sessionId, label: flow.name || 'flow', run: () => handleExecuteFlow(flow) }),
    getCurrentSessionId: () => useSessionStore.getState().currentSessionId,
    ensureSession: () => useSessionStore.getState().ensureSession(),
  });


  // Composer input: slash commands, send, load-from-library, stop.
  // See hooks/useChatCommands.ts.
  const { handleSend, handleLoadFromLibrary, handleStopExecution } = useChatCommands({
    dispatcher,
    executionStream,
    handleRefine,
    lastGeneratedRef,
    lastUserPromptRef,
    setPendingRun,
  });

  /**
   * "Use existing" matched nothing — build one instead, at the answer mode the
   * user already had selected.
   *
   * Deliberately a user action, not a fallback. Silently generating here would
   * run a full crew nobody asked for; this flips the source back and re-sends
   * the SAME prompt, so the only thing that changed is the one choice they just
   * made.
   */
  const handleBuildInstead = useCallback(
    (messageId: string) => {
      const all = useSessionStore.getState().messages;
      const index = all.findIndex((m) => m.id === messageId);
      // The prompt that produced this answer is the nearest user message above
      // it — reading `lastUserPrompt` instead would re-send whatever was typed
      // most recently, which after a session switch is a different question.
      const prompt = all
        .slice(0, index === -1 ? all.length : index)
        .reverse()
        .find((m) => m.role === 'user')?.content;
      useExecutionStore.getState().setPreferExisting(false);
      if (prompt) void handleSend(prompt);
    },
    [handleSend],
  );


  // Session list: new / switch / delete / rename. See hooks/useChatSessionActions.ts.
  const {
    handleNewChat,
    handleSwitchSession,
    handleDeleteSession,
    handleStartRename,
    handleFinishRename,
    renamingSessionId,
    setRenamingSessionId,
    renameValue,
    setRenameValue,
    contextMenu,
    setContextMenu,
  } = useChatSessionActions({ setPendingRun });


  return (
    <div id="kasal-chat-root" className="kasal-chat-root h-full w-full flex">
      {/* Sidebar — collapses to a slim icon rail, never fully disappears */}
      {!sidebarOpen && <CollapsedRail onNewChat={handleNewChat} />}
      {sidebarOpen && (
        <aside
          className="w-64 flex flex-col flex-shrink-0"
          style={{ backgroundColor: 'var(--bg-rail)' }}
        >
          {/* Icon-only header row — just "+" (new chat); the sidebar toggle is
              the one fixed control in the top bar (SidebarToggle). */}
          <div className="px-3 pt-3 pb-1 flex items-center justify-end">
            <button
              type="button"
              onClick={handleNewChat}
              className="w-9 h-9 rounded-xl flex-shrink-0 flex items-center justify-center transition-colors hover:bg-[var(--bg-rail-hover)]"
              style={{ color: 'var(--text-secondary)' }}
              aria-label="New chat"
            >
              <svg className="w-[18px] h-[18px]" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
                <path strokeLinecap="round" strokeLinejoin="round" d="M12 4.5v15m7.5-7.5h-15" />
              </svg>
            </button>
          </div>

          {/* Saved catalog library (Crews / Flows) — replaces /list commands */}
          <CatalogLibrary
            crews={libraryCrews}
            flows={libraryFlows}
            onLoadCrew={(name) => handleLoadFromLibrary('crew', name)}
            onLoadFlow={(name) => handleLoadFromLibrary('flow', name)}
          />

          {/* Schedules — created from a run's clock action, managed here */}
          <ScheduleLibrary />

          {/* Section label */}
          {sessions.length > 0 && (
            <div className="px-3 pt-4 pb-1.5">
              <span
                className="text-[11px] font-semibold uppercase tracking-[0.08em]"
                style={{ color: 'var(--text-muted)' }}
              >
                Recent
              </span>
            </div>
          )}

          {/* Session list — generous bottom padding so the last row keeps a bit of
              breathing room and never sits flush against the sidebar's edge. */}
          <div className="flex-1 overflow-y-auto px-2 pb-6">
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
                    className="kasal-rename-input w-full pl-5 pr-3 py-1.5 my-0.5 rounded-lg text-[13px]"
                    style={{
                      backgroundColor: 'var(--bg-input)',
                      color: 'var(--text-primary)',
                      border: '1px solid var(--border-color)',
                    }}
                  />
                ) : (
                  <div
                    className="kasal-session flex items-center rounded-lg group my-0.5"
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
                      className="flex-1 flex items-center gap-2 text-left min-w-0"
                      // Padding is set INLINE, not via Tailwind `pl-*`/`py-*`: the
                      // global `#kasal-chat-root button { padding: 0 }` reset uses an
                      // ID selector that out-specifies the class-scoped utilities, so
                      // a `pl-5` on a <button> is silently overridden. Inline wins.
                      style={{
                        color: isActive ? 'var(--text-primary)' : 'var(--text-secondary)',
                        padding: '6px 4px 6px 14px',
                      }}
                      title={s.title}
                    >
                      <SessionSpinner sessionId={s.id} />
                      <span className={`kasal-session-title truncate text-[13px] ${isActive ? 'font-semibold' : 'font-medium'}`}>{s.title}</span>
                    </button>
                    {/* Kebab menu button */}
                    <button
                      onClick={(e) => {
                        e.stopPropagation();
                        const rect = (e.target as HTMLElement).getBoundingClientRect();
                        setContextMenu({ sessionId: s.id, x: rect.right, y: rect.bottom });
                      }}
                      className="flex-shrink-0 w-6 h-6 mr-1.5 rounded-md flex items-center justify-center opacity-0 group-hover:opacity-100 transition-opacity hover:bg-[var(--bg-rail-hover)]"
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

          {/* Sidebar footer — dark-mode toggle, pinned at the bottom. A divider +
              padding above it leaves clear space between the scrolling session
              list and the toggle. */}
          <div
            className="flex-shrink-0 px-2 pt-2 pb-3 mt-1"
            style={{ borderTop: '1px solid var(--border-color)' }}
          >
            <button
              onClick={() => useAppStore.getState().toggleTheme()}
              className="w-full flex items-center gap-2.5 px-3 py-2 rounded-lg text-[13px] font-medium transition-colors hover:bg-[var(--bg-rail-hover)]"
              style={{ color: 'var(--text-secondary)' }}
              title={chatThemeIsDark ? 'Switch to light mode' : 'Switch to dark mode'}
              aria-label={chatThemeIsDark ? 'Switch to light mode' : 'Switch to dark mode'}
            >
              {chatThemeIsDark ? (
                // Sun — currently dark, click for light
                <svg className="w-4 h-4 flex-shrink-0" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={1.8}>
                  <circle cx="12" cy="12" r="4" />
                  <path strokeLinecap="round" strokeLinejoin="round" d="M12 2v2m0 16v2M4.93 4.93l1.41 1.41m11.32 11.32l1.41 1.41M2 12h2m16 0h2M4.93 19.07l1.41-1.41m11.32-11.32l1.41-1.41" />
                </svg>
              ) : (
                // Moon — currently light, click for dark
                <svg className="w-4 h-4 flex-shrink-0" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={1.8}>
                  <path strokeLinecap="round" strokeLinejoin="round" d="M21 12.79A9 9 0 1 1 11.21 3 7 7 0 0 0 21 12.79z" />
                </svg>
              )}
              {chatThemeIsDark ? 'Light mode' : 'Dark mode'}
            </button>
          </div>

          {/* Context menu */}
          {contextMenu && (
            <>
              <div data-testid="context-menu-backdrop" className="fixed inset-0 z-40" onClick={() => setContextMenu(null)} />
              <div
                className="kasal-popover fixed z-50 rounded-2xl overflow-hidden p-1.5 shadow-xl"
                style={{
                  left: contextMenu.x,
                  top: contextMenu.y,
                  minWidth: 190,
                  backgroundColor: 'var(--bg-input)',
                  border: '1px solid var(--border-color)',
                }}
              >
                <button
                  onClick={() => {
                    const session = sessions.find((s) => s.id === contextMenu.sessionId);
                    if (session) handleStartRename(session.id, session.title);
                  }}
                  className="w-full flex items-center gap-3 text-left !px-3.5 !py-2.5 text-[13.5px] font-medium rounded-xl transition-colors hover:bg-[var(--bg-rail-hover)]"
                  style={{ color: 'var(--text-primary)' }}
                >
                  <svg className="w-[18px] h-[18px] flex-shrink-0" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={1.7}>
                    <path strokeLinecap="round" strokeLinejoin="round" d="M16.862 4.487l1.687-1.688a1.875 1.875 0 112.652 2.652L10.582 16.07a4.5 4.5 0 01-1.897 1.13L6 18l.8-2.685a4.5 4.5 0 011.13-1.897l8.932-8.931zM19.5 7.125L16.875 4.5" />
                  </svg>
                  Rename
                </button>
                <button
                  onClick={() => handleDeleteSession(contextMenu.sessionId)}
                  className="w-full flex items-center gap-3 text-left !px-3.5 !py-2.5 text-[13.5px] font-medium rounded-xl transition-colors hover:bg-[rgba(239,68,68,0.10)] hover:!text-[#ef4444]"
                  style={{ color: 'var(--text-primary)' }}
                >
                  <svg className="w-[18px] h-[18px] flex-shrink-0" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={1.7}>
                    <path strokeLinecap="round" strokeLinejoin="round" d="M14.74 9l-.346 9m-4.788 0L9.26 9m9.968-3.21c.342.052.682.107 1.022.166m-1.022-.165L18.16 19.673a2.25 2.25 0 01-2.244 2.077H8.084a2.25 2.25 0 01-2.244-2.077L4.772 5.79m14.456 0a48.108 48.108 0 00-3.478-.397m-12 .562c.34-.059.68-.114 1.022-.165m0 0a48.11 48.11 0 013.478-.397m7.5 0v-.916c0-1.18-.91-2.164-2.09-2.201a51.964 51.964 0 00-3.32 0c-1.18.037-2.09 1.022-2.09 2.201v.916m7.5 0a48.667 48.667 0 00-7.5 0" />
                  </svg>
                  Delete
                </button>
              </div>
            </>
          )}
        </aside>
      )}

      {/* Main content — chat panel */}
      {/* Chat hides full-screen ONLY for a real deliverable the user collapsed to;
          the build skeleton never hides chat — the activity must stay visible. */}
      {!(chatCollapsed && previewPaneOpen && previewContent) && (
        <main className="flex-1 flex flex-col overflow-hidden relative" style={{ flex: previewPaneVisible ? '1 1 50%' : '1 1 100%' }}>
          {/* No header bar of its own: the sidebar toggle lives in the sidebar /
              collapsed rail, keeping the main area vertically stable. */}

          {/* Chat container — the reopen-preview pill is rendered inside it,
              anchored above the composer, so it never overlaps the input. */}
          <div className="flex-1 overflow-hidden">
            <ChatContainer
              messages={messages}
              hydrating={hydrating}
              onSend={handleSend}
              onCommand={handleSend}
              onExecuteCrew={handleExecuteCrew}
              onExecuteFlow={handleExecuteFlow}
              onExecuteGenerated={handleExecuteGenerated}
              onSaveCrew={handleSaveCrew}
              onSaveAnswerToCatalog={handleSaveAnswerToCatalog}
              onSubmitVariables={handleVariablesSubmit}
              onBuildInstead={handleBuildInstead}
              onStopExecution={handleStopExecution}
              isLoading={viewIsLoading}
              isExecuting={viewIsExecuting}
              isGenerating={viewIsGenerating}
              executionContext={viewExecutionContext}
              // The timeline always lives here, in the chat. The pane on the
              // right is where a clicked step's content opens — showing the
              // list in both places put the same rows on both halves.
              hideLiveTimeline={false}
              // The run in flight. A segment's own job id comes from a message,
              // and the message that carries it lands part-way through the run —
              // without this the live segment has no run to open until then.
              liveJobId={activeExecution?.jobId}
              onShowRunInPane={handleShowRunInPane}
              models={models}
              selectedModel={selectedModel}
              onModelChange={(m) => useAppStore.getState().setSelectedModel(m)}
              sessionId={currentSessionId}
              memoryEnabled={memoryEnabled}
              onMemoryEnabledChange={setMemoryEnabled}
              pendingRunLabel={pendingRun && pendingRun.sessionId === currentSessionId ? pendingRun.label : undefined}
              onRunPending={() => {
                if (pendingRun && pendingRun.sessionId === currentSessionId) {
                  const run = pendingRun.run;
                  setPendingRun(null);
                  run();
                }
              }}
              onOpenMcpConfig={() => setMcpConfigOpen(true)}
            />
          </div>
        </main>
      )}

      {/* Preview panel — right side. Opt-in: shown only when the user opened it. */}
      {previewPaneOpen && previewContent && (
        <PreviewPanel
          key={currentSessionId}
          content={previewContent}
          onClose={() => { setFocusedRunJobId(null); setFocusedRunStep(null); useExecutionStore.getState().clearPreview(); }}
          chatCollapsed={chatCollapsed}
          onToggleChat={() => useExecutionStore.getState().toggleChatCollapsed()}
          onRefine={handleRefine}
          onStyleChange={(data) => useExecutionStore.getState().updatePreviewData(data)}
          history={previewHistory}
          index={previewIndex}
          onNavigate={navigatePreview}
          // A clicked step ROW pre-opens that step's content in the pane.
          focusStep={focusedRunStep}
          onMoveActivityToChat={() => { setFocusedRunJobId(null); setFocusedRunStep(null); useExecutionStore.getState().setActivityPlacement('chat'); }}
        />
      )}

      {/* Preview skeleton — the single run monitor (a clickable step timeline)
          shown WHILE the viewed session's run builds its deliverable (no preview
          yet). Mutually exclusive with PreviewPanel. */}
      {showPreviewSkeleton && (
        <PreviewSkeleton
          running={viewIsExecuting}
          focusStep={focusedRunStep}
          onMoveActivityToChat={() => {
            // Collapse activity back to the chat bar AND close the pane — the
            // skeleton only ever shows when there's no deliverable, so leaving the
            // pane "open" would let the next deliverable auto-expand it (the pane
            // must open only on a manual expand click).
            const st = useExecutionStore.getState();
            setFocusedRunJobId(null);
            setFocusedRunStep(null);
            st.setActivityPlacement('chat');
            st.clearPreview();
          }}
        />
      )}

      {/* Chat-native MCP dialog — opened from the composer picker's "Connect a
          tool" action. Styled with chat tokens (not the MUI config dialog, which
          stays for the Agent Builder). The picker refetches its list on reopen,
          so a server enabled here shows up next time without extra wiring. */}
      <ChatMcpDialog
        open={mcpConfigOpen}
        onClose={() => setMcpConfigOpen(false)}
      />

    </div>
  );
};

/** Tiny component to show spinner for sessions with active executions */
const SessionSpinner: React.FC<{ sessionId: string }> = ({ sessionId }) => {
  const hasActive = useExecutionStore((s) => s.hasActiveExecution(sessionId));
  if (!hasActive) return null;
  // A clearly-visible accent ring (was an 8px hairline that read as a static dot)
  // so an in-progress session is obvious at a glance in the list.
  return (
    <span
      role="status"
      aria-label="Running"
      title="Running…"
      className="w-3.5 h-3.5 rounded-full border-2 border-t-transparent animate-spin flex-shrink-0"
      style={{ borderColor: 'var(--accent)', borderTopColor: 'transparent' }}
    />
  );
};

export default ChatWorkspace;
