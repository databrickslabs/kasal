import React, { useState, useCallback, useEffect, useRef } from 'react';
import { useChatRunStream } from './hooks/useChatRunStream';
import { useRunActivity } from './hooks/useRunActivity';
import { useChatCommands } from './hooks/useChatCommands';
import { useChatLibraryActions } from './hooks/useChatLibraryActions';
import { useChatExecutionActions } from './hooks/useChatExecutionActions';
import { useSessionStore } from '../../app/sessions/sessionStore';
import { useExecutionStore } from './store/executionStore';
import { useAppStore } from './store/appStore';
import { useDispatcher } from './hooks/useDispatcher';
import { startGenerationStream } from './utils/generationStreamManager';
import { GenerationCompleteData } from './types/dispatcher';
import ChatContainer from './components/Chat/ChatContainer';
import PreviewPanel from './components/Preview/PreviewPanel';
import PreviewSkeleton, { shouldShowPreviewSkeleton } from './components/Preview/PreviewSkeleton';
import { useUILayoutStore } from '../../store/uiLayout';
import { useThemeStore } from '../../store/theme';
import ChatMcpDialog from './components/Chat/ChatMcpDialog';
import './chat.css';



const ChatWorkspace: React.FC<{ onOpenSettings?: () => void }> = () => {
  // --- Zustand Stores ---
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


  // Saved-catalog library shown in the rail (replaces /list crews & /list flows).
  // Lives in the Zustand appStore so it's shared + refreshed consistently.
  const refreshLibrary = useAppStore((s) => s.loadCatalog);
  // A crew/flow loaded from the catalog that the chat submit button will run.
  // Session-scoped so it only applies to the session it was loaded into.
  const [pendingRun, setPendingRun] = useState<{ sessionId: string | null; label: string; run: () => void } | null>(null);
  // MCP config dialog opened from the composer's "+" picker ("Connect a tool").
  const [mcpConfigOpen, setMcpConfigOpen] = useState(false);

  const chatThemeIsDark = useThemeStore((s) => s.isDarkMode);

  // --- Initialize stores on mount ---
  useEffect(() => {
    useAppStore.getState().init();
    useAppStore.getState().loadModels();
    useAppStore.getState().loadTools();

  }, []);

  // Chat sessions are per workspace. When the user switches workspace (the
  // group store fires 'group-changed'), re-list sessions for the new group and
  // rehydrate that group's active session — so the sidebar + chat only ever
  // show the current workspace's conversations.
  useEffect(() => {
    const onGroupChange = () => {
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
  const pendingActionsRef = useRef<{ data: GenerationCompleteData; ownerSession: string | null;
    jobId?: string | null; mode?: string; usedWorkspaceMemory?: boolean; capability?: string } | null>(null);

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
            // The auto-executed run's id rides on generation_complete; a
            // generate-only turn has none yet and binds when its stream starts.
            jobId: ((raw as { execution_id?: string }).execution_id as string) || null,
            mode: 'chat',
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
   * "Use existing" matched nothing — answer the same prompt directly.
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


  // The common sidebar owns navigation; catalog loading still uses Chat's
  // existing execution/variable handling.
  useEffect(() => {
    const load = (event: Event) => {
      const { kind, name } = (event as CustomEvent<{ kind: 'crew' | 'flow'; name: string }>).detail;
      if (useUILayoutStore.getState().appMode === 'chat') handleLoadFromLibrary(kind, name);
    };
    window.addEventListener('sessionCatalogLoad', load);
    return () => window.removeEventListener('sessionCatalogLoad', load);
  }, [handleLoadFromLibrary]);

  return (
    <div id="kasal-chat-root" data-theme={chatThemeIsDark ? 'dark' : 'light'} className="kasal-chat-root h-full w-full flex">
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

export default ChatWorkspace;
