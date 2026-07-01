import { create } from 'zustand';
import { persist } from 'zustand/middleware';
import { ExecutionStatus } from '../types/execution';
import { ExecutionContext } from '../components/Chat/ChatContainer';
import { PreviewContent, parsePreviewContent } from '../components/Preview/PreviewPanel';
import { useSessionStore } from './sessionStore';
import {
  saveSessionPreview,
  getSessionPreview,
  getSessionMessages,
} from '../db/sessionApi';
import {
  persistActiveExecution,
  clearActiveExecution,
} from './activeExecutionMarker';
import { deriveSessionPreviews } from '../utils/sessionPreview';
import type { Surface } from '../../../shared/a2ui';

interface SessionExecSnapshot {
  activeExecution: { jobId: string; status: ExecutionStatus } | null;
  isExecuting: boolean;
  isGenerating: boolean;
  isLoading: boolean;
  executionContext: ExecutionContext | null;
  previewContent: PreviewContent | null;
  previewHistory?: PreviewContent[];
  previewIndex?: number;
}

export interface ExecutionLogEntry {
  id: string;
  timestamp: number;
  kind: 'trace' | 'task_output' | 'status';
  label: string;
  detail?: string;
}

interface ExecutionState {
  activeExecution: { jobId: string; status: ExecutionStatus } | null;
  isExecuting: boolean;
  isGenerating: boolean;
  isLoading: boolean;
  executionContext: ExecutionContext | null;
  previewContent: PreviewContent | null;
  /**
   * The session the currently-held `previewContent` belongs to. The preview
   * pane must only render when this matches the session being viewed —
   * otherwise a late SSE callback (or a re-render during a session switch) from
   * a job started in another session can surface that session's preview against
   * the one you switched to. Gating render on this is what isolates previews.
   */
  previewOwnerSessionId: string | null;
  /**
   * Every previewable task output produced by the current run, in order. The
   * preview pane shows `previewContent` (the item at `previewIndex`) and lets
   * the user page back/forward through earlier task outputs. Defaults to the
   * latest (the final task's output shows first).
   */
  previewHistory: PreviewContent[];
  previewIndex: number;
  /**
   * Whether the side preview pane is OPEN. Decoupled from `previewContent`: a
   * deliverable is still captured into `previewContent`/`previewHistory` while the
   * pane stays closed, so it renders inline in the chat by default and the user
   * opens the pane on demand (the per-surface "expand" control → `openPreviewPane`,
   * or the reopen pill → `reopenPreview`). Reset on session switch.
   */
  previewPaneOpen: boolean;
  /**
   * The chat message id whose surface is currently shown in the pane (set by the
   * per-surface "expand" control). The inline chat copy of THAT message hides
   * while the pane shows it, so a deliverable isn't visible in two places. null
   * when the pane was opened by something other than a surface expand.
   */
  previewSourceMessageId: string | null;
  chatCollapsed: boolean;
  executionOwnerSessionId: string | null;
  executionLog: ExecutionLogEntry[];
  /**
   * "Workspace memory" recall scope for the next run. true (default) = recall
   * workspace-wide; false = restrict recall to this chat session only. Lives in
   * the store (not ChatInput local state) so the choice survives the
   * empty→conversation input swap and any remount — otherwise toggling
   * "Session only" silently reverts to workspace on the next message.
   */
  workspaceMemory: boolean;
  /**
   * Whether crews run WITH semantic memory. This is the flag the composer's
   * memory pill toggles: true = "Workspace memory" (semantic memory on), false
   * (default) = "Session memory" (semantic memory off; recall comes only from
   * this chat's history). Session-only is the default so a new chat doesn't pull
   * in unrelated workspace history unless the user opts in. Lives in the store
   * for the same persistence reason as ``workspaceMemory``.
   */
  memoryEnabled: boolean;
  /**
   * ChatMode answer mode chosen in the chat input's mode pill:
   *   'chat'     – a single light agent (Agent.kickoff_async), fast, no crew;
   *   'research' – a full crew with reasoning;
   *   'deep'     – a full crew with planning + reasoning.
   * Persisted (like ``memoryEnabled``) so the choice survives a reload.
   */
  chatModeType: 'chat' | 'research' | 'deep';
  /**
   * MCP servers (Kasal server NAMES) selected via the chat input's "+" picker.
   * At execution time these are injected into every generated agent's
   * tool_configs.MCP_SERVERS so the crew gets the servers' tools. Lives in the
   * store for the same persistence reason as ``workspaceMemory``.
   */
  selectedMcpServers: string[];
  /**
   * Agent Bricks serving-endpoint names picked in the chat "+" menu. Each
   * equips the generated agents with the AgentBricksTool configured for that
   * endpoint (tool_configs.AgentBricksTool.endpointName). Stored for the same
   * persistence reason as ``selectedMcpServers``.
   */
  selectedAgentBricksEndpoints: string[];
  /**
   * Epoch ms when the current execution started (or null when idle). Lives in
   * the store — not the skeleton's local state — so the "Running agent…" elapsed
   * timer reflects the true run duration and survives switching away and back.
   */
  runStartedAt: number | null;
  /**
   * In-flight job id per session id. The Zustand store is a singleton that
   * survives session switches (unlike the per-session snapshot, which the live
   * slot can lose), so this is the source of truth for "does this session have a
   * running job?" when you switch BACK to it — the switch handler reads it to
   * re-attach the run and bring the monitoring back. NOT persisted (a stale
   * reload must not resurrect a dead run — refresh reconnect uses the IndexedDB
   * marker instead); cleared when the run finalizes.
   */
  runningJobBySession: Record<string, string>;
  /**
   * Where the run-activity (the "thinking" stream) is shown: 'chat' (the default —
   * collapsed into the chat's "Working…" bar, expandable to the same stream) or
   * 'preview' (the right preview pane). Defaults to 'chat' so the preview pane stays
   * closed until the user opens it. A user preference — persisted like the other
   * chat toggles.
   */
  activityPlacement: 'preview' | 'chat';
}

interface ExecutionActions {
  setIsLoading: (loading: boolean) => void;
  setExecutionContext: (ctx: ExecutionContext | null) => void;
  setPreviewContent: (content: PreviewContent | null) => void;
  /** Replace the CURRENT preview's data in place (no new history entry) and
   *  persist it. Used by the in-preview "Customize" panel for deterministic
   *  restyles, which edit the artifact you're viewing rather than appending a
   *  new version. */
  updatePreviewData: (data: string) => void;
  navigatePreview: (index: number) => void;
  /** Open the side preview pane, optionally focused on a specific surface (the
   *  per-surface "expand" control passes the clicked content; it's focused in
   *  history, appended if not already there). `sourceMessageId` is the chat
   *  message the surface came from, so its inline copy can hide while the pane
   *  shows it. With no argument, opens the pane on the current/last preview. */
  openPreviewPane: (content?: PreviewContent, sourceMessageId?: string) => void;
  setChatCollapsed: (collapsed: boolean) => void;
  toggleChatCollapsed: () => void;
  setWorkspaceMemory: (value: boolean) => void;
  setMemoryEnabled: (value: boolean) => void;
  setChatModeType: (mode: 'chat' | 'research' | 'deep') => void;
  toggleMcpServer: (name: string) => void;
  setSelectedMcpServers: (names: string[]) => void;
  toggleAgentBricksEndpoint: (name: string) => void;
  setSelectedAgentBricksEndpoints: (names: string[]) => void;
  setActivityPlacement: (placement: 'preview' | 'chat') => void;
  clearPreview: () => void;
  reopenPreview: () => void;
  appendLog: (entry: Omit<ExecutionLogEntry, 'id' | 'timestamp'>) => void;
  clearLog: () => void;

  // Execution lifecycle
  startExecution: (jobId: string, sessionId?: string, opts?: { preservePreview?: boolean }) => void;
  updateExecutionStatus: (status: ExecutionStatus) => void;
  // jobId routes the completion to the session that OWNS that job, so a run
  // finishing in a backgrounded session (parallel sessions) lands in the right
  // place instead of the single global slot. Omitting it keeps the legacy
  // single-run behavior (route by the current live owner).
  completeExecution: (resultText: string, jobId?: string, surface?: Surface) => void;
  failExecution: (error: string, jobId?: string) => void;
  /** The session that started a still-tracked job (parallel-session routing). */
  jobOwnerOf: (jobId: string) => string | null;
  /** Drop a job's owner mapping (e.g. when a reconnect finalizes it directly). */
  clearJobOwner: (jobId: string) => void;
  /**
   * Abandon a tracked job whose execution row no longer exists for this
   * workspace (deleted, or it belongs to a group you no longer have selected).
   * Unlike failExecution this posts NO chat message — the run isn't a failure,
   * it's just gone — it only drops the running banner/Stop button AND the durable
   * IndexedDB reconnect marker, so the trace poller and the refresh-reconnect
   * stop resurrecting a dead job and looping 404s. Idempotent (no-op once the
   * job is untracked / already finalized).
   */
  abandonExecution: (jobId: string) => void;

  // Generation lifecycle
  startGeneration: (sessionId?: string) => void;
  completeGeneration: (ownerSessionId?: string) => void;
  failGeneration: (error: string, ownerSessionId?: string) => void;

  // Session-aware state management
  saveSessionState: (sessionId: string) => void;
  restoreSessionState: (sessionId: string) => void;
  hasActiveExecution: (sessionId: string) => boolean;
  /**
   * Park a preview into a BACKGROUNDED session's snapshot (and its history)
   * without touching the live slot. Task outputs of a run whose session isn't
   * on screen reach the UI this way, so the preview is there on switch-back.
   */
  stashSessionPreview: (sessionId: string, preview: PreviewContent) => void;

  // Reset
  resetForSession: () => void;
}

type ExecutionStore = ExecutionState & ExecutionActions;

// Per-session snapshots stored outside Zustand to avoid re-renders
const sessionSnapshots = new Map<string, SessionExecSnapshot>();

// jobId -> owning sessionId, for PARALLEL-SESSION completion routing. The store
// holds a single live "view" slot; this map lets a job that finishes while its
// session is backgrounded resolve its real owner (and land its result/preview
// in that session's snapshot) instead of being dropped or misrouted. Lives
// outside Zustand (pure routing data, no re-render needed), like sessionSnapshots.
const jobOwners = new Map<string, string>();

// Load a session's preview into the live slot when there's no in-memory
// snapshot. Single source of truth: derive each run's deliverable from its
// stored execution.result (survives navigating away mid-run), falling back to
// the legacy persisted preview copy for sessions whose runs predate executionId
// stamping. Shared by restoreSessionState and reopenPreview. No-ops if the user
// navigates away while the (cached) results are fetched.
async function loadDerivedOrStoredPreview(
  sessionId: string,
  set: (partial: Partial<ExecutionStore>) => void,
): Promise<void> {
  const stillHere = () =>
    useSessionStore.getState().currentSessionId === sessionId;
  let history: PreviewContent[] = [];
  try {
    const msgs = await getSessionMessages(sessionId);
    history = (await deriveSessionPreviews(msgs)).history;
  } catch {
    /* fall through to the legacy persisted preview */
  }
  if (!stillHere()) return;
  if (history.length) {
    set({
      previewContent: history[history.length - 1],
      previewOwnerSessionId: sessionId,
      previewHistory: history,
      previewIndex: history.length - 1,
    });
    return;
  }
  const stored = await getSessionPreview(sessionId);
  if (stored && stillHere()) {
    const content: PreviewContent = {
      type: stored.type as PreviewContent['type'],
      data: stored.data,
      title: stored.title,
    };
    set({
      previewContent: content,
      previewOwnerSessionId: sessionId,
      previewHistory: [content],
      previewIndex: 0,
    });
  }
}

export const useExecutionStore = create<ExecutionStore>()(
  persist(
    (set, get) => ({
  // --- State ---
  activeExecution: null,
  isExecuting: false,
  isGenerating: false,
  isLoading: false,
  executionContext: null,
  previewContent: null,
  previewOwnerSessionId: null,
  previewHistory: [],
  previewIndex: 0,
  previewPaneOpen: false,
  previewSourceMessageId: null,
  chatCollapsed: false,
  executionOwnerSessionId: null,
  executionLog: [],
  workspaceMemory: true,
  memoryEnabled: false,
  chatModeType: 'chat',
  selectedMcpServers: [],
  selectedAgentBricksEndpoints: [],
  runStartedAt: null,
  runningJobBySession: {},
  // 'chat' by default so the run-activity stream shows inline in the chat and the
  // side preview pane stays closed until the user opens it (per-surface "expand").
  activityPlacement: 'chat',

  setActivityPlacement: (placement) => set({ activityPlacement: placement }),

  appendLog: (entry) => set((s) => ({
    executionLog: [
      ...s.executionLog,
      {
        ...entry,
        id: `${Date.now()}-${Math.random().toString(36).slice(2, 8)}`,
        timestamp: Date.now(),
      },
    ],
  })),
  clearLog: () => set({ executionLog: [] }),

  // --- Basic setters ---
  setIsLoading: (loading) => set({ isLoading: loading }),
  setExecutionContext: (ctx) => set({ executionContext: ctx }),
  // Stamp the preview with the session currently being viewed so the pane can
  // gate rendering on ownership (see previewOwnerSessionId), and append it to
  // the session's preview history so earlier task outputs stay browsable.
  setPreviewContent: (content) =>
    set((s) => {
      if (!content) {
        return { previewContent: null, previewOwnerSessionId: null, previewPaneOpen: false };
      }
      const last = s.previewHistory[s.previewHistory.length - 1];
      const isDup = last && last.type === content.type && last.data === content.data;
      const previewHistory = isDup ? s.previewHistory : [...s.previewHistory, content];
      return {
        previewContent: content,
        previewOwnerSessionId: useSessionStore.getState().currentSessionId,
        previewHistory,
        previewIndex: previewHistory.length - 1,
      };
    }),
  // Deterministically restyle the current artifact: swap its data in place,
  // both in the live slot and in the history entry it occupies, and persist to
  // the owning session so the restyle survives a reload. No new history entry —
  // a Look change edits the version you're viewing, it isn't a new revision.
  updatePreviewData: (data) =>
    set((s) => {
      if (!s.previewContent) return {};
      const updated: PreviewContent = { ...s.previewContent, data };
      const previewHistory = s.previewHistory.slice();
      // Replace the entry the user is viewing, if it's a real history slot.
      if (previewHistory[s.previewIndex]) {
        previewHistory[s.previewIndex] = updated;
      }
      const owner = s.previewOwnerSessionId;
      if (owner) {
        void saveSessionPreview(owner, { type: updated.type, data: updated.data, title: updated.title });
      }
      return { previewContent: updated, previewHistory };
    }),
  // Page back/forward through the captured task-output previews.
  navigatePreview: (index) =>
    set((s) => {
      if (index < 0 || index >= s.previewHistory.length) return {};
      return { previewContent: s.previewHistory[index], previewIndex: index };
    }),
  // Open the side pane, optionally focusing a specific surface. The per-surface
  // "expand" control passes the clicked content (focused in history, appended if
  // new); with no argument it opens on the current/last preview.
  openPreviewPane: (content, sourceMessageId) =>
    set((s) => {
      const sessionId = useSessionStore.getState().currentSessionId;
      const sourceId = sourceMessageId ?? null;
      if (!content) {
        if (s.previewContent) return { previewPaneOpen: true, previewSourceMessageId: sourceId };
        if (s.previewHistory.length) {
          const idx =
            s.previewIndex >= 0 && s.previewIndex < s.previewHistory.length
              ? s.previewIndex
              : s.previewHistory.length - 1;
          return {
            previewPaneOpen: true,
            previewSourceMessageId: sourceId,
            previewContent: s.previewHistory[idx],
            previewIndex: idx,
            previewOwnerSessionId: sessionId,
          };
        }
        return { previewPaneOpen: true, previewSourceMessageId: sourceId };
      }
      const existingIdx = s.previewHistory.findIndex(
        (p) => p.type === content.type && p.data === content.data,
      );
      const previewHistory =
        existingIdx >= 0 ? s.previewHistory : [...s.previewHistory, content];
      const previewIndex = existingIdx >= 0 ? existingIdx : previewHistory.length - 1;
      return {
        previewPaneOpen: true,
        previewSourceMessageId: sourceId,
        previewContent: previewHistory[previewIndex],
        previewOwnerSessionId: sessionId,
        previewHistory,
        previewIndex,
      };
    }),
  setChatCollapsed: (collapsed) => set({ chatCollapsed: collapsed }),
  toggleChatCollapsed: () => set((s) => ({ chatCollapsed: !s.chatCollapsed })),
  setWorkspaceMemory: (value) => set({ workspaceMemory: value }),
  setMemoryEnabled: (value) => set({ memoryEnabled: value }),
  setChatModeType: (mode) => set({ chatModeType: mode }),
  toggleMcpServer: (name) =>
    set((s) => ({
      selectedMcpServers: s.selectedMcpServers.includes(name)
        ? s.selectedMcpServers.filter((n) => n !== name)
        : [...s.selectedMcpServers, name],
    })),
  setSelectedMcpServers: (names) => set({ selectedMcpServers: names }),
  toggleAgentBricksEndpoint: (name) =>
    set((s) => ({
      selectedAgentBricksEndpoints: s.selectedAgentBricksEndpoints.includes(name)
        ? s.selectedAgentBricksEndpoints.filter((n) => n !== name)
        : [...s.selectedAgentBricksEndpoints, name],
    })),
  setSelectedAgentBricksEndpoints: (names) => set({ selectedAgentBricksEndpoints: names }),
  clearPreview: () => {
    // Close the pane only — keep previewContent/history so the user can reopen
    // instantly (the deliverable still renders inline in the chat).
    set({ previewPaneOpen: false, chatCollapsed: false });
  },

  reopenPreview: () => {
    const sessionId = useSessionStore.getState().currentSessionId;
    if (!sessionId) return;
    const s0 = get();
    // Fast path: content is still held (we keep it on close) or in history.
    if (s0.previewContent) {
      set({ previewPaneOpen: true });
      return;
    }
    if (s0.previewHistory.length) {
      const idx =
        s0.previewIndex >= 0 && s0.previewIndex < s0.previewHistory.length
          ? s0.previewIndex
          : s0.previewHistory.length - 1;
      set({
        previewContent: s0.previewHistory[idx],
        previewOwnerSessionId: sessionId,
        previewPaneOpen: true,
      });
      return;
    }
    // History was dropped (e.g. after a page reload): open the pane and derive the
    // content from each run's stored execution.result (single source), legacy
    // persisted preview as fallback.
    set({ previewPaneOpen: true });
    void loadDerivedOrStoredPreview(sessionId, set);
  },

  // --- Execution lifecycle ---
  startExecution: (jobId, sessionId, opts) => {
    const owner = sessionId || useSessionStore.getState().currentSessionId;
    // Remember which session owns this job so its completion routes correctly
    // even if the user switches away and another session's run takes the slot.
    if (owner) jobOwners.set(jobId, owner);
    // Persist so a page refresh can reconnect to this still-running job.
    if (owner) persistActiveExecution(owner, jobId);
    // Zustand source of truth for switch-back detection (survives session
    // switches in memory; not persisted — refresh reconnect uses the marker).
    if (owner) set((s) => ({ runningJobBySession: { ...s.runningJobBySession, [owner]: jobId } }));
    // A refine continues the same artifact lineage, so it keeps the existing
    // preview + history and just appends the revised output. A fresh run clears
    // the previous preview so an unrelated prompt doesn't inherit stale output.
    const preserve = opts?.preservePreview;
    const currentSessionId = useSessionStore.getState().currentSessionId;
    const isViewingOwner = !owner || owner === currentSessionId;
    if (isViewingOwner) {
      const s = get();
      set({
        executionOwnerSessionId: owner,
        isExecuting: true,
        isLoading: true,
        activeExecution: { jobId, status: 'running' },
        previewContent: preserve ? s.previewContent : null,
        previewOwnerSessionId: preserve ? s.previewOwnerSessionId : null,
        previewHistory: preserve ? s.previewHistory : [],
        previewIndex: preserve ? s.previewIndex : 0,
        runStartedAt: Date.now(),
        executionLog: [],
      });
    } else {
      // Backgrounded run (started for a session that isn't on screen — e.g. a
      // generation that finished after you switched away). Park a RUNNING
      // snapshot so switching to that session restores its Stop button + tracker,
      // and leave the live slot (the viewed session) untouched. Completion still
      // routes by job owner via the global poller.
      const prev = sessionSnapshots.get(owner);
      sessionSnapshots.set(owner, {
        activeExecution: { jobId, status: 'running' },
        isExecuting: true,
        isGenerating: false,
        isLoading: true,
        executionContext: prev?.executionContext ?? null,
        previewContent: preserve ? prev?.previewContent ?? null : null,
        previewHistory: preserve ? prev?.previewHistory ?? [] : [],
        previewIndex: preserve ? prev?.previewIndex ?? 0 : 0,
      });
    }
  },

  updateExecutionStatus: (status) => {
    set((s) => ({
      activeExecution: s.activeExecution
        ? { ...s.activeExecution, status }
        : null,
    }));
  },

  completeExecution: (resultText: string, jobId?: string, surface?: Surface) => {
    const state = get();
    // Route to the job's OWNER (parallel sessions), falling back to the single
    // live owner for the legacy no-jobId path.
    const ownerSession = (jobId ? jobOwners.get(jobId) : undefined) ?? state.executionOwnerSessionId;
    // Idempotency: a tracked job finalizes exactly once. A duplicate event
    // (SSE + poller, or a late re-poll) is a no-op so it can't double-post.
    if (jobId) {
      if (!jobOwners.has(jobId)) return;
      jobOwners.delete(jobId);
    }
    const currentSessionId = useSessionStore.getState().currentSessionId;
    const isViewingOwner = currentSessionId === ownerSession;
    const sessionStore = useSessionStore.getState();
    // Anchor this run's message to its execution so the preview pane can derive
    // the deliverable from execution.result on demand (survives navigating away).
    // A composed A2UI surface rides ALONG with the message as resultType:'a2ui'
    // so it renders INLINE in the chat by default (the preview pane is opt-in),
    // and persists for free through packExtras like any other rich-card message.
    const runExtra =
      jobId || surface
        ? {
            ...(jobId ? { executionId: jobId } : {}),
            ...(surface ? { resultType: 'a2ui', resultData: surface } : {}),
          }
        : undefined;

    // Run is over — drop the persisted reconnect marker.
    if (ownerSession) clearActiveExecution(ownerSession);
    // Run finalized — drop the switch-back detection entry for this session.
    if (ownerSession) set((s) => {
      if (!(ownerSession in s.runningJobBySession)) return {};
      const next = { ...s.runningJobBySession };
      delete next[ownerSession];
      return { runningJobBySession: next };
    });

    // Parse preview content if any
    let preview: PreviewContent | null = null;
    if (resultText) {
      preview = parsePreviewContent(resultText);
      // A composed A2UI surface is the canonical rich rendering and MUST render
      // inline on the message (it carries its own "expand" control). When one
      // exists, never divert to the opt-in (hidden) preview pane just because the
      // raw answer text ALSO looks previewable — that path posts no message, so
      // `runExtra` (the inline surface) would be silently dropped. This bit deep
      // mode specifically: planning produces a structured deck `text` that trips
      // parsePreviewContent, while research's conversational text falls through to
      // the inline path — so the presentation showed for research but not deep.
      if (preview && !surface) {
        // Only surface the preview pane if the user is currently viewing the
        // session that owns this execution — otherwise it would leak the
        // owner session's HTML into whatever session is on screen now.
        if (isViewingOwner) {
          set((s) => {
            const last = s.previewHistory[s.previewHistory.length - 1];
            const isDup = last && last.type === preview!.type && last.data === preview!.data;
            const previewHistory = isDup ? s.previewHistory : [...s.previewHistory, preview!];
            return {
              previewContent: preview,
              previewOwnerSessionId: ownerSession,
              previewHistory,
              previewIndex: previewHistory.length - 1,
            };
          });
        }
        // Persist preview to IndexedDB so it survives page refreshes
        if (ownerSession) {
          saveSessionPreview(ownerSession, preview);
        }
      } else {
        // Route text message to the correct session. When a composed surface
        // renders the SAME previewable artifact (e.g. a presentation deck whose
        // markdown `text` IS the deck), drop the bulky raw text so only the
        // inline surface shows — otherwise the deck would print twice (raw
        // markdown above the rendered deck). Plain-prose answers keep their text.
        const body = surface && preview ? '' : resultText;
        if (ownerSession) {
          sessionStore.addMessageToTargetSession(
            ownerSession,
            'assistant',
            body,
            runExtra,
          );
        } else {
          sessionStore.addMessage('assistant', body, runExtra);
        }
      }
    } else {
      if (ownerSession) {
        sessionStore.addMessageToTargetSession(
          ownerSession,
          'assistant',
          'Execution completed.',
          runExtra,
        );
      } else {
        sessionStore.addMessage('assistant', 'Execution completed.', runExtra);
      }
    }

    if (isViewingOwner) {
      set({
        activeExecution: state.activeExecution
          ? { ...state.activeExecution, status: 'completed' }
          : null,
        isExecuting: false,
        executionContext: null,
        isLoading: false,
        executionOwnerSessionId: null,
        // Keep the live feed so the finished preview can show the run timeline
        // collapsed above the result; just stop the elapsed timer.
        runStartedAt: null,
      });
      if (ownerSession) sessionSnapshots.delete(ownerSession);
    } else if (ownerSession) {
      // Finalize the backgrounded session's snapshot for switch-back. Preserve
      // any preview already parked by this run's task outputs (those reached
      // only the snapshot, never the live slot, while the session was off
      // screen) — the final result often carries NO preview, so overwriting
      // here is exactly what dropped the run's app on switch-back.
      const prevSnap = sessionSnapshots.get(ownerSession);
      const prevHistory = prevSnap?.previewHistory ?? [];
      let nextHistory = prevHistory;
      if (preview) {
        const last = prevHistory[prevHistory.length - 1];
        nextHistory = last && last.type === preview.type && last.data === preview.data
          ? prevHistory
          : [...prevHistory, preview];
      }
      const nextPreview = preview ?? prevSnap?.previewContent ?? null;
      sessionSnapshots.set(ownerSession, {
        activeExecution: null,
        isExecuting: false,
        isGenerating: false,
        isLoading: false,
        executionContext: null,
        previewContent: nextPreview,
        previewHistory: nextHistory,
        previewIndex: Math.max(0, nextHistory.length - 1),
      });
      // Only clear the live slot's owner if the job that finished is the one
      // it holds — a DIFFERENT (backgrounded) session finishing must not blank
      // the currently-viewed session's running banner.
      set((s) => (s.executionOwnerSessionId === ownerSession ? { executionOwnerSessionId: null } : {}));
    }
  },

  failExecution: (error: string, jobId?: string) => {
    const state = get();
    const ownerSession = (jobId ? jobOwners.get(jobId) : undefined) ?? state.executionOwnerSessionId;
    if (jobId) {
      if (!jobOwners.has(jobId)) return; // already finalized — no-op
      jobOwners.delete(jobId);
    }
    const currentSessionId = useSessionStore.getState().currentSessionId;
    const isViewingOwner = currentSessionId === ownerSession;
    const sessionStore = useSessionStore.getState();

    // Run is over — drop the persisted reconnect marker.
    if (ownerSession) clearActiveExecution(ownerSession);
    // Run finalized — drop the switch-back detection entry for this session.
    if (ownerSession) set((s) => {
      if (!(ownerSession in s.runningJobBySession)) return {};
      const next = { ...s.runningJobBySession };
      delete next[ownerSession];
      return { runningJobBySession: next };
    });

    if (ownerSession) {
      sessionStore.addMessageToTargetSession(
        ownerSession,
        'assistant',
        `Execution failed: ${error}`,
      );
    } else {
      sessionStore.addMessage('assistant', `Execution failed: ${error}`);
    }

    if (isViewingOwner) {
      set({
        activeExecution: state.activeExecution
          ? { ...state.activeExecution, status: 'failed' }
          : null,
        isExecuting: false,
        executionContext: null,
        isLoading: false,
        executionOwnerSessionId: null,
        runStartedAt: null,
      });
      if (ownerSession) sessionSnapshots.delete(ownerSession);
    } else if (ownerSession) {
      // Keep any preview the run produced before failing so switch-back still
      // shows partial output rather than a blank pane. Leave history/index
      // undefined when there was no prior snapshot — restore fills the defaults.
      const prevSnap = sessionSnapshots.get(ownerSession);
      sessionSnapshots.set(ownerSession, {
        activeExecution: null,
        isExecuting: false,
        isGenerating: false,
        isLoading: false,
        executionContext: null,
        previewContent: prevSnap?.previewContent ?? null,
        previewHistory: prevSnap?.previewHistory,
        previewIndex: prevSnap?.previewIndex,
      });
      // Only clear the live slot's owner if it belongs to the job that failed —
      // a backgrounded session failing must not blank the viewed session.
      set((s) => (s.executionOwnerSessionId === ownerSession ? { executionOwnerSessionId: null } : {}));
    }
  },

  abandonExecution: (jobId: string) => {
    // Untracked or already finalized — nothing to do (keeps double calls, e.g.
    // the reconnect backstop AND a late poller 'jobNotFound', a clean no-op).
    if (!jobId || !jobOwners.has(jobId)) return;
    const ownerSession = jobOwners.get(jobId)!;
    jobOwners.delete(jobId);

    // Drop the durable reconnect marker + switch-back entry so neither a page
    // refresh nor a session switch re-detects and re-polls this dead job.
    clearActiveExecution(ownerSession);
    set((s) => {
      if (!(ownerSession in s.runningJobBySession)) return {};
      const next = { ...s.runningJobBySession };
      delete next[ownerSession];
      return { runningJobBySession: next };
    });

    // Clear the running banner / Stop button. If the dead job holds the live
    // slot, reset it; otherwise scrub the backgrounded session's snapshot so a
    // switch-back doesn't restore a stale "running" state for a job that's gone.
    const state = get();
    const ownsLiveSlot =
      state.executionOwnerSessionId === ownerSession ||
      state.activeExecution?.jobId === jobId;
    if (ownsLiveSlot) {
      set({
        activeExecution: null,
        isExecuting: false,
        isLoading: false,
        executionContext: null,
        executionOwnerSessionId: null,
        runStartedAt: null,
      });
      sessionSnapshots.delete(ownerSession);
    } else {
      const prevSnap = sessionSnapshots.get(ownerSession);
      if (prevSnap) {
        sessionSnapshots.set(ownerSession, {
          ...prevSnap,
          activeExecution: null,
          isExecuting: false,
          isGenerating: false,
          isLoading: false,
        });
      }
    }
  },

  // --- Generation lifecycle ---
  startGeneration: (sessionId) => {
    const owner = sessionId || useSessionStore.getState().currentSessionId;
    set({
      executionOwnerSessionId: owner,
      isGenerating: true,
      isLoading: true,
    });
  },

  completeGeneration: (ownerSessionId?: string) => {
    const state = get();
    // Route to the session that STARTED this generation (passed in), falling
    // back to the live owner. Reading the global owner alone is wrong once a
    // parallel session has taken the slot — it would blank the wrong session.
    const ownerSession = ownerSessionId ?? state.executionOwnerSessionId;
    const currentSessionId = useSessionStore.getState().currentSessionId;
    const isViewingOwner = currentSessionId === ownerSession;

    if (isViewingOwner) {
      set({
        isGenerating: false,
        isLoading: false,
        executionOwnerSessionId: null,
      });
      if (ownerSession) sessionSnapshots.delete(ownerSession);
    } else if (ownerSession) {
      sessionSnapshots.set(ownerSession, {
        activeExecution: null,
        isExecuting: false,
        isGenerating: false,
        isLoading: false,
        executionContext: null,
        previewContent: null,
      });
      // Only release the live slot if THIS generation owns it — a background
      // generation finishing must not clear a foreground run's owner.
      set((s) => (s.executionOwnerSessionId === ownerSession ? { executionOwnerSessionId: null } : {}));
    }
  },

  failGeneration: (error: string, ownerSessionId?: string) => {
    const state = get();
    const ownerSession = ownerSessionId ?? state.executionOwnerSessionId;
    const currentSessionId = useSessionStore.getState().currentSessionId;
    const isViewingOwner = currentSessionId === ownerSession;
    const sessionStore = useSessionStore.getState();

    if (ownerSession) {
      sessionStore.addMessageToTargetSession(
        ownerSession,
        'assistant',
        `Generation failed: ${error}`,
      );
    } else {
      sessionStore.addMessage('assistant', `Generation failed: ${error}`);
    }

    if (isViewingOwner) {
      set({
        isGenerating: false,
        isLoading: false,
        executionOwnerSessionId: null,
      });
      if (ownerSession) sessionSnapshots.delete(ownerSession);
    } else if (ownerSession) {
      sessionSnapshots.set(ownerSession, {
        activeExecution: null,
        isExecuting: false,
        isGenerating: false,
        isLoading: false,
        executionContext: null,
        previewContent: null,
      });
      set((s) => (s.executionOwnerSessionId === ownerSession ? { executionOwnerSessionId: null } : {}));
    }
  },

  // --- Session-aware state management ---
  // Which session a job belongs to, or null once it has finalized / was never
  // tracked. Used by ChatWorkspace to route poller completion events to the
  // right session even when the global slot holds a different (foreground) run.
  jobOwnerOf: (jobId: string) => jobOwners.get(jobId) ?? null,
  clearJobOwner: (jobId: string) => {
    jobOwners.delete(jobId);
  },

  stashSessionPreview: (sessionId: string, preview: PreviewContent) => {
    const prev = sessionSnapshots.get(sessionId);
    const history = prev?.previewHistory ?? [];
    const last = history[history.length - 1];
    // Append unless it repeats the latest entry (a task often re-emits its
    // output, and the final result usually duplicates the last task output).
    const nextHistory = last && last.type === preview.type && last.data === preview.data
      ? history
      : [...history, preview];
    sessionSnapshots.set(sessionId, {
      // Preserve any in-flight run flags so the snapshot still restores the
      // running banner on switch-back; default to an idle shell if none yet.
      activeExecution: prev?.activeExecution ?? null,
      isExecuting: prev?.isExecuting ?? false,
      isGenerating: prev?.isGenerating ?? false,
      isLoading: prev?.isLoading ?? false,
      executionContext: prev?.executionContext ?? null,
      previewContent: preview,
      previewHistory: nextHistory,
      previewIndex: Math.max(0, nextHistory.length - 1),
    });
  },

  saveSessionState: (sessionId: string) => {
    const state = get();
    // Only snapshot state that BELONGS to this session. The store has a single
    // global execution/preview slot; if it currently holds a DIFFERENT
    // session's run (you switched away from a running one), snapshotting it
    // here would leak that run into this session and surface a stale Stop /
    // preview in the wrong chat. Scope each by its owner so a session's
    // snapshot only ever contains its own run + preview.
    const ownsExec = state.executionOwnerSessionId === sessionId;
    const ownsPreview = state.previewOwnerSessionId === sessionId;
    const isExecuting = ownsExec && state.isExecuting;
    const isGenerating = ownsExec && state.isGenerating;
    const previewContent = ownsPreview ? state.previewContent : null;
    if (isExecuting || isGenerating || previewContent) {
      sessionSnapshots.set(sessionId, {
        activeExecution: ownsExec ? state.activeExecution : null,
        isExecuting,
        isGenerating,
        isLoading: ownsExec ? state.isLoading : false,
        executionContext: ownsExec ? state.executionContext : null,
        previewContent,
        previewHistory: ownsPreview ? state.previewHistory : [],
        previewIndex: ownsPreview ? state.previewIndex : 0,
      });
    } else {
      sessionSnapshots.delete(sessionId);
    }
  },

  restoreSessionState: (sessionId: string) => {
    // The single global execution/preview slot holds whatever session is in
    // view. On a switch the caller first parks the OUTGOING session via
    // saveSessionState, so by the time we get here a still-running incumbent is
    // safely in its own snapshot and we can load THIS session's snapshot into
    // the slot — that's what lets a backgrounded run's tracker/preview come back
    // when you switch to it, while its completion still routes by jobId.
    const liveOwner = get().executionOwnerSessionId;
    // Already viewing the live slot's owner — its state is shown; nothing to do.
    if (liveOwner === sessionId) return;
    // A run owns the slot but was never parked (no snapshot). Don't clobber a
    // live run that hasn't been safely stashed.
    if (liveOwner && !sessionSnapshots.has(liveOwner)) return;
    const snap = sessionSnapshots.get(sessionId);
    if (snap) {
      const previewHistory = snap.previewHistory ?? [];
      const restoringRun = snap.isExecuting || snap.isGenerating;
      set({
        activeExecution: snap.activeExecution,
        isExecuting: snap.isExecuting,
        isGenerating: snap.isGenerating,
        isLoading: snap.isLoading,
        executionContext: snap.executionContext,
        previewContent: snap.previewContent,
        // The snapshot's preview (if any) belongs to this session.
        previewOwnerSessionId: snap.previewContent ? sessionId : null,
        previewHistory,
        previewIndex: snap.previewIndex ?? Math.max(0, previewHistory.length - 1),
        // Switching sessions closes the pane — the user re-opens it per session.
        previewPaneOpen: false,
        previewSourceMessageId: null,
        // Restoring a still-running snapshot makes THIS session the live slot
        // owner again, so its banner/tracker reappear and its trace ticks match.
        // A completed-preview snapshot leaves the slot ownerless (no live run).
        ...(restoringRun ? { executionOwnerSessionId: sessionId } : {}),
      });
      // A run that COMPLETED while this session was backgrounded finalizes its
      // snapshot with NO preview (its deliverable never reached the live slot).
      // Derive it from the run's stored execution.result so the deliverable
      // shows on switch-back instead of a blank pane.
      if (!snap.previewContent && !restoringRun) {
        void loadDerivedOrStoredPreview(sessionId, set);
      }
    } else {
      // No in-memory snapshot — try to load persisted preview from IndexedDB
      set({
        activeExecution: null,
        isExecuting: false,
        isGenerating: false,
        isLoading: false,
        executionContext: null,
        previewContent: null,
        previewOwnerSessionId: null,
        previewHistory: [],
        previewIndex: 0,
        previewPaneOpen: false,
        previewSourceMessageId: null,
      });
      // Derive the deliverable from each run's stored execution.result (single
      // source of truth), falling back to the legacy persisted preview.
      void loadDerivedOrStoredPreview(sessionId, set);
    }
  },

  hasActiveExecution: (sessionId: string) => {
    const state = get();
    if (state.executionOwnerSessionId === sessionId) return true;
    // Only show spinner if the snapshot indicates a running execution/generation,
    // not just a saved preview from a completed one
    const snap = sessionSnapshots.get(sessionId);
    return !!(snap && (snap.isExecuting || snap.isGenerating));
  },

  resetForSession: () => {
    set({
      activeExecution: null,
      isExecuting: false,
      isGenerating: false,
      isLoading: false,
      executionContext: null,
      previewContent: null,
      previewOwnerSessionId: null,
      previewHistory: [],
      previewIndex: 0,
    });
  },
    }),
    {
      name: 'kasal-chatmode-mcp-selection',
      // v1: the run-activity stream moved to the chat event box by default (the
      // preview pane is now opt-in). Reset any persisted 'preview' placement once
      // so existing browsers pick up the new default instead of keeping the old
      // value forever.
      // v2: the composer's memory pill now defaults to "Session memory"
      // (memoryEnabled=false) so a new chat doesn't pull in unrelated workspace
      // history unless the user opts in. Reset the persisted value once so existing
      // browsers pick up the new default instead of keeping the old "Workspace".
      version: 2,
      migrate: (persisted, version) => {
        if (version < 1 && persisted && typeof persisted === 'object') {
          (persisted as { activityPlacement?: string }).activityPlacement = 'chat';
        }
        if (version < 2 && persisted && typeof persisted === 'object') {
          (persisted as { memoryEnabled?: boolean }).memoryEnabled = false;
        }
        return persisted as ExecutionStore;
      },
      // Persist ONLY stable USER PREFERENCES so a page refresh (or a switch to a
      // new chat) keeps them — the chat "+" picker selections (MCP servers /
      // Agent Bricks endpoints), the activity placement, and the memory mode
      // (workspace / session / no memory) chosen in the chat input. Users
      // complained when any of these reset. Everything else here is volatile,
      // per-run / per-session state (active execution, preview, transient feed)
      // that MUST NOT survive a reload: persisting it would resurrect a stale
      // "running" banner or a dead preview against a job that's long gone.
      partialize: (s) => ({
        selectedMcpServers: s.selectedMcpServers,
        selectedAgentBricksEndpoints: s.selectedAgentBricksEndpoints,
        activityPlacement: s.activityPlacement,
        workspaceMemory: s.workspaceMemory,
        memoryEnabled: s.memoryEnabled,
        chatModeType: s.chatModeType,
      }),
    },
  ),
);
