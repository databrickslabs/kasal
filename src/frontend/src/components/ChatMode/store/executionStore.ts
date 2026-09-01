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
import { applyA2uiMessage } from '../../../shared/a2ui/stream';
import type { A2uiMessage } from '../../../shared/a2ui/stream';
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
   * The published capability the CURRENT run was routed to, if any. Recorded so
   * the answer message can be persisted with it — the backend router reads it
   * back next turn to know a capability is mid-conversation. Cleared at the
   * start of every run, so a later un-routed run cannot inherit it.
   */
  routedCapability: string | null;
  setRoutedCapability: (name: string | null) => void;
  /**
   * The capability currently holding this conversation — set when a routed run
   * used one that holds conversations, cleared when the user leaves it or when
   * something else answers. Drives the "Continuing X" pill.
   */
  heldConversation: string | null;
  setHeldConversation: (name: string | null) => void;
  /**
   * True for exactly one turn after the user leaves a held conversation: the
   * next dispatch tells the router not to continue, and the router decides on
   * the message alone.
   */
  skipContinuation: boolean;
  setSkipContinuation: (skip: boolean) => void;
  /**
   * ChatMode answer mode chosen in the chat input's mode pill:
   *   'chat'     – a single light agent (Agent.kickoff_async), fast, no crew;
   *   'research' – a full crew with balanced model reasoning;
   *   'deep'     – a full crew with maximum model reasoning.
   * Persisted (like ``memoryEnabled``) so the choice survives a reload.
   */
  chatModeType: 'chat' | 'research' | 'deep';
  /**
   * The SOURCE the next prompt is answered from: build something new (false,
   * the default) or run something already published to chat (true).
   *
   * A separate axis from ``chatModeType``, not a fourth value of it. The
   * catalogue only stores crews, so reuse could never honour 'chat' — putting
   * it in the answer-mode pill would create a value that silently invalidates
   * its own neighbours, and a "reuse" mode that found no match would quietly
   * become Research.
   *
   * ``chatModeType`` is deliberately NOT cleared while this is on: the user gets
   * their selection back when they switch source, and it is what the "build one
   * instead" offer runs at when nothing matches.
   */
  preferExisting: boolean;
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
   * Skill NAMES picked in the chat "+" menu. At execution time these are
   * attached to every generated agent (the kernel builder injects each skill's
   * <available_skills> block + load_skill/read_skill_file tools). Stored by name
   * (not id) — skills resolve per workspace, overrides preferred over builtins —
   * and persisted for the same reason as ``selectedMcpServers``.
   */
  selectedSkills: string[];
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
  setPreferExisting: (preferExisting: boolean) => void;
  toggleMcpServer: (name: string) => void;
  setSelectedMcpServers: (names: string[]) => void;
  toggleAgentBricksEndpoint: (name: string) => void;
  setSelectedAgentBricksEndpoints: (names: string[]) => void;
  toggleSkill: (name: string) => void;
  setSelectedSkills: (names: string[]) => void;
  setActivityPlacement: (placement: 'preview' | 'chat') => void;
  clearPreview: () => void;
  reopenPreview: () => void;

  // Execution lifecycle
  startExecution: (jobId: string, sessionId?: string, opts?: { preservePreview?: boolean }) => void;
  updateExecutionStatus: (status: ExecutionStatus) => void;
  /**
   * Append a token-stream chunk (SSE `llm_chunk`) to this job's live bubble,
   * creating the bubble on the first chunk. Display-only nicety: chunks are
   * dropped unless the user is viewing the job's owner session (appendToMessage
   * writes to the on-screen message list); the terminal message from
   * completeExecution is authoritative either way.
   */
  appendStreamChunk: (jobId: string, chunk: string) => void;
  /**
   * End the current streaming bubble at a task boundary, flushing any paced
   * text still queued. The next chunk opens a fresh bubble, so a multi-task
   * crew reads as header → tokens → header → tokens instead of one bubble with
   * every task's headers piled up after it.
   */
  closeStreamBubble: (jobId: string) => void;
  /**
   * Whether the CURRENT task's tokens are already on screen for this job —
   * an open stream bubble, or text still queued in the pacer for one.
   *
   * A crew announces each task's answer twice: live as ``llm_chunk`` tokens,
   * then again in the ``task_completed`` trace that carries the finished text.
   * While the subprocess event pipe was broken only the second ever arrived, so
   * rendering both looked correct. With streaming working, posting the trace
   * body too prints every answer twice.
   *
   * False when nothing streamed — streaming off, a model that cannot stream, or
   * the owner session off screen (``appendStreamChunk`` returns before opening a
   * bubble). In all of those the trace body is the ONLY copy and must still post.
   */
  hasStreamedTaskText: (jobId: string) => boolean;
  /**
   * Whether this run already reached a terminal state. A `task_completed` trace
   * that arrives afterwards carries output the final answer has superseded, so
   * rendering it prints the answer twice.
   */
  isRunFinalized: (jobId: string) => boolean;
  // jobId routes the completion to the session that OWNS that job, so a run
  // finishing in a backgrounded session (parallel sessions) lands in the right
  // place instead of the single global slot. Omitting it keeps the legacy
  // single-run behavior (route by the current live owner).
  completeExecution: (resultText: string, jobId?: string, surface?: Surface) => void;
  /**
   * Attach a surface that arrived AFTER its run had already finalized.
   * Returns true when it landed.
   *
   * The crew subprocess announces a run twice by design: the plain answer the
   * moment the crew has it, then a second one carrying the composed A2UI surface,
   * which the parent can only build after the subprocess exits. Composition
   * took 44s on one measured run — long after the UI had finalized on the first
   * announcement and dropped the second as a duplicate, which is why a
   * presentation arrived as raw markdown.
   *
   * Deliberately NOT "make completeExecution idempotent". Completion posts a
   * message, moves the preview, clears run state and fires the actions row; the
   * second announcement must do exactly ONE of those things — swap the text for
   * the surface. Anything else and a slow compose double-posts.
   */
  attachSurface: (
    jobId: string | undefined,
    surface: Surface,
    resultText?: string,
  ) => boolean;
  /**
   * Fold one streamed A2UI message into the surface being composed for a run.
   *
   * Composition is slow (a deck measured 140s+) and used to deliver nothing
   * until it was entirely done. The backend now ships each piece as it lands, and
   * this paints them onto the run's LIVE STREAM BUBBLE — the same message
   * `completeExecution` later folds the final answer into. That choice is what
   * keeps this from double-posting: when the finished surface arrives it
   * overwrites this message rather than adding a second one, so no extra
   * bookkeeping is needed on the completion path.
   *
   * A run with no stream bubble (nothing was typed in front of the reader) is
   * skipped: there is no message to paint on, and its surface still arrives whole
   * at completion exactly as before.
   */
  applySurfaceDelta: (
    jobId: string | undefined,
    message: A2uiMessage,
    seq?: number,
  ) => void;
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

// jobId -> message id of the live token-streaming bubble (SSE `llm_chunk`).
// Same lifecycle discipline as jobOwners: entries die when the job finalizes.
const streamBubbles = new Map<string, string>();

// The surface each run has streamed SO FAR, keyed by job. Held here rather than
// read back off the message because the reducer needs the previous surface to
// fold the next message into, and the message only carries the rendered result.
// Cleared wherever streamBubbles is cleared on a terminal path.
const streamingSurfaces = new Map<string, Surface | null>();

// Runs whose TEXT was actually painted. Distinct from `streamBubbleSeq`, which
// used to stand in for it: a bubble is now also opened by a surface arriving
// first (the instant deck shell), so the sequence counter no longer answers
// "did the reader see prose?" — and getting that wrong prints the whole answer
// next to the deck it was composed into.
const textPainted = new Set<string>();

// Runs a SURFACE led: it was on screen before a single token of prose. Only the
// instant deck shell can do that, which is exactly the case where the prose is
// source material rather than the deliverable — the reader asked for slides, and
// watching the raw answer type itself out beside the deck being built from it
// reads as the same thing happening twice.
const surfaceLedRuns = new Set<string>();

// The highest delta sequence applied per run.
//
// A run's SSE stream belongs to whichever session is on screen, so switching
// away closes it and switching back reconnects — and a reconnect REPLAYS the
// snapshots (`createSurface`), which is what lets a late joiner see the deck at
// all. But a snapshot replaces the whole surface, so replaying the shell at a
// client that had already accumulated forty slides threw all of them away: the
// deck visibly reverted to an empty frame. Sequence numbers make replay
// idempotent — already-seen messages are skipped, new ones still apply.
const lastSurfaceSeq = new Map<string, number>();

// Surface kinds the backend may still drop as prose-only (`GATED_SURFACE_KINDS`
// in services/a2ui/stream.py). A shell for one of these is provisional, so it
// never silences the answer's text.
const RETRACTABLE_SURFACE_KINDS = new Set(['dashboard', 'document']);

// jobId -> the LAST bubble this job painted, kept across task boundaries.
//
// `streamBubbles` is emptied by closeStreamBubble at every boundary, so a run
// whose final task had already closed its bubble reached completion with no
// bubble to finalize — and posted the answer as a NEW message underneath the
// streamed copy the reader was already looking at. `supersedeTruncatedTail`
// could not save it either: that scan looks for a CAPPED tail, and a streamed
// bubble holds the full text.
//
// Same reasoning as `streamBubbleSeq`, which already survives closes for the
// same reason. Cleared wherever streamBubbles is cleared on a terminal path.
const lastStreamBubble = new Map<string, string>();

// jobIds that reached a TERMINAL state (completed or failed).
//
// `_relay_task_events` (agent_builder's process executor) broadcasts
// `task_completed` — carrying the task's full output — from its own queue-driven
// relay, with no DB id, so the frontend's trace de-dupe (which keys on the DB id)
// cannot collapse it. It routinely lands AFTER the run has completed, and a task
// body arriving then is stale by definition: the final answer is already on
// screen. Posting it printed the answer a second time under the copy the reader
// had been watching.
//
// Deliberately NOT "did this job ever stream": a later task that produced no
// tokens still needs its body while the run is live. Bounded, and cleared on
// abandon.
const finalizedJobs = new Set<string>();
const _MAX_FINALIZED_JOBS = 200;

function markRunFinalized(jobId?: string): void {
  if (!jobId) return;
  finalizedJobs.add(jobId);
  while (finalizedJobs.size > _MAX_FINALIZED_JOBS) {
    const oldest = finalizedJobs.values().next().value;
    if (oldest === undefined) break;
    finalizedJobs.delete(oldest);
  }
}

// jobId -> message id of the last TASK-OUTPUT line this run posted.
//
// That line is a preview: a long step output is posted capped, on the
// understanding that completion replaces it with the full text. Finding it
// again by scanning the message list and prefix-matching kept failing for
// reasons that had nothing to do with the answer — trace pills crowding a fixed
// window, and `addMessageToTargetSession` only appending to the in-memory array
// when the owner session is the one on screen, so a backgrounded run's line is
// persisted but invisible to any scan.
//
// The producer knows the id. Recording it here turns "find the message that
// looks like this text" into "update this message", which cannot miss. The scan
// remains as a fallback for lines posted before this was wired.
const taskOutputMessages = new Map<string, string>();

/** Register the message holding a run's latest task output (see above). */
export function rememberTaskOutputMessage(jobId: string, messageId: string): void {
  if (jobId && messageId) taskOutputMessages.set(jobId, messageId);
}

// jobId -> where this run's finished message landed, kept AFTER it finalizes.
//
// The crew subprocess announces a run twice on purpose: plain text the moment
// the crew has its answer, then a second one carrying the A2UI surface, which the
// parent can only compose once the subprocess has exited. Composition is not
// fast — 44s for a 50-component deck on a local model — so the gap is real, and
// everything else about finalization is deliberately once-only (a double
// completion double-posts, to the wrong session).
//
// So that late surface needs somewhere to land. This is that: the one thing
// that outlives finalization, holding just enough to find the message again.
// Deliberately NOT a reopening of the completion path — see attachSurface.
const finalizedRunMessages = new Map<
  string,
  { sessionId: string | null; messageId: string | null }
>();
const _MAX_FINALIZED_TRACKED = 32;

/**
 * Fold the final answer into the intermediate message that already showed it.
 *
 * A run posts each task's output as it completes, and the LAST task's output IS
 * the run's result — so the same content arrived twice: once as the task
 * summary (capped at 300 chars by summarizeTaskOutput) and again in full at
 * completion. Returns true when it superseded one, meaning the caller must not
 * post again.
 *
 * Matched by PREFIX, not equality: the intermediate may be capped, and it may
 * carry a "**Task name** — " header the final text does not. Bounded to the
 * last few messages so an older answer that happens to share an opening cannot
 * be rewritten.
 *
 * Returns the id of the message it folded into, so the caller can register it —
 * a surface arriving later must attach to THAT message rather than posting the
 * same answer a third time.
 */
function supersedeTruncatedTail(
  sessionId: string | null,
  fullText: string,
): string | null {
  const text = (fullText || '').trim();
  if (!text) return null;
  const sessionStore = useSessionStore.getState();
  const messages = sessionStore.messages || [];

  // Bounded by how many PLAIN messages are inspected, not by raw list position.
  // Every trace — each tool pill, memory read, memory write, LLM call — is also
  // an assistant message (`processTrace` posts them with resultType 'trace'), so
  // a task that runs for half a minute puts dozens of them after its own output.
  // A fixed "last 4 entries" window counted those, so the capped line fell out
  // of range within seconds and the fold silently did nothing: the run posted
  // its full answer as a NEW message and the 300-char stub stayed above it.
  //
  // The outer bound only stops an unbounded walk of a long conversation; the
  // real limit is CANDIDATES, and the prefix match itself is specific enough
  // (>=40 chars, and the full text must start with it) that widening the reach
  // cannot rewrite an unrelated message.
  const CANDIDATES = 6;
  const REACH = 300;
  let considered = 0;
  for (
    let i = messages.length - 1;
    i >= 0 && i >= messages.length - REACH && considered < CANDIDATES;
    i -= 1
  ) {
    const message = messages[i];
    // Only a plain task-output line: a rich card carries its own meaning.
    // Cards and traces do NOT count towards the budget — being outnumbered by
    // activity is the normal case, not a reason to stop looking.
    if (message.role !== 'assistant' || message.resultType) continue;
    considered += 1;

    const body = (message.content || '')
      .replace(/^\*\*[^*]*\*\*\s+—\s+/, '')
      .replace(/…$/, '')
      .trim();
    // Short lines ("Calling tools.") share prefixes by accident.
    if (body.length < 40) continue;
    if (!text.startsWith(body)) continue;

    // `isStreaming: false` because the message matched is very often the LIVE
    // bubble — its streamed text is a prefix of the final answer, which is
    // exactly what this looks for. Folding into it without clearing the flag
    // left the typing dots bouncing under a finished run.
    const done = { content: text, isStreaming: false };
    if (sessionId) {
      sessionStore.updateMessageInTargetSession(sessionId, message.id, done);
    } else {
      sessionStore.updateMessage(message.id, done);
    }
    return message.id;
  }
  return null;
}

function rememberFinalizedMessage(
  jobId: string | undefined,
  sessionId: string | null,
  messageId: string | null,
): void {
  if (!jobId) return;
  finalizedRunMessages.set(jobId, { sessionId, messageId });
  // Bounded: this store lives for the whole session and a surface that never
  // arrives must not accumulate.
  while (finalizedRunMessages.size > _MAX_FINALIZED_TRACKED) {
    const oldest = finalizedRunMessages.keys().next().value;
    if (oldest === undefined) break;
    finalizedRunMessages.delete(oldest);
  }
}
// jobId -> how many bubbles this run has opened, so each task boundary can
// start a new one with a distinct message id.
const streamBubbleSeq = new Map<string, number>();

// ── Stream pacing ───────────────────────────────────────────────────────────
// Chunks used to be painted the instant an SSE frame arrived. The backend
// coalesces tokens, so what reached the eye was a burst — a paragraph appearing
// at once, a pause, another burst. It reads as stuttering, not as typing, and on
// a fast model the whole answer could land in one frame.
//
// Frames are buffered per job and drained on a rAF tick at a fixed character
// budget, so text flows at a readable, steady rate no matter how the server
// batches. The buffer only ever DELAYS text — nothing is dropped, and every
// terminal path (task boundary, completion, failure) flushes what remains.
const streamBuffers = new Map<string, string>();
const streamTimers = new Map<string, number>();
// ~55 chars per frame ≈ 3.3k/s at 60fps: faster than anyone reads, slow enough
// to look continuous. Tunable in one place if it feels off in the live app.
const STREAM_CHARS_PER_TICK = 55;

/**
 * Stop pacing a job. `flush` paints whatever is still queued (terminal paths
 * that keep the bubble); otherwise the queue is dropped (the bubble is gone).
 */
function discardStreamPacing(jobId: string, flush: boolean): void {
  if (flush) {
    flushStreamBuffer(jobId);
    streamBubbleSeq.delete(jobId);
    return;
  }
  const timer = streamTimers.get(jobId);
  if (timer !== undefined) cancelAnimationFrame(timer);
  streamTimers.delete(jobId);
  streamBuffers.delete(jobId);
  streamBubbleSeq.delete(jobId);
}

/**
 * The message id this job is currently streaming into, opening one if needed.
 *
 * Shared by the paced painter and the flush: a task that finishes before the
 * first frame paints still has to land its text somewhere, and looking up a
 * bubble that had not been created yet silently dropped the whole answer.
 */
/**
 * Paint `text` into this job's bubble, opening one if needed.
 *
 * The bubble is CREATED WITH its first text, never empty-then-updated. addMessage
 * persists fire-and-forget and awaits ensureSession() first, while
 * appendToMessage's update fires immediately — so an empty insert can land AFTER
 * the update that carried the content and overwrite the row with ''. Leaving the
 * session and returning then showed the headers but no streamed text, and took
 * the A2UI surface with it (completeExecution writes the surface INTO this same
 * bubble). Creating with content means the insert always carries the answer.
 */
/**
 * The message a run's live output paints into, created on first use.
 *
 * Shared by the two things that can be first: streamed TEXT, and a streamed
 * A2UI surface. The instant deck shell now ships before the agent writes a
 * token, so the surface genuinely arrives first on a presentation turn — and if
 * this only existed inside the text path, that shell would have nowhere to land
 * and would be silently dropped.
 */
function ensureStreamBubble(
  jobId: string,
  initialText = '',
  card?: { resultType: string; resultData: unknown },
): string {
  const existing = streamBubbles.get(jobId);
  if (existing) return existing;
  // Route by the job's OWNER, not by what is on screen. `addMessage` writes to
  // the CURRENT session, so a run whose surface opened its bubble while the
  // reader had switched away created the deck in the wrong conversation and
  // persisted it there — the session that asked for it came back to nothing.
  // The target variant updates memory only when that session is being viewed
  // and persists either way, which is what makes switching away lossless.
  const owner = jobOwners.get(jobId);
  // The id carries a sequence: a crew closes the bubble at each task boundary
  // (closeStreamBubble) so the NEXT task's tokens open a fresh one below its own
  // header, instead of every task pouring into a single bubble with all the
  // headers stacked at the end.
  const seq = (streamBubbleSeq.get(jobId) ?? 0) + 1;
  streamBubbleSeq.set(jobId, seq);
  const bubbleId = `stream-${jobId}-${seq}`;
  streamBubbles.set(jobId, bubbleId);
  lastStreamBubble.set(jobId, bubbleId);
  // The card rides along AT CREATION, never as a follow-up update. The messages
  // API declares `content: str = Field(..., min_length=1)`, so a row posted with
  // empty content is rejected 422 and never exists — and a later PUT against a
  // row that was never created is lost with it. With a card present the writer
  // substitutes CARD_PLACEHOLDER for the missing text, which persists. This is
  // why a surface-led run used to vanish the moment you switched sessions: its
  // bubble had no text by design, so it was never stored at all.
  const extra = { id: bubbleId, isStreaming: true, ...(card ?? {}) };
  const sessionStore = useSessionStore.getState();
  if (owner) sessionStore.addMessageToTargetSession(owner, 'assistant', initialText, extra);
  else sessionStore.addMessage('assistant', initialText, extra);
  return bubbleId;
}

function paintStreamText(jobId: string, text: string): void {
  textPainted.add(jobId);
  const existing = streamBubbles.get(jobId);
  if (existing) {
    useSessionStore.getState().appendToMessage(existing, text);
    return;
  }
  ensureStreamBubble(jobId, text);
}

/** Drain a job's buffer into its bubble immediately (no pacing). */
function flushStreamBuffer(jobId: string): void {
  const timer = streamTimers.get(jobId);
  if (timer !== undefined) {
    cancelAnimationFrame(timer);
    streamTimers.delete(jobId);
  }
  const pending = streamBuffers.get(jobId);
  streamBuffers.delete(jobId);
  if (pending) paintStreamText(jobId, pending);
}

/**
 * Queue text for the paced painter. The bubble is opened lazily on the first
 * tick that actually has something to paint, so an empty bubble never appears
 * ahead of its content.
 */
function enqueueStreamText(jobId: string, chunk: string): void {
  streamBuffers.set(jobId, (streamBuffers.get(jobId) ?? '') + chunk);
  if (streamTimers.has(jobId)) return;

  const tick = () => {
    streamTimers.delete(jobId);
    const pending = streamBuffers.get(jobId) ?? '';
    if (!pending) return;
    // Break on whitespace when one is near the budget so words are not sliced
    // mid-token — a word appearing letter by letter reads as a glitch.
    let take = Math.min(STREAM_CHARS_PER_TICK, pending.length);
    if (take < pending.length) {
      const nextBreak = pending.slice(take, take + 20).search(/\s/);
      if (nextBreak >= 0) take += nextBreak + 1;
    }
    paintStreamText(jobId, pending.slice(0, take));
    const rest = pending.slice(take);
    if (rest) {
      streamBuffers.set(jobId, rest);
      streamTimers.set(jobId, requestAnimationFrame(tick));
    } else {
      streamBuffers.delete(jobId);
    }
  };
  streamTimers.set(jobId, requestAnimationFrame(tick));
}

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
    // The session switch that triggers this restore has ALREADY loaded the
    // session's messages into the store — reuse them instead of re-downloading
    // the whole (surface-laden) message page a second time per switch.
    const sess = useSessionStore.getState();
    const loaded =
      sess.currentSessionId === sessionId && Array.isArray(sess.messages) && sess.messages.length > 0
        ? sess.messages
        : null;
    const msgs = loaded ?? (await getSessionMessages(sessionId));
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
  workspaceMemory: true,
  memoryEnabled: false,
  routedCapability: null,
  heldConversation: null,
  skipContinuation: false,
  chatModeType: 'chat',
  preferExisting: false,
  selectedMcpServers: [],
  selectedAgentBricksEndpoints: [],
  selectedSkills: [],
  runStartedAt: null,
  runningJobBySession: {},
  // 'chat' by default so the run-activity stream shows inline in the chat and the
  // side preview pane stays closed until the user opens it (per-surface "expand").
  activityPlacement: 'chat',

  setActivityPlacement: (placement) => set({ activityPlacement: placement }),

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
      // Round-trip the restyle to the owning MESSAGE's resultData (persisted via
      // the session API), mirroring the inline Look picker. Session restore
      // derives previews from message.resultData first (deriveSessionPreviews),
      // so without this a pane "Customize → Look" palette only lives in this
      // in-memory slot and is lost on the next session switch.
      const msgId = updated.sourceMessageId ?? s.previewSourceMessageId;
      if (msgId && updated.type === 'ui' && owner) {
        try {
          const restyled = JSON.parse(updated.data);
          useSessionStore
            .getState()
            .updateMessageInTargetSession(owner, msgId, { resultData: restyled });
        } catch {
          /* non-JSON preview data — nothing to persist on the message */
        }
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
  setRoutedCapability: (name) => set({ routedCapability: name }),
  setHeldConversation: (name) => set({ heldConversation: name }),
  setSkipContinuation: (skip) => set({ skipContinuation: skip }),
  setChatModeType: (mode) => set({ chatModeType: mode }),
  setPreferExisting: (preferExisting) => set({ preferExisting }),
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
  toggleSkill: (name) =>
    set((s) => ({
      selectedSkills: s.selectedSkills.includes(name)
        ? s.selectedSkills.filter((n) => n !== name)
        : [...s.selectedSkills, name],
    })),
  setSelectedSkills: (names) => set({ selectedSkills: names }),
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
        // The pane NEVER opens (or stays open) on its own: clearing the content
        // while leaving the pane open turned it into an empty run monitor the
        // user did not ask for. A fresh run closes it — opening is always the
        // user's click; the preserve flow (refine) keeps their open pane.
        previewPaneOpen: preserve ? s.previewPaneOpen : false,
        chatCollapsed: preserve ? s.chatCollapsed : false,
        runStartedAt: Date.now(),
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

  appendStreamChunk: (jobId, chunk) => {
    if (!jobId || !chunk) return;
    const ownerSession = jobOwners.get(jobId);
    if (!ownerSession) return; // untracked or already finalized
    const sessionStore = useSessionStore.getState();
    // appendToMessage mutates the CURRENT session's message list, so only
    // stream while the owner session is on screen. Switching away just pauses
    // the live text; completeExecution still routes the final answer by owner.
    if (sessionStore.currentSessionId !== ownerSession) return;
    // A surface got here first, so the prose is the deck's SOURCE, not the
    // answer — dropping it leaves the deliverable alone on screen instead of
    // racing a wall of markdown against the slides being built from it. The
    // text is not lost: completion still folds it in if no surface survives.
    if (surfaceLedRuns.has(jobId)) return;
    // Paced, not painted per SSE frame — see enqueueStreamText.
    enqueueStreamText(jobId, chunk);
  },

  isRunFinalized: (jobId) => Boolean(jobId) && finalizedJobs.has(jobId),

  hasStreamedTaskText: (jobId) => {
    if (!jobId) return false;
    // Deliberately NOT `textPainted`: this question is per-TASK, and must go
    // false again when a task boundary closes the bubble so the next task's
    // trace body still posts. `textPainted` is per-RUN (it answers
    // `readerSawText`, which must survive those closes) — the two look alike and
    // are not interchangeable.
    return Boolean(streamBubbles.get(jobId) || streamBuffers.get(jobId));
  },

  closeStreamBubble: (jobId) => {
    if (!jobId) return;
    // Flush whatever is still buffered into the bubble before letting go of it,
    // otherwise the tail of a task's answer would be dropped at the boundary.
    flushStreamBuffer(jobId);
    const bubbleId = streamBubbles.get(jobId);
    if (!bubbleId) return;
    streamBubbles.delete(jobId);
    // lastStreamBubble deliberately KEPT: completion folds the final answer into
    // this bubble, and a boundary close must not hide it.
    useSessionStore.getState().updateMessage(bubbleId, { isStreaming: false });
  },

  applySurfaceDelta: (jobId, message, seq) => {
    if (!jobId || !message) return;
    // Replay-safe: the reconnect after a session switch re-sends every snapshot.
    if (typeof seq === 'number') {
      const seen = lastSurfaceSeq.get(jobId);
      if (seen !== undefined && seq <= seen) return;
      lastSurfaceSeq.set(jobId, seq);
    }
    // Paint onto the run's live bubble, opening one if the surface is first.
    // No owner, no paint. `appendStreamChunk` has always bailed out here, and
    // the surface path must too: opening a bubble for an unattributable run
    // writes it into whichever conversation happens to be on screen, which is
    // how a deck turned up above an unrelated prompt in an older session.
    if (!jobOwners.get(jobId)) return;
    const prev = streamingSurfaces.get(jobId) ?? null;
    const next = applyA2uiMessage(prev, message);
    if (next === prev) return; // a message the reducer could not use
    streamingSurfaces.set(jobId, next);

    // `streamBubbles` first, then `lastStreamBubble` — the same order completion
    // uses, so a delta arriving just after a task boundary still lands on the
    // message the reader is watching rather than starting a stray one. Only
    // OPEN a bubble for a surface that exists: a retraction with no bubble has
    // nothing to take back, and creating one to do it would post an empty message.
    const hadBubble = Boolean(streamBubbles.get(jobId) ?? lastStreamBubble.get(jobId));
    // A surface that beat every token to the screen LEADS the run: from here the
    // prose is the deck's source material, not the answer, so it stops being
    // painted (see appendStreamChunk). Only the instant shell can win this race,
    // which is precisely the case the reader asked for slides rather than text.
    if (next && !hadBubble && !textPainted.has(jobId) && !streamBuffers.get(jobId)) {
      // ...but only for a surface that CANNOT be taken back. A dashboard or a
      // document is dropped to plain text when the answer turns out to carry no
      // real data, so silencing the prose under one of those frames risks the
      // frame vanishing with nothing left in its place. Those keep streaming
      // text underneath, which makes a retraction cost the reader nothing.
      if (!RETRACTABLE_SURFACE_KINDS.has(next.surfaceKind)) {
        surfaceLedRuns.add(jobId);
      }
    }
    // A retraction hands the run back to prose — the turn is answering in text
    // after all, so let it through again.
    if (!next) surfaceLedRuns.delete(jobId);

    const bubbleId =
      streamBubbles.get(jobId) ??
      lastStreamBubble.get(jobId) ??
      (next ? ensureStreamBubble(jobId, '', { resultType: 'a2ui', resultData: next }) : undefined);
    if (!bubbleId) return;

    const sessionStore = useSessionStore.getState();
    const owner = jobOwners.get(jobId);
    // A retraction takes the surface off the bubble but leaves the TEXT: the
    // reader has been reading it the whole time, and the run is about to
    // complete with that text as the answer.
    const update = next
      ? { resultType: 'a2ui', resultData: next }
      : { resultType: undefined, resultData: undefined };
    if (owner) sessionStore.updateMessageInTargetSession(owner, bubbleId, update);
    else sessionStore.updateMessage(bubbleId, update);

    // The preview pane FOLLOWS a live surface it was opened from. Without
    // this it held a frozen click-time snapshot while the inline copy kept
    // streaming — the reader saw a stale pane beside a live chat copy (and a
    // retraction left the pane showing a surface that no longer existed).
    const paneState = get();
    if (paneState.previewPaneOpen && paneState.previewSourceMessageId === bubbleId) {
      if (next) {
        const data = JSON.stringify(next);
        set((st) => {
          const history = [...st.previewHistory];
          if (st.previewIndex >= 0 && st.previewIndex < history.length) {
            history[st.previewIndex] = { ...history[st.previewIndex], type: 'ui', data };
          }
          return {
            previewContent: { ...(st.previewContent ?? { type: 'ui' }), type: 'ui', data },
            previewHistory: history,
          };
        });
      } else {
        get().clearPreview();
      }
    }
  },

  attachSurface: (
    jobId: string | undefined,
    surface: Surface,
    resultText?: string,
  ) => {
    if (!jobId || !surface) return false;
    const target = finalizedRunMessages.get(jobId);
    if (!target) return false;
    finalizedRunMessages.delete(jobId); // one late surface per run
    const sessionStore = useSessionStore.getState();
    // The answer text STAYS, which is the opposite of what the completion path
    // does when the surface arrives in time (`body = surface ? '' : resultText`).
    // Not an inconsistency — the rule is: never take away something the reader
    // has already seen, and never add something they never saw.
    //
    // When composition is fast the text never rendered, so dropping it costs
    // nothing and avoids printing the same deck twice (once as prose, once
    // rendered) — the duplication that gate was added to fix. When composition
    // takes 25-45s the reader has been reading that text the whole time, and
    // blanking it mid-read looks like the answer was retracted.
    // Nothing to update: the run finished with no readable text, so it posted no
    // message at all. The surface posts one now — which is what lets the
    // "Execution completed." notice be dropped without losing a deliverable
    // that arrives after it.
    //
    // It carries the run's answer TEXT as well when one came with it. A flow's
    // second announcement brings both — the composed surface and the answer it
    // was composed from — and they are not the same thing: the surface is a
    // mindmap, the text is the news it was drawn from. Posting the surface
    // alone silently dropped half of what the run produced.
    if (!target.messageId) {
      const extra = { executionId: jobId, resultType: 'a2ui', resultData: surface };
      const body = (resultText || '').trim();
      if (target.sessionId) {
        sessionStore.addMessageToTargetSession(
          target.sessionId,
          'assistant',
          body,
          extra,
        );
      } else {
        sessionStore.addMessage('assistant', body, extra);
      }
      return true;
    }

    // There IS text, and it stays.
    const update = { resultType: 'a2ui', resultData: surface };
    if (target.sessionId) {
      sessionStore.updateMessageInTargetSession(
        target.sessionId,
        target.messageId,
        update,
      );
    } else {
      sessionStore.updateMessage(target.messageId, update);
    }
    return true;
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
    // Live token bubble for this job (if any). The terminal result is
    // authoritative (it may be post-processed text or an A2UI surface), so the
    // bubble is finalized in place below rather than left as a duplicate.
    // Drain the pacing buffer before finalizing: any text still queued belongs
    // in the bubble (or would otherwise vanish under the terminal result).
    // Did this run put text in front of the reader? `streamBubbles` alone does
    // not answer that — a bubble is closed at every task boundary, so a run can
    // have shown several screens of text and hold no open bubble at the end.
    // The per-job counter survives those closes. Read BEFORE the flush below,
    // which clears it. Text still queued counts: the flush is about to paint it.
    const readerSawText = Boolean(
      jobId && (textPainted.has(jobId) || streamBuffers.get(jobId)),
    );
    if (jobId) discardStreamPacing(jobId, true);
    // The live bubble, or the last one this job painted if a task boundary
    // already closed it — otherwise the answer posts a second time below the
    // streamed copy the reader has been watching.
    const streamBubbleId = jobId
      ? (streamBubbles.get(jobId) ?? lastStreamBubble.get(jobId))
      : undefined;
    // Did the live bubble already receive a STREAMED surface? Read before the
    // cleanup below clears it. It matters because the answer does not always
    // fold into that bubble — when a task-output message is superseded instead,
    // the surface is applied THERE while the bubble keeps its own copy, and the
    // deck renders twice, once under the other. Only became reachable when the
    // streamed bubble started persisting its surface at all.
    const bubbleCarriesSurface = Boolean(jobId && streamingSurfaces.get(jobId));
    if (jobId) {
      streamBubbles.delete(jobId);
      lastStreamBubble.delete(jobId);
      streamingSurfaces.delete(jobId);
      textPainted.delete(jobId);
      surfaceLedRuns.delete(jobId);
      lastSurfaceSeq.delete(jobId);
      // Terminal: a task_completed trace arriving after this is stale.
      markRunFinalized(jobId);
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
      // mode specifically: it produces a structured deck `text` that trips
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
        // No chat message on this path — just stop the bubble's typing state.
        if (streamBubbleId && ownerSession) {
          sessionStore.updateMessageInTargetSession(ownerSession, streamBubbleId, {
            isStreaming: false,
          });
        }
      } else {
        // Route text message to the correct session. A composed surface is the
        // canonical rendering of the answer (a Genie table, a dashboard, a deck…),
        // so the raw text is dropped when one exists — otherwise the SAME content
        // prints twice: the full markdown answer in the bubble AND the rendered
        // surface below it (e.g. a 100-row restaurant list shown as prose above
        // the interactive Table).
        //
        // …but ONLY when the reader never saw that text. This is the rule
        // `attachSurface` already states for the late-surface path: never take
        // away something the reader has already seen, never add something they
        // never saw. Completion assumed "the surface arrived in time, so nothing
        // rendered" — false whenever tokens streamed. Composition takes tens of
        // seconds, and the stream exists precisely so the answer is readable
        // while it runs; blanking it at the end read as the answer being
        // retracted, and for a dashboard that only carries headline numbers most
        // of the answer went with it.
        //
        // Nothing streamed (fast compose, no bubble) → unchanged: the surface is
        // the first and only rendering, and printing the prose too would be the
        // duplication this gate exists to stop.
        const body = surface && !readerSawText ? '' : resultText;
        // The last task's output already printed this. Fold the full text into
        // that message rather than printing it a second time.
        //
        // NOT an early return: everything below this block — the session
        // snapshot, clearing the run's owner — still has to happen, or the
        // "running" banner never goes away.
        // Attempted with `resultText`, NOT `body` — a truncated task line is on
        // screen whichever way `body` went. Each task output is posted capped at
        // 300 chars + "…" (summarizeTaskOutput) on the understanding that the
        // full answer replaces it here. Gating this on `body` meant a run that
        // composed a surface never even looked: `body` was '', the fold was
        // skipped, and the ONLY thing left on screen was 300 characters ending
        // mid-word. Leaving the reader with a capped answer is the one outcome
        // that is simply wrong — worse than duplicating, worse than dropping.
        // The registered id first — exact, and immune to the message not being
        // in the on-screen array. The scan stays as a fallback.
        const registeredId = jobId ? taskOutputMessages.get(jobId) : undefined;
        if (jobId) taskOutputMessages.delete(jobId);

        let supersededId: string | null = null;
        if (resultText && registeredId) {
          // `isStreaming: false` on EVERY supersede path, not just the bubble
          // branch below. Superseding skips that branch, and it used to be the
          // only place the typing indicator was cleared — so a folded message
          // kept its three bouncing dots after the run had finished. The
          // message the fold lands on is frequently the live bubble itself (its
          // streamed text is a prefix of the final answer, so the scan matches
          // it), which is why this is not a rare corner.
          const done = { content: resultText, isStreaming: false };
          if (ownerSession) {
            sessionStore.updateMessageInTargetSession(ownerSession, registeredId, done);
          } else {
            sessionStore.updateMessage(registeredId, done);
          }
          supersededId = registeredId;
        } else if (resultText) {
          supersededId = supersedeTruncatedTail(ownerSession, resultText);
        }
        if (supersededId) {
          // A bubble left open by an earlier task is not the message carrying
          // the answer, but it must still stop claiming to be typing — and it
          // must give up any streamed surface, which now belongs to the message
          // the answer folded into. Two messages holding the same deck renders
          // it twice.
          if (streamBubbleId && streamBubbleId !== supersededId && ownerSession) {
            sessionStore.updateMessageInTargetSession(ownerSession, streamBubbleId, {
              isStreaming: false,
            });
          }
          // The surface must ride along with the text we just folded into.
          // Until the text survived a composed surface, `body` was always empty
          // when one existed, so this branch could never be reached with a
          // surface and never applied `runExtra`. It can now — a run whose
          // bubble closed at a task boundary lands here — and without this the
          // answer would keep its text and silently lose its surface.
          //
          // ...unless the live bubble is ALREADY showing this run's streamed
          // surface. Then it has a home, and copying it here would render the
          // deck twice, once under the other. Clearing the bubble instead is not
          // an option: the persisted envelope is merged forward, so a surface
          // cannot be un-set by omission — leaving it where it already is, is
          // both simpler and the only thing that survives a reload.
          const surfaceHasAHome = bubbleCarriesSurface && streamBubbleId;
          if (runExtra && !surfaceHasAHome) {
            if (ownerSession) {
              sessionStore.updateMessageInTargetSession(
                ownerSession,
                supersededId,
                runExtra,
              );
            } else {
              sessionStore.updateMessage(supersededId, runExtra);
            }
          } else if (runExtra && surfaceHasAHome && ownerSession) {
            // The execution id still belongs on the folded answer.
            const { resultType: _t, resultData: _d, ...rest } = runExtra as Record<string, unknown>;
            if (Object.keys(rest).length) {
              sessionStore.updateMessageInTargetSession(ownerSession, supersededId, rest);
            }
          }
          // Register the message we folded into. Registering null here made the
          // late surface post the answer a SECOND time — the run's text showed
          // immediately, then again a few seconds later beneath the mindmap.
          rememberFinalizedMessage(jobId, ownerSession, supersededId);
        } else if (streamBubbleId && ownerSession) {
          // Finalize the live bubble in place — no flicker, no duplicate.
          sessionStore.updateMessageInTargetSession(ownerSession, streamBubbleId, {
            content: body,
            isStreaming: false,
            ...(runExtra ?? {}),
          });
          rememberFinalizedMessage(jobId, ownerSession, streamBubbleId);
        } else if (ownerSession) {
          const id = sessionStore.addMessageToTargetSession(
            ownerSession,
            'assistant',
            body,
            runExtra,
          );
          rememberFinalizedMessage(jobId, ownerSession, id);
        } else {
          const id = sessionStore.addMessage('assistant', body, runExtra);
          rememberFinalizedMessage(jobId, null, id);
        }
      }
    } else {
      // NO terminal text, and NO message posted for it.
      //
      // A flow lands here routinely: its early announcement carries the last
      // crew's raw output, which is often not readable text. "Execution
      // completed." was a status line dressed as an answer — the run-activity
      // row already says the run finished, so the notice added nothing and read
      // as the reply.
      //
      // The run is still REGISTERED, with no message id. A surface composed
      // afterwards (14s on a measured flow) then POSTS its own message rather
      // than filling a placeholder — see attachSurface. That is what lets the
      // notice go without the deliverable going with it.
      if (streamBubbleId && ownerSession) {
        // Streamed text exists but the terminal result is empty — keep the
        // streamed answer rather than replacing it with nothing.
        sessionStore.updateMessageInTargetSession(ownerSession, streamBubbleId, {
          isStreaming: false,
          ...(runExtra ?? {}),
        });
        rememberFinalizedMessage(jobId, ownerSession, streamBubbleId);
      } else {
        rememberFinalizedMessage(jobId, ownerSession, null);
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
    // Keep whatever streamed before the failure — it is often the only clue.
    if (jobId) discardStreamPacing(jobId, true);
    const failStreamBubbleId = jobId
      ? (streamBubbles.get(jobId) ?? lastStreamBubble.get(jobId))
      : undefined;
    if (jobId) {
      streamBubbles.delete(jobId);
      lastStreamBubble.delete(jobId);
      streamingSurfaces.delete(jobId);
      textPainted.delete(jobId);
      surfaceLedRuns.delete(jobId);
      lastSurfaceSeq.delete(jobId);
      // Terminal: a task_completed trace arriving after this is stale.
      markRunFinalized(jobId);
    }
    const currentSessionId = useSessionStore.getState().currentSessionId;
    const isViewingOwner = currentSessionId === ownerSession;
    const sessionStore = useSessionStore.getState();
    // Stop the live bubble's typing state; the error posts as its own message.
    if (failStreamBubbleId && ownerSession) {
      sessionStore.updateMessageInTargetSession(ownerSession, failStreamBubbleId, {
        isStreaming: false,
      });
    }

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
    // Abandoned: the run is gone, so queued text has nowhere to land.
    discardStreamPacing(jobId, false);
    streamBubbles.delete(jobId);
    lastStreamBubble.delete(jobId);
    streamingSurfaces.delete(jobId);
    textPainted.delete(jobId);
    surfaceLedRuns.delete(jobId);
    lastSurfaceSeq.delete(jobId);
    finalizedJobs.delete(jobId);
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
    discardStreamPacing(jobId, false);
    streamBubbles.delete(jobId);
    lastStreamBubble.delete(jobId);
    streamingSurfaces.delete(jobId);
    textPainted.delete(jobId);
    surfaceLedRuns.delete(jobId);
    lastSurfaceSeq.delete(jobId);
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
        selectedSkills: s.selectedSkills,
        activityPlacement: s.activityPlacement,
        workspaceMemory: s.workspaceMemory,
        memoryEnabled: s.memoryEnabled,
        chatModeType: s.chatModeType,
        preferExisting: s.preferExisting,
      }),
    },
  ),
);
