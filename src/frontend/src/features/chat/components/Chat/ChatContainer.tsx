import React, { useRef, useEffect, useState, useMemo, useCallback } from 'react';
import { ChatMessage as ChatMessageType, ImageRef } from '../../types/chat';
import { ModelConfigResponse, GenerationCompleteData } from '../../types/dispatcher';
import { PlanData, FlowData } from '../../hooks/useDispatcher';
import ChatMessageComponent, { TraceEntryData } from './ChatMessage';
import { findInlineTraceRenderer } from './traces/index';
import ChatInput from './ChatInput';
import ChatEmptyState from './ChatEmptyState';
import RunProgress from './RunProgress';
export { liveStepLine } from './RunProgress';
import type { RunStep } from '../Preview/traceEventStep';
import type { PreviewContent } from '../../types/preview';
import type { ExecutionContext } from '../../types/execution';

/**
 * Group trace messages for a readable timeline (shown inside the RunProgress
 * container):
 *
 *  - Tools that render their result INLINE (e.g. the Genie answer card) are
 *    never collapsed — the whole point is to show that answer in the chat.
 *  - Everything else — memory recall/search/save INCLUDED — collapses runs of
 *    consecutive same-label traces into one expandable line, preserving
 *    chronological order. (Memory is no longer folded into a single group: now
 *    that all activity lives in the collapsed container, the timeline reads
 *    better in the order things actually happened.)
 *
 * A non-trace message — or a different tool — breaks a run.
 */
type TraceGroupItem = { kind: 'traceGroup'; key: string; label: string; msgs: ChatMessageType[] };
type RenderItem = { kind: 'msg'; msg: ChatMessageType } | TraceGroupItem;

function groupChatItems(messages: ChatMessageType[]): RenderItem[] {
  const items: RenderItem[] = [];
  for (const msg of messages) {
    const trace = msg.resultType === 'trace' ? (msg.resultData as TraceEntryData | undefined) : undefined;
    const traceLabel = trace?.label;

    // Inline-rendered tool results (Genie) always stand alone so the answer
    // shows directly in the chat instead of behind a collapsed group.
    const rendersInline =
      trace?.kind === 'tool_result' && trace.detail
        ? Boolean(findInlineTraceRenderer(trace.detail, traceLabel))
        : false;
    if (traceLabel && rendersInline) {
      items.push({ kind: 'msg', msg });
      continue;
    }

    // Collapse consecutive same-tool runs (memory included), in chronological order.
    const last = items[items.length - 1];
    if (traceLabel && last && last.kind === 'traceGroup' && last.label === traceLabel) {
      last.msgs.push(msg);
    } else if (traceLabel) {
      items.push({ kind: 'traceGroup', key: msg.id, label: traceLabel, msgs: [msg] });
    } else {
      items.push({ kind: 'msg', msg });
    }
  }
  return items;
}

interface ChatContainerProps {
  messages: ChatMessageType[];
  /** True while a persisted session is still being restored on load — suppresses
   *  the empty "new chat" greeting so a refresh doesn't flash it before the
   *  conversation appears. */
  hydrating?: boolean;
  onSend: (
    message: string,
    meta?: {
      tools?: string[];
      dispatchSuffix?: string;
      attachments?: string[];
      knowledgeFilePaths?: string[];
      images?: ImageRef[];
    },
  ) => void;
  onCommand?: (command: string) => void;
  onExecuteCrew?: (plan: PlanData) => void;
  onExecuteFlow?: (flow: FlowData) => void;
  onExecuteGenerated?: (data: GenerationCompleteData, spaceId?: string) => void;
  onSaveCrew?: (data: GenerationCompleteData, opts?: { overwrite?: boolean; spaceId?: string }) => Promise<{ id: string; name: string }>;
  /** Answer mode: distill a reusable crew from the conversation and save it. */
  onSaveAnswerToCatalog?: (sessionId?: string) => void | Promise<void>;
  onSubmitVariables?: (messageId: string, inputs: Record<string, string>) => void;
  /** "Use existing" matched nothing — flip the source back and build one. */
  onBuildInstead?: (messageId: string) => void;
  onStopExecution?: () => void;
  isLoading: boolean;
  isExecuting?: boolean;
  isGenerating?: boolean;
  executionContext?: ExecutionContext | null;
  /** While the live run is monitored in the RIGHT preview pane (the clickable
   *  step timeline), suppress THIS chat's in-conversation live timeline so the
   *  steps aren't shown twice. The status row stays; only the
   *  expandable timeline of the live segment is hidden. Completed (historical)
   *  segments keep their timeline. */
  hideLiveTimeline?: boolean;
  /** The run currently in flight, so the LIVE segment can show its activity
   *  before the message that carries the execution id has arrived. */
  liveJobId?: string;
  /** Open a run in the side preview pane — its deliverable (A2UI surface or the
   *  plain-text answer) with the activity collapsed above. Wired to the per-run
   *  pane icon AND to individual step rows (which pass `focusStep` so the pane
   *  opens directly on that step's content); the pane is opt-in — click only. */
  onShowRunInPane?: (deliverable: PreviewContent | undefined, jobId?: string, focusStep?: RunStep) => void;
  models: ModelConfigResponse[];
  selectedModel: string;
  onModelChange: (model: string) => void;
  sessionId?: string | null;
  /** "Workspace memory" toggle — owned by the store, forwarded to the input. */
  /** "No memory" toggle — when false, crews run without memory. */
  memoryEnabled?: boolean;
  onMemoryEnabledChange?: (value: boolean) => void;
  /** A crew/flow loaded from the catalog that the submit button will run. */
  pendingRunLabel?: string;
  onRunPending?: () => void;
  /** A closed-but-persisted preview exists and can be reopened. Renders a
   *  "Show preview" pill ABOVE the composer (anchored to it, so it never
   *  overlaps the input the way a fixed-offset floating button did). */
  showReopenPreview?: boolean;
  onReopenPreview?: () => void;
}

const ChatContainer: React.FC<ChatContainerProps> = ({
  messages,
  hydrating,
  onSend,
  onCommand,
  onExecuteCrew,
  onExecuteFlow,
  onExecuteGenerated,
  onSaveCrew,
  onSaveAnswerToCatalog,
  onSubmitVariables,
  onBuildInstead,
  onStopExecution,
  isLoading,
  isExecuting,
  isGenerating,
  hideLiveTimeline,
  liveJobId,
  onShowRunInPane,
  models,
  selectedModel,
  onModelChange,
  sessionId,
  memoryEnabled,
  onMemoryEnabledChange,
  pendingRunLabel,
  onRunPending,
  showReopenPreview,
  onReopenPreview,
}) => {
  const messagesEndRef = useRef<HTMLDivElement>(null);
  // Suggestion chips drop text into the empty-state composer without sending; the
  // nonce lets re-picking the same chip re-apply (see ChatInput's prefill effect).
  const [prefill, setPrefill] = useState<{ text: string; nonce: number } | undefined>(undefined);
  const prefillComposer = (text: string) => setPrefill({ text, nonce: Date.now() });

  const scrollContainerRef = useRef<HTMLDivElement>(null);
  // "Jump to latest" affordance: streamed surfaces (iframes) resize while the
  // reader is scrolled up, which makes scrolling back down by hand feel like
  // fighting the page. Shown once the reader is meaningfully above the bottom.
  const [showJumpToLatest, setShowJumpToLatest] = useState(false);
  useEffect(() => {
    const el = scrollContainerRef.current;
    if (!el) return;
    const measure = () => {
      setShowJumpToLatest(el.scrollHeight - el.scrollTop - el.clientHeight > 300);
    };
    el.addEventListener('scroll', measure, { passive: true });
    // Content HEIGHT changes must re-measure too: a collapsing timeline or a
    // finished stream shrinks scrollHeight without any scroll event, and the
    // stale flag left the pill floating over an already-bottomed-out view.
    const ro =
      typeof ResizeObserver !== 'undefined' ? new ResizeObserver(measure) : null;
    ro?.observe(el);
    if (el.firstElementChild) ro?.observe(el.firstElementChild);
    return () => {
      el.removeEventListener('scroll', measure);
      ro?.disconnect();
    };
  }, []);
  const jumpToLatest = () => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' });
    setShowJumpToLatest(false);
  };
  const scrollToBottom = () => {
    // Don't fight the user: while they've scrolled up to read something older,
    // incoming trace ticks must not yank the view back down. Only follow when
    // already near the bottom — and jump instantly ('auto') instead of
    // restarting a smooth animation on every appended trace.
    const el = scrollContainerRef.current;
    if (el && el.scrollHeight - el.scrollTop - el.clientHeight > 160) return;
    messagesEndRef.current?.scrollIntoView({ behavior: 'auto' });
  };

  // Only follow the conversation to the bottom when a NEW message arrives or the
  // LATEST message updates (streaming). Editing an OLDER message in place — e.g.
  // restyling a surface's colors from a previous turn — must NOT yank the view to
  // the end (the user scrolled up deliberately). (count starts at 0 so the first
  // render still scrolls a freshly opened session to its latest message.)
  const prevCountRef = useRef(0);
  const prevLastRef = useRef<unknown>(undefined);
  useEffect(() => {
    const last = messages[messages.length - 1];
    const grew = messages.length > prevCountRef.current;
    const latestChanged = last !== prevLastRef.current;
    prevCountRef.current = messages.length;
    prevLastRef.current = last;
    if (grew || latestChanged) scrollToBottom();
  }, [messages]);

  // Stable identity so memoized message bubbles aren't invalidated per render.
  const handleCommand = useCallback((command: string) => {
    if (onCommand) {
      onCommand(command);
    } else {
      onSend(command);
    }
  }, [onCommand, onSend]);

  // Grouping + segmentation derive ONLY from messages; memoized so re-renders
  // from unrelated state (poll ticks, pane toggles, input state) don't regroup
  // the whole transcript. Deliverables stay UNSERIALIZED here — JSON.stringify
  // of every A2UI surface (decks, 100-row tables) on every render was a
  // main-thread tax; the string is built lazily when a pane icon is clicked.
  const grouped = useMemo(() => {
    // Generation/tool steps arrive as trace entries and fold into a
    // run-activity container — ONE PER PROMPT: each user message starts a new
    // run segment, so a follow-up prompt in the same session gets its own
    // activity section instead of merging into the previous one. Legacy
    // generation_complete messages render as normal bubbles.
    const items = groupChatItems(messages);
    let seg = 0;
    const itemsWithSeg = items.map((item) => {
      if (item.kind === 'msg' && item.msg.role === 'user') seg += 1;
      return { item, seg };
    });
    const lastSeg = seg;
    const segTraces = new Map<number, ChatMessageType[]>();
    for (const { item, seg: s } of itemsWithSeg) {
      if (item.kind === 'traceGroup') {
        const arr = segTraces.get(s) ?? [];
        arr.push(...item.msgs);
        segTraces.set(s, arr);
      }
    }
    // Each run's deliverable, so its pane icon opens the right artifact: an
    // A2UI surface wins; otherwise the plain-text answer
    // (chat mode). A later A2UI surface overrides an earlier text answer;
    // a text answer never displaces an A2UI surface already found.
    const segDeliverables = new Map<
      number,
      { type: 'ui'; raw: unknown } | { type: 'text'; data: string } | undefined
    >();
    for (const { item, seg: s } of itemsWithSeg) {
      if (item.kind !== 'msg') continue;
      const m = item.msg;
      if (m.role !== 'assistant') continue;
      if (m.resultType === 'a2ui' && m.resultData) {
        segDeliverables.set(s, { type: 'ui', raw: m.resultData });
      } else if (!m.resultType && m.content && m.content.trim()) {
        if (segDeliverables.get(s)?.type !== 'ui') {
          segDeliverables.set(s, { type: 'text', data: m.content });
        }
      }
    }
    // The run each segment's activity belongs to. The timeline is read from the
    // trace API by job id, so a segment with no execution id simply has no
    // activity to show (a plain answer with no run behind it).
    const segJobs = new Map<number, string>();
    for (const { item, seg: s } of itemsWithSeg) {
      // Both shapes carry it: the answer/result message, and the trace messages
      // folded into the activity group. Scanning only the former missed runs
      // whose id had so far appeared on a trace alone.
      const candidates = item.kind === 'msg' ? [item.msg] : item.msgs;
      for (const m of candidates) {
        if (m.executionId && !segJobs.has(s)) segJobs.set(s, m.executionId);
      }
    }
    return { itemsWithSeg, lastSeg, segTraces, segDeliverables, segJobs };
  }, [messages]);

  /** Serialize a deliverable only at click time (lazy JSON.stringify). */
  const toPreviewContent = useCallback(
    (d?: { type: 'ui'; raw: unknown } | { type: 'text'; data: string }): PreviewContent | undefined =>
      d === undefined ? undefined : d.type === 'ui' ? { type: 'ui', data: JSON.stringify(d.raw) } : d,
    [],
  );

  // While a persisted session is being restored on load, don't treat the
  // (momentarily) empty message list as a new chat — otherwise the greeting
  // flashes for a frame before the restored conversation arrives.
  const isEmpty = messages.length === 0 && !hydrating;

  // Reopen-preview pill: a closed-but-persisted deliverable can be brought back.
  // Anchored to the TOP of the composer (bottom-full) so it floats just above the
  // input and never overlaps it — a fixed bottom offset in the parent collided
  // with the variable-height composer box. Rendered inside whichever composer
  // wrapper is active (both made `relative`).
  const reopenPreviewPill = showReopenPreview && onReopenPreview ? (
    <div className="absolute bottom-full right-2 mb-2 z-10">
      <button
        onClick={onReopenPreview}
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
  ) : null;

  // Empty state: greeting on top, the composer as the centered hero, and the
  // first-run launchpad (mode chips + builder/docs bridge) BELOW it — the standard
  // LLM-chat zero-state layout (input is primary; starter chips are the fallback
  // the eye finds next).
  if (isEmpty && !isExecuting) {
    return (
      <div className="flex flex-col items-center justify-center h-full px-6">
        <div className="kasal-landing-hero w-full max-w-3xl">
          {/* Greeting — the rotating composer placeholder advertises what Kasal can
              build (dashboards, presentations, quizzes, …), so no subtitle needed. */}
          <div className="text-center mb-6">
            <h1 className="text-2xl font-semibold" style={{ color: 'var(--text-primary)' }}>
              What can I help you with?
            </h1>
          </div>

          {/* Input — centered hero */}
          <div className="relative">
            {reopenPreviewPill}
            <ChatInput
              onSend={onSend}
              disabled={isLoading}
              models={models}
              selectedModel={selectedModel}
              onModelChange={onModelChange}
              sessionId={sessionId}
              memoryEnabled={memoryEnabled}
              onMemoryEnabledChange={onMemoryEnabledChange}
              prefill={prefill}

              isLanding
            />
          </div>

          {/* Starter chips + builder/docs bridge — below the composer */}
          <ChatEmptyState onPrefill={prefillComposer} />
        </div>
      </div>
    );
  }

  // Conversation / executing state
  return (
    <div className="relative flex flex-col h-full">
      {/* Run/generation status is shown inline in the chat input (with a Stop
          control) rather than a top-of-screen banner — see ChatInput. */}

      {/* Messages — the wrapper is the pill's anchor: pinned to the SCROLL
          AREA's bottom edge, it floats over the last visible messages and can
          never land on the composer below (whose height varies). */}
      <div className="relative flex-1 min-h-0">
      {showJumpToLatest && (
        <button
          type="button"
          onClick={jumpToLatest}
          aria-label="Jump to latest"
          className="absolute bottom-4 left-1/2 -translate-x-1/2 z-30 flex items-center gap-1.5 rounded-full px-3.5 py-1.5 text-xs font-medium transition-colors hover:opacity-90"
          style={{
            backgroundColor: 'var(--bg-input)',
            boxShadow: 'var(--shadow-popover)',
            color: 'var(--text-secondary)',
          }}
        >
          <svg className="w-3.5 h-3.5" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
            <path strokeLinecap="round" strokeLinejoin="round" d="M19.5 13.5L12 21m0 0l-7.5-7.5M12 21V3" />
          </svg>
          Latest
        </button>
      )}
      <div ref={scrollContainerRef} className="h-full overflow-y-auto">
        <div className="py-6 max-w-3xl mx-auto w-full">
          {(() => {
            const { itemsWithSeg, lastSeg, segTraces, segDeliverables, segJobs } = grouped;
            const running = Boolean(isExecuting || isGenerating);
            const placedSegs = new Set<number>();
            const renderRunProgress = (s: number) => {
              const live = running && s === lastSeg;
              // 'preview' placement: the LATEST run's activity lives in the RIGHT
              // preview pane — don't duplicate it here. While live we keep a compact
              // status row; once done there's nothing left to show, so skip.
              const inPreviewPane = Boolean(hideLiveTimeline) && s === lastSeg;
              if (inPreviewPane && !live) return null;
              const msgs = inPreviewPane ? [] : (segTraces.get(s) ?? []);
              // The run this segment's activity belongs to — what the pane is
              // pinned to when opened from here, and what the expanded timeline
              // reads. The LATEST run is left unpinned so the pane tracks the
              // live run as it goes.
              const jobForSeg = segJobs.get(s) ?? (live ? liveJobId : undefined);
              const pinnedJob = s === lastSeg ? undefined : jobForSeg;
              return (
                <RunProgress
                  key={`run-progress-${s}`}
                  latestStep={msgs[msgs.length - 1]?.resultData as TraceEntryData | undefined}
                  running={live}
                  generating={live && Boolean(isGenerating)}
                  jobId={jobForSeg}
                  // Pane icon on every run card — opens THIS run's deliverable +
                  // activity in the side pane. Opt-in: nothing opens until clicked
                  // (deliverable serialization also happens only at click time).
                  onShowInPane={onShowRunInPane ? () => onShowRunInPane(toPreviewContent(segDeliverables.get(s)), pinnedJob) : undefined}
                  // A step ROW opens the pane focused on that step's content.
                  onSelectStep={
                    onShowRunInPane
                      ? (step) => onShowRunInPane(toPreviewContent(segDeliverables.get(s)), pinnedJob, step)
                      : undefined
                  }
                />
              );
            };

            return (
              <>
                {itemsWithSeg.map(({ item, seg: s }) => {
                  // A segment's whole trace activity renders once, anchored at
                  // its first trace position; inline answers render as usual.
                  if (item.kind === 'traceGroup') {
                    if (placedSegs.has(s)) return null;
                    placedSegs.add(s);
                    return renderRunProgress(s);
                  }
                  const msg = item.msg;
                  // Anchor the activity directly UNDER the question that started
                  // the run, not wherever its first trace happened to land.
                  //
                  // Those are not the same position. Tokens open the answer
                  // bubble on the model's first word, which is often before any
                  // trace has arrived — so the activity anchored itself BELOW
                  // the finished answer, reading as though the work happened
                  // after the reply. Work first, then the answer.
                  if (
                    msg.role === 'user' &&
                    !placedSegs.has(s) &&
                    ((segTraces.get(s)?.length ?? 0) > 0 || (running && s === lastSeg))
                  ) {
                    placedSegs.add(s);
                    return (
                      <React.Fragment key={`seg-${s}`}>
                        <ChatMessageComponent
                          key={msg.id}
                          message={msg}
                          onCommand={handleCommand}
                          onExecuteCrew={onExecuteCrew}
                          onExecuteFlow={onExecuteFlow}
                          onExecuteGenerated={onExecuteGenerated}
                          onSaveCrew={onSaveCrew}
                          onSaveAnswerToCatalog={onSaveAnswerToCatalog}
                          onSubmitVariables={onSubmitVariables}
                          onBuildInstead={onBuildInstead}
                        />
                        {renderRunProgress(s)}
                      </React.Fragment>
                    );
                  }
                  const bubble = (
                    <ChatMessageComponent
                      key={msg.id}
                      message={msg}
                      onCommand={handleCommand}
                      onExecuteCrew={onExecuteCrew}
                      onExecuteFlow={onExecuteFlow}
                      onExecuteGenerated={onExecuteGenerated}
                      onSaveCrew={onSaveCrew}
                      onSaveAnswerToCatalog={onSaveAnswerToCatalog}
                      onSubmitVariables={onSubmitVariables}
                      onBuildInstead={onBuildInstead}
                    />
                  );
                  return bubble;
                })}
                {/* Working with no trace for the current prompt yet → a fresh
                    container (Thinking…) sits at the end of the response. */}
                {running && !placedSegs.has(lastSeg) && renderRunProgress(lastSeg)}
                <div ref={messagesEndRef} />
              </>
            );
          })()}
        </div>
      </div>
      </div>

      {/* Input pinned to bottom — also surfaces run/generation status + Stop */}
      <div className="max-w-3xl mx-auto w-full relative">
        {reopenPreviewPill}
        <ChatInput
          onSend={onSend}
          disabled={isLoading}
          models={models}
          selectedModel={selectedModel}
          onModelChange={onModelChange}
          sessionId={sessionId}
          isExecuting={isExecuting}
          isGenerating={isGenerating}
          onStopExecution={onStopExecution}
          memoryEnabled={memoryEnabled}
          onMemoryEnabledChange={onMemoryEnabledChange}
          pendingRunLabel={pendingRunLabel}
          onRunPending={onRunPending}

        />
      </div>
    </div>
  );
};

export default ChatContainer;
