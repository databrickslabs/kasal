import React, { useRef, useEffect, useState } from 'react';
import { PanelRight } from 'lucide-react';
import { ChatMessage as ChatMessageType } from '../../types/chat';
import { ModelConfigResponse, GenerationCompleteData } from '../../types/dispatcher';
import { PlanData, FlowData } from '../../hooks/useDispatcher';
import ChatMessageComponent, { TraceEntryData } from './ChatMessage';
import { findInlineTraceRenderer } from './traces';
import ChatInput from './ChatInput';
import ChatEmptyState from './ChatEmptyState';
import ThinkingStream from '../Preview/ThinkingStream';
import type { RunStep } from '../Preview/RunTimeline';
import type { PreviewContent } from '../Preview/PreviewPanel';

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

/** One-line live status for the collapsed header: the LATEST step's name plus
 *  the first line of its query/answer, so the box visibly progresses while the
 *  crew works (agent → task → memory query → memory answer → tool call → …)
 *  instead of sitting on a static "Working…". Full detail stays behind the
 *  expand chevron. For Memory results the interesting line is the retrieved
 *  context (detail), not the generic "context retrieved" sublabel. */
export function liveStepLine(step: TraceEntryData): { name: string; line: string } {
  const src = step.label === 'Memory' ? step.detail || step.sublabel : step.sublabel || step.detail;
  const line = (src || '').split('\n').map((l) => l.trim()).find((l) => l !== '') || '';
  return { name: step.label, line: line.length > 100 ? `${line.slice(0, 100)}…` : line };
}

/** Convert a segment's chat trace groups into RunStep[] (tool_result steps only)
 *  for the thinking stream — the same shape the preview pane uses. */
function deriveStepsFromGroups(groups: TraceGroupItem[]): RunStep[] {
  const out: RunStep[] = [];
  groups.flatMap((g) => g.msgs).forEach((m, i) => {
    const t = m.resultData as TraceEntryData | undefined;
    if (!t || t.kind !== 'tool_result' || !t.label) return;
    out.push({ id: m.id || `step-${i}`, label: t.label, sublabel: t.sublabel, detail: t.detail, durationMs: t.durationMs });
  });
  return out;
}

/**
 * A single, collapsible "run activity" container shown in the conversation flow:
 * a status row (pulsing dot while running, check when done) + a chevron that
 * expands into the timeline of background activity (Memory, tool calls, …), with
 * the Stop control on the right. The genie answer / final result render in the
 * chat itself — only the background plumbing lives in here.
 *
 * When the generated crew structure is available it is mounted (via `crewCard`)
 * in an ALWAYS-VISIBLE region just below the header — the collapse chevron only
 * gates the activity timeline, so the crew's Genie-space selector + Run button
 * are never hidden behind a collapsed section.
 */
const RunProgress: React.FC<{
  groups: TraceGroupItem[];
  running: boolean;
  generating: boolean;
  onStop?: () => void;
  /** When provided, the expanded section shows the "thinking" stream (the chat
   *  placement of the run activity) instead of the raw timeline. */
  streamSteps?: RunStep[];
  /** Open THIS run in the side preview pane (its deliverable + activity). Shown as
   *  a pane icon on every run card; the pane is opt-in, so it opens only on click. */
  onShowInPane?: () => void;
}> = ({ groups, running, generating, onStop, streamSteps, onShowInPane }) => {
  const [open, setOpen] = useState(false);
  // Transient feedback: the moment Stop is pressed we show "Stopping…" (the
  // backend takes a beat to actually halt the run); cleared once it ends.
  const [stopping, setStopping] = useState(false);
  useEffect(() => {
    if (!running) setStopping(false);
  }, [running]);
  // The activity ALWAYS renders as the thinking stream: use the caller-supplied
  // steps (the latest run) or derive them from this segment's trace groups
  // (historical runs) — the legacy raw timeline is gone.
  const displaySteps = streamSteps ?? deriveStepsFromGroups(groups);
  const hasTimeline = displaySteps.length > 0;
  // The latest streamed step drives a live one-liner in the header while the
  // run is active — the static labels are only fallbacks for the gaps before
  // the first trace arrives and after the run ends.
  const steps = groups.flatMap((g) => g.msgs.map((m) => m.resultData as TraceEntryData));
  const liveStep =
    !stopping && (running || generating) && steps.length > 0
      ? liveStepLine(steps[steps.length - 1])
      : null;
  // Done state is always "Run activity": the container only renders for
  // segments that HAVE trace activity (or while live, which the arms above
  // handle), so there is no idle/no-timeline rendering to label.
  const label = stopping
    ? 'Stopping…'
    : generating
      ? 'Thinking'
      : running
        ? 'Working…'
        : 'Run activity';

  return (
    <div className="px-4 my-2 max-w-3xl animate-fade-in">
      {/* No `overflow-hidden`: the crew card's Genie-space dropdown is an
          absolutely-positioned popover that must escape the container's bounds.
          The rounded border + bg already round the corners without clipping. */}
      <div
        className="rounded-xl"
        style={{ backgroundColor: 'var(--bg-primary)', border: '1px solid var(--border-color)' }}
      >
        <div className="flex items-center gap-2 px-3 py-2">
          {stopping ? (
            <div
              className="w-3 h-3 rounded-full border-2 border-t-transparent animate-spin flex-shrink-0"
              style={{ borderColor: 'var(--border-color)', borderTopColor: 'var(--accent)' }}
              aria-hidden="true"
            />
          ) : running ? (
            <span className="relative flex h-2 w-2 flex-shrink-0" aria-hidden="true">
              <span
                className="animate-ping absolute inline-flex h-full w-full rounded-full opacity-60"
                style={{ backgroundColor: 'var(--accent)' }}
              />
              <span className="relative inline-flex rounded-full h-2 w-2" style={{ backgroundColor: 'var(--accent)' }} />
            </span>
          ) : (
            <svg className="w-3.5 h-3.5 flex-shrink-0" style={{ color: 'var(--accent)' }} fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
              <path strokeLinecap="round" strokeLinejoin="round" d="M4.5 12.75l6 6 9-13.5" />
            </svg>
          )}
          <button
            type="button"
            onClick={() => setOpen((v) => !v)}
            disabled={!hasTimeline}
            className="flex items-center gap-1.5 flex-1 text-left min-w-0"
            style={{ cursor: hasTimeline ? 'pointer' : 'default' }}
            aria-label={hasTimeline ? (open ? 'Collapse run activity' : 'Expand run activity') : undefined}
          >
            {liveStep ? (
              <span
                className="text-xs animate-pulse min-w-0 overflow-hidden whitespace-nowrap text-ellipsis text-left"
                style={{ color: 'var(--text-secondary)' }}
                title={liveStep.line ? `${liveStep.name} — ${liveStep.line}` : liveStep.name}
              >
                <span className="font-semibold" style={{ color: 'var(--text-primary)' }}>{liveStep.name}</span>
                {liveStep.line && <span> — {liveStep.line}</span>}
              </span>
            ) : (
              <span
                className={`text-xs font-medium ${running ? 'animate-pulse' : ''}`}
                style={{ color: 'var(--text-secondary)' }}
              >
                {label}
                {generating && !stopping && (
                  <span className="kasal-thinking-dots" aria-hidden="true">
                    <span>.</span>
                    <span>.</span>
                    <span>.</span>
                  </span>
                )}
              </span>
            )}
            {hasTimeline && (
              <svg
                className="w-3.5 h-3.5 flex-shrink-0 transition-transform"
                style={{ color: 'var(--text-muted)', transform: open ? 'rotate(90deg)' : 'rotate(0deg)' }}
                fill="none"
                viewBox="0 0 24 24"
                stroke="currentColor"
                strokeWidth={2}
              >
                <path strokeLinecap="round" strokeLinejoin="round" d="M8.25 4.5l7.5 7.5-7.5 7.5" />
              </svg>
            )}
          </button>
          {onShowInPane && (
            <button
              type="button"
              onClick={onShowInPane}
              aria-label="Show in panel"
              className="w-6 h-6 rounded-md flex items-center justify-center flex-shrink-0 transition-colors hover:opacity-80"
              style={{ color: 'var(--text-muted)' }}
              title="Show this run in the preview panel"
            >
              <PanelRight size={14} aria-hidden="true" />
            </button>
          )}
          {onStop && (
            <button
              type="button"
              onClick={() => {
                setStopping(true);
                onStop();
              }}
              disabled={stopping}
              aria-label={stopping ? 'Stopping…' : 'Stop execution'}
              title={stopping ? 'Stopping…' : 'Stop execution'}
              className="ml-auto w-6 h-6 rounded-md flex items-center justify-center transition-colors hover:opacity-80 flex-shrink-0 disabled:cursor-default"
              style={{ color: 'var(--text-secondary)', backgroundColor: 'var(--bg-secondary)', border: '1px solid var(--border-color)' }}
            >
              {stopping ? (
                <div
                  className="w-3 h-3 rounded-full border-2 border-t-transparent animate-spin"
                  style={{ borderColor: 'var(--border-color)', borderTopColor: 'var(--accent)' }}
                  aria-hidden="true"
                />
              ) : (
                <svg className="w-3 h-3" viewBox="0 0 24 24" fill="currentColor" aria-hidden="true">
                  <rect x="6" y="6" width="12" height="12" rx="2" />
                </svg>
              )}
            </button>
          )}
        </div>
        {open && hasTimeline && (
          <div className="px-4 py-3 max-h-[60vh] overflow-y-auto" style={{ borderTop: '1px solid var(--border-color)' }}>
            <ThinkingStream steps={displaySteps} live={running} />
          </div>
        )}
      </div>
    </div>
  );
};

export interface ExecutionContext {
  crewName: string;
  agents: { name: string; role?: string }[];
  tasks: { name: string }[];
}

interface ChatContainerProps {
  messages: ChatMessageType[];
  /** True while a persisted session is still being restored on load — suppresses
   *  the empty "new chat" greeting so a refresh doesn't flash it before the
   *  conversation appears. */
  hydrating?: boolean;
  onSend: (
    message: string,
    meta?: { tools?: string[]; dispatchSuffix?: string; attachments?: string[] },
  ) => void;
  onCommand?: (command: string) => void;
  onExecuteCrew?: (plan: PlanData) => void;
  onExecuteFlow?: (flow: FlowData) => void;
  onExecuteGenerated?: (data: GenerationCompleteData, spaceId?: string) => void;
  onSaveCrew?: (data: GenerationCompleteData, opts?: { overwrite?: boolean; spaceId?: string }) => Promise<{ id: string; name: string }>;
  /** Answer mode: distill a reusable crew from the conversation and save it. */
  onSaveAnswerToCatalog?: (sessionId?: string) => void | Promise<void>;
  onSubmitVariables?: (messageId: string, inputs: Record<string, string>) => void;
  onStopExecution?: () => void;
  isLoading: boolean;
  isExecuting?: boolean;
  isGenerating?: boolean;
  executionContext?: ExecutionContext | null;
  /** While the live run is monitored in the RIGHT preview pane (the clickable
   *  step timeline), suppress THIS chat's in-conversation live timeline so the
   *  steps aren't shown twice. The status row + Stop control stay; only the
   *  expandable timeline of the live segment is hidden. Completed (historical)
   *  segments keep their timeline. */
  hideLiveTimeline?: boolean;
  /** The latest run's steps (for the chat thinking stream). */
  runSteps?: RunStep[];
  /** Open a run in the side preview pane — its deliverable (A2UI surface or the
   *  plain-text answer) with the activity collapsed above. Wired to the per-run
   *  pane icon; the pane is opt-in, so it opens only on this click. */
  onShowRunInPane?: (deliverable: PreviewContent | undefined, steps: RunStep[]) => void;
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
  /** Open the MCP configuration dialog (from the composer's "+" picker). */
  onOpenMcpConfig?: () => void;
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
  onStopExecution,
  isLoading,
  isExecuting,
  isGenerating,
  hideLiveTimeline,
  runSteps,
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
  onOpenMcpConfig,
}) => {
  const messagesEndRef = useRef<HTMLDivElement>(null);
  // Suggestion chips drop text into the empty-state composer without sending; the
  // nonce lets re-picking the same chip re-apply (see ChatInput's prefill effect).
  const [prefill, setPrefill] = useState<{ text: string; nonce: number } | undefined>(undefined);
  const prefillComposer = (text: string) => setPrefill({ text, nonce: Date.now() });

  const scrollToBottom = () => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' });
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

  const handleCommand = (command: string) => {
    if (onCommand) {
      onCommand(command);
    } else {
      onSend(command);
    }
  };

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
        <div className="w-full max-w-3xl">
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
              onOpenMcpConfig={onOpenMcpConfig}
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
    <div className="flex flex-col h-full">
      {/* Run/generation status is shown inline in the chat input (with a Stop
          control) rather than a top-of-screen banner — see ChatInput. */}

      {/* Messages */}
      <div className="flex-1 overflow-y-auto">
        <div className="py-6 max-w-3xl mx-auto w-full">
          {(() => {
            // Generation/tool steps arrive as trace entries and fold into a
            // run-activity container — ONE PER PROMPT: each user message
            // starts a new run segment, so a follow-up prompt in the same
            // session gets its own activity section instead of merging into
            // the previous one. Legacy generation_complete messages render
            // as normal bubbles (ChatMessage decides what they show).
            const items = groupChatItems(messages);
            const running = Boolean(isExecuting || isGenerating);

            // Assign each item to the run segment opened by the latest user
            // message, then merge that segment's trace groups into one timeline.
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
            // A2UI surface (research/deep) wins; otherwise the plain-text answer
            // (chat mode). A later A2UI surface overrides an earlier text answer;
            // a text answer never displaces an A2UI surface already found.
            const segDeliverables = new Map<number, PreviewContent | undefined>();
            for (const { item, seg: s } of itemsWithSeg) {
              if (item.kind !== 'msg') continue;
              const m = item.msg;
              if (m.role !== 'assistant') continue;
              if (m.resultType === 'a2ui' && m.resultData) {
                segDeliverables.set(s, { type: 'ui', data: JSON.stringify(m.resultData) });
              } else if (!m.resultType && m.content && m.content.trim()) {
                if (segDeliverables.get(s)?.type !== 'ui') {
                  segDeliverables.set(s, { type: 'text', data: m.content });
                }
              }
            }
            const placedSegs = new Set<number>();
            const renderRunProgress = (s: number) => {
              const live = running && s === lastSeg;
              // 'preview' placement: the LATEST run's activity lives in the RIGHT
              // preview pane — don't duplicate it here. While live we keep a compact
              // status row + Stop; once done there's nothing left to show, so skip.
              const inPreviewPane = Boolean(hideLiveTimeline) && s === lastSeg;
              if (inPreviewPane && !live) return null;
              // Whenever the CHAT hosts the latest run's activity — 'chat' placement,
              // OR a run that produced no preview-pane deliverable — render the same
              // thinking stream (not the legacy raw timeline). Older segments keep
              // their own historical timeline (runSteps only covers the latest run).
              const useStream = s === lastSeg && !inPreviewPane && Array.isArray(runSteps);
              const msgs = inPreviewPane ? [] : (segTraces.get(s) ?? []);
              // Steps for THIS run when opened in the pane. The LATEST run is left
              // empty so the pane tracks the live/durable `runActivitySteps` (which
              // keep updating as the run streams); a HISTORICAL run is pinned to its
              // own trace steps so its pane shows the activity that belongs to it.
              const traceMsgs = segTraces.get(s) ?? [];
              const stepsForSeg: RunStep[] = s === lastSeg
                ? []
                : deriveStepsFromGroups([{ kind: 'traceGroup', key: `seg-${s}`, label: 'run', msgs: traceMsgs }]);
              return (
                <RunProgress
                  key={`run-progress-${s}`}
                  groups={msgs.length > 0 ? [{ kind: 'traceGroup', key: `seg-${s}`, label: 'run', msgs }] : []}
                  running={live}
                  generating={live && Boolean(isGenerating)}
                  onStop={live && isExecuting && onStopExecution ? onStopExecution : undefined}
                  streamSteps={useStream ? (runSteps ?? []) : undefined}
                  // Pane icon on every run card — opens THIS run's deliverable +
                  // activity in the side pane. Opt-in: nothing opens until clicked.
                  onShowInPane={onShowRunInPane ? () => onShowRunInPane(segDeliverables.get(s), stepsForSeg) : undefined}
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
          onOpenMcpConfig={onOpenMcpConfig}
        />
      </div>
    </div>
  );
};

export default ChatContainer;
