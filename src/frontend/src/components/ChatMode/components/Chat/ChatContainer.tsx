import React, { useRef, useEffect, useState } from 'react';
import { ChatMessage as ChatMessageType } from '../../types/chat';
import { ModelConfigResponse } from '../../types/dispatcher';
import { PlanData, FlowData } from '../../hooks/useDispatcher';
import { GenerationCompleteData } from '../../hooks/useGenerationStream';
import ChatMessageComponent, { TraceEntryData, formatDurationMs } from './ChatMessage';
import { findInlineTraceRenderer } from './traces';
import ChatInput from './ChatInput';

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

/** One step on the RunProgress timeline: a dot on the left rail, the call name
 *  in bold, its duration, a short summary line, and — behind a per-step toggle —
 *  the retrieved context / full tool output (kept hidden so the timeline stays
 *  scannable; the user expands a step only when they want to read what it pulled
 *  in). */
const TimelineStep: React.FC<{ step: TraceEntryData; last: boolean }> = ({ step, last }) => {
  const [open, setOpen] = useState(false);
  const name = step.label; // always set: a label-less trace never reaches the timeline
  const duration = formatDurationMs(step.durationMs);
  const summary = step.sublabel || ''; // short line, always visible
  // Retrieved context / full output — only when it adds something beyond the summary.
  const context = step.detail && step.detail !== step.sublabel ? step.detail : '';
  return (
    <li className="relative pl-5 pb-3 last:pb-0">
      <span aria-hidden="true" className="absolute left-0 top-1 w-2 h-2 rounded-full" style={{ backgroundColor: 'var(--accent)' }} />
      {!last && (
        <span aria-hidden="true" className="absolute left-[3px] top-3 bottom-0 w-px" style={{ backgroundColor: 'var(--border-color)' }} />
      )}
      <div className="flex items-baseline gap-2">
        <span className="text-xs font-semibold" style={{ color: 'var(--text-primary)' }}>{name}</span>
        {duration && (
          <span className="text-[10px] font-mono flex-shrink-0" style={{ color: 'var(--text-muted)' }}>{duration}</span>
        )}
      </div>
      {summary && (
        <div className="text-xs mt-1 whitespace-pre-wrap break-words" style={{ color: 'var(--text-secondary)' }}>
          {summary}
        </div>
      )}
      {context && (
        <div className="mt-1">
          <button
            type="button"
            onClick={() => setOpen((v) => !v)}
            aria-label={`${open ? 'Hide' : 'Show'} context for ${name}`}
            className="flex items-center gap-1 text-[10px] font-medium transition-colors hover:opacity-80"
            style={{ color: 'var(--text-muted)' }}
          >
            <svg
              className="w-3 h-3 transition-transform"
              style={{ transform: open ? 'rotate(90deg)' : 'rotate(0deg)' }}
              fill="none"
              viewBox="0 0 24 24"
              stroke="currentColor"
              strokeWidth={2}
            >
              <path strokeLinecap="round" strokeLinejoin="round" d="M8.25 4.5l7.5 7.5-7.5 7.5" />
            </svg>
            {open ? 'Hide context' : 'Show context'}
          </button>
          {open && (
            <div
              className="text-xs mt-1 whitespace-pre-wrap break-words max-h-40 overflow-y-auto rounded-md p-2"
              style={{ color: 'var(--text-secondary)', backgroundColor: 'var(--bg-secondary)', border: '1px solid var(--border-color)' }}
            >
              {context}
            </div>
          )}
        </div>
      )}
    </li>
  );
};

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
}> = ({ groups, running, generating, onStop }) => {
  const [open, setOpen] = useState(false);
  // Transient feedback: the moment Stop is pressed we show "Stopping…" (the
  // backend takes a beat to actually halt the run); cleared once it ends.
  const [stopping, setStopping] = useState(false);
  useEffect(() => {
    if (!running) setStopping(false);
  }, [running]);
  const hasTimeline = groups.length > 0;
  // The latest streamed step drives a live one-liner in the header while the
  // run is active — the static labels are only fallbacks for the gaps before
  // the first trace arrives and after the run ends.
  const steps = groups.flatMap((g) => g.msgs.map((m) => m.resultData as TraceEntryData));
  const liveStep =
    !stopping && (running || generating) && steps.length > 0
      ? liveStepLine(steps[steps.length - 1])
      : null;
  const label = stopping
    ? 'Stopping…'
    : generating
      ? 'Thinking'
      : running
        ? 'Working…'
        : hasTimeline
          ? 'Run activity'
          : 'Crew ready';

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
          <ol className="list-none flex flex-col px-4 py-3" style={{ borderTop: '1px solid var(--border-color)' }}>
            {groups
              .flatMap((g) => g.msgs.map((m) => m.resultData as TraceEntryData))
              .map((step, i, arr) => (
                <TimelineStep key={i} step={step} last={i === arr.length - 1} />
              ))}
          </ol>
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
  onSend: (
    message: string,
    meta?: { tools?: string[]; dispatchSuffix?: string; attachments?: string[] },
  ) => void;
  onCommand?: (command: string) => void;
  onExecuteCrew?: (plan: PlanData) => void;
  onExecuteFlow?: (flow: FlowData) => void;
  onExecuteGenerated?: (data: GenerationCompleteData, spaceId?: string) => void;
  onSaveCrew?: (data: GenerationCompleteData, opts?: { overwrite?: boolean; spaceId?: string }) => Promise<{ id: string; name: string }>;
  onSubmitVariables?: (messageId: string, inputs: Record<string, string>) => void;
  onStopExecution?: () => void;
  isLoading: boolean;
  isExecuting?: boolean;
  isGenerating?: boolean;
  executionContext?: ExecutionContext | null;
  models: ModelConfigResponse[];
  selectedModel: string;
  onModelChange: (model: string) => void;
  sessionId?: string | null;
  /** "Workspace memory" toggle — owned by the store, forwarded to the input. */
  workspaceMemory?: boolean;
  onWorkspaceMemoryChange?: (value: boolean) => void;
  /** "No memory" toggle — when false, crews run without memory. */
  memoryEnabled?: boolean;
  onMemoryEnabledChange?: (value: boolean) => void;
  /** A crew/flow loaded from the catalog that the submit button will run. */
  pendingRunLabel?: string;
  onRunPending?: () => void;
}

const ChatContainer: React.FC<ChatContainerProps> = ({
  messages,
  onSend,
  onCommand,
  onExecuteCrew,
  onExecuteFlow,
  onExecuteGenerated,
  onSaveCrew,
  onSubmitVariables,
  onStopExecution,
  isLoading,
  isExecuting,
  isGenerating,
  models,
  selectedModel,
  onModelChange,
  sessionId,
  workspaceMemory,
  onWorkspaceMemoryChange,
  memoryEnabled,
  onMemoryEnabledChange,
  pendingRunLabel,
  onRunPending,
}) => {
  const messagesEndRef = useRef<HTMLDivElement>(null);

  const scrollToBottom = () => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  };

  useEffect(() => {
    scrollToBottom();
  }, [messages]);

  const handleCommand = (command: string) => {
    if (onCommand) {
      onCommand(command);
    } else {
      onSend(command);
    }
  };

  const isEmpty = messages.length === 0;

  // Empty state: everything centered vertically — greeting + input
  if (isEmpty && !isExecuting) {
    return (
      <div className="flex flex-col items-center justify-center h-full px-6">
        <div className="w-full max-w-3xl">
          {/* Greeting */}
          <div className="text-center mb-8">
            <h1
              className="text-2xl font-semibold mb-2"
              style={{ color: 'var(--text-primary)' }}
            >
              What can I help you with?
            </h1>
            <p
              className="text-sm leading-relaxed"
              style={{ color: 'var(--text-secondary)' }}
            >
              Create agents, build crews, and execute workflows through natural conversation.
            </p>
          </div>

          {/* Input — centered */}
          <ChatInput
            onSend={onSend}
            disabled={isLoading}
            models={models}
            selectedModel={selectedModel}
            onModelChange={onModelChange}
            sessionId={sessionId}
            workspaceMemory={workspaceMemory}
            onWorkspaceMemoryChange={onWorkspaceMemoryChange}
            memoryEnabled={memoryEnabled}
            onMemoryEnabledChange={onMemoryEnabledChange}
          />
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
            const placedSegs = new Set<number>();
            const renderRunProgress = (s: number) => {
              const live = running && s === lastSeg;
              const msgs = segTraces.get(s) ?? [];
              return (
                <RunProgress
                  key={`run-progress-${s}`}
                  groups={msgs.length > 0 ? [{ kind: 'traceGroup', key: `seg-${s}`, label: 'run', msgs }] : []}
                  running={live}
                  generating={live && Boolean(isGenerating)}
                  onStop={live && isExecuting && onStopExecution ? onStopExecution : undefined}
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
      <div className="max-w-3xl mx-auto w-full">
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
          workspaceMemory={workspaceMemory}
          onWorkspaceMemoryChange={onWorkspaceMemoryChange}
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
