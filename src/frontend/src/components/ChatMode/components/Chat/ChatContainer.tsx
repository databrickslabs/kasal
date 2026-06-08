import React, { useRef, useEffect, useState } from 'react';
import { ChatMessage as ChatMessageType } from '../../types/chat';
import { ModelConfigResponse } from '../../types/dispatcher';
import { PlanData, FlowData } from '../../hooks/useDispatcher';
import { GenerationCompleteData } from '../../hooks/useGenerationStream';
import ChatMessageComponent, { TraceEntryData, formatDurationMs, GenerationCompleteCard } from './ChatMessage';
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
  crewCard?: React.ReactNode;
  onStop?: () => void;
}> = ({ groups, running, generating, crewCard, onStop }) => {
  const [open, setOpen] = useState(false);
  // Transient feedback: the moment Stop is pressed we show "Stopping…" (the
  // backend takes a beat to actually halt the run); cleared once it ends.
  const [stopping, setStopping] = useState(false);
  useEffect(() => {
    if (!running) setStopping(false);
  }, [running]);
  const hasTimeline = groups.length > 0;
  const label = stopping
    ? 'Stopping…'
    : generating
      ? 'Generating crew…'
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
            <span
              className={`text-xs font-medium ${running ? 'animate-pulse' : ''}`}
              style={{ color: 'var(--text-secondary)' }}
            >
              {label}
            </span>
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
        {/* Crew structure (agents/tasks + Genie selector + Run) — always visible
            so the Genie space can be picked / the crew run even while the activity
            timeline below stays collapsed. */}
        {crewCard && (
          <div className="px-3 pb-3" style={{ borderTop: '1px solid var(--border-color)' }}>
            {crewCard}
          </div>
        )}
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
  onSaveCrew?: (data: GenerationCompleteData) => Promise<{ id: string; name: string }>;
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
}

const ChatContainer: React.FC<ChatContainerProps> = ({
  messages,
  onSend,
  onCommand,
  onExecuteCrew,
  onExecuteFlow,
  onExecuteGenerated,
  onSaveCrew,
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
            // The generated crew structure lives INSIDE the run-activity container
            // (not as its own bubble), so the whole "working" experience reads as
            // one unit. Pull the latest generation_complete out of the flow and
            // mount its card in the container.
            const genMsg = messages.filter((m) => m.resultType === 'generation_complete').pop();
            const crewCard = genMsg ? (
              <GenerationCompleteCard
                data={genMsg.resultData as GenerationCompleteData}
                messageId={genMsg.id}
                onExecute={onExecuteGenerated}
                onSaveCrew={onSaveCrew}
              />
            ) : undefined;

            const items = groupChatItems(messages.filter((m) => m.resultType !== 'generation_complete'));
            const traceGroups = items.filter((i): i is TraceGroupItem => i.kind === 'traceGroup');
            const running = Boolean(isExecuting || isGenerating);
            const firstTraceKey = items.find((i) => i.kind === 'traceGroup')?.key;
            const runProgress = (
              <RunProgress
                key="run-progress"
                groups={traceGroups}
                running={running}
                generating={Boolean(isGenerating)}
                crewCard={crewCard}
                onStop={isExecuting && onStopExecution ? onStopExecution : undefined}
              />
            );
            return (
              <>
                {items.map((item) => {
                  // All background trace activity folds into the single RunProgress
                  // container (anchored at the first trace); everything else — incl.
                  // the inline Genie answer — renders in the conversation as usual.
                  if (item.kind === 'traceGroup') {
                    return item.key === firstTraceKey ? runProgress : null;
                  }
                  const msg = item.msg;
                  return (
                    <ChatMessageComponent
                      key={msg.id}
                      message={msg}
                      onCommand={handleCommand}
                      onExecuteCrew={onExecuteCrew}
                      onExecuteFlow={onExecuteFlow}
                      onExecuteGenerated={onExecuteGenerated}
                      onSaveCrew={onSaveCrew}
                    />
                  );
                })}
                {/* No trace activity yet, but we're generating/working or a crew
                    is ready to run → the container sits at the end of the response. */}
                {(running || crewCard) && traceGroups.length === 0 && runProgress}
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
        />
      </div>
    </div>
  );
};

export default ChatContainer;
