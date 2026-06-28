import React, { useRef, useEffect, useState } from 'react';
import Box from '@mui/material/Box';
import { buttonResetSx, pulseSx, fadeInSx, pingSx } from '../../chatSx';
import { ChatMessage as ChatMessageType } from '../../types/chat';
import { ModelConfigResponse, GenerationCompleteData } from '../../types/dispatcher';
import { PlanData, FlowData } from '../../hooks/useDispatcher';
import ChatMessageComponent, { TraceEntryData } from './ChatMessage';
import { findInlineTraceRenderer } from './traces';
import ChatInput from './ChatInput';
import ThinkingStream from '../Preview/ThinkingStream';
import type { RunStep } from '../Preview/RunTimeline';
import OpenInFullIcon from '@mui/icons-material/OpenInFull';

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
  /** Dock the activity to the preview pane instead of this chat bar. */
  onTogglePlacement?: () => void;
}> = ({ groups, running, generating, onStop, streamSteps, onTogglePlacement }) => {
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

  // 12px ring spinner (shared by the leading "stopping" dot + the Stop button).
  // The original `border-t-transparent` Tailwind class is `!important`, so it
  // beat the inline accent borderTopColor — i.e. the top has always rendered
  // transparent; reproduced faithfully here.
  const spinnerSx = {
    width: 12,
    height: 12,
    borderRadius: '50%',
    border: '2px solid',
    borderColor: 'divider',
    borderTopColor: 'transparent',
    flexShrink: 0,
    animation: 'kasalSpin 1s linear infinite',
    '@keyframes kasalSpin': { to: { transform: 'rotate(360deg)' } },
  } as const;

  return (
    <Box sx={{ px: 2, my: 1, maxWidth: '48rem', ...fadeInSx }}>
      {/* No `overflow-hidden`: the crew card's Genie-space dropdown is an
          absolutely-positioned popover that must escape the container's bounds.
          The rounded border + bg already round the corners without clipping. */}
      <Box
        sx={{
          borderRadius: '12px',
          backgroundColor: 'background.default',
          border: 1,
          borderColor: 'divider',
        }}
      >
        <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, px: 1.5, py: 1 }}>
          {stopping ? (
            <Box sx={spinnerSx} aria-hidden="true" />
          ) : running ? (
            <Box component="span" sx={{ position: 'relative', display: 'flex', height: 8, width: 8, flexShrink: 0 }} aria-hidden="true">
              <Box
                component="span"
                sx={{ position: 'absolute', display: 'inline-flex', height: '100%', width: '100%', borderRadius: '9999px', opacity: 0.6, backgroundColor: 'primary.main', ...pingSx }}
              />
              <Box component="span" sx={{ position: 'relative', display: 'inline-flex', borderRadius: '9999px', height: 8, width: 8, backgroundColor: 'primary.main' }} />
            </Box>
          ) : (
            <Box component="svg" sx={{ width: 14, height: 14, flexShrink: 0, color: 'primary.main' }} fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
              <path strokeLinecap="round" strokeLinejoin="round" d="M4.5 12.75l6 6 9-13.5" />
            </Box>
          )}
          <Box
            component="button"
            type="button"
            onClick={() => setOpen((v) => !v)}
            disabled={!hasTimeline}
            sx={{
              ...buttonResetSx,
              display: 'flex',
              alignItems: 'center',
              gap: 0.75,
              flex: 1,
              textAlign: 'left',
              minWidth: 0,
              cursor: hasTimeline ? 'pointer' : 'default',
            }}
            aria-label={hasTimeline ? (open ? 'Collapse run activity' : 'Expand run activity') : undefined}
          >
            {liveStep ? (
              <Box
                component="span"
                sx={{ fontSize: 12, minWidth: 0, overflow: 'hidden', whiteSpace: 'nowrap', textOverflow: 'ellipsis', textAlign: 'left', color: 'text.secondary', ...pulseSx }}
                title={liveStep.line ? `${liveStep.name} — ${liveStep.line}` : liveStep.name}
              >
                <Box component="span" sx={{ fontWeight: 600, color: 'text.primary' }}>{liveStep.name}</Box>
                {liveStep.line && <span> — {liveStep.line}</span>}
              </Box>
            ) : (
              <Box
                component="span"
                sx={{ fontSize: 12, fontWeight: 500, color: 'text.secondary', ...(running ? pulseSx : {}) }}
              >
                {label}
                {generating && !stopping && (
                  <Box
                    component="span"
                    aria-hidden="true"
                    data-testid="thinking-dots"
                    sx={{
                      '& span': { opacity: 0, animation: 'kasalThinkingDot 1.2s infinite' },
                      '& span:nth-of-type(2)': { animationDelay: '0.2s' },
                      '& span:nth-of-type(3)': { animationDelay: '0.4s' },
                      '@keyframes kasalThinkingDot': { '0%, 60%, 100%': { opacity: 0 }, '20%, 40%': { opacity: 1 } },
                    }}
                  >
                    <span>.</span>
                    <span>.</span>
                    <span>.</span>
                  </Box>
                )}
              </Box>
            )}
            {hasTimeline && (
              <Box
                component="svg"
                sx={{ width: 14, height: 14, flexShrink: 0, transition: 'transform 0.15s', color: 'text.disabled', transform: open ? 'rotate(90deg)' : 'rotate(0deg)' }}
                fill="none"
                viewBox="0 0 24 24"
                stroke="currentColor"
                strokeWidth={2}
              >
                <path strokeLinecap="round" strokeLinejoin="round" d="M8.25 4.5l7.5 7.5-7.5 7.5" />
              </Box>
            )}
          </Box>
          {onTogglePlacement && (
            <Box
              component="button"
              type="button"
              onClick={onTogglePlacement}
              sx={{
                ...buttonResetSx,
                flexShrink: 0,
                width: 24,
                height: 24,
                borderRadius: '6px',
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'center',
                transition: 'color 0.15s, background-color 0.15s',
                color: 'text.disabled',
                '&:hover': { opacity: 0.8 },
              }}
              title="Open the activity in the side panel"
              aria-label="Open the activity in the side panel"
            >
              <OpenInFullIcon sx={{ fontSize: 15 }} />
            </Box>
          )}
          {onStop && (
            <Box
              component="button"
              type="button"
              onClick={() => {
                setStopping(true);
                onStop();
              }}
              disabled={stopping}
              aria-label={stopping ? 'Stopping…' : 'Stop execution'}
              title={stopping ? 'Stopping…' : 'Stop execution'}
              sx={{
                ...buttonResetSx,
                ml: 'auto',
                width: 24,
                height: 24,
                borderRadius: '6px',
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'center',
                transition: 'color 0.15s, background-color 0.15s',
                flexShrink: 0,
                color: 'text.secondary',
                backgroundColor: (t) => t.chat.bgSecondary,
                border: 1,
                borderColor: 'divider',
                '&:hover': { opacity: 0.8 },
              }}
            >
              {stopping ? (
                <Box sx={spinnerSx} aria-hidden="true" />
              ) : (
                <Box component="svg" sx={{ width: 12, height: 12 }} viewBox="0 0 24 24" fill="currentColor" aria-hidden="true">
                  <rect x="6" y="6" width="12" height="12" rx="2" />
                </Box>
              )}
            </Box>
          )}
        </Box>
        {open && hasTimeline && (
          <Box sx={{ px: 2, py: 1.5, maxHeight: '60vh', overflowY: 'auto', borderTop: 1, borderColor: 'divider' }}>
            <ThinkingStream steps={displaySteps} live={running} />
          </Box>
        )}
      </Box>
    </Box>
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
  /** When true, the run activity lives HERE in the chat: the "Working…" bar
   *  expands to the same thinking stream as the preview pane. */
  activityInChat?: boolean;
  /** The latest run's steps (for the chat thinking stream when activityInChat). */
  runSteps?: RunStep[];
  /** Toggle the activity between the chat bar and the preview pane. */
  onToggleActivityPlacement?: () => void;
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
  hydrating,
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
  hideLiveTimeline,
  activityInChat,
  runSteps,
  onToggleActivityPlacement,
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

  // While a persisted session is being restored on load, don't treat the
  // (momentarily) empty message list as a new chat — otherwise the greeting
  // flashes for a frame before the restored conversation arrives.
  const isEmpty = messages.length === 0 && !hydrating;


  // Empty state: everything centered vertically — greeting + input
  if (isEmpty && !isExecuting) {
    return (
      <Box sx={{ display: 'flex', flexDirection: 'column', alignItems: 'center', justifyContent: 'center', height: '100%', px: 3 }}>
        <Box sx={{ width: '100%', maxWidth: '48rem' }}>
          {/* Greeting */}
          <Box sx={{ textAlign: 'center', mb: 4 }}>
            <Box component="h1" sx={{ fontSize: '1.5rem', lineHeight: '2rem', fontWeight: 600, mb: 1, color: 'text.primary' }}>
              What can I help you with?
            </Box>
            <Box component="p" sx={{ fontSize: 14, lineHeight: 1.625, color: 'text.secondary' }}>
              Create agents, build crews, and execute workflows through natural conversation.
            </Box>
          </Box>

          {/* Input — centered */}
          <Box sx={{ position: 'relative' }}>
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
              menuPlacement="down"
            />
          </Box>
        </Box>
      </Box>
    );
  }

  // Conversation / executing state
  return (
    <Box sx={{ display: 'flex', flexDirection: 'column', height: '100%' }}>
      {/* Run/generation status is shown inline in the chat input (with a Stop
          control) rather than a top-of-screen banner — see ChatInput. */}

      {/* Messages */}
      <Box sx={{ flex: 1, overflowY: 'auto' }}>
        <Box sx={{ py: 3, maxWidth: '48rem', mx: 'auto', width: '100%' }}>
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
              return (
                <RunProgress
                  key={`run-progress-${s}`}
                  groups={msgs.length > 0 ? [{ kind: 'traceGroup', key: `seg-${s}`, label: 'run', msgs }] : []}
                  running={live}
                  generating={live && Boolean(isGenerating)}
                  onStop={live && isExecuting && onStopExecution ? onStopExecution : undefined}
                  streamSteps={useStream ? (runSteps ?? []) : undefined}
                  // The "Show in panel" toggle only makes sense in 'chat' placement.
                  onTogglePlacement={useStream && activityInChat ? onToggleActivityPlacement : undefined}
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
                {/* After the run ends the live trace messages may be gone, but the
                    activity is still available via runSteps (persistent / restored
                    from the job's traces). Keep the "Run activity" bar — expandable
                    inline AND openable in the side panel — so it doesn't vanish. */}
                {!running &&
                  Array.isArray(runSteps) &&
                  runSteps.length > 0 &&
                  !placedSegs.has(lastSeg) &&
                  renderRunProgress(lastSeg)}
                <div ref={messagesEndRef} />
              </>
            );
          })()}
        </Box>
      </Box>

      {/* Input pinned to bottom — also surfaces run/generation status + Stop */}
      <Box sx={{ maxWidth: '48rem', mx: 'auto', width: '100%', position: 'relative' }}>
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
      </Box>
    </Box>
  );
};

export default ChatContainer;
