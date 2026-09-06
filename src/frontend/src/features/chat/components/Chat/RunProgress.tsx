import React, { useEffect, useState } from 'react';
import { PanelRight } from 'lucide-react';
import type { TraceEntryData } from './ChatMessage';
import RunTraceTimeline from '../Preview/RunTraceTimeline';
import { traceEventToRunStep, type RunStep } from '../Preview/traceEventStep';
import { useRunTimeline } from '../../hooks/useRunTimeline';

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
  latestStep?: TraceEntryData;
  /** Let the surrounding conversation own spacing and scrolling. */
  inline?: boolean;
  running: boolean;
  generating: boolean;
  onStop?: () => void;
  /** Open THIS run in the side preview pane (its deliverable + activity). Shown as
   *  a pane icon on every run card; the pane is opt-in, so it opens only on click. */
  onShowInPane?: () => void;
  /** Click an individual step row in the expanded timeline → open THAT step's
   *  context in the preview pane (not just the whole run via the pane icon). */
  onSelectStep?: (step: RunStep) => void;
  /** The run whose trace the expanded activity renders. */
  jobId?: string;
}> = ({ latestStep, inline = false, running, generating, onStop, onShowInPane, onSelectStep, jobId }) => {
  const [open, setOpen] = useState(inline && running);
  useEffect(() => {
    if (inline && running) setOpen(true);
  }, [inline, running, jobId]);
  // Transient feedback: the moment Stop is pressed we show "Stopping…" (the
  // backend takes a beat to actually halt the run); cleared once it ends.
  const [stopping, setStopping] = useState(false);
  useEffect(() => {
    if (!running) setStopping(false);
  }, [running]);
  // The expanded body is the run's trace, read from the trace API — the same
  // record the Execution Trace Timeline renders, so the rows ARE the events
  // rather than a second reading of them. Fetched only once expanded.
  const { processed, loading: timelineLoading } = useRunTimeline(jobId, running, open);
  // A run exists → there is activity to open. Gating on fetched rows instead
  // would leave the chevron dead until the first fetch returned.
  const hasTimeline = Boolean(jobId);
  // The latest streamed step drives a live one-liner in the header while the
  // run is active — the static labels are only fallbacks for the gaps before
  // the first trace arrives and after the run ends.
  const liveStep =
    !stopping && (running || generating) && latestStep
      ? liveStepLine(latestStep)
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
    <div data-run-activity className={inline ? "my-2" : "px-4 my-2 max-w-3xl"}>
      {/* No `overflow-hidden`: the crew card's Genie-space dropdown is an
          absolutely-positioned popover that must escape the container's bounds.
          The rounded border + bg already round the corners without clipping. */}
      {/* No card surface at all — the activity sits directly on the stage,
          exactly like the conversation text around it. Any box (opaque,
          frosted, bordered) read as a foreign panel against the vignette. */}
      <div className="rounded-xl">
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
          <div className={inline ? "pb-3 pt-1" : "px-4 pb-3 pt-1 max-h-[60vh] overflow-y-auto"}>
            {/* Rows with content are clickable: they open that step's output in
                the preview panel (same master→detail the pane itself offers). */}
            <RunTraceTimeline
              processed={processed}
              loading={timelineLoading}
              live={running}
              onSelectEvent={onSelectStep ? (event) => onSelectStep(traceEventToRunStep(event)) : undefined}
            />
          </div>
        )}
      </div>
    </div>
  );
};

export default RunProgress;
