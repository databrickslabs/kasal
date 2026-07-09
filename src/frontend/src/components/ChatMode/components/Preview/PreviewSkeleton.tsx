import React, { useEffect, useState } from 'react';
import { useExecutionStore } from '../../store/executionStore';
import { friendlyStep, type RunStep } from './RunTimeline';
import ThinkingStream from './ThinkingStream';
import LogSurface from './LogSurface';

export { friendlyStep };

/**
 * Whether the preview pane should show the in-progress skeleton: the viewed
 * session has a run underway (`runActive`) and no deliverable has rendered yet
 * (`hasPreview` is false). Once the first A2UI document arrives, `hasPreview`
 * flips true and the real {@link PreviewPanel} takes over (and shows the same
 * timeline collapsed above the result).
 *
 * Extracted as a pure predicate so the visibility rule is unit-testable without
 * mounting the whole ChatWorkspace.
 */
export function shouldShowPreviewSkeleton(args: { runActive: boolean; hasPreview: boolean }): boolean {
  return args.runActive && !args.hasPreview;
}

/** Format seconds as m:ss. */
function mmss(seconds: number): string {
  return `${Math.floor(seconds / 60)}:${String(seconds % 60).padStart(2, '0')}`;
}

/**
 * In-progress preview shown while a run is EXECUTING and no deliverable has
 * rendered yet. It is THE single run monitor (the chat suppresses its live
 * timeline while a preview pane is up): a {@link RunTimeline} of friendly phase
 * steps that tick in live, each CLICKABLE to reveal the context that step pulled
 * in. Plus a step count + elapsed timer; the deliverable "assembles" below.
 */
interface PreviewSkeletonProps {
  /** The run's steps (sourced from the persistent chat trace messages). */
  steps: RunStep[];
  /** Open directly on THIS step's content (master→detail pre-selected) — set
   *  when the user clicks a step ROW in the chat's activity dropdown. */
  focusStep?: RunStep | null;
  /** Dock the activity into the chat's "Working…" bar instead of this pane. */
  onMoveActivityToChat?: () => void;
  /** False once the run has ended but the activity stays docked into the pane —
   *  it relabels ("Run activity", no pulsing "WORKING" badge, no ticking
   *  elapsed) so it never claims to be running when it isn't. Defaults true. */
  running?: boolean;
}

/**
 * Isolated elapsed timer — its OWN component so the 1s tick re-renders only this
 * tiny node, never the whole skeleton / timeline (which otherwise flickered).
 * Anchored to the run's store-backed start time so it reflects the true duration
 * and survives a re-render.
 */
const RunElapsed: React.FC = () => {
  const runStartedAt = useExecutionStore((s) => s.runStartedAt);
  const [mountedAt] = useState(() => Date.now());
  const [now, setNow] = useState(() => Date.now());
  useEffect(() => {
    const id = setInterval(() => setNow(Date.now()), 1000);
    return () => clearInterval(id);
  }, []);
  const elapsed = Math.max(0, Math.floor((now - (runStartedAt ?? mountedAt)) / 1000));
  return (
    <span className="font-mono tabular-nums" data-testid="preview-skeleton-elapsed">
      {mmss(elapsed)}
    </span>
  );
};

const PreviewSkeleton: React.FC<PreviewSkeletonProps> = ({ steps, focusStep, running = true, onMoveActivityToChat }) => {
  const stepCount = steps.length;
  // A step the user opened to read its full context WHILE the run is still going
  // (null = show the live thinking stream). "Back" returns to the stream.
  const [activeStep, setActiveStep] = useState<RunStep | null>(null);
  // A step ROW clicked in the chat's activity dropdown lands here pre-selected.
  useEffect(() => {
    if (focusStep) setActiveStep(focusStep);
  }, [focusStep]);

  return (
    <aside
      className="flex flex-col h-full"
      style={{
        flex: '1 1 50%',
        minWidth: '300px',
        backgroundColor: 'var(--bg-primary)',
        borderLeft: '1px solid var(--border-color)',
      }}
      aria-busy={running}
      aria-label={running ? 'Building preview' : 'Run activity'}
    >
      {/* Header — mirrors PreviewPanel's so the swap to the live renderer is seamless */}
      <div
        className="flex items-center gap-2 px-4 py-3 flex-shrink-0"
        style={{ borderBottom: '1px solid var(--border-color)' }}
      >
        <span className="text-sm font-medium" style={{ color: 'var(--text-primary)' }}>
          {running ? 'Running agent…' : 'Run activity'}
        </span>
        {running && (
          <span
            className="text-[10px] px-1.5 py-0.5 rounded font-mono inline-flex items-center gap-1"
            style={{ color: 'var(--text-muted)', backgroundColor: 'var(--bg-secondary)' }}
          >
            <span className="w-1.5 h-1.5 rounded-full animate-pulse" style={{ backgroundColor: 'var(--accent)' }} />
            WORKING
          </span>
        )}
        {onMoveActivityToChat && (
          <button
            type="button"
            onClick={onMoveActivityToChat}
            className="ml-auto text-[11px] transition-colors hover:opacity-80"
            style={{ color: 'var(--text-muted)' }}
            title="Show the activity in the chat instead"
          >
            Show in chat
          </button>
        )}
      </div>

      {activeStep ? (
        // A chosen step's context on its own page — readable mid-run too.
        <div className="flex-1 min-h-0 flex flex-col">
          <button
            type="button"
            onClick={() => setActiveStep(null)}
            className="flex items-center gap-1.5 w-full px-4 py-2 flex-shrink-0 text-left text-[11px] font-medium transition-colors hover:opacity-80"
            style={{ color: 'var(--text-muted)', borderBottom: '1px solid var(--border-color)' }}
            aria-label="Back to the run activity"
          >
            <svg className="w-3 h-3 flex-shrink-0" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
              <path strokeLinecap="round" strokeLinejoin="round" d="M15.75 19.5L8.25 12l7.5-7.5" />
            </svg>
            Back · {friendlyStep(activeStep.label)}
          </button>
          <div className="flex-1 min-h-0 overflow-y-auto" data-testid="run-step-context">
            <LogSurface body={activeStep.detail || ''} />
          </div>
        </div>
      ) : (
        // Body — the live "thinking" stream; each step is clickable to read its
        // full context without waiting for the run to finish.
        <div className="flex-1 min-h-0 flex flex-col p-6">
          {/* Meta: steps so far + elapsed */}
          <div className="flex-shrink-0 flex items-center justify-between mb-4 text-[11px]" style={{ color: 'var(--text-muted)' }}>
            <span>
              {stepCount > 0
                ? `${stepCount} step${stepCount === 1 ? '' : 's'}${running ? ' so far' : ''}`
                : running
                  ? 'Starting…'
                  : 'No activity'}
            </span>
            {running && <RunElapsed />}
          </div>
          <div className="flex-1 min-h-0" data-testid="preview-skeleton-body">
            <ThinkingStream steps={steps} live={running} onSelect={setActiveStep} />
          </div>
        </div>
      )}
    </aside>
  );
};

export default PreviewSkeleton;
