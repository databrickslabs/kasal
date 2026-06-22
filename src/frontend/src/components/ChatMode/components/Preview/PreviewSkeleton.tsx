import React, { useEffect, useState } from 'react';
import { useExecutionStore } from '../../store/executionStore';
import { useSessionStore } from '../../store/sessionStore';
import ShowContextToggle from './ShowContextToggle';

/**
 * Whether the preview pane should show the in-progress skeleton: the viewed
 * session has a run underway (`runActive`) and no deliverable has rendered yet
 * (`hasPreview` is false). Once the first A2UI document arrives, `hasPreview`
 * flips true and the real {@link PreviewPanel} takes over.
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
 * Placeholder shown in the preview pane WHILE a run is executing and no
 * deliverable has rendered yet. For a long-running job a static shimmer reads as
 * "stalled", so this surfaces HONEST signs of life — the current activity, a
 * step count, and an elapsed timer — without revealing the (opt-in) retrieved
 * context. It mirrors PreviewPanel's `<aside>` shell + themed tokens so the
 * hand-off to the real renderer is seamless.
 */
const PreviewSkeleton: React.FC = () => {
  // Live progress signals, derived from the trace feed already accumulated in
  // the store (owner-gated). The full content stays behind the "Show context"
  // checkbox; here we only surface the latest activity LABEL + a count.
  const items = useExecutionStore((s) => s.transientPreview);
  const owner = useExecutionStore((s) => s.transientPreviewOwnerSessionId);
  const currentSession = useSessionStore((s) => s.currentSessionId);
  const owned = owner === currentSession ? items : [];
  const stepCount = owned.length;
  const latest = stepCount > 0 ? owned[stepCount - 1].label : '';

  // Elapsed timer — the skeleton mounts when execution starts, so mount time is
  // a good-enough run start. Ticks once a second; cleaned up on unmount.
  const [elapsed, setElapsed] = useState(0);
  useEffect(() => {
    const start = Date.now();
    const id = setInterval(() => setElapsed(Math.floor((Date.now() - start) / 1000)), 1000);
    return () => clearInterval(id);
  }, []);

  return (
    <aside
      className="flex flex-col h-full"
      style={{
        flex: '1 1 50%',
        minWidth: '300px',
        backgroundColor: 'var(--bg-primary)',
        borderLeft: '1px solid var(--border-color)',
      }}
      aria-busy="true"
      aria-label="Building preview"
    >
      {/* Header — mirrors PreviewPanel's so the swap to the live renderer is seamless */}
      <div
        className="flex items-center justify-between px-4 py-3 flex-shrink-0"
        style={{ borderBottom: '1px solid var(--border-color)' }}
      >
        <div className="flex items-center gap-2">
          <span className="text-sm font-medium" style={{ color: 'var(--text-primary)' }}>
            Running agent…
          </span>
          <span
            className="text-[10px] px-1.5 py-0.5 rounded font-mono inline-flex items-center gap-1"
            style={{ color: 'var(--text-muted)', backgroundColor: 'var(--bg-secondary)' }}
          >
            <span
              className="w-1.5 h-1.5 rounded-full animate-pulse"
              style={{ backgroundColor: 'var(--accent)' }}
            />
            WORKING
          </span>
        </div>
        <ShowContextToggle />
      </div>

      {/* Body */}
      <div className="flex-1 overflow-hidden p-6">
        {/* Live status line — honest progress so a long run never looks stalled. */}
        <div className="flex items-center gap-2 mb-5 text-[12px]" style={{ color: 'var(--text-muted)' }}>
          <span className="w-1.5 h-1.5 rounded-full animate-pulse flex-shrink-0" style={{ backgroundColor: 'var(--accent)' }} />
          <span className="truncate" style={{ color: 'var(--text-secondary)' }}>
            {latest ? `Running ${latest}` : 'Thinking…'}
          </span>
          {stepCount > 0 && (
            <span className="flex-shrink-0">· {stepCount} step{stepCount === 1 ? '' : 's'}</span>
          )}
          <span className="ml-auto font-mono tabular-nums flex-shrink-0" data-testid="preview-skeleton-elapsed">
            {mmss(elapsed)}
          </span>
        </div>

        {/* Shimmer body — neutral placeholder blocks, no agent content. */}
        <div className="animate-pulse flex flex-col gap-4" data-testid="preview-skeleton-body">
          <div className="h-8 rounded-lg" style={{ backgroundColor: 'var(--bg-secondary)', width: '60%' }} />
          <div className="h-4 rounded" style={{ backgroundColor: 'var(--bg-secondary)', width: '90%' }} />
          <div className="h-4 rounded" style={{ backgroundColor: 'var(--bg-secondary)', width: '80%' }} />
          <div className="h-4 rounded" style={{ backgroundColor: 'var(--bg-secondary)', width: '85%' }} />
          <div className="mt-4 grid grid-cols-2 gap-4">
            <div className="h-24 rounded-xl" style={{ backgroundColor: 'var(--bg-secondary)' }} />
            <div className="h-24 rounded-xl" style={{ backgroundColor: 'var(--bg-secondary)' }} />
          </div>
        </div>
      </div>
    </aside>
  );
};

export default PreviewSkeleton;
