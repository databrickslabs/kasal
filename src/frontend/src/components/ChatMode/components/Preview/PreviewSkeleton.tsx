import React, { useEffect, useState } from 'react';
import Box from '@mui/material/Box';
import { useExecutionStore } from '../../store/executionStore';
import { friendlyStep, type RunStep } from './RunTimeline';
import ThinkingStream from './ThinkingStream';
import LogSurface from './LogSurface';
import { buttonResetSx, pulseSx } from '../../chatSx';

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
  /** Dock the activity into the chat's "Working…" bar instead of this pane. */
  onMoveActivityToChat?: () => void;
  /**
   * Whether the run is still in flight. Defaults to `true` (the live monitor).
   * When `false` the same surface reviews a FINISHED run that the user expanded
   * into the pane — it relabels ("Run activity", no pulsing "WORKING" badge, no
   * ticking elapsed) so it never claims to be running when it isn't.
   */
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
    <Box component="span" sx={{ fontFamily: 'monospace', fontVariantNumeric: 'tabular-nums' }} data-testid="preview-skeleton-elapsed">
      {mmss(elapsed)}
    </Box>
  );
};

const PreviewSkeleton: React.FC<PreviewSkeletonProps> = ({
  steps,
  onMoveActivityToChat,
  running = true,
}) => {
  const stepCount = steps.length;
  // A step the user opened to read its full context WHILE the run is still going
  // (null = show the live thinking stream). "Back" returns to the stream.
  const [activeStep, setActiveStep] = useState<RunStep | null>(null);

  return (
    <Box
      component="aside"
      sx={{
        display: 'flex',
        flexDirection: 'column',
        height: '100%',
        flex: '1 1 50%',
        minWidth: '300px',
        backgroundColor: 'background.default',
        borderLeft: 1,
        borderColor: 'divider',
      }}
      aria-busy={running}
      aria-label={running ? 'Building preview' : 'Run activity'}
    >
      {/* Header — mirrors PreviewPanel's so the swap to the live renderer is seamless */}
      <Box
        sx={{ display: 'flex', alignItems: 'center', gap: 1, px: 2, py: 1.5, flexShrink: 0, borderBottom: 1, borderColor: 'divider' }}
      >
        <Box component="span" sx={{ fontSize: 14, fontWeight: 500, color: 'text.primary' }}>
          {running ? 'Running agent…' : 'Run activity'}
        </Box>
        <Box
          component="span"
          sx={{
            fontSize: 10,
            px: 0.75,
            py: 0.25,
            borderRadius: '4px',
            fontFamily: 'monospace',
            display: 'inline-flex',
            alignItems: 'center',
            gap: 0.5,
            color: 'text.disabled',
            backgroundColor: (t) => t.chat.bgSecondary,
          }}
        >
          {running ? (
            <>
              <Box component="span" sx={{ width: 6, height: 6, borderRadius: '9999px', backgroundColor: 'primary.main', ...pulseSx }} />
              WORKING
            </>
          ) : (
            'DONE'
          )}
        </Box>
        {onMoveActivityToChat && (
          <Box
            component="button"
            type="button"
            onClick={onMoveActivityToChat}
            title="Show the activity in the chat instead"
            sx={{ ...buttonResetSx, ml: 'auto', fontSize: 11, transition: 'opacity 0.15s', color: 'text.disabled', '&:hover': { opacity: 0.8 } }}
          >
            Show in chat
          </Box>
        )}
      </Box>

      {activeStep ? (
        // A chosen step's context on its own page — readable mid-run too.
        <Box sx={{ flex: 1, minHeight: 0, display: 'flex', flexDirection: 'column' }}>
          <Box
            component="button"
            type="button"
            onClick={() => setActiveStep(null)}
            aria-label="Back to the run activity"
            sx={{
              ...buttonResetSx,
              display: 'flex',
              alignItems: 'center',
              gap: 0.75,
              width: '100%',
              px: 2,
              py: 1,
              flexShrink: 0,
              textAlign: 'left',
              fontSize: 11,
              fontWeight: 500,
              transition: 'opacity 0.15s',
              color: 'text.disabled',
              borderBottom: 1,
              borderColor: 'divider',
              '&:hover': { opacity: 0.8 },
            }}
          >
            <Box component="svg" sx={{ width: 12, height: 12, flexShrink: 0 }} fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
              <path strokeLinecap="round" strokeLinejoin="round" d="M15.75 19.5L8.25 12l7.5-7.5" />
            </Box>
            Back · {friendlyStep(activeStep.label)}
          </Box>
          <Box sx={{ flex: 1, minHeight: 0, overflowY: 'auto' }} data-testid="run-step-context">
            <LogSurface body={activeStep.detail || ''} />
          </Box>
        </Box>
      ) : (
        // Body — the live "thinking" stream; each step is clickable to read its
        // full context without waiting for the run to finish.
        <Box sx={{ flex: 1, minHeight: 0, display: 'flex', flexDirection: 'column', p: 3 }}>
          {/* Meta: steps so far + elapsed */}
          <Box sx={{ flexShrink: 0, display: 'flex', alignItems: 'center', justifyContent: 'space-between', mb: 2, fontSize: 11, color: 'text.disabled' }}>
            <span>
              {stepCount > 0
                ? `${stepCount} step${stepCount === 1 ? '' : 's'}${running ? ' so far' : ''}`
                : running
                  ? 'Starting…'
                  : 'No activity'}
            </span>
            {running && <RunElapsed />}
          </Box>
          <Box sx={{ flex: 1, minHeight: 0 }} data-testid="preview-skeleton-body">
            <ThinkingStream steps={steps} live={running} onSelect={setActiveStep} />
          </Box>
        </Box>
      )}
    </Box>
  );
};

export default PreviewSkeleton;
