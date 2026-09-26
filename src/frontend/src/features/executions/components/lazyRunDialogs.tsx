import React, { Suspense, lazy, useState } from 'react';
import { ErrorBoundary } from '../../../shared/errors/ErrorBoundary';

/**
 * Run-detail dialogs, loaded on demand.
 *
 * The result, trace and log viewers pull in the markdown stack, the A2UI
 * renderer (recharts, d3) and the log viewer. None of it is needed to show
 * the run list, so Run History loads each dialog the first time it opens.
 */
export const LazyShowResult = lazy(() => import('./ShowResult'));
export const LazyShowTraceTimeline = lazy(() => import('./ShowTraceTimeline'));
export const LazyShowLogs = lazy(() => import('./ShowLogs'));
export const LazyRecipeEffectivenessDialog = lazy(() => import('./RecipeEffectivenessDialog'));

/**
 * A lazy dialog behind an error boundary. If its chunk fails to load (usually
 * a redeploy since the page was opened), the user gets a dialog offering a
 * reload instead of a blank screen: without a boundary the rejection
 * unmounts the whole app.
 */
export const LazyDialogBoundary: React.FC<{ children: React.ReactNode }> = ({ children }) => (
  <ErrorBoundary variant="dialog">
    <Suspense fallback={null}>{children}</Suspense>
  </ErrorBoundary>
);

interface MountWhenOpenedProps {
  open: boolean;
  children: React.ReactNode;
}

/**
 * Renders nothing until `open` first becomes true, then stays mounted so the
 * dialog keeps its close transition and internal state, exactly as the
 * eagerly-imported dialogs did.
 */
export const MountWhenOpened: React.FC<MountWhenOpenedProps> = ({ open, children }) => {
  const [opened, setOpened] = useState(open);
  if (open && !opened) setOpened(true);
  if (!opened && !open) return null;
  return <LazyDialogBoundary>{children}</LazyDialogBoundary>;
};
