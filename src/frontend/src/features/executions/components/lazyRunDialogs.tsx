import React, { Suspense, lazy, useState } from 'react';

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
  return <Suspense fallback={null}>{children}</Suspense>;
};
