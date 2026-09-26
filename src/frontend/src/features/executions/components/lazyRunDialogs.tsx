import React, { Suspense, useState } from 'react';
import { ErrorBoundary } from '../../../shared/errors/ErrorBoundary';
import { retryableLazy } from '../../../shared/errors/retryableLazy';

/**
 * Run-detail dialogs, loaded on demand.
 *
 * The result, trace and log viewers pull in the markdown stack, the A2UI
 * renderer (recharts, d3) and the log viewer. None of it is needed to show
 * the run list, so Run History loads each dialog the first time it opens.
 *
 * `retryableLazy`, not `React.lazy`: a plain lazy component caches a failed
 * import forever, so reopening the dialog could never load it again.
 */
export const LazyShowResult = retryableLazy(() => import('./ShowResult'));
export const LazyShowTraceTimeline = retryableLazy(() => import('./ShowTraceTimeline'));
export const LazyShowLogs = retryableLazy(() => import('./ShowLogs'));
export const LazyRecipeEffectivenessDialog = retryableLazy(
  () => import('./RecipeEffectivenessDialog'),
);

/**
 * Counts how many times `open` has gone from false to true. Used as the error
 * boundary's reset key: a dialog dismissed after a failed load stays dismissed
 * until the user opens it again, and that reopening retries the import.
 */
function useOpenCount(open: boolean | undefined): number {
  const [state, setState] = useState({ open, count: open ? 1 : 0 });
  if (open !== state.open) {
    setState({ open, count: open ? state.count + 1 : state.count });
  }
  // React re-renders straight away after a render-phase setState.
  return state.count;
}

interface LazyDialogBoundaryProps {
  children: React.ReactNode;
  /** The dialog's `open` prop; reopening after a failure retries the load. */
  open?: boolean;
  /** The dialog's `onClose`; closing the error dialog also closes the dialog. */
  onClose?: () => void;
}

/**
 * A lazy dialog behind an error boundary. If its chunk fails to load (usually
 * a redeploy since the page was opened), the user gets a dialog offering a
 * reload instead of a blank screen: without a boundary the rejection
 * unmounts the whole app. Closing that dialog really closes it (and tells the
 * owner through `onClose`); opening the dialog again retries the import.
 */
export const LazyDialogBoundary: React.FC<LazyDialogBoundaryProps> = ({
  children,
  open,
  onClose,
}) => {
  const openCount = useOpenCount(open);
  return (
    <ErrorBoundary variant="dialog" resetKeys={[openCount]} onDismiss={onClose}>
      <Suspense fallback={null}>{children}</Suspense>
    </ErrorBoundary>
  );
};

interface MountWhenOpenedProps {
  open: boolean;
  onClose?: () => void;
  children: React.ReactNode;
}

/**
 * Renders nothing until `open` first becomes true, then stays mounted so the
 * dialog keeps its close transition and internal state, exactly as the
 * eagerly-imported dialogs did.
 */
export const MountWhenOpened: React.FC<MountWhenOpenedProps> = ({ open, onClose, children }) => {
  const [opened, setOpened] = useState(open);
  if (open && !opened) setOpened(true);
  if (!opened && !open) return null;
  return (
    <LazyDialogBoundary open={open} onClose={onClose}>
      {children}
    </LazyDialogBoundary>
  );
};
