import React from 'react';
import {
  Alert,
  AlertTitle,
  Box,
  Button,
  Dialog,
  DialogActions,
  DialogContent,
  DialogTitle,
  Typography,
} from '@mui/material';
import { isChunkLoadError, reloadPage } from './chunkErrors';
import { LazyRetryGeneration } from './retryableLazy';

/**
 * Catches a render error below it, most importantly a lazy chunk that failed
 * to load, and shows a way out instead of unmounting the whole tree.
 *
 * Without a boundary, React unmounts everything on an uncaught render error:
 * one missing chunk (the usual cause is a redeploy while the tab was open)
 * blanks the entire app. Put one around every `Suspense` that wraps `lazy()`
 * components.
 *
 * - `variant="page"` fills the main area (routes).
 * - `variant="section"` renders inline (a tab or panel).
 * - `variant="dialog"` renders as a dialog, for lazily loaded dialogs, whose
 *   inline position is usually not visible.
 *
 * `resetKeys`: when any of them changes (a route, a tab, a dialog reopening),
 * the boundary clears its error so navigating away recovers without a reload.
 *
 * Closing the `dialog` variant DISMISSES the boundary: it renders nothing until
 * a reset key changes. Re-rendering the children instead would only rethrow a
 * lazy chunk's cached rejection and reopen the same error dialog. `onDismiss`
 * lets the owner of the dialog's `open` state hear about it, so its state
 * agrees with what is on screen. Pair lazy dialogs with `retryableLazy` so the
 * render after a reset re-imports the chunk.
 */
export type ErrorBoundaryVariant = 'page' | 'section' | 'dialog';

interface ErrorBoundaryProps {
  children: React.ReactNode;
  variant?: ErrorBoundaryVariant;
  resetKeys?: ReadonlyArray<unknown>;
  /** Called when the user closes the `dialog` variant's fallback. */
  onDismiss?: () => void;
}

interface ErrorBoundaryState {
  error: unknown;
  dismissed: boolean;
  /** Bumped on every reset, so `retryableLazy` children re-import. */
  generation: number;
}

const changed = (a: ReadonlyArray<unknown> = [], b: ReadonlyArray<unknown> = []) =>
  a.length !== b.length || a.some((value, index) => !Object.is(value, b[index]));

export class ErrorBoundary extends React.Component<ErrorBoundaryProps, ErrorBoundaryState> {
  state: ErrorBoundaryState = { error: null, dismissed: false, generation: 0 };

  static getDerivedStateFromError(error: unknown): Partial<ErrorBoundaryState> {
    return { error: error ?? new Error('Unknown render error') };
  }

  componentDidCatch(error: unknown, info: React.ErrorInfo): void {
    console.error('[ErrorBoundary] render failed', error, info.componentStack);
  }

  componentDidUpdate(prev: ErrorBoundaryProps): void {
    const { error, dismissed } = this.state;
    if ((error || dismissed) && changed(prev.resetKeys, this.props.resetKeys)) {
      this.reset();
    }
  }

  reset = (): void => {
    this.setState(({ generation }) => ({ error: null, dismissed: false, generation: generation + 1 }));
  };

  dismiss = (): void => {
    this.setState({ error: null, dismissed: true });
    this.props.onDismiss?.();
  };

  render(): React.ReactNode {
    const { error, dismissed } = this.state;
    if (dismissed) return null;
    if (!error) {
      return (
        <LazyRetryGeneration.Provider value={this.state.generation}>
          {this.props.children}
        </LazyRetryGeneration.Provider>
      );
    }
    return (
      <ErrorFallback
        chunk={isChunkLoadError(error)}
        variant={this.props.variant ?? 'section'}
        onReset={this.reset}
        onDismiss={this.dismiss}
      />
    );
  }
}

interface ErrorFallbackProps {
  chunk: boolean;
  variant: ErrorBoundaryVariant;
  onReset: () => void;
  /** Closes the `dialog` variant; defaults to `onReset`. */
  onDismiss?: () => void;
}

export const ErrorFallback: React.FC<ErrorFallbackProps> = ({
  chunk,
  variant,
  onReset,
  onDismiss = onReset,
}) => {
  const title = chunk ? 'Kasal has been updated' : 'Something went wrong';
  const body = chunk
    ? 'This part of the app could not be loaded, usually because a new version was deployed after this page was opened. Reload the page to continue.'
    : 'This part of the app hit an unexpected error. Try again, or reload the page.';
  const actions = (
    <>
      {!chunk && (
        <Button onClick={onReset} color="inherit">
          Try again
        </Button>
      )}
      <Button onClick={reloadPage} variant="contained" size="small">
        Reload page
      </Button>
    </>
  );

  if (variant === 'dialog') {
    return (
      <Dialog open onClose={onDismiss} aria-labelledby="error-boundary-title">
        <DialogTitle id="error-boundary-title">{title}</DialogTitle>
        <DialogContent>
          <Typography variant="body2">{body}</Typography>
        </DialogContent>
        <DialogActions>
          <Button onClick={onDismiss} color="inherit">
            Close
          </Button>
          {actions}
        </DialogActions>
      </Dialog>
    );
  }

  const alert = (
    <Alert
      role="alert"
      severity={chunk ? 'info' : 'error'}
      action={<Box sx={{ display: 'flex', gap: 1, alignItems: 'center' }}>{actions}</Box>}
    >
      <AlertTitle>{title}</AlertTitle>
      {body}
    </Alert>
  );

  if (variant === 'page') {
    return (
      <Box
        sx={{ display: 'flex', justifyContent: 'center', alignItems: 'center', minHeight: '60vh', p: 3 }}
      >
        <Box sx={{ maxWidth: 640, width: '100%' }}>{alert}</Box>
      </Box>
    );
  }
  return <Box sx={{ p: 2 }}>{alert}</Box>;
};

export default ErrorBoundary;
