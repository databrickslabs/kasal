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
 * `resetKeys`: when any of them changes (a route, a tab), the boundary clears
 * its error so navigating away recovers without a reload.
 */
export type ErrorBoundaryVariant = 'page' | 'section' | 'dialog';

interface ErrorBoundaryProps {
  children: React.ReactNode;
  variant?: ErrorBoundaryVariant;
  resetKeys?: ReadonlyArray<unknown>;
}

interface ErrorBoundaryState {
  error: unknown;
}

const changed = (a: ReadonlyArray<unknown> = [], b: ReadonlyArray<unknown> = []) =>
  a.length !== b.length || a.some((value, index) => !Object.is(value, b[index]));

export class ErrorBoundary extends React.Component<ErrorBoundaryProps, ErrorBoundaryState> {
  state: ErrorBoundaryState = { error: null };

  static getDerivedStateFromError(error: unknown): ErrorBoundaryState {
    return { error: error ?? new Error('Unknown render error') };
  }

  componentDidCatch(error: unknown, info: React.ErrorInfo): void {
    console.error('[ErrorBoundary] render failed', error, info.componentStack);
  }

  componentDidUpdate(prev: ErrorBoundaryProps): void {
    if (this.state.error && changed(prev.resetKeys, this.props.resetKeys)) {
      this.reset();
    }
  }

  reset = (): void => {
    this.setState({ error: null });
  };

  render(): React.ReactNode {
    const { error } = this.state;
    if (!error) return this.props.children;
    return (
      <ErrorFallback
        chunk={isChunkLoadError(error)}
        variant={this.props.variant ?? 'section'}
        onReset={this.reset}
      />
    );
  }
}

interface ErrorFallbackProps {
  chunk: boolean;
  variant: ErrorBoundaryVariant;
  onReset: () => void;
}

export const ErrorFallback: React.FC<ErrorFallbackProps> = ({ chunk, variant, onReset }) => {
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
      <Dialog open onClose={onReset} aria-labelledby="error-boundary-title">
        <DialogTitle id="error-boundary-title">{title}</DialogTitle>
        <DialogContent>
          <Typography variant="body2">{body}</Typography>
        </DialogContent>
        <DialogActions>
          {chunk && (
            <Button onClick={onReset} color="inherit">
              Close
            </Button>
          )}
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
