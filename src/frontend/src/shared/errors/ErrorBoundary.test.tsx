import React, { lazy, Suspense } from 'react';
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import '@testing-library/jest-dom';
import { ErrorBoundary } from './ErrorBoundary';
import * as chunkErrors from './chunkErrors';

const CHUNK_ERROR = new TypeError(
  'Failed to fetch dynamically imported module: https://example.com/assets/Page-abc.js',
);

let shouldThrow: unknown = null;
const Bomb: React.FC = () => {
  if (shouldThrow) throw shouldThrow;
  return <div>content</div>;
};

describe('ErrorBoundary', () => {
  let consoleError: ReturnType<typeof vi.spyOn>;
  let reload: ReturnType<typeof vi.spyOn>;

  beforeEach(() => {
    shouldThrow = null;
    // React logs caught render errors; keep the test output readable.
    consoleError = vi.spyOn(console, 'error').mockImplementation(() => undefined);
    reload = vi.spyOn(chunkErrors, 'reloadPage').mockImplementation(() => undefined);
  });

  afterEach(() => {
    consoleError.mockRestore();
    reload.mockRestore();
  });

  it('renders its children when nothing fails', () => {
    render(<ErrorBoundary><Bomb /></ErrorBoundary>);
    expect(screen.getByText('content')).toBeInTheDocument();
  });

  it('turns a rejected lazy chunk into a reload prompt instead of unmounting', async () => {
    const Broken = lazy(() => Promise.reject(CHUNK_ERROR));
    render(
      <div>
        <span>shell stays</span>
        <ErrorBoundary variant="page">
          <Suspense fallback={<span>loading</span>}>
            <Broken />
          </Suspense>
        </ErrorBoundary>
      </div>,
    );

    expect(await screen.findByText('Kasal has been updated')).toBeInTheDocument();
    expect(screen.getByText('shell stays')).toBeInTheDocument();
    // A chunk failure is not retryable in place: only a reload helps.
    expect(screen.queryByRole('button', { name: 'Try again' })).not.toBeInTheDocument();
    fireEvent.click(screen.getByRole('button', { name: 'Reload page' }));
    expect(reload).toHaveBeenCalledTimes(1);
  });

  it('offers "Try again" for other errors, which re-renders the children', () => {
    shouldThrow = new Error('render bug');
    render(<ErrorBoundary><Bomb /></ErrorBoundary>);

    expect(screen.getByRole('alert')).toHaveTextContent('Something went wrong');
    shouldThrow = null;
    fireEvent.click(screen.getByRole('button', { name: 'Try again' }));
    expect(screen.getByText('content')).toBeInTheDocument();
  });

  it('clears the error when a reset key changes (navigation)', () => {
    shouldThrow = new Error('render bug');
    const { rerender } = render(
      <ErrorBoundary resetKeys={['/runs']}><Bomb /></ErrorBoundary>,
    );
    expect(screen.getByText('Something went wrong')).toBeInTheDocument();

    shouldThrow = null;
    rerender(<ErrorBoundary resetKeys={['/runs']}><Bomb /></ErrorBoundary>);
    expect(screen.getByText('Something went wrong')).toBeInTheDocument();

    rerender(<ErrorBoundary resetKeys={['/workflow']}><Bomb /></ErrorBoundary>);
    expect(screen.getByText('content')).toBeInTheDocument();
  });

  it('renders as a dialog for lazily loaded dialogs, and can be closed', async () => {
    shouldThrow = CHUNK_ERROR;
    render(<ErrorBoundary variant="dialog"><Bomb /></ErrorBoundary>);

    const dialog = await screen.findByRole('dialog');
    expect(dialog).toHaveTextContent('Kasal has been updated');
    fireEvent.click(screen.getByRole('button', { name: 'Reload page' }));
    expect(reload).toHaveBeenCalledTimes(1);

    shouldThrow = null;
    fireEvent.click(screen.getByRole('button', { name: 'Close' }));
    expect(screen.getByText('content')).toBeInTheDocument();
  });

  it('logs the caught error', () => {
    shouldThrow = new Error('render bug');
    render(<ErrorBoundary><Bomb /></ErrorBoundary>);
    expect(consoleError).toHaveBeenCalledWith(
      '[ErrorBoundary] render failed',
      shouldThrow,
      expect.anything(),
    );
  });
});
