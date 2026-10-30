import React, { lazy } from 'react';
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import '@testing-library/jest-dom';
import { LazyDialogBoundary, MountWhenOpened } from './lazyRunDialogs';
import { retryableLazy } from '../../../shared/errors/retryableLazy';

describe('lazy run dialogs', () => {
  let consoleError: ReturnType<typeof vi.spyOn>;
  beforeEach(() => {
    consoleError = vi.spyOn(console, 'error').mockImplementation(() => undefined);
  });
  afterEach(() => consoleError.mockRestore());

  it('mounts nothing until first opened', () => {
    const { container } = render(<MountWhenOpened open={false}><span>dialog</span></MountWhenOpened>);
    expect(container).toBeEmptyDOMElement();
  });

  it('shows a reload dialog when the dialog chunk fails to load', async () => {
    const Broken = lazy(() => Promise.reject(new TypeError('Importing a module script failed.')));
    render(<MountWhenOpened open><Broken /></MountWhenOpened>);
    expect(await screen.findByRole('dialog')).toHaveTextContent('Kasal has been updated');
  });

  it('renders the dialog once its chunk has loaded', async () => {
    const Loaded = lazy(async () => ({ default: () => <span>loaded dialog</span> }));
    render(<LazyDialogBoundary><Loaded /></LazyDialogBoundary>);
    expect(await screen.findByText('loaded dialog')).toBeInTheDocument();
  });

  it('Close dismisses a failed dialog, tells the owner, and reopening retries the import', async () => {
    let available = false;
    const LoadedDialog: React.FC<{ open: boolean }> = ({ open }) =>
      open ? <span>loaded dialog</span> : null;
    const factory = vi.fn(async () => {
      if (!available) throw new TypeError('Importing a module script failed.');
      return { default: LoadedDialog };
    });
    const Dialog = retryableLazy(factory);
    const onClose = vi.fn();
    const tree = (open: boolean) => (
      <MountWhenOpened open={open} onClose={onClose}>
        <Dialog open={open} />
      </MountWhenOpened>
    );

    const { rerender } = render(tree(true));
    await screen.findByRole('dialog');
    fireEvent.click(screen.getByRole('button', { name: 'Close' }));
    expect(onClose).toHaveBeenCalledTimes(1);
    expect(screen.queryByRole('dialog')).not.toBeInTheDocument();

    // The owner closes its state; nothing reloads while it is closed.
    const failedCalls = factory.mock.calls.length;
    rerender(tree(false));
    expect(factory).toHaveBeenCalledTimes(failedCalls);

    available = true;
    rerender(tree(true));
    expect(await screen.findByText('loaded dialog')).toBeInTheDocument();
    expect(factory).toHaveBeenCalledTimes(failedCalls + 1);
  });
});
