import React, { lazy } from 'react';
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen } from '@testing-library/react';
import '@testing-library/jest-dom';
import { LazyDialogBoundary, MountWhenOpened } from './lazyRunDialogs';

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
});
