import React from 'react';
import { render, screen, act } from '@testing-library/react';
import { vi, describe, it, expect, beforeEach, afterEach } from 'vitest';
import { HtmlPreviewDialog } from './HtmlPreviewDialog';

// ---------------------------------------------------------------------------
// Mocks
// ---------------------------------------------------------------------------

// Mock ShowResult to inspect props without rendering a heavy Dialog
const mockOnClose = vi.fn();
vi.mock('../../Jobs/ShowResult', () => ({
  default: ({ open, onClose, result }: {
    open: boolean;
    onClose: () => void;
    result: Record<string, unknown>;
  }) => {
    // Stash onClose so tests can invoke it
    mockOnClose.mockImplementation(onClose);
    return (
      <div data-testid="show-result" data-open={open}>
        {JSON.stringify(result)}
      </div>
    );
  },
}));

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function dispatchPreview(html: string) {
  act(() => {
    window.dispatchEvent(
      new CustomEvent('codeBlockPreview', { detail: { html } })
    );
  });
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe('HtmlPreviewDialog', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  afterEach(() => {
    // Clean up any remaining event listeners by unmounting
  });

  it('renders nothing when no preview event has been dispatched', () => {
    const { container } = render(<HtmlPreviewDialog />);
    expect(container.querySelector('[data-testid="show-result"]')).toBeNull();
  });

  it('renders ShowResult after receiving a codeBlockPreview event', () => {
    render(<HtmlPreviewDialog />);

    dispatchPreview('<p>hello</p>');

    const el = screen.getByTestId('show-result');
    expect(el).toBeInTheDocument();
    expect(el.getAttribute('data-open')).toBe('true');
    expect(el.textContent).toContain('HTML Preview');
    expect(el.textContent).toContain('<p>hello</p>');
  });

  it('passes a stable result object to ShowResult', () => {
    render(<HtmlPreviewDialog />);

    dispatchPreview('<div>test</div>');

    const el = screen.getByTestId('show-result');
    const parsed = JSON.parse(el.textContent || '{}');
    expect(parsed).toEqual({ 'HTML Preview': '<div>test</div>' });
  });

  it('closes ShowResult and returns to null when onClose is called', () => {
    const { container } = render(<HtmlPreviewDialog />);

    dispatchPreview('<div>open</div>');
    expect(screen.getByTestId('show-result')).toBeInTheDocument();

    // Simulate close
    act(() => {
      mockOnClose();
    });

    expect(container.querySelector('[data-testid="show-result"]')).toBeNull();
  });

  it('updates preview content when a new event is dispatched', () => {
    render(<HtmlPreviewDialog />);

    dispatchPreview('<p>first</p>');
    expect(screen.getByTestId('show-result').textContent).toContain('first');

    dispatchPreview('<p>second</p>');
    expect(screen.getByTestId('show-result').textContent).toContain('second');
  });

  it('cleans up event listener on unmount', () => {
    const spy = vi.spyOn(window, 'removeEventListener');
    const { unmount } = render(<HtmlPreviewDialog />);

    unmount();

    const removedEvents = spy.mock.calls.map(c => c[0]);
    expect(removedEvents).toContain('codeBlockPreview');
    spy.mockRestore();
  });
});
