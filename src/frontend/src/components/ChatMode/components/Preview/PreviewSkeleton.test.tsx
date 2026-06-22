import React from 'react';
import { describe, it, expect } from 'vitest';
import { render, screen } from '@testing-library/react';
import PreviewSkeleton, { shouldShowPreviewSkeleton } from './PreviewSkeleton';

describe('shouldShowPreviewSkeleton', () => {
  it('shows while a run is active and no deliverable has rendered', () => {
    expect(shouldShowPreviewSkeleton({ runActive: true, hasPreview: false })).toBe(true);
  });

  it('hides once a preview (deliverable) has rendered — the real panel takes over', () => {
    expect(shouldShowPreviewSkeleton({ runActive: true, hasPreview: true })).toBe(false);
  });

  it('hides when no run is active', () => {
    expect(shouldShowPreviewSkeleton({ runActive: false, hasPreview: false })).toBe(false);
    expect(shouldShowPreviewSkeleton({ runActive: false, hasPreview: true })).toBe(false);
  });
});

describe('PreviewSkeleton', () => {
  it('renders a busy placeholder with a working indicator', () => {
    render(<PreviewSkeleton />);
    expect(screen.getByLabelText('Building preview')).toHaveAttribute('aria-busy', 'true');
    expect(screen.getByText(/working/i)).toBeInTheDocument();
    expect(screen.getByTestId('preview-skeleton-body')).toBeInTheDocument();
  });

  it('is a fixed 50% side pane so it never hides the chat/activity', () => {
    render(<PreviewSkeleton />);
    // It must sit BESIDE the chat (half width), never full-screen.
    expect(screen.getByLabelText('Building preview')).toHaveStyle({ flex: '1 1 50%' });
  });

  it('surfaces honest progress (status line + elapsed timer) so a long run is not blank', () => {
    render(<PreviewSkeleton />);
    // No steps yet → "Thinking…"; elapsed timer starts at 0:00.
    expect(screen.getByText('Thinking…')).toBeInTheDocument();
    expect(screen.getByTestId('preview-skeleton-elapsed')).toHaveTextContent('0:00');
  });
});
