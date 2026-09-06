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
    expect(screen.getByLabelText('Building preview')).toHaveStyle({ flex: '1 1 50%' });
  });

  it('surfaces honest progress (elapsed timer + what it is doing)', () => {
    render(<PreviewSkeleton />);
    expect(screen.getByTestId('preview-skeleton-elapsed')).toHaveTextContent('0:00');
    expect(screen.getByText('Building the answer…')).toBeInTheDocument();
  });

  it('points at the chat for the steps instead of listing them again', () => {
    // The run's timeline lives in the chat, on the left. This pane is where a
    // chosen step's content opens — the list belongs in one place, not two.
    render(<PreviewSkeleton />);
    expect(screen.getByTestId('preview-skeleton-body')).toHaveTextContent(
      'Open a step in the run activity to read what it did.',
    );
    expect(screen.queryByText('postgres_execute_sql (output)')).not.toBeInTheDocument();
  });

  it('opens directly on a focused step (row-clicked in the chat dropdown)', () => {
    render(
      <PreviewSkeleton
       
        focusStep={{ id: '1', label: 'postgres_execute_sql (output)', detail: 'CREATE TABLE' }}
      />,
    );
    expect(screen.getByTestId('run-step-context')).toBeInTheDocument();
    expect(screen.getByLabelText('Back to the run activity')).toBeInTheDocument();
  });
});

describe('PreviewSkeleton — running prop (live vs ended-but-docked)', () => {
  it('defaults to running: live label, WORKING badge, busy, ticking elapsed', () => {
    render(<PreviewSkeleton />);
    const pane = screen.getByLabelText('Building preview');
    expect(pane).toHaveAttribute('aria-busy', 'true');
    expect(screen.getByText('Running agent…')).toBeInTheDocument();
    expect(screen.getByText('WORKING')).toBeInTheDocument();
    expect(screen.getByTestId('preview-skeleton-elapsed')).toBeInTheDocument();
  });

  it('running={false}: relabels to "Run activity", drops the WORKING badge, elapsed and busy state', () => {
    render(<PreviewSkeleton running={false} />);
    // The pane no longer claims to be running.
    const pane = screen.getByLabelText('Run activity');
    expect(pane).toHaveAttribute('aria-busy', 'false');
    expect(screen.getByText('Run activity')).toBeInTheDocument();
    expect(screen.queryByText('Running agent…')).not.toBeInTheDocument();
    expect(screen.queryByText('WORKING')).not.toBeInTheDocument();
    expect(screen.queryByLabelText('Building preview')).not.toBeInTheDocument();
    // No ticking elapsed timer once the run has ended.
    expect(screen.queryByTestId('preview-skeleton-elapsed')).not.toBeInTheDocument();
  });

  it('running={false} stops claiming to be building', () => {
    render(<PreviewSkeleton running={false} />);
    expect(screen.getByText('No deliverable')).toBeInTheDocument();
    expect(screen.queryByText('Building the answer…')).not.toBeInTheDocument();
  });

  it('renders a plan step as a checklist rather than its raw payload', () => {
    render(
      <PreviewSkeleton
       
        focusStep={{
          id: '1',
          label: 'todo (output)',
          detail: '[ ] 1. Store all companies',
          plan: [{ id: '1', content: 'Store all companies in the database', status: 'completed' }],
        }}
      />,
    );
    expect(screen.getByText('Plan')).toBeInTheDocument();
    expect(screen.getByText('1/1 done')).toBeInTheDocument();
    expect(screen.getByText('Store all companies in the database')).toBeInTheDocument();
  });
});
