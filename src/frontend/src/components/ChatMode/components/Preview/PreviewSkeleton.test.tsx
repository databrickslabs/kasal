import React from 'react';
import { describe, it, expect } from 'vitest';
import { render, screen } from '@testing-library/react';
import PreviewSkeleton, { shouldShowPreviewSkeleton, friendlyStep } from './PreviewSkeleton';
import type { RunStep } from './RunTimeline';

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

describe('friendlyStep', () => {
  it('maps raw tool labels to business-readable phases', () => {
    expect(friendlyStep('Memory')).toBe('Recalling context');
    expect(friendlyStep('GenieTool')).toBe('Querying your data');
    expect(friendlyStep('PerplexityTool')).toBe('Searching the web');
    expect(friendlyStep('AgentBricksTool')).toBe('Consulting an agent');
  });

  it('falls back to the raw label when unknown', () => {
    expect(friendlyStep('SomeCustomTool')).toBe('SomeCustomTool');
  });
});

describe('PreviewSkeleton', () => {
  it('renders a busy placeholder with a working indicator', () => {
    render(<PreviewSkeleton steps={[]} />);
    expect(screen.getByLabelText('Building preview')).toHaveAttribute('aria-busy', 'true');
    expect(screen.getByText(/working/i)).toBeInTheDocument();
    expect(screen.getByTestId('preview-skeleton-body')).toBeInTheDocument();
  });

  it('is a fixed 50% side pane so it never hides the chat/activity', () => {
    render(<PreviewSkeleton steps={[]} />);
    expect(screen.getByLabelText('Building preview')).toHaveStyle({ flex: '1 1 50%' });
  });

  it('surfaces honest progress (elapsed timer + a getting-started state before any step)', () => {
    render(<PreviewSkeleton steps={[]} />);
    expect(screen.getByText('Getting started…')).toBeInTheDocument();
    expect(screen.getByTestId('preview-skeleton-elapsed')).toHaveTextContent('0:00');
    expect(screen.getByText('Starting…')).toBeInTheDocument(); // meta: no steps yet
  });

  it('narrates the steps as a thinking stream (friendly phase headings, not raw tool names)', () => {
    const steps: RunStep[] = [
      { id: '1', label: 'GenieTool', timestamp: 1, durationMs: 3200 },
      { id: '2', label: 'Memory', timestamp: 2 },
    ];
    render(<PreviewSkeleton steps={steps} />);
    expect(screen.getByText('Querying your data')).toBeInTheDocument();
    expect(screen.getByText('Recalling context')).toBeInTheDocument();
    expect(screen.getByText('2 steps so far')).toBeInTheDocument();
    expect(screen.getByText('Thinking…')).toBeInTheDocument(); // live pulse at the tail
  });
});
