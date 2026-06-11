import { vi, describe, it, expect } from 'vitest';
import React from 'react';
import { render, screen, fireEvent } from '@testing-library/react';
import { GroupedTraceMessages } from './GroupedTraceMessages';
import { ChatMessage } from '../types';

// ---------------------------------------------------------------------------
// Mocks
// ---------------------------------------------------------------------------

vi.mock('../utils/textProcessing', () => ({
  stripAnsiEscapes: (text: string) => text,
}));

// MUI Fade can interfere with rendering in tests -- simplify it
vi.mock('@mui/material/Fade', () => ({
  default: ({ children }: { children: React.ReactElement }) => children,
}));

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function makeTrace(overrides: Partial<ChatMessage> = {}): ChatMessage {
  return {
    id: `trace-${Math.random()}`,
    type: 'trace',
    content: 'Agent analyzed the input data',
    timestamp: new Date('2025-06-15T12:00:00Z'),
    eventType: 'agent_execution',
    eventSource: 'agent',
    eventContext: 'Research Task',
    jobId: 'job-1',
    ...overrides,
  };
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe('GroupedTraceMessages (run-activity container)', () => {
  it('renders nothing when there are no messages and the run is not live', () => {
    const { container } = render(<GroupedTraceMessages messages={[]} />);
    expect(container.firstChild).toBeNull();
  });

  it('shows "Working…" with no expandable timeline while running with no traces yet', () => {
    render(<GroupedTraceMessages messages={[]} running />);
    expect(screen.getByText('Working…')).toBeInTheDocument();
    expect(screen.queryByLabelText('Expand run activity')).not.toBeInTheDocument();
  });

  it('shows the latest step as a live one-liner while traces stream in', () => {
    render(
      <GroupedTraceMessages
        messages={[
          makeTrace({ id: 't1', eventType: 'agent_execution', content: 'first step' }),
          makeTrace({ id: 't2', eventType: 'task_completed', content: 'Summary written\nmore detail below' }),
        ]}
        running
      />
    );
    // The header tracks the LATEST step: name + first line of its output.
    expect(screen.getByText('Task Completed')).toBeInTheDocument();
    expect(screen.getByText(/— Summary written/)).toBeInTheDocument();
    // The static label and the done label are both replaced by the live line.
    expect(screen.queryByText('Working…')).not.toBeInTheDocument();
    expect(screen.queryByText('Run activity')).not.toBeInTheDocument();
  });

  it('shows "Run activity" once the run is done', () => {
    render(<GroupedTraceMessages messages={[makeTrace()]} />);
    expect(screen.getByText('Run activity')).toBeInTheDocument();
    expect(screen.queryByText('Working…')).not.toBeInTheDocument();
  });

  it('keeps the trace timeline collapsed by default', () => {
    render(<GroupedTraceMessages messages={[makeTrace()]} />);
    expect(screen.queryByText('Agent analyzed the input data')).not.toBeInTheDocument();
  });

  it('expands into a timeline of steps with humanized event names', () => {
    render(
      <GroupedTraceMessages
        messages={[
          makeTrace({ id: 't1', eventType: 'agent_execution' }),
          makeTrace({ id: 't2', eventType: 'task_completed', content: 'All done' }),
        ]}
      />
    );
    fireEvent.click(screen.getByLabelText('Expand run activity'));
    expect(screen.getByText('Agent Execution')).toBeInTheDocument();
    expect(screen.getByText('Task Completed')).toBeInTheDocument();
    expect(screen.getByText('Agent analyzed the input data')).toBeInTheDocument();
    expect(screen.getByText('All done')).toBeInTheDocument();
  });

  it('truncates long output to a summary and reveals the full text behind "Show context"', () => {
    const longContent = 'x'.repeat(200) + '\nsecond line of the long output';
    render(<GroupedTraceMessages messages={[makeTrace({ content: longContent })]} />);
    fireEvent.click(screen.getByLabelText('Expand run activity'));

    // Summary is truncated; the full content is hidden behind the toggle.
    expect(screen.getByText('x'.repeat(140) + '…')).toBeInTheDocument();
    expect(screen.queryByText(/second line of the long output/)).not.toBeInTheDocument();

    fireEvent.click(screen.getByText('Show context'));
    expect(screen.getByText(/second line of the long output/)).toBeInTheDocument();

    fireEvent.click(screen.getByText('Hide context'));
    expect(screen.queryByText(/second line of the long output/)).not.toBeInTheDocument();
  });

  it('opens execution logs for the group job id', () => {
    const onOpenLogs = vi.fn();
    render(<GroupedTraceMessages messages={[makeTrace({ jobId: 'job-42' })]} onOpenLogs={onOpenLogs} />);
    fireEvent.click(screen.getByLabelText('View execution logs'));
    expect(onOpenLogs).toHaveBeenCalledWith('job-42');
  });
});
