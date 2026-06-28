import { describe, it, expect, vi, beforeEach } from 'vitest';
import { screen, fireEvent } from '@testing-library/react';
import ExecutionCard from './ExecutionCard';
import { renderWithChatTheme as render } from '../../chatTestRender';
import type { ExecutionStatus } from '../../types/execution';

describe('ExecutionCard', () => {
  const scrollIntoViewMock = vi.fn();

  beforeEach(() => {
    scrollIntoViewMock.mockClear();
    // jsdom does not implement scrollIntoView
    window.HTMLElement.prototype.scrollIntoView = scrollIntoViewMock;
  });

  it('renders all status variants with proper label', () => {
    const statuses: ExecutionStatus[] = [
      'queued',
      'running',
      'completed',
      'failed',
      'stopped',
    ];
    const labels: Record<ExecutionStatus, string> = {
      queued: 'Queued',
      running: 'Running',
      completed: 'Completed',
      failed: 'Failed',
      stopped: 'Stopped',
    };

    statuses.forEach((status) => {
      const { unmount } = render(
        <ExecutionCard jobId="abcdef123456" status={status} traces={[]} />,
      );
      expect(
        screen.getByText(`Execution: ${labels[status]}`),
      ).toBeInTheDocument();
      unmount();
    });
  });

  it('marks the status dot as running only when running (drives the pulse animation)', () => {
    const { rerender } = render(
      <ExecutionCard jobId="abc" status="running" traces={[]} />,
    );
    // The dot pulses (sx animation) iff running; assert the stable state hook
    // that drives it rather than the emotion-generated animation class.
    expect(screen.getByTestId('execution-status-dot')).toHaveAttribute('data-running', 'true');

    rerender(<ExecutionCard jobId="abc" status="completed" traces={[]} />);
    expect(screen.getByTestId('execution-status-dot')).toHaveAttribute('data-running', 'false');
  });

  it('falls back to queued config when status is empty/falsy', () => {
    // status is typed but we exercise the runtime fallback (status || 'queued')
    render(
      <ExecutionCard
        jobId="abc"
        status={'' as unknown as ExecutionStatus}
        traces={[]}
      />,
    );
    expect(screen.getByText('Execution: Queued')).toBeInTheDocument();
  });

  it('falls back to queued config when status is unknown (cfg fallback)', () => {
    // An unknown status string exercises `statusConfig[safeStatus] || statusConfig.queued`
    render(
      <ExecutionCard
        jobId="abc"
        status={'unknown-status' as unknown as ExecutionStatus}
        traces={[]}
      />,
    );
    expect(screen.getByText('Execution: Queued')).toBeInTheDocument();
  });

  it('renders truncated jobId when jobId is present', () => {
    render(
      <ExecutionCard jobId="abcdefghijklmnop" status="queued" traces={[]} />,
    );
    expect(screen.getByText('abcdefgh...')).toBeInTheDocument();
  });

  it('does not render jobId span when jobId is empty', () => {
    render(<ExecutionCard jobId="" status="queued" traces={[]} />);
    expect(screen.queryByText(/\.\.\./)).not.toBeInTheDocument();
  });

  it('renders traces and scrolls into view when traces present', () => {
    render(
      <ExecutionCard
        jobId="abc"
        status="running"
        traces={['trace one', 'trace two']}
      />,
    );
    expect(screen.getByText('trace one')).toBeInTheDocument();
    expect(screen.getByText('trace two')).toBeInTheDocument();
    expect(scrollIntoViewMock).toHaveBeenCalledWith({ behavior: 'smooth' });
  });

  it('does not render traces section when traces empty', () => {
    render(<ExecutionCard jobId="abc" status="running" traces={[]} />);
    expect(screen.queryByText('trace one')).not.toBeInTheDocument();
    // useEffect runs but the ref is not mounted, so scrollIntoView not called
    expect(scrollIntoViewMock).not.toHaveBeenCalled();
  });

  it('renders result section when result provided', () => {
    render(
      <ExecutionCard
        jobId="abc"
        status="completed"
        traces={[]}
        result="the final answer"
      />,
    );
    expect(screen.getByText('Result:')).toBeInTheDocument();
    expect(screen.getByText('the final answer')).toBeInTheDocument();
  });

  it('does not render result section when result absent', () => {
    render(<ExecutionCard jobId="abc" status="completed" traces={[]} />);
    expect(screen.queryByText('Result:')).not.toBeInTheDocument();
  });

  it('renders error section when error provided', () => {
    render(
      <ExecutionCard
        jobId="abc"
        status="failed"
        traces={[]}
        error="something broke"
      />,
    );
    expect(screen.getByText('something broke')).toBeInTheDocument();
  });

  it('does not render error section when error absent', () => {
    render(<ExecutionCard jobId="abc" status="failed" traces={[]} />);
    expect(screen.queryByText('something broke')).not.toBeInTheDocument();
  });

  it('renders Stop button and calls onStop when running', () => {
    const onStop = vi.fn();
    render(
      <ExecutionCard
        jobId="abc"
        status="running"
        traces={[]}
        onStop={onStop}
      />,
    );
    const button = screen.getByRole('button', { name: 'Stop' });
    fireEvent.click(button);
    expect(onStop).toHaveBeenCalledTimes(1);
  });

  it('renders Stop button when queued and onStop provided', () => {
    const onStop = vi.fn();
    render(
      <ExecutionCard
        jobId="abc"
        status="queued"
        traces={[]}
        onStop={onStop}
      />,
    );
    expect(screen.getByRole('button', { name: 'Stop' })).toBeInTheDocument();
  });

  it('does not render Stop button when running but onStop missing', () => {
    render(<ExecutionCard jobId="abc" status="running" traces={[]} />);
    expect(screen.queryByRole('button', { name: 'Stop' })).not.toBeInTheDocument();
  });

  it('does not render Stop button for non-running/queued status even with onStop', () => {
    const onStop = vi.fn();
    render(
      <ExecutionCard
        jobId="abc"
        status="completed"
        traces={[]}
        onStop={onStop}
      />,
    );
    expect(screen.queryByRole('button', { name: 'Stop' })).not.toBeInTheDocument();
  });

  it('renders all sections together (traces, result, error)', () => {
    const onStop = vi.fn();
    render(
      <ExecutionCard
        jobId="abcdefgh1234"
        status="running"
        traces={['t1']}
        result="r1"
        error="e1"
        onStop={onStop}
      />,
    );
    expect(screen.getByText('t1')).toBeInTheDocument();
    expect(screen.getByText('r1')).toBeInTheDocument();
    expect(screen.getByText('e1')).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Stop' })).toBeInTheDocument();
  });
});
