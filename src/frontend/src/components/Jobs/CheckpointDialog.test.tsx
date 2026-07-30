import React from 'react';
import { describe, expect, it, vi, beforeEach } from 'vitest';
import { render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import CheckpointDialog from './CheckpointDialog';
import ExecutionCheckpointService, {
  ExecutionCheckpoint,
} from '../../api/execution/ExecutionCheckpointService';

vi.mock('../../api/execution/ExecutionCheckpointService');
vi.mock('react-i18next', () => ({
  useTranslation: () => ({ t: (key: string) => key }),
}));

const mocked = vi.mocked(ExecutionCheckpointService);

function makeCheckpoint(overrides: Partial<ExecutionCheckpoint> = {}): ExecutionCheckpoint {
  return {
    job_id: 'job-1',
    execution_id: 42,
    kind: 'crew',
    version: 1,
    status: 'active',
    execution_status: 'FAILED',
    run_name: 'My Run',
    created_at: '2026-07-30T10:00:00Z',
    unit_count: 3,
    completed_count: 2,
    truncated: false,
    derived: false,
    resumable: true,
    blocked_reason: null,
    units: [
      {
        key: '0',
        name: 'research',
        agent: 'worker',
        output_preview: 'found things',
        truncated: false,
        completed_at: '2026-07-30T10:01:00Z',
      },
      {
        key: '1',
        name: 'write',
        agent: 'worker',
        output_preview: 'wrote things',
        truncated: false,
        completed_at: '2026-07-30T10:02:00Z',
      },
    ],
    ...overrides,
  };
}

describe('CheckpointDialog', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('shows how much of the run completed', async () => {
    mocked.getCheckpoint.mockResolvedValue(makeCheckpoint());

    render(<CheckpointDialog open jobId="job-1" onClose={vi.fn()} />);

    expect(await screen.findByText('2 / 3 tasks complete')).toBeInTheDocument();
  });

  it('calls a flow execution’s units crews, not tasks', async () => {
    mocked.getCheckpoint.mockResolvedValue(
      makeCheckpoint({ kind: 'flow', unit_count: 2, completed_count: 2 }),
    );

    render(<CheckpointDialog open jobId="job-1" onClose={vi.fn()} />);

    // One component, two vocabularies — the only path-specific thing here.
    expect(await screen.findByText('2 / 2 crews complete')).toBeInTheDocument();
  });

  it('surfaces truncation rather than hiding it', async () => {
    mocked.getCheckpoint.mockResolvedValue(makeCheckpoint({ truncated: true }));

    render(<CheckpointDialog open jobId="job-1" onClose={vi.fn()} />);

    expect(await screen.findByText('Output truncated')).toBeInTheDocument();
  });

  it('flags a checkpoint migrated from an older payload', async () => {
    mocked.getCheckpoint.mockResolvedValue(makeCheckpoint({ derived: true }));

    render(<CheckpointDialog open jobId="job-1" onClose={vi.fn()} />);

    expect(await screen.findByText('Legacy checkpoint')).toBeInTheDocument();
  });

  it('explains why a run cannot be resumed and disables the button', async () => {
    mocked.getCheckpoint.mockResolvedValue(
      makeCheckpoint({
        resumable: false,
        blocked_reason: 'This checkpoint has already been resumed',
      }),
    );

    render(<CheckpointDialog open jobId="job-1" onClose={vi.fn()} />);

    expect(
      await screen.findByText('This checkpoint has already been resumed'),
    ).toBeInTheDocument();
    expect(screen.getByRole('button', { name: /resume/i })).toBeDisabled();
  });

  it('says a run with no checkpoint would start over', async () => {
    mocked.getCheckpoint.mockResolvedValue(null);

    render(<CheckpointDialog open jobId="job-1" onClose={vi.fn()} />);

    expect(await screen.findByText(/has no checkpoint/i)).toBeInTheDocument();
  });

  it('resumes from the crash point by default', async () => {
    mocked.getCheckpoint.mockResolvedValue(makeCheckpoint());
    mocked.resume.mockResolvedValue({
      execution_id: 'new-job',
      status: 'RUNNING',
      run_name: 'My Run',
    });
    const onResumed = vi.fn();

    render(
      <CheckpointDialog open jobId="job-1" onClose={vi.fn()} onResumed={onResumed} />,
    );
    await screen.findByText('2 / 3 tasks complete');
    await userEvent.click(screen.getByRole('button', { name: /resume/i }));

    await waitFor(() => {
      expect(mocked.resume).toHaveBeenCalledWith('job-1', undefined);
    });
    expect(onResumed).toHaveBeenCalledWith('new-job');
  });

  it('resumes at a chosen unit when the user rewinds', async () => {
    mocked.getCheckpoint.mockResolvedValue(makeCheckpoint());
    mocked.resume.mockResolvedValue({
      execution_id: 'new-job',
      status: 'RUNNING',
      run_name: 'My Run',
    });

    render(<CheckpointDialog open jobId="job-1" onClose={vi.fn()} />);
    await screen.findByText('2 / 3 tasks complete');

    await userEvent.click(screen.getByRole('radio', { name: /Redo from "write"/ }));
    await userEvent.click(screen.getByRole('button', { name: /resume/i }));

    await waitFor(() => {
      expect(mocked.resume).toHaveBeenCalledWith('job-1', '1');
    });
  });

  it('reports a failed resume instead of closing silently', async () => {
    mocked.getCheckpoint.mockResolvedValue(makeCheckpoint());
    mocked.resume.mockRejectedValue(new Error('already resumed'));
    const onClose = vi.fn();

    render(<CheckpointDialog open jobId="job-1" onClose={onClose} />);
    await screen.findByText('2 / 3 tasks complete');
    await userEvent.click(screen.getByRole('button', { name: /resume/i }));

    expect(await screen.findByText('already resumed')).toBeInTheDocument();
    expect(onClose).not.toHaveBeenCalled();
  });
});
