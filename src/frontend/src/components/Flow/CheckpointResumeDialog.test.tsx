import React from 'react';
import { describe, expect, it, vi } from 'vitest';
import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import CheckpointResumeDialog from './CheckpointResumeDialog';
import { FlowCheckpoint } from '../../api/workflow/FlowService';

vi.mock('react-i18next', () => ({
  useTranslation: () => ({ t: (key: string) => key }),
}));

function makeCheckpoint(): FlowCheckpoint {
  return {
    execution_id: 1,
    job_id: 'job-1',
    flow_uuid: 'uuid-1',
    checkpoint_method: null,
    checkpoint_status: 'active',
    created_at: '2026-07-30T10:00:00Z',
    run_name: 'My Flow Run',
    crew_checkpoints: [
      {
        crew_name: 'research',
        sequence: 1,
        status: 'completed',
        output_preview: 'found it',
        completed_at: '2026-07-30T10:01:00Z',
      },
      {
        crew_name: 'write',
        sequence: 2,
        status: 'completed',
        output_preview: 'wrote it',
        completed_at: '2026-07-30T10:02:00Z',
      },
    ],
  };
}

function renderDialog(onResumeFromCheckpoint = vi.fn()) {
  render(
    <CheckpointResumeDialog
      open
      onClose={vi.fn()}
      checkpoints={[makeCheckpoint()]}
      loading={false}
      error={null}
      flowName="My Flow"
      onStartFresh={vi.fn()}
      onResumeFromCheckpoint={onResumeFromCheckpoint}
      onDeleteCheckpoint={vi.fn()}
      onRefresh={vi.fn()}
    />,
  );
  return onResumeFromCheckpoint;
}

/**
 * The backend skips crews with `sequence < resume_from_crew_sequence`, so the
 * value it wants is the crew to RUN. These options name a crew to resume
 * AFTER. Every case here is one that previously re-ran work the user asked to
 * keep.
 */
describe('CheckpointResumeDialog resume boundary', () => {
  it('resumes AFTER the chosen crew, not at it', async () => {
    const onResume = renderDialog();

    await userEvent.click(screen.getByRole('button', { name: /expand|resume/i }));
    await userEvent.click(await screen.findByRole('radio', { name: /research/ }));
    await userEvent.click(screen.getByRole('button', { name: /Resume after/ }));

    // Picking crew #1 must skip crew #1 → the backend needs 2, not 1.
    expect(onResume).toHaveBeenCalledWith(expect.objectContaining({ job_id: 'job-1' }), 2);
  });

  it('resuming from the end skips every completed crew', async () => {
    const onResume = renderDialog();

    await userEvent.click(screen.getByRole('button', { name: /expand|resume/i }));
    await userEvent.click(await screen.findByRole('button', { name: /Resume from End/i }));

    // Previously passed undefined, which skipped NOTHING and re-ran the whole
    // flow while the label promised the opposite.
    expect(onResume).toHaveBeenCalledWith(expect.objectContaining({ job_id: 'job-1' }), 3);
  });

  it('passes nothing when no crew completed', async () => {
    const onResume = vi.fn();
    render(
      <CheckpointResumeDialog
        open
        onClose={vi.fn()}
        checkpoints={[{ ...makeCheckpoint(), crew_checkpoints: [] }]}
        loading={false}
        error={null}
        onStartFresh={vi.fn()}
        onResumeFromCheckpoint={onResume}
        onDeleteCheckpoint={vi.fn()}
        onRefresh={vi.fn()}
      />,
    );

    await userEvent.click(screen.getByRole('button', { name: /resume/i }));

    // Nothing completed, so there is nothing to skip.
    expect(onResume).toHaveBeenCalledWith(
      expect.objectContaining({ job_id: 'job-1' }),
      undefined,
    );
  });
});
