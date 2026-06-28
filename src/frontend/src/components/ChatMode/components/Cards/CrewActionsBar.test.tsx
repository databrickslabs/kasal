/**
 * Post-generation actions row: bookmark to catalog + thumbs feedback.
 * Thumbs-down requires a comment; everything persists on the message and
 * lands in the catalog for the AI engineer.
 */
import React from 'react';
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';

const updateMessage = vi.fn();
vi.mock('../../store/sessionStore', () => ({
  useSessionStore: { getState: () => ({ updateMessage }) },
}));
const postCrewFeedback = vi.fn(async () => ({}));
vi.mock('../../api/crews', async (importOriginal) => {
  const mod = await importOriginal<typeof import('../../api/crews')>();
  return { ...mod, postCrewFeedback: (...a: unknown[]) => postCrewFeedback(...a) };
});

// Memory-enabled toggle drives whether the run's memory-graph button shows.
let memoryEnabledMock = true;
vi.mock('../../store/executionStore', () => ({
  useExecutionStore: (sel: (s: { memoryEnabled: boolean }) => unknown) =>
    sel({ memoryEnabled: memoryEnabledMock }),
}));

// Stub the heavy browser; assert it opens scoped to the run + graph view.
vi.mock('../../../MemoryBackend/MemoryRecordsBrowser', () => ({
  MemoryRecordsBrowser: (props: { open: boolean; initialRunId?: string; initialView?: string }) =>
    props.open ? (
      <div data-testid="memory-graph" data-run={props.initialRunId} data-view={props.initialView} />
    ) : null,
}));

import CrewActionsBar from './CrewActionsBar';
import { CrewNameConflictError } from '../../api/crews';

const DATA = { agents: [{ name: 'A' }], tasks: [{ name: 'T' }] } as never;

beforeEach(() => {
  updateMessage.mockClear();
  postCrewFeedback.mockClear();
  memoryEnabledMock = true;
});

describe('CrewActionsBar', () => {
  it('bookmarks the crew to the catalog and shows the saved name', async () => {
    const onSaveCrew = vi.fn(async () => ({ id: 'crew-1', name: 'Oil Crew' }));
    render(<CrewActionsBar data={DATA} messageId={`a-${Math.random()}`} onSaveCrew={onSaveCrew} />);

    fireEvent.click(screen.getByText('Save to catalog'));
    await waitFor(() => expect(screen.getByText(/Saved — Oil Crew/)).toBeInTheDocument());
    expect(onSaveCrew).toHaveBeenCalled();
    expect(updateMessage).toHaveBeenCalledWith(expect.any(String), expect.objectContaining({
      resultType: 'crew_actions',
      resultData: expect.objectContaining({ savedCrewId: 'crew-1', savedName: 'Oil Crew' }),
    }));
  });

  it('thumbs-up auto-saves first, then posts the vote', async () => {
    const onSaveCrew = vi.fn(async () => ({ id: 'crew-2', name: 'N' }));
    render(<CrewActionsBar data={DATA} messageId={`a-${Math.random()}`} onSaveCrew={onSaveCrew} />);

    fireEvent.click(screen.getByLabelText('Thumbs up'));
    await waitFor(() => expect(postCrewFeedback).toHaveBeenCalledWith('crew-2', 'up', undefined));
    expect(onSaveCrew).toHaveBeenCalled();
  });

  it('thumbs-down requires a comment before submitting', async () => {
    const onSaveCrew = vi.fn(async () => ({ id: 'crew-3', name: 'N' }));
    render(<CrewActionsBar data={DATA} messageId={`a-${Math.random()}`} onSaveCrew={onSaveCrew} />);

    fireEvent.click(screen.getByLabelText('Thumbs down'));
    const submit = screen.getByText('Submit feedback');
    expect(submit).toBeDisabled(); // no comment yet

    fireEvent.change(screen.getByPlaceholderText(/What went wrong/), {
      target: { value: 'images were broken' },
    });
    fireEvent.click(screen.getByText('Submit feedback'));

    await waitFor(() =>
      expect(postCrewFeedback).toHaveBeenCalledWith('crew-3', 'down', 'images were broken'),
    );
    expect(screen.getByText(/Feedback recorded/)).toBeInTheDocument();
  });

  it('rehydrates saved/voted state from persisted resultData', () => {
    const persisted = { ...(DATA as object), savedCrewId: 'c9', savedName: 'Kept', voted: 'up' } as never;
    render(<CrewActionsBar data={persisted} messageId={`a-${Math.random()}`} onSaveCrew={vi.fn()} />);
    expect(screen.getByText(/Saved — Kept/)).toBeInTheDocument();
    expect(screen.getByLabelText('Thumbs up')).toBeDisabled();
  });

  it('voting on an already-saved crew skips re-saving (and "Saved" shows without a name)', async () => {
    // Persisted save without a savedName: button reads just "Saved".
    const persisted = { ...(DATA as object), savedCrewId: 'c9' } as never;
    const onSaveCrew = vi.fn();
    render(<CrewActionsBar data={persisted} messageId={`a-${Math.random()}`} onSaveCrew={onSaveCrew} />);
    expect(screen.getByText('Saved')).toBeInTheDocument();

    fireEvent.click(screen.getByLabelText('Thumbs up'));
    await waitFor(() => expect(postCrewFeedback).toHaveBeenCalledWith('c9', 'up', undefined));
    expect(onSaveCrew).not.toHaveBeenCalled();
  });

  it('bookmark without an onSaveCrew handler reports the error', async () => {
    render(<CrewActionsBar data={DATA} messageId={`a-${Math.random()}`} />);
    fireEvent.click(screen.getByText('Save to catalog'));
    await waitFor(() => expect(screen.getByText('Could not save the crew')).toBeInTheDocument());
  });

  it('bookmark retries with overwrite when the crew name already exists', async () => {
    const onSaveCrew = vi.fn()
      .mockRejectedValueOnce(new CrewNameConflictError('Oil Crew'))
      .mockResolvedValueOnce({ id: 'crew-4', name: 'Oil Crew' });
    render(<CrewActionsBar data={DATA} messageId={`a-${Math.random()}`} onSaveCrew={onSaveCrew} />);

    fireEvent.click(screen.getByText('Save to catalog'));
    await waitFor(() => expect(screen.getByText(/Saved — Oil Crew/)).toBeInTheDocument());
    expect(onSaveCrew).toHaveBeenNthCalledWith(1, DATA, undefined);
    expect(onSaveCrew).toHaveBeenNthCalledWith(2, DATA, { overwrite: true });
  });

  it('bookmark reports the error when the overwrite retry also fails', async () => {
    const onSaveCrew = vi.fn()
      .mockRejectedValueOnce(new CrewNameConflictError('Oil Crew'))
      .mockRejectedValueOnce(new Error('still broken'));
    render(<CrewActionsBar data={DATA} messageId={`a-${Math.random()}`} onSaveCrew={onSaveCrew} />);

    fireEvent.click(screen.getByText('Save to catalog'));
    await waitFor(() => expect(screen.getByText('Could not save the crew')).toBeInTheDocument());
  });

  it('voting retries the save with overwrite on a name conflict, then posts the vote', async () => {
    const onSaveCrew = vi.fn()
      .mockRejectedValueOnce(new CrewNameConflictError('N'))
      .mockResolvedValueOnce({ id: 'crew-5', name: 'N' });
    render(<CrewActionsBar data={DATA} messageId={`a-${Math.random()}`} onSaveCrew={onSaveCrew} />);

    fireEvent.click(screen.getByLabelText('Thumbs up'));
    await waitFor(() => expect(postCrewFeedback).toHaveBeenCalledWith('crew-5', 'up', undefined));
    expect(onSaveCrew).toHaveBeenNthCalledWith(2, DATA, { overwrite: true });
  });

  it('voting reports the error when the save fails for a non-conflict reason', async () => {
    const onSaveCrew = vi.fn().mockRejectedValue(new Error('nope'));
    render(<CrewActionsBar data={DATA} messageId={`a-${Math.random()}`} onSaveCrew={onSaveCrew} />);

    fireEvent.click(screen.getByLabelText('Thumbs up'));
    await waitFor(() =>
      expect(screen.getByText('Could not record the feedback')).toBeInTheDocument(),
    );
    expect(postCrewFeedback).not.toHaveBeenCalled();
  });

  it('voting reports the error when the feedback post fails', async () => {
    postCrewFeedback.mockRejectedValueOnce(new Error('db down'));
    const onSaveCrew = vi.fn(async () => ({ id: 'crew-6', name: 'N' }));
    render(<CrewActionsBar data={DATA} messageId={`a-${Math.random()}`} onSaveCrew={onSaveCrew} />);

    fireEvent.click(screen.getByLabelText('Thumbs up'));
    await waitFor(() =>
      expect(screen.getByText('Could not record the feedback')).toBeInTheDocument(),
    );
  });

  it('shows the memory-graph button only when memory is enabled and a run id is present', () => {
    render(
      <CrewActionsBar
        data={DATA}
        messageId={`a-${Math.random()}`}
        onSaveCrew={vi.fn()}
        executionId="job-9"
      />,
    );
    expect(screen.getByLabelText('View memory graph')).toBeInTheDocument();
  });

  it('opens the run-scoped memory graph (graph view) on click', () => {
    render(
      <CrewActionsBar
        data={DATA}
        messageId={`a-${Math.random()}`}
        onSaveCrew={vi.fn()}
        executionId="job-9"
      />,
    );
    fireEvent.click(screen.getByLabelText('View memory graph'));
    const graph = screen.getByTestId('memory-graph');
    expect(graph).toHaveAttribute('data-run', 'job-9');
    expect(graph).toHaveAttribute('data-view', 'graph');
  });

  it('hides the memory-graph button when there is no run id', () => {
    render(<CrewActionsBar data={DATA} messageId={`a-${Math.random()}`} onSaveCrew={vi.fn()} />);
    expect(screen.queryByLabelText('View memory graph')).toBeNull();
  });

  it('hides the memory-graph button when memory is disabled', () => {
    memoryEnabledMock = false;
    render(
      <CrewActionsBar
        data={DATA}
        messageId={`a-${Math.random()}`}
        onSaveCrew={vi.fn()}
        executionId="job-9"
      />,
    );
    expect(screen.queryByLabelText('View memory graph')).toBeNull();
  });
});
