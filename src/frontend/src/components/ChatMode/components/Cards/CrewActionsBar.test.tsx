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

import CrewActionsBar from './CrewActionsBar';

const DATA = { agents: [{ name: 'A' }], tasks: [{ name: 'T' }] } as never;

beforeEach(() => {
  updateMessage.mockClear();
  postCrewFeedback.mockClear();
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
});
