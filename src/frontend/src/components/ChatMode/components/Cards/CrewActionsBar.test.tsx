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

// App-mode switch: the "Open in …" buttons flip the canvas via this store.
const setAppMode = vi.fn();
vi.mock('../../../../store/uiLayout', () => ({
  useUILayoutStore: (sel: (s: { setAppMode: (m: string) => void }) => unknown) =>
    sel({ setAppMode }),
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
// A crew with real agent/task ids — buildCrewGraph needs ids to synthesize nodes.
const DATA_WITH_IDS = {
  agents: [{ id: 'a1', name: 'A', role: 'r' }],
  tasks: [{ id: 't1', name: 'T', agent_id: 'a1' }],
} as never;

beforeEach(() => {
  updateMessage.mockClear();
  postCrewFeedback.mockClear();
  setAppMode.mockClear();
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

  it('shows the memory-graph button only when the run used workspace memory and a run id is present', () => {
    render(
      <CrewActionsBar
        data={DATA}
        messageId={`a-${Math.random()}`}
        onSaveCrew={vi.fn()}
        executionId="job-9"
        usedWorkspaceMemory
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
        usedWorkspaceMemory
      />,
    );
    fireEvent.click(screen.getByLabelText('View memory graph'));
    const graph = screen.getByTestId('memory-graph');
    expect(graph).toHaveAttribute('data-run', 'job-9');
    expect(graph).toHaveAttribute('data-view', 'graph');
  });

  it('hides the memory-graph button when there is no run id', () => {
    render(
      <CrewActionsBar
        data={DATA}
        messageId={`a-${Math.random()}`}
        onSaveCrew={vi.fn()}
        usedWorkspaceMemory
      />,
    );
    expect(screen.queryByLabelText('View memory graph')).toBeNull();
  });

  it('hides the memory-graph button when the run did NOT use workspace memory (even with a run id)', () => {
    // Session-only run: a later toggle to workspace memory must not reveal it.
    render(
      <CrewActionsBar
        data={DATA}
        messageId={`a-${Math.random()}`}
        onSaveCrew={vi.fn()}
        executionId="job-9"
        usedWorkspaceMemory={false}
      />,
    );
    expect(screen.queryByLabelText('View memory graph')).toBeNull();
  });

  // -------------------------------------------------------------------------
  // Open-on-canvas buttons (Agent Builder / Flow Builder)
  // -------------------------------------------------------------------------

  it('opens the crew on the Agent Builder canvas (dispatches catalogLoadCrew + switches to crew mode)', () => {
    const dispatchSpy = vi.spyOn(window, 'dispatchEvent');
    render(<CrewActionsBar data={DATA_WITH_IDS} messageId={`a-${Math.random()}`} onSaveCrew={vi.fn()} />);

    fireEvent.click(screen.getByLabelText('Open in Agent Builder'));

    const evt = dispatchSpy.mock.calls
      .map((c) => c[0] as CustomEvent)
      .find((e) => e.type === 'catalogLoadCrew');
    expect(evt).toBeTruthy();
    const detail = (evt as CustomEvent).detail;
    expect(Array.isArray(detail.nodes)).toBe(true);
    expect(detail.nodes.length).toBeGreaterThan(0);
    expect(setAppMode).toHaveBeenCalledWith('crew');
    dispatchSpy.mockRestore();
  });

  it('opens the crew on the Flow Builder canvas (saves first, dispatches catalogLoadFlow + switches to flow mode)', async () => {
    const dispatchSpy = vi.spyOn(window, 'dispatchEvent');
    const onSaveCrew = vi.fn(async () => ({ id: 'crew-fl', name: 'Flowable' }));
    render(<CrewActionsBar data={DATA_WITH_IDS} messageId={`a-${Math.random()}`} onSaveCrew={onSaveCrew} />);

    fireEvent.click(screen.getByLabelText('Open in Flow Builder'));

    await waitFor(() => expect(setAppMode).toHaveBeenCalledWith('flow'));
    expect(onSaveCrew).toHaveBeenCalled();
    const evt = dispatchSpy.mock.calls
      .map((c) => c[0] as CustomEvent)
      .find((e) => e.type === 'catalogLoadFlow');
    expect(evt).toBeTruthy();
    const detail = (evt as CustomEvent).detail;
    expect(detail.nodes).toHaveLength(1);
    expect(detail.nodes[0].type).toBe('crewNode');
    expect(detail.nodes[0].data.crewId).toBe('crew-fl');
    dispatchSpy.mockRestore();
  });

  it('hides the builder buttons in answer ("chat") mode', () => {
    const chatData = { ...(DATA as object), chatModeType: 'chat' } as never;
    render(
      <CrewActionsBar
        data={chatData}
        messageId={`a-${Math.random()}`}
        onSaveAnswerToCatalog={vi.fn()}
      />,
    );
    expect(screen.queryByLabelText('Open in Agent Builder')).toBeNull();
    expect(screen.queryByLabelText('Open in Flow Builder')).toBeNull();
  });

  // -------------------------------------------------------------------------
  // Answer ("chat") mode: "Save to catalog" distills a crew from the whole
  // conversation and saves it in one shot (no second confirmation click).
  // -------------------------------------------------------------------------

  it('answer mode: Save to catalog calls onSaveAnswerToCatalog with the run session and shows "Saved to catalog"', async () => {
    const chatData = { ...(DATA as object), chatModeType: 'chat', sessionId: 's-1' } as never;
    const onSaveAnswerToCatalog = vi.fn(async () => {});
    const onSaveCrew = vi.fn();
    render(
      <CrewActionsBar
        data={chatData}
        messageId={`a-${Math.random()}`}
        onSaveCrew={onSaveCrew}
        onSaveAnswerToCatalog={onSaveAnswerToCatalog}
      />,
    );

    fireEvent.click(screen.getByText('Save to catalog'));
    await waitFor(() => expect(screen.getByText('Saved to catalog')).toBeInTheDocument());
    // Distills from THIS run's conversation, not the generic single-agent crew.
    expect(onSaveAnswerToCatalog).toHaveBeenCalledWith('s-1');
    expect(onSaveCrew).not.toHaveBeenCalled();
  });

  it('answer mode: hides the crew-feedback votes (the generic assistant has nothing to rate)', () => {
    const chatData = { ...(DATA as object), chatModeType: 'chat' } as never;
    render(
      <CrewActionsBar
        data={chatData}
        messageId={`a-${Math.random()}`}
        onSaveAnswerToCatalog={vi.fn()}
      />,
    );
    expect(screen.queryByLabelText('Thumbs up')).toBeNull();
    expect(screen.queryByLabelText('Thumbs down')).toBeNull();
  });

  it('answer mode: reports an error when the distill/save fails', async () => {
    const chatData = { ...(DATA as object), chatModeType: 'chat', sessionId: 's-1' } as never;
    const onSaveAnswerToCatalog = vi.fn().mockRejectedValue(new Error('boom'));
    render(
      <CrewActionsBar
        data={chatData}
        messageId={`a-${Math.random()}`}
        onSaveAnswerToCatalog={onSaveAnswerToCatalog}
      />,
    );

    fireEvent.click(screen.getByText('Save to catalog'));
    await waitFor(() =>
      expect(screen.getByText('Could not save a crew from this conversation')).toBeInTheDocument(),
    );
  });

  it('falls back to normal bookmark save when not in answer mode even if onSaveAnswerToCatalog is provided', async () => {
    // No chatModeType -> isChatMode false -> the normal crew-save path runs.
    const onSaveCrew = vi.fn(async () => ({ id: 'crew-x', name: 'Normal' }));
    const onSaveAnswerToCatalog = vi.fn();
    render(
      <CrewActionsBar
        data={DATA}
        messageId={`a-${Math.random()}`}
        onSaveCrew={onSaveCrew}
        onSaveAnswerToCatalog={onSaveAnswerToCatalog}
      />,
    );
    fireEvent.click(screen.getByText('Save to catalog'));
    await waitFor(() => expect(screen.getByText(/Saved — Normal/)).toBeInTheDocument());
    expect(onSaveCrew).toHaveBeenCalled();
    expect(onSaveAnswerToCatalog).not.toHaveBeenCalled();
  });
});
