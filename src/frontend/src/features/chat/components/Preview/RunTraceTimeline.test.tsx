import React from 'react';
import { describe, it, expect, vi } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import RunTraceTimeline from './RunTraceTimeline';
import { makeEvent, makeProcessedTraces } from './testTraceFixture';

describe('RunTraceTimeline', () => {
  it('shows the events themselves, under their agent and task', () => {
    render(<RunTraceTimeline processed={makeProcessedTraces()} />);
    expect(screen.getByText('Database Researcher and Data Storage Specialist')).toBeInTheDocument();
    expect(screen.getByText('Research and collect 300 Swiss tech companies')).toBeInTheDocument();
    expect(screen.getByText('LLM Request — KAT-Coder (3,585 chars)')).toBeInTheDocument();
    expect(screen.getByText('postgres_execute_sql (output)')).toBeInTheDocument();
  });

  it('names the task without its description', () => {
    // The row has to stay scannable: the chat used to print the whole task
    // description (and, for anything it could not label, the raw event JSON)
    // into the activity list. The description is available on hover and in the
    // step's own content.
    render(<RunTraceTimeline processed={makeProcessedTraces()} />);
    expect(screen.queryByText(/design a PostgreSQL schema/)).not.toBeInTheDocument();
    expect(screen.getByTitle(/design a PostgreSQL schema/)).toBeInTheDocument();
  });

  it('collapses an agent and its tasks', () => {
    render(<RunTraceTimeline processed={makeProcessedTraces()} />);
    fireEvent.click(screen.getByText('Database Researcher and Data Storage Specialist'));
    expect(screen.queryByText('postgres_execute_sql (output)')).not.toBeInTheDocument();
  });

  it('opens a row that has output, and leaves one without it inert', () => {
    const onSelectEvent = vi.fn();
    render(
      <RunTraceTimeline
        processed={makeProcessedTraces([
          makeEvent({ description: 'postgres_execute_sql (output)', output: 'CREATE TABLE', traceId: 1 }),
          // No output → nothing to open, exactly as in the trace dialog.
          makeEvent({ type: 'agent_start', description: 'Agent started', output: undefined, traceId: 2 }),
        ])}
        onSelectEvent={onSelectEvent}
      />,
    );
    fireEvent.click(screen.getByText('Agent started'));
    expect(onSelectEvent).not.toHaveBeenCalled();
    fireEvent.click(screen.getByText('postgres_execute_sql (output)'));
    expect(onSelectEvent).toHaveBeenCalledTimes(1);
    expect(onSelectEvent.mock.calls[0][0]).toMatchObject({ description: 'postgres_execute_sql (output)' });
  });

  it('says the run is starting rather than empty while it is still live', () => {
    // An empty timeline mid-run means the first rows have not been written
    // yet — reporting "no activity" there would be wrong, not just unhelpful.
    const { rerender } = render(<RunTraceTimeline processed={null} live />);
    expect(screen.getByText('Getting started…')).toBeInTheDocument();
    rerender(<RunTraceTimeline processed={null} />);
    expect(screen.getByText('No activity recorded for this run.')).toBeInTheDocument();
  });
});
