import React from 'react';
import { describe, it, expect, vi } from 'vitest';
import { render, screen } from '@testing-library/react';
import EdgeConfigDialog from './EdgeConfigDialog';

/**
 * AND/OR describe a JOIN — several connections arriving at one target. They are
 * not about how many TASKS the upstream crew happens to contain: a crew with two
 * tasks is still one source, and it finishes when it finishes.
 *
 * Counting its tasks disabled "None (Default)" on every ordinary edge out of a
 * multi-task crew and silently forced AND — which is what a two-node flow was
 * showing.
 */
const twoTaskCrew = {
  crewName: "Gather Today's Lebanese News",
  tasks: [
    { id: 't1', name: "Gather Today's Lebanese News" },
    { id: 't2', name: 'Structure and Summarize Lebanese News Briefing' },
  ],
};

const edge = { id: 'e1', source: 'crew-a', target: 'crew-b', data: {} };

function renderDialog(aggregatedSourceTasks: typeof twoTaskCrew[]) {
  return render(
    <EdgeConfigDialog
      open
      edge={edge as never}
      nodes={[] as never}
      edges={[] as never}
      onClose={vi.fn()}
      onSave={vi.fn()}
      aggregatedSourceTasks={aggregatedSourceTasks as never}
      targetTasks={[{ id: 't9', name: 'Send Email to Nehme Tomhe' }] as never}
    />,
  );
}

describe('Flow logic type', () => {
  it('stays on None for a single source, however many tasks it holds', () => {
    renderDialog([twoTaskCrew]);
    // The closed Select renders its current value: a two-task crew is still one
    // source, so the connection is sequential.
    expect(screen.getByText(/None \(Default\)/)).toBeInTheDocument();
    expect(screen.queryByText(/AND Logic/)).not.toBeInTheDocument();
  });

  it('switches to a join only when several connections arrive', () => {
    renderDialog([twoTaskCrew, { crewName: 'Another Crew', tasks: [{ id: 't3', name: 'x' }] }]);
    expect(screen.getByText(/AND Logic/)).toBeInTheDocument();
    expect(screen.queryByText(/None \(Default\)/)).not.toBeInTheDocument();
  });
});
