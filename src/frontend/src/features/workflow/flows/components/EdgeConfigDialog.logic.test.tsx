import React from 'react';
import { describe, it, expect, vi } from 'vitest';
import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import { SchemaService } from '../../../../api/workflow/SchemaService';
import { TaskService } from '../../../../api/workflow/TaskService';
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


it('opens generated same-item conditions with their flow-local schema and saves without updating catalog tasks', async () => {
  vi.spyOn(SchemaService.getInstance(), 'getSchemas').mockResolvedValue([]);
  const updateTask = vi.spyOn(TaskService, 'updateTask');
  const save = vi.fn();
  const contract = { crew_id: 'a', task_id: 't1', name: 'News relevance', schema_definition: {
    type: 'object', required: ['articles'], properties: { articles: { type: 'array', items: {
      type: 'object', required: ['importance', 'category'], properties: {
        importance: { type: 'number' }, category: { type: 'string' },
      },
    } } },
  } };
  render(<EdgeConfigDialog open onClose={vi.fn()} onSave={save}
    edge={{ ...edge, data: { logicType: 'ROUTER', routerSchema: contract.name,
      routerCondition: "where('articles', category='policy', importance__gte=7)" } }}
    nodes={[
      { id: 'crew-a', position: {x: 0, y: 0}, data: { crewId: 'a', crewName: 'News', allTasks: [{id: 't1', name: 'News task'}], outputContract: contract } },
      { id: 'crew-b', position: {x: 200, y: 0}, data: { crewId: 'b', allTasks: [{id: 't2', name: 'Notify team'}] } },
    ]} />);
  expect(screen.getByText('News relevance · This flow')).toBeVisible();
  expect(screen.getByDisplayValue('policy')).toBeVisible();
  expect(screen.getByDisplayValue('7')).toBeVisible();
  fireEvent.click(screen.getByRole('button', {name: 'Review output fields'}));
  expect(screen.getByText(/Other uses of the catalog crew keep their original output/)).toBeVisible();
  fireEvent.click(screen.getByRole('button', {name: 'Save', exact: true}));
  await waitFor(() => expect(save).toHaveBeenCalled());
  expect(save.mock.calls[0][1].outputContract).toEqual(contract);
  expect(save.mock.calls[0][1].routerCondition).toContain('importance__gte=7');
  expect(updateTask).not.toHaveBeenCalled();
  vi.restoreAllMocks();
});


it('allows human approval without a checkpoint prerequisite', async () => {
  const save = vi.fn();
  render(<EdgeConfigDialog open edge={edge as never} nodes={[]} edges={[]}
    onClose={vi.fn()} onSave={save} aggregatedSourceTasks={[twoTaskCrew] as never}
    targetTasks={[{ id: 't9', name: 'Next step' }] as never} />);
  expect(screen.queryByLabelText(/Enable Checkpoint/)).not.toBeInTheDocument();
  const approval = screen.getByRole('checkbox', { name: 'Require human approval' });
  expect(approval).toBeEnabled();
  fireEvent.click(approval);
  fireEvent.click(screen.getByRole('button', { name: /^Save$/ }));
  await waitFor(() => expect(save).toHaveBeenCalledWith('e1', expect.objectContaining({ hitl: expect.objectContaining({ enabled: true }) })));
});
