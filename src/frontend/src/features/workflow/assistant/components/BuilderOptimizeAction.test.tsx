import { fireEvent, render, screen } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { usePermissionStore } from '../../../../store/permissions';
import type { Run } from '../../../../types/execution/run';
import BuilderOptimizeAction from './BuilderOptimizeAction';

vi.mock('../../crews/components/CrewOptimizeDialog', () => ({ default: ({ crewId, crewName }: { crewId: string; crewName: string }) => <div role="dialog">{crewName} · {crewId}</div> }));
const getFlow = vi.hoisted(() => vi.fn());
vi.mock('../../../../api/workflow/FlowService', () => ({ FlowService: { getFlow } }));
const run = { job_id: 'finished-job', run_name: 'Swiss news', crew_id: 'saved-crew', execution_type: 'crew' } as Run;
beforeEach(() => { vi.clearAllMocks(); usePermissionStore.setState({ allowAgentBuilder: true, userRole: 'admin' }); });
describe('Optimize from a completed run', () => {
  it('opens the saved crew belonging to the execution', () => {
    render(<BuilderOptimizeAction run={run} />);
    fireEvent.click(screen.getByRole('button', { name: 'Optimize crew' }));
    expect(screen.getByRole('dialog')).toHaveTextContent('Swiss news · saved-crew');
  });
  it('lets a flow’s crews be optimized individually', async () => {
    render(<BuilderOptimizeAction run={{ ...run, execution_type: 'flow', inputs: { nodes: [
      { type: 'crewNode', data: { crewId: 'research', crewName: 'Research' } },
      { type: 'crewNode', data: { crewId: 'report', crewName: 'Report' } },
    ] } }} />);
    fireEvent.click(screen.getByRole('button', { name: 'Optimize crew' }));
    fireEvent.click(await screen.findByRole('menuitem', { name: 'Report' }));
    expect(screen.getByRole('dialog')).toHaveTextContent('Report · report');
  });
  it('loads the saved flow when the historical run omitted its graph', async () => {
    getFlow.mockResolvedValue({ nodes: [{ type: 'crewNode', data: { crewId: 'research', crewName: 'Research' } }] });
    render(<BuilderOptimizeAction run={{ ...run, execution_type: 'flow', flow_id: 'saved-flow' }} />);
    fireEvent.click(screen.getByRole('button', { name: 'Optimize crew' }));
    expect(await screen.findByRole('dialog')).toHaveTextContent('Research · research');
    expect(getFlow).toHaveBeenCalledWith('saved-flow');
  });
  it('does not substitute an unrelated canvas for an unsaved execution', () => {
    render(<BuilderOptimizeAction run={{ ...run, crew_id: undefined }} />);
    expect(screen.getByRole('button', { name: 'Optimize crew' })).toBeDisabled();
  });
  it('respects operator access', () => {
    usePermissionStore.setState({ userRole: 'operator' });
    render(<BuilderOptimizeAction run={run} />);
    expect(screen.queryByRole('button', { name: 'Optimize crew' })).not.toBeInTheDocument();
  });
});
