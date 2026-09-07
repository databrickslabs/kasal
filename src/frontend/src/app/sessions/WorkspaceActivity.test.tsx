import { render, screen, fireEvent } from '@testing-library/react';
import { beforeEach, expect, it, vi } from 'vitest';
import { WorkspaceActivity } from './WorkspaceActivity';
import { useGroupStore } from '../../store/groups';
import { usePermissionStore } from '../../store/permissions';

vi.mock('./useSessionRunIds', () => ({ useSessionRunIds: () => ({ ids: ['session-run'], sessionKey: 'crew:one' }) }));
vi.mock('../../features/executions/components/ExecutionHistory', () => ({ default: ({ jobIds }: { jobIds?: string[] }) => <div>{jobIds ? jobIds.join(',') : 'Teamspace executions'}</div> }));
vi.mock('../../features/workflow/scheduling/components/ScheduleDialog', () => ({ default: ({ embedded }: { embedded?: boolean }) => <div>{embedded ? 'Schedule management' : 'Separate schedule dialog'}</div> }));
vi.mock('../../features/executions/components/LLMLogs', () => ({ default: () => <div>Existing model logs</div> }));
beforeEach(() => {
  useGroupStore.setState({ currentGroupId: 'g' });
  usePermissionStore.setState({ allowAgentBuilder: true, allowFlowBuilder: true });
});
it('keeps executions, schedules and assistant logs within one Activity dialog', async () => {
  render(<WorkspaceActivity expanded />);
  fireEvent.click(screen.getByRole('button', { name: 'Activity' }));
  expect(await screen.findByText('session-run')).toBeVisible();
  fireEvent.click(screen.getByRole('button', { name: 'All teamspace runs' }));
  expect(await screen.findByText('Teamspace executions')).toBeVisible();
  fireEvent.click(screen.getByRole('button', { name: 'Schedules', exact: true }));
  expect(await screen.findByText('Schedule management')).toBeVisible();
  expect(screen.queryByRole('button', { name: 'This session' })).not.toBeInTheDocument();
  expect(screen.getAllByRole('dialog')).toHaveLength(1);
  fireEvent.click(screen.getByRole('button', { name: 'Assistant logs' }));
  expect(await screen.findByText('Existing model logs')).toBeVisible();
  fireEvent.click(screen.getByRole('button', { name: 'Executions' }));
  expect(await screen.findByText('Teamspace executions')).toBeVisible();
  fireEvent.click(screen.getByRole('button', { name: 'Close activity' }));
});
it('keeps the existing builder capability restriction for schedules and model logs', () => {
  usePermissionStore.setState({ allowAgentBuilder: false, allowFlowBuilder: false });
  render(<WorkspaceActivity expanded={false} />);
  fireEvent.click(screen.getByRole('button', { name: 'Activity' }));
  expect(screen.queryByRole('button', { name: 'Schedules' })).not.toBeInTheDocument();
  expect(screen.queryByRole('button', { name: 'Assistant logs' })).not.toBeInTheDocument();
  expect(screen.getByRole('button', { name: 'Executions' })).toBeVisible();
});
