import React from 'react';
import { render, screen, waitFor } from '@testing-library/react';
import { beforeEach, expect, it, vi } from 'vitest';
import RunActivityRow from './RunActivityRow';
import { runService, type Run } from '../../../api/execution/ExecutionHistoryService';

vi.mock('../../../api/execution/ExecutionHistoryService', () => ({ runService: { withPayload: vi.fn() } }));
vi.mock('./ExecutionStatusBadge', () => ({ default: () => null }));
vi.mock('./ExecutionMemoryButton', () => ({ default: () => null }));
vi.mock('./RecipeCurationButton', () => ({ default: () => null }));
vi.mock('./RunDuration', () => ({ default: () => null }));

// A list row: summary scalars, no inputs.
const summary = {
  id: 'job-1', job_id: 'job-1', status: 'completed', created_at: '2026-09-01T10:00:00Z', updated_at: '',
  run_name: 'Report', agents_yaml: '', tasks_yaml: '', execution_type: 'crew', model: 'databricks-claude-sonnet-4-5',
} as Run;

const row = (expanded: boolean) => (
  <RunActivityRow run={summary} expanded={expanded} showSubmitter={false} onToggle={() => {}} onStatusChange={() => {}} actions={null} />
);

beforeEach(() => {
  vi.mocked(runService.withPayload).mockReset();
  vi.mocked(runService.withPayload).mockImplementation(async run => ({
    ...run,
    inputs: { agents_yaml: { a: {}, b: {} }, tasks_yaml: { t: {} } },
  }));
});

it('does not fetch the detail for a collapsed row', () => {
  render(row(false));
  expect(screen.getByText(/Crew/)).toBeTruthy();
  expect(runService.withPayload).not.toHaveBeenCalled();
});

it('shows the model scalar and fetches the counts when expanded', async () => {
  render(row(true));
  expect(screen.getByText('databricks-claude-sonnet-4-5')).toBeTruthy();
  await waitFor(() => expect(screen.getByText(/Crew · 2 agents · 1 task/)).toBeTruthy());
  expect(runService.withPayload).toHaveBeenCalledTimes(1);
});
