/**
 * GET /executions returns run SUMMARIES: a `result_preview` and small scalars,
 * never `result` or `inputs`. Anything that needs the payload upgrades the row
 * through `runService.withPayload`, which reads GET /executions/{job_id}.
 */
import { describe, it, expect, vi, beforeEach, Mock } from 'vitest';
import apiClient from '../../shared/api/client';
import { runService } from './ExecutionHistoryService';

vi.mock('../../shared/api/client', () => ({
  default: { get: vi.fn(), post: vi.fn(), delete: vi.fn() },
}));

const mockGet = apiClient.get as Mock;

const listRow = {
  execution_id: 'job-1',
  status: 'completed',
  created_at: '2026-09-01T10:00:00',
  completed_at: '2026-09-01T10:05:00',
  run_name: 'Quarterly report',
  group_id: 'g1',
  group_email: 'a@example.com',
  execution_type: 'crew',
  harness: 'kasal',
  model: 'databricks-claude-sonnet-4-5',
  result_preview: '{"content": "The answer',
};

const detail = {
  execution_id: 'job-1',
  status: 'completed',
  created_at: '2026-09-01T10:00:00',
  run_name: 'Quarterly report',
  result: { content: 'The answer is 42.' },
  inputs: {
    model: 'databricks-claude-sonnet-4-5',
    agents_yaml: { analyst: { role: 'Analyst' } },
    tasks_yaml: { report: { description: 'Write it' } },
  },
};

function respond(url: string) {
  if (url === '/executions' || url.startsWith('/executions?')) return { data: [listRow] };
  if (url === '/executions/job-1') return { data: detail };
  throw new Error(`unexpected GET ${url}`);
}

describe('run list summaries', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    runService.invalidateRunsCache();
    mockGet.mockImplementation(async (url: string) => respond(url));
  });

  it('maps the summary scalars and leaves the payload undefined', async () => {
    const { runs } = await runService.getRuns(50, 0);
    const run = runs[0];
    expect(run.result).toBeUndefined();
    expect(run.inputs).toBeUndefined();
    expect(run.agents_yaml).toBe('');
    expect(run.result_preview).toBe('{"content": "The answer');
    expect(run.model).toBe('databricks-claude-sonnet-4-5');
    expect(run.harness).toBe('kasal');
    expect(run.group_id).toBe('g1');
  });

  it('withPayload fetches the detail and keeps the list-only fields', async () => {
    const { runs } = await runService.getRuns(50, 0);
    const full = await runService.withPayload(runs[0]);

    expect(mockGet).toHaveBeenCalledWith('/executions/job-1');
    expect(full.result).toEqual({ content: 'The answer is 42.' });
    expect(full.inputs?.agents_yaml).toEqual({ analyst: { role: 'Analyst' } });
    expect(JSON.parse(full.tasks_yaml)).toEqual({ report: { description: 'Write it' } });
    // The detail response has no group_id; the row's must survive the merge.
    expect(full.group_id).toBe('g1');
    expect(full.id).toBe(runs[0].id);
  });

  it('withPayload does not refetch a run that already has its payload', async () => {
    const run = { ...(await runService.getRuns(50, 0)).runs[0], result: { content: 'x' } };
    mockGet.mockClear();
    expect(await runService.withPayload(run)).toBe(run);
    expect(mockGet).not.toHaveBeenCalled();
  });

  it('withPayload falls back to the row when the detail cannot be read', async () => {
    const { runs } = await runService.getRuns(50, 0);
    mockGet.mockImplementation(async (url: string) => {
      if (url === '/executions/job-1') throw new Error('404');
      return respond(url);
    });
    expect(await runService.withPayload(runs[0])).toBe(runs[0]);
  });
});
