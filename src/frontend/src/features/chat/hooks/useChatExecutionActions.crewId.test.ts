/**
 * A chat run of a saved crew says which crew it is.
 *
 * Emit-on-completion rules and trigger chains find a run by the saved crew it
 * was built from. The chat built its run from the crew's nodes but sent no
 * crew_id, so the backend recorded none and none of those ever fired for a
 * crew run from the chat. The backend records `crew_id` from the request
 * (tests/unit/services/execution/test_execution_crew_link.py).
 *
 * It is marked trigger_type "chat": the chat changes the crew for this run
 * (memory off, Agent Bricks endpoints, the request in the first task), so a
 * resume must use the run's own snapshot rather than rebuild from the saved
 * crew (checkpointing/resume_config.py).
 */
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { act, renderHook } from '@testing-library/react';
import { useChatExecutionActions } from './useChatExecutionActions';
import { createExecution } from '../api/executions';
import { buildCrewConfig } from '../utils/crewConfigBuilder';
import type { PlanData } from './useDispatcher';

vi.mock('../api/executions', () => ({ createExecution: vi.fn() }));

const CREW_ID = '3f2a9c1e-5b7d-4e8f-9a0b-1c2d3e4f5a6b';

const plan: PlanData = {
  id: CREW_ID,
  name: 'Weekly digest',
  nodes: [
    { id: 'agent-1', type: 'agentNode', data: { role: 'Writer', goal: 'Write', backstory: 'Writes' } },
    { id: 'task-1', type: 'taskNode', data: { description: 'Write the digest', expected_output: 'A digest' } },
  ],
  edges: [{ id: 'e1', source: 'agent-1', target: 'task-1' }],
};

beforeEach(() => {
  vi.clearAllMocks();
  vi.mocked(createExecution).mockResolvedValue({ job_id: 'job-1' } as never);
});

describe('chat runs of a saved crew', () => {
  it('send the saved crew id with the run', async () => {
    const handleStartExecutionStream = vi.fn();
    const { result } = renderHook(() => useChatExecutionActions({ handleStartExecutionStream }));

    await act(async () => { await result.current.handleExecuteCrew(plan); });

    expect(createExecution).toHaveBeenCalledOnce();
    expect(vi.mocked(createExecution).mock.calls[0][0]).toEqual(expect.objectContaining({ crew_id: CREW_ID }));
    expect(handleStartExecutionStream).toHaveBeenCalledWith('job-1', undefined);
  });

  it('marks the run as a chat run, so its resume keeps what the chat applied', async () => {
    const { result } = renderHook(() => useChatExecutionActions({ handleStartExecutionStream: vi.fn() }));

    await act(async () => { await result.current.handleExecuteCrew(plan); });

    expect(vi.mocked(createExecution).mock.calls[0][0]).toEqual(
      expect.objectContaining({ crew_id: CREW_ID, trigger_type: 'chat' }));
  });

  it('a plan with no saved crew carries no crew id', () => {
    expect(buildCrewConfig({ ...plan, id: undefined })).not.toHaveProperty('crew_id');
    expect(buildCrewConfig(plan)).toEqual(expect.objectContaining({ crew_id: CREW_ID }));
  });
});
