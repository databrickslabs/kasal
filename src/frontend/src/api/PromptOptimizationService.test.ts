/**
 * Tests for PromptOptimizationService.
 *
 * Covers every method's endpoint, payload shape (undefined-stripping for
 * optional fields), URL encoding of judge names, and response mapping
 * (list unwrapping with empty-list fallbacks, boolean coercion).
 */

import { describe, it, expect, vi, beforeEach, Mock } from 'vitest';
import { PromptOptimizationService } from './PromptOptimizationService';
import apiClient from '../config/api/ApiConfig';

vi.mock('../config/api/ApiConfig', () => ({
  default: {
    get: vi.fn(),
    post: vi.fn(),
    put: vi.fn(),
    delete: vi.fn(),
  },
}));

const mockGet = apiClient.get as Mock;
const mockPost = apiClient.post as Mock;
const mockPut = apiClient.put as Mock;
const mockDelete = apiClient.delete as Mock;

const JSON_HEADERS = { headers: { 'Content-Type': 'application/json' } };

beforeEach(() => {
  vi.clearAllMocks();
});

describe('startOptimization', () => {
  it('posts the request to /optimize and returns the ack', async () => {
    const ack = { run_id: 'r1', status: 'pending', dataset_size: 5 };
    mockPost.mockResolvedValueOnce({ data: ack });
    const request = { template_name: 'detect_intent', examples: ['a', 'b'] };
    const result = await PromptOptimizationService.startOptimization(request);
    expect(mockPost).toHaveBeenCalledWith(
      '/prompt-optimization/optimize',
      request,
      JSON_HEADERS,
    );
    expect(result).toEqual(ack);
  });
});

describe('startCrewOptimization', () => {
  it('posts the request to /optimize-crew', async () => {
    const ack = { run_id: 'r2', status: 'pending', dataset_size: 1 };
    mockPost.mockResolvedValueOnce({ data: ack });
    const request = { crew_id: 'c1', max_metric_calls: 10 };
    const result = await PromptOptimizationService.startCrewOptimization(request);
    expect(mockPost).toHaveBeenCalledWith(
      '/prompt-optimization/optimize-crew',
      request,
      JSON_HEADERS,
    );
    expect(result).toEqual(ack);
  });
});

describe('listRuns', () => {
  it('unwraps the runs array', async () => {
    const runs = [{ run_id: 'r1' }];
    mockGet.mockResolvedValueOnce({ data: { runs } });
    expect(await PromptOptimizationService.listRuns()).toEqual(runs);
    expect(mockGet).toHaveBeenCalledWith('/prompt-optimization/runs');
  });

  it('returns [] when the payload is empty', async () => {
    mockGet.mockResolvedValueOnce({ data: undefined });
    expect(await PromptOptimizationService.listRuns()).toEqual([]);
  });
});

describe('getRun', () => {
  it('fetches a single run by id', async () => {
    const run = { run_id: 'r1', status: 'completed' };
    mockGet.mockResolvedValueOnce({ data: run });
    expect(await PromptOptimizationService.getRun('r1')).toEqual(run);
    expect(mockGet).toHaveBeenCalledWith('/prompt-optimization/runs/r1');
  });
});

describe('listCrewEvals', () => {
  it('unwraps evals for the crew', async () => {
    const evals = [{ trace_id: 't1', deliverable: 'x', assessment_count: 0 }];
    mockGet.mockResolvedValueOnce({ data: { evals } });
    expect(await PromptOptimizationService.listCrewEvals('crew1')).toEqual(evals);
    expect(mockGet).toHaveBeenCalledWith('/prompt-optimization/crew-evals/crew1');
  });

  it('returns [] when the payload is empty', async () => {
    mockGet.mockResolvedValueOnce({ data: {} });
    expect(await PromptOptimizationService.listCrewEvals('crew1')).toEqual([]);
  });
});

describe('addEvalFeedback', () => {
  it('posts grade, comment and expectation to the trace feedback endpoint', async () => {
    mockPost.mockResolvedValueOnce({ data: { ok: true } });
    const ok = await PromptOptimizationService.addEvalFeedback(
      't1',
      7,
      'good but wrong region',
      'german side only',
    );
    expect(ok).toBe(true);
    expect(mockPost).toHaveBeenCalledWith(
      '/prompt-optimization/crew-evals/t1/feedback',
      { value: 7, comment: 'good but wrong region', expectation: 'german side only' },
      JSON_HEADERS,
    );
  });

  it('strips empty optional fields to undefined', async () => {
    mockPost.mockResolvedValueOnce({ data: { ok: true } });
    await PromptOptimizationService.addEvalFeedback('t1', undefined, '', '');
    expect(mockPost).toHaveBeenCalledWith(
      '/prompt-optimization/crew-evals/t1/feedback',
      { value: undefined, comment: undefined, expectation: undefined },
      JSON_HEADERS,
    );
  });

  it('keeps a zero grade (0 is a valid harsh grade, not "missing")', async () => {
    mockPost.mockResolvedValueOnce({ data: { ok: true } });
    await PromptOptimizationService.addEvalFeedback('t1', 0);
    const body = mockPost.mock.calls[0][1];
    expect(body.value).toBe(0);
  });

  it('coerces a missing ok to false', async () => {
    mockPost.mockResolvedValueOnce({ data: {} });
    expect(await PromptOptimizationService.addEvalFeedback('t1', 5)).toBe(false);
  });
});

describe('judges', () => {
  it('listJudges unwraps the judges array with [] fallback', async () => {
    const judges = [{ name: 'acc' }];
    mockGet.mockResolvedValueOnce({ data: { judges } });
    expect(await PromptOptimizationService.listJudges()).toEqual(judges);
    expect(mockGet).toHaveBeenCalledWith('/prompt-optimization/judges');
    mockGet.mockResolvedValueOnce({ data: undefined });
    expect(await PromptOptimizationService.listJudges()).toEqual([]);
  });

  it('createJudge posts name/instructions and strips empty model/crew', async () => {
    mockPost.mockResolvedValueOnce({ data: { name: 'acc' } });
    await PromptOptimizationService.createJudge('acc', 'criteria');
    expect(mockPost).toHaveBeenCalledWith(
      '/prompt-optimization/judges',
      { name: 'acc', instructions: 'criteria', model: undefined, crew_id: undefined },
      JSON_HEADERS,
    );
  });

  it('createJudge forwards model and crew id when provided', async () => {
    mockPost.mockResolvedValueOnce({ data: { name: 'acc' } });
    await PromptOptimizationService.createJudge('acc', 'criteria', 'qwen', 'crew-1');
    expect(mockPost.mock.calls[0][1]).toEqual({
      name: 'acc',
      instructions: 'criteria',
      model: 'qwen',
      crew_id: 'crew-1',
    });
  });

  it('assignJudge posts the crew id under the encoded judge name', async () => {
    mockPost.mockResolvedValueOnce({ data: { full_name: 'crew_x__my judge' } });
    await PromptOptimizationService.assignJudge('my judge', 'crew-1');
    expect(mockPost).toHaveBeenCalledWith(
      '/prompt-optimization/judges/my%20judge/assign',
      { crew_id: 'crew-1' },
      JSON_HEADERS,
    );
  });

  it('updateJudge PUTs changes and strips empty fields', async () => {
    mockPut.mockResolvedValueOnce({ data: { name: 'acc' } });
    await PromptOptimizationService.updateJudge('crew_x__acc', {
      instructions: 'new criteria',
    });
    expect(mockPut).toHaveBeenCalledWith(
      '/prompt-optimization/judges/crew_x__acc',
      { instructions: 'new criteria', model: undefined },
      JSON_HEADERS,
    );
  });

  it('deleteJudge deletes by encoded name and coerces ok', async () => {
    mockDelete.mockResolvedValueOnce({ data: { ok: true } });
    expect(await PromptOptimizationService.deleteJudge('a judge')).toBe(true);
    expect(mockDelete).toHaveBeenCalledWith(
      '/prompt-optimization/judges/a%20judge',
    );
    mockDelete.mockResolvedValueOnce({ data: {} });
    expect(await PromptOptimizationService.deleteJudge('a judge')).toBe(false);
  });
});

describe('run control', () => {
  it('cancelRun posts to the cancel endpoint and coerces the flag', async () => {
    mockPost.mockResolvedValueOnce({ data: { cancelling: true } });
    expect(await PromptOptimizationService.cancelRun('r1')).toBe(true);
    expect(mockPost).toHaveBeenCalledWith('/prompt-optimization/runs/r1/cancel');
    mockPost.mockResolvedValueOnce({ data: {} });
    expect(await PromptOptimizationService.cancelRun('r1')).toBe(false);
  });

  it('applyRun posts to the apply endpoint and coerces the flag', async () => {
    mockPost.mockResolvedValueOnce({ data: { applied: true } });
    expect(await PromptOptimizationService.applyRun('r1')).toBe(true);
    expect(mockPost).toHaveBeenCalledWith('/prompt-optimization/runs/r1/apply');
  });
});
