/**
 * Tests for GenerateService.suggestGuardrail
 *
 * Covers:
 * - POSTs to /task-generation/suggest-guardrail with the correct body
 *   (description, expected_output, model — undefined when omitted)
 * - Returns the suggested description string on success
 * - Returns null when the API client throws
 */

import { describe, it, expect, vi, beforeEach, afterEach, Mock } from 'vitest';
import { GenerateService } from './GenerateService';
import apiClient from '../config/api/ApiConfig';

// Mock the shared API client (default export, matching GenerateService's import)
vi.mock('../config/api/ApiConfig', () => ({
  default: {
    post: vi.fn(),
  },
}));

const mockPost = apiClient.post as Mock;

describe('GenerateService.suggestGuardrail', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    // Silence the console.error in the catch branch
    vi.spyOn(console, 'error').mockImplementation(vi.fn());
  });

  afterEach(() => {
    (console.error as Mock).mockRestore();
  });

  it('posts to the correct URL with the full body including the model', async () => {
    mockPost.mockResolvedValueOnce({ data: { description: 'Validate that output is valid JSON.' } });

    await GenerateService.suggestGuardrail(
      'Summarize the report',
      'A concise paragraph',
      'gpt-4o-mini',
    );

    expect(mockPost).toHaveBeenCalledTimes(1);
    expect(mockPost).toHaveBeenCalledWith(
      '/task-generation/suggest-guardrail',
      {
        description: 'Summarize the report',
        expected_output: 'A concise paragraph',
        model: 'gpt-4o-mini',
      },
      { headers: { 'Content-Type': 'application/json' } },
    );
  });

  it('sends expected_output and model as undefined when omitted', async () => {
    mockPost.mockResolvedValueOnce({ data: { description: 'some guardrail' } });

    await GenerateService.suggestGuardrail('Just a description');

    expect(mockPost).toHaveBeenCalledWith(
      '/task-generation/suggest-guardrail',
      {
        description: 'Just a description',
        expected_output: undefined,
        model: undefined,
      },
      { headers: { 'Content-Type': 'application/json' } },
    );
  });

  it('coerces empty-string expected_output and model to undefined', async () => {
    mockPost.mockResolvedValueOnce({ data: { description: 'g' } });

    await GenerateService.suggestGuardrail('desc', '', '');

    expect(mockPost).toHaveBeenCalledWith(
      '/task-generation/suggest-guardrail',
      {
        description: 'desc',
        expected_output: undefined,
        model: undefined,
      },
      { headers: { 'Content-Type': 'application/json' } },
    );
  });

  it('returns the suggested description string on success', async () => {
    mockPost.mockResolvedValueOnce({
      data: { description: 'Ensure the answer cites at least one source.' },
    });

    const result = await GenerateService.suggestGuardrail('desc', 'out', 'model-x');

    expect(result).toBe('Ensure the answer cites at least one source.');
  });

  it('returns null when the response has no description', async () => {
    mockPost.mockResolvedValueOnce({ data: {} });

    const result = await GenerateService.suggestGuardrail('desc');

    expect(result).toBeNull();
  });

  it('returns null when the response data is missing', async () => {
    mockPost.mockResolvedValueOnce({});

    const result = await GenerateService.suggestGuardrail('desc');

    expect(result).toBeNull();
  });

  it('returns null when the API client throws', async () => {
    mockPost.mockRejectedValueOnce(new Error('network down'));

    const result = await GenerateService.suggestGuardrail('desc', 'out', 'model-x');

    expect(result).toBeNull();
    expect(console.error).toHaveBeenCalled();
  });
});
