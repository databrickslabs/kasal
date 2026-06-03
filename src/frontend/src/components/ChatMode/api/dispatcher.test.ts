import { describe, it, expect, vi, beforeEach } from 'vitest';
import { dispatch, detectIntent } from './dispatcher';
import { getClient } from './client';
import type {
  DispatcherResponse,
  DispatchResult,
} from '../types/dispatcher';

vi.mock('./client', () => ({
  getClient: vi.fn(),
}));

const mockedGetClient = vi.mocked(getClient);

describe('dispatcher api', () => {
  const post = vi.fn();

  beforeEach(() => {
    vi.clearAllMocks();
    mockedGetClient.mockReturnValue({ post } as never);
  });

  describe('dispatch', () => {
    const dispatchResult: DispatchResult = {
      dispatcher: {
        intent: 'generate_agent',
        confidence: 0.9,
        extracted_info: {},
      },
      generation_result: { ok: true },
      service_called: 'agent_service',
    };

    it('sends only the message when model and tools are omitted', async () => {
      post.mockResolvedValue({ data: dispatchResult });

      const result = await dispatch('hello');

      expect(getClient).toHaveBeenCalledTimes(1);
      expect(post).toHaveBeenCalledWith('/dispatcher/dispatch', {
        message: 'hello',
      });
      expect(result).toEqual(dispatchResult);
    });

    it('includes the model when provided', async () => {
      post.mockResolvedValue({ data: dispatchResult });

      await dispatch('hello', 'gpt-4');

      expect(post).toHaveBeenCalledWith('/dispatcher/dispatch', {
        message: 'hello',
        model: 'gpt-4',
      });
    });

    it('includes the tools when provided', async () => {
      post.mockResolvedValue({ data: dispatchResult });

      await dispatch('hello', undefined, ['search']);

      expect(post).toHaveBeenCalledWith('/dispatcher/dispatch', {
        message: 'hello',
        tools: ['search'],
      });
    });

    it('includes both model and tools when provided', async () => {
      post.mockResolvedValue({ data: dispatchResult });

      const result = await dispatch('hello', 'gpt-4', ['search', 'browse']);

      expect(post).toHaveBeenCalledWith('/dispatcher/dispatch', {
        message: 'hello',
        model: 'gpt-4',
        tools: ['search', 'browse'],
      });
      expect(result).toBe(dispatchResult);
    });
  });

  describe('detectIntent', () => {
    it('posts the message and returns the response data', async () => {
      const response: DispatcherResponse = {
        intent: 'conversation',
        confidence: 0.5,
        extracted_info: { foo: 'bar' },
        suggested_prompt: 'try this',
      };
      post.mockResolvedValue({ data: response });

      const result = await detectIntent('what can you do?');

      expect(getClient).toHaveBeenCalledTimes(1);
      expect(post).toHaveBeenCalledWith('/dispatcher/detect-intent', {
        message: 'what can you do?',
      });
      expect(result).toEqual(response);
    });
  });
});
