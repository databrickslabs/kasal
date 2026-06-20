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

    it('includes ChatMode run settings + the clean original prompt', async () => {
      post.mockResolvedValue({ data: dispatchResult });

      await dispatch(
        'create a crew plan with agents and tasks: top customers',
        'm',
        undefined,
        {
          auto_execute: true,
          session_id: 'chat-1',
          memory_workspace_scope: false,
          disable_memory: true,
          mcp_servers: ['Databricks Genie: Sales'],
        },
        'top customers',
      );

      expect(post).toHaveBeenCalledWith('/dispatcher/dispatch', {
        message: 'create a crew plan with agents and tasks: top customers',
        model: 'm',
        original_prompt: 'top customers',
        auto_execute: true,
        session_id: 'chat-1',
        memory_workspace_scope: false,
        disable_memory: true,
        mcp_servers: ['Databricks Genie: Sales'],
      });
    });

    it('omits auto_execute when false/absent (crew canvas path)', async () => {
      post.mockResolvedValue({ data: dispatchResult });

      await dispatch('hi', undefined, undefined, {
        auto_execute: false,
        memory_workspace_scope: true,
        disable_memory: false,
        mcp_servers: [],
      });

      // auto_execute:false is omitted so the backend default (false) applies —
      // the crew canvas renders the plan and runs it via Play, not on dispatch.
      expect(post).toHaveBeenCalledWith('/dispatcher/dispatch', {
        message: 'hi',
        memory_workspace_scope: true,
        disable_memory: false,
      });
    });

    it('omits empty mcp_servers and falsy run settings from the payload', async () => {
      post.mockResolvedValue({ data: dispatchResult });

      await dispatch('hi', undefined, undefined, {
        session_id: undefined,
        memory_workspace_scope: true,
        disable_memory: false,
        mcp_servers: [],
      });

      // memory_workspace_scope/disable_memory are sent even when default
      // (booleans are meaningful), but no session_id and no empty mcp_servers.
      expect(post).toHaveBeenCalledWith('/dispatcher/dispatch', {
        message: 'hi',
        memory_workspace_scope: true,
        disable_memory: false,
      });
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
