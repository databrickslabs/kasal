import { getClient } from './client';
import {
  DispatcherRequest,
  DispatcherResponse,
  DispatchResult,
  DispatchRunSettings,
} from '../types/dispatcher';

export async function dispatch(
  message: string,
  model?: string,
  tools?: string[],
  runSettings?: DispatchRunSettings,
  originalPrompt?: string
): Promise<DispatchResult> {
  const request: DispatcherRequest = { message };
  if (model) request.model = model;
  if (tools) request.tools = tools;
  // The clean user message (message may carry an intent-steering prefix).
  if (originalPrompt) request.original_prompt = originalPrompt;
  // ChatMode run settings: the backend auto-executes the generated crew with
  // these (session-scoped memory, attached MCP data sources).
  if (runSettings) {
    if (runSettings.auto_execute) request.auto_execute = true;
    if (runSettings.session_id) request.session_id = runSettings.session_id;
    if (runSettings.memory_workspace_scope !== undefined)
      request.memory_workspace_scope = runSettings.memory_workspace_scope;
    if (runSettings.disable_memory !== undefined)
      request.disable_memory = runSettings.disable_memory;
    if (runSettings.mcp_servers && runSettings.mcp_servers.length > 0)
      request.mcp_servers = runSettings.mcp_servers;
  }

  const response = await getClient().post<DispatchResult>(
    '/dispatcher/dispatch',
    request
  );
  return response.data;
}

export async function detectIntent(
  message: string
): Promise<DispatcherResponse> {
  const request: DispatcherRequest = { message };
  const response = await getClient().post<DispatcherResponse>(
    '/dispatcher/detect-intent',
    request
  );
  return response.data;
}
