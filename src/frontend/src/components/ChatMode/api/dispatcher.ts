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
  // ChatMode always builds a crew. "create a task"/"create an agent" entity
  // creation is only for the AgentBuilder / crew canvas; this flag tells the
  // backend to collapse those intents to generate_crew for ChatMode prompts.
  request.chat_mode = true;
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
    if (runSettings.agentbricks_endpoints && runSettings.agentbricks_endpoints.length > 0)
      request.agentbricks_endpoints = runSettings.agentbricks_endpoints;
    if (runSettings.knowledge_file_paths && runSettings.knowledge_file_paths.length > 0)
      request.knowledge_file_paths = runSettings.knowledge_file_paths;
    if (runSettings.chat_mode_type) request.chat_mode_type = runSettings.chat_mode_type;
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
