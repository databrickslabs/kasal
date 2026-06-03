import { getClient } from './client';
import {
  DispatcherRequest,
  DispatcherResponse,
  DispatchResult,
} from '../types/dispatcher';

export async function dispatch(
  message: string,
  model?: string,
  tools?: string[]
): Promise<DispatchResult> {
  const request: DispatcherRequest = { message };
  if (model) request.model = model;
  if (tools) request.tools = tools;

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
