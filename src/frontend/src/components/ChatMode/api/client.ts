import { AxiosInstance } from 'axios';
import { AppConfig } from '../types/chat';
import apiClient, { config as kasalApiConfig } from '../../../config/api/ApiConfig';

/*
 * The embedded Chat workspace reuses Kasal's shared axios client so it inherits
 * the same auth (`X-Forwarded-Email`) and tenant isolation (`group_id` from
 * `selectedGroupId`) as the rest of the app — see src/config/api/ApiConfig.ts.
 *
 * The standalone chat app maintained its own client and per-user config; in the
 * integrated app that responsibility belongs to Kasal, so `updateClient` is a
 * no-op kept only for API compatibility with the chat stores.
 */

export function updateClient(_config: AppConfig): void {
  // Auth/group context is managed centrally by Kasal's apiClient interceptors.
}

export function getClient(): AxiosInstance {
  return apiClient;
}

export function getBaseUrl(): string {
  return apiClient.defaults.baseURL || kasalApiConfig.apiUrl || '';
}
