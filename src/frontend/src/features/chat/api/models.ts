import { ModelConfigResponse } from '../types/dispatcher';
import { fetchEnabledModelRows } from '../../../api/config/EnabledModelsService';

/**
 * The workspace's enabled models. Kept for callers of the chat API; model
 * menus read the shared, live list in store/models.ts instead.
 */
export async function fetchEnabledModels(): Promise<ModelConfigResponse[]> {
  return fetchEnabledModelRows();
}
