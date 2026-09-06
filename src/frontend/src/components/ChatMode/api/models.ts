import { getClient } from '../../../features/chat/api/client';
import { ModelConfigResponse } from '../../../features/chat/types/dispatcher';

interface ModelListResponse {
  models: ModelConfigResponse[];
  count: number;
}

export async function fetchEnabledModels(): Promise<ModelConfigResponse[]> {
  const response = await getClient().get<ModelListResponse>('/models/enabled');
  return response.data.models;
}
