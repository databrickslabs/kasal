import { getClient } from './client';
import { ModelConfigResponse } from '../types/dispatcher';

interface ModelListResponse {
  models: ModelConfigResponse[];
  count: number;
}

export async function fetchEnabledModels(): Promise<ModelConfigResponse[]> {
  const response = await getClient().get<ModelListResponse>('/models/enabled');
  return response.data.models;
}
