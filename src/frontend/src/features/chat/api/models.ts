import { getClient } from './client';
import { ModelConfigResponse } from '../types/dispatcher';
import { setServerDefaultModel } from '../../../config/defaultModel';

interface ModelListResponse {
  models: ModelConfigResponse[];
  count: number;
  default_model?: string;
}

export async function fetchEnabledModels(): Promise<ModelConfigResponse[]> {
  const response = await getClient().get<ModelListResponse>('/models/enabled');
  setServerDefaultModel(response.data.default_model);
  return response.data.models;
}
