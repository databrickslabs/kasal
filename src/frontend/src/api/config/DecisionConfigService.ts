import { apiClient } from '../../shared/api/client';

export interface DecisionConfig {
  enabled: boolean;
  api_key_configured: boolean;
}

export class DecisionConfigService {
  static async recommend(prompt: string): Promise<{ model: string | null; effort: string | null }> {
    return (await apiClient.post('/decision-config/recommend', { prompt })).data;
  }
  static async getConfig(): Promise<DecisionConfig> {
    return (await apiClient.get<DecisionConfig>('/decision-config')).data;
  }

  static async saveConfig(enabled: boolean): Promise<DecisionConfig> {
    return (await apiClient.put<DecisionConfig>('/decision-config', {
      enabled,
    })).data;
  }
}
