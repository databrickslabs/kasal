import { apiClient } from '../../shared/api/client';

export interface DecisionConfig {
  enabled: boolean;
  api_key_configured: boolean;
  /** URL set, workspace opted in and keyed: the chat selector offers Auto. */
  available?: boolean;
  /** The deployment's connection, and the workspace key it needs. */
  connection?: 'jev' | 'openrouter';
  api_key_name?: string;
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
