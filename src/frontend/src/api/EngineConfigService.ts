import { apiClient } from '../config/api/ApiConfig';

export interface EngineConfig {
  id: number;
  engine_name: string;
  engine_type: string;
  config_key: string;
  config_value: string;
  enabled: boolean;
  description?: string;
  created_at: string;
  updated_at: string;
}

export interface EngineConfigListResponse {
  configs: EngineConfig[];
  count: number;
}

export interface CrewAIFlowConfigUpdate {
  flow_enabled: boolean;
}

export interface CrewAIFlowStatusResponse {
  flow_enabled: boolean;
}

export interface CrewAIDebugTracingStatusResponse {
  debug_tracing: boolean;
}



export class EngineConfigService {
  private static baseUrl = `/engine-config`;

  /**
   * Get all engine configurations
   */
  static async getEngineConfigs(): Promise<EngineConfigListResponse> {
    const response = await apiClient.get<EngineConfigListResponse>(`${this.baseUrl}`);
    return response.data;
  }

  /**
   * Get enabled engine configurations
   */
  static async getEnabledEngineConfigs(): Promise<EngineConfigListResponse> {
    const response = await apiClient.get<EngineConfigListResponse>(`${this.baseUrl}/enabled`);
    return response.data;
  }

  /**
   * Get engine configuration by engine name
   */
  static async getEngineConfig(engineName: string): Promise<EngineConfig> {
    const response = await apiClient.get<EngineConfig>(`${this.baseUrl}/engine/${engineName}`);
    return response.data;
  }

  /**
   * Get engine configuration by engine name and config key
   */
  static async getEngineConfigByKey(engineName: string, configKey: string): Promise<EngineConfig> {
    const response = await apiClient.get<EngineConfig>(`${this.baseUrl}/engine/${engineName}/config/${configKey}`);
    return response.data;
  }

  /**
   * Get CrewAI flow enabled status
   */
  static async getCrewAIFlowEnabled(): Promise<CrewAIFlowStatusResponse> {
    const response = await apiClient.get<CrewAIFlowStatusResponse>(`${this.baseUrl}/crewai/flow-enabled`);
    return response.data;
  }

  /**
   * Set CrewAI flow enabled status
   */
  static async setCrewAIFlowEnabled(enabled: boolean): Promise<{ success: boolean; flow_enabled: boolean }> {
    const response = await apiClient.patch<{ success: boolean; flow_enabled: boolean }>(
      `${this.baseUrl}/crewai/flow-enabled`,
      { flow_enabled: enabled }
    );
    return response.data;
  }

  /**
   * Get CrewAI debug tracing status
   */
  static async getCrewAIDebugTracing(): Promise<CrewAIDebugTracingStatusResponse> {
    const response = await apiClient.get<CrewAIDebugTracingStatusResponse>(`${this.baseUrl}/crewai/debug-tracing`);
    return response.data;
  }

  /**
   * Set CrewAI debug tracing status
   */
  static async setCrewAIDebugTracing(enabled: boolean): Promise<{ success: boolean; debug_tracing: boolean }> {
    const response = await apiClient.patch<{ success: boolean; debug_tracing: boolean }>(
      `${this.baseUrl}/crewai/debug-tracing`,
      { debug_tracing: enabled }
    );
    return response.data;
  }

  /**
   * Toggle engine configuration enabled status
   */
  static async toggleEngineEnabled(engineName: string, enabled: boolean): Promise<EngineConfig> {
    const response = await apiClient.patch<EngineConfig>(
      `${this.baseUrl}/engine/${engineName}/toggle`,
      { enabled }
    );
    return response.data;
  }

  /**
   * Update engine configuration value
   */
  static async updateConfigValue(engineName: string, configKey: string, configValue: string): Promise<EngineConfig> {
    const response = await apiClient.patch<EngineConfig>(
      `${this.baseUrl}/engine/${engineName}/config/${configKey}/value`,
      { config_value: configValue }
    );
    return response.data;
  }
}