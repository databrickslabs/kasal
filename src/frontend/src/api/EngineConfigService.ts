import { config } from '../config/api';

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

export class EngineConfigService {
  private static baseUrl = `${config.apiUrl}/engine-config`;

  /**
   * Get all engine configurations
   */
  static async getEngineConfigs(): Promise<EngineConfigListResponse> {
    const response = await fetch(`${this.baseUrl}`, {
      method: 'GET',
      headers: {
        'Content-Type': 'application/json',
      },
    });

    if (!response.ok) {
      throw new Error(`Failed to fetch engine configurations: ${response.statusText}`);
    }

    return response.json();
  }

  /**
   * Get enabled engine configurations
   */
  static async getEnabledEngineConfigs(): Promise<EngineConfigListResponse> {
    const response = await fetch(`${this.baseUrl}/enabled`, {
      method: 'GET',
      headers: {
        'Content-Type': 'application/json',
      },
    });

    if (!response.ok) {
      throw new Error(`Failed to fetch enabled engine configurations: ${response.statusText}`);
    }

    return response.json();
  }

  /**
   * Get engine configuration by engine name
   */
  static async getEngineConfig(engineName: string): Promise<EngineConfig> {
    const response = await fetch(`${this.baseUrl}/engine/${engineName}`, {
      method: 'GET',
      headers: {
        'Content-Type': 'application/json',
      },
    });

    if (!response.ok) {
      throw new Error(`Failed to fetch engine configuration: ${response.statusText}`);
    }

    return response.json();
  }

  /**
   * Get engine configuration by engine name and config key
   */
  static async getEngineConfigByKey(engineName: string, configKey: string): Promise<EngineConfig> {
    const response = await fetch(`${this.baseUrl}/engine/${engineName}/config/${configKey}`, {
      method: 'GET',
      headers: {
        'Content-Type': 'application/json',
      },
    });

    if (!response.ok) {
      throw new Error(`Failed to fetch engine configuration: ${response.statusText}`);
    }

    return response.json();
  }

  /**
   * Get CrewAI flow enabled status
   */
  static async getCrewAIFlowEnabled(): Promise<CrewAIFlowStatusResponse> {
    const response = await fetch(`${this.baseUrl}/crewai/flow-enabled`, {
      method: 'GET',
      headers: {
        'Content-Type': 'application/json',
      },
    });

    if (!response.ok) {
      throw new Error(`Failed to fetch CrewAI flow status: ${response.statusText}`);
    }

    return response.json();
  }

  /**
   * Set CrewAI flow enabled status
   */
  static async setCrewAIFlowEnabled(enabled: boolean): Promise<{ success: boolean; flow_enabled: boolean }> {
    const response = await fetch(`${this.baseUrl}/crewai/flow-enabled`, {
      method: 'PATCH',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({ flow_enabled: enabled }),
    });

    if (!response.ok) {
      throw new Error(`Failed to update CrewAI flow status: ${response.statusText}`);
    }

    return response.json();
  }

  /**
   * Toggle engine configuration enabled status
   */
  static async toggleEngineEnabled(engineName: string, enabled: boolean): Promise<EngineConfig> {
    const response = await fetch(`${this.baseUrl}/engine/${engineName}/toggle`, {
      method: 'PATCH',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({ enabled }),
    });

    if (!response.ok) {
      throw new Error(`Failed to toggle engine configuration: ${response.statusText}`);
    }

    return response.json();
  }

  /**
   * Update engine configuration value
   */
  static async updateConfigValue(engineName: string, configKey: string, configValue: string): Promise<EngineConfig> {
    const response = await fetch(`${this.baseUrl}/engine/${engineName}/config/${configKey}/value`, {
      method: 'PATCH',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({ config_value: configValue }),
    });

    if (!response.ok) {
      throw new Error(`Failed to update config value: ${response.statusText}`);
    }

    return response.json();
  }
} 