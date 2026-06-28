import { apiClient } from '../config/api/ApiConfig';

/**
 * Per-workspace "Predefined UI" configuration. When enabled, crews produce
 * structured, design-system UI (rendered consistently in the chat preview)
 * instead of arbitrary HTML. The structured format conforms to the A2UI
 * protocol; naming here is intentionally UI-centric.
 */
export interface UIConfig {
  enabled: boolean;
  catalog_type: 'minimal' | 'full' | 'custom';
  catalog_json?: string | null;
  style_json?: string | null;
  id?: number | null;
  group_id?: string | null;
  created_by_email?: string | null;
  created_at?: string | null;
  updated_at?: string | null;
}

export type UIConfigUpdate = Pick<
  UIConfig,
  'enabled' | 'catalog_type' | 'catalog_json' | 'style_json'
>;

export class UIConfigService {
  private static baseUrl = '/ui-config';

  /** Get the current workspace's Predefined UI configuration. */
  static async getConfig(): Promise<UIConfig> {
    const response = await apiClient.get<UIConfig>(this.baseUrl);
    return response.data;
  }

  /** Update the current workspace's Predefined UI configuration (admins only). */
  static async updateConfig(config: UIConfigUpdate): Promise<UIConfig> {
    const response = await apiClient.put<UIConfig>(this.baseUrl, config);
    return response.data;
  }
}
