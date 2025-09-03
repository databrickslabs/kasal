import { apiClient } from '../config/api/ApiConfig';

export interface DatabricksEnvironmentInfo {
  is_databricks_apps: boolean;
  databricks_app_name?: string;
  databricks_host?: string;
  workspace_id?: string;
  has_oauth_credentials: boolean;
  message: string;
}

class DatabricksEnvironmentService {
  private static cachedEnvironment: DatabricksEnvironmentInfo | null = null;

  /**
   * Get information about the Databricks environment
   * Results are cached for the session
   */
  static async getEnvironmentInfo(): Promise<DatabricksEnvironmentInfo> {
    // Return cached value if available
    if (this.cachedEnvironment) {
      return this.cachedEnvironment;
    }

    try {
      const response = await apiClient.get<DatabricksEnvironmentInfo>('/databricks/environment');
      this.cachedEnvironment = response.data;
      return response.data;
    } catch (error) {
      console.error('Error fetching Databricks environment info:', error);
      // Return default values on error
      return {
        is_databricks_apps: false,
        has_oauth_credentials: false,
        message: 'Unable to determine environment'
      };
    }
  }

  /**
   * Check if running in Databricks Apps environment
   */
  static async isDatabricksApps(): Promise<boolean> {
    const env = await this.getEnvironmentInfo();
    return env.is_databricks_apps;
  }

  /**
   * Clear cached environment info (useful for testing)
   */
  static clearCache(): void {
    this.cachedEnvironment = null;
  }
}

export default DatabricksEnvironmentService;