import { apiClient } from '../config/api/ApiConfig';

export interface DatabasePermissionResponse {
  has_permission: boolean;
  is_databricks_apps: boolean;
  databricks_app_name?: string;
  user_email: string;
  reason: string;
  error?: string;
}

export interface DatabaseInfoResponse {
  success: boolean;
  database_type: string;
  tables: Record<string, number>;
  total_tables: number;
  memory_backends: string[];
  size_mb?: number;
  created_at?: string;
  modified_at?: string;
  database_path?: string;
}

export interface ExportRequest {
  catalog: string;
  schema: string;
  volume_name: string;
  export_format: 'sql' | 'sqlite';
}

export interface ExportResponse {
  success: boolean;
  exported_to: string;
  backup_filename: string;
  volume_path: string;
  databricks_url: string;
  timestamp: string;
  database_type: string;
  size_mb?: number;
  export_format?: string;
  error?: string;
}

export interface ImportRequest {
  catalog: string;
  schema: string;
  volume_name: string;
  backup_filename: string;
}

export interface ImportResponse {
  success: boolean;
  imported_from: string;
  backup_filename: string;
  volume_path: string;
  timestamp: string;
  database_type: string;
  size_mb?: number;
  restored_tables?: string[];
  error?: string;
}

export interface ListBackupsRequest {
  catalog: string;
  schema: string;
  volume_name: string;
}

export interface BackupFile {
  name: string;
  path: string;
  size?: number;
  modified?: string;
  databricks_url: string;
}

export interface ListBackupsResponse {
  success: boolean;
  backups: BackupFile[];
  volume_path: string;
  error?: string;
}

class DatabaseManagementService {
  /**
   * Check if the current user has permission to access Database Management
   */
  static async checkPermission(): Promise<DatabasePermissionResponse> {
    const response = await apiClient.get<DatabasePermissionResponse>(
      '/database-management/check-permission'
    );
    return response.data;
  }

  /**
   * Get database information
   */
  static async getDatabaseInfo(): Promise<DatabaseInfoResponse> {
    const response = await apiClient.get<DatabaseInfoResponse>(
      '/database-management/info'
    );
    return response.data;
  }

  /**
   * Export database to Databricks volume
   */
  static async exportDatabase(request: ExportRequest): Promise<ExportResponse> {
    const response = await apiClient.post<ExportResponse>(
      '/database-management/export',
      request
    );
    return response.data;
  }

  /**
   * Import database from Databricks volume
   */
  static async importDatabase(request: ImportRequest): Promise<ImportResponse> {
    const response = await apiClient.post<ImportResponse>(
      '/database-management/import',
      request
    );
    return response.data;
  }

  /**
   * List available backups in Databricks volume
   */
  static async listBackups(request: ListBackupsRequest): Promise<ListBackupsResponse> {
    const response = await apiClient.post<ListBackupsResponse>(
      '/database-management/list-backups',
      request
    );
    return response.data;
  }
}

export default DatabaseManagementService;