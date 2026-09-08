export interface LakebaseConfig {
  installation_managed?: boolean;
  database_name?: string;
  enabled: boolean;
  instance_name: string;
  capacity: string;
  retention_days: number;
  node_count: number;
  instance_status?: 'NOT_CREATED' | 'CREATING' | 'READY' | 'STOPPED' | 'ERROR' | 'NOT_FOUND';
  endpoint?: string;
  created_at?: string;
  migration_status?: 'pending' | 'in_progress' | 'completed' | 'failed';
  migration_completed?: boolean;
  migration_result?: {
    total_tables: number;
    total_rows: number;
    migrated_tables?: Array<{table: string; rows: number}>;
  };
  migration_error?: string;
}

