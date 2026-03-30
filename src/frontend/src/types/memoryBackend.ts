/**
 * Memory backend configuration types for AI agent memory storage.
 */

export enum MemoryBackendType {
  DEFAULT = 'default', // CrewAI's default (ChromaDB + SQLite)
  DATABRICKS = 'databricks', // Databricks Vector Search
  LAKEBASE = 'lakebase', // Lakebase pgvector
}

export interface DatabricksMemoryConfig {
  // Memory endpoint configuration (Direct Access for dynamic data)
  endpoint_name: string;
  
  // Document endpoint configuration (Storage Optimized for static data)
  document_endpoint_name?: string;
  
  // Index names for different memory types
  short_term_index: string;
  long_term_index?: string;
  entity_index?: string;
  
  // Document embeddings index (for storage optimized endpoint)
  document_index?: string;
  
  // Database configuration
  catalog?: string;
  schema?: string;
  
  // Authentication (optional - can use environment variables)
  workspace_url?: string;
  auth_type?: 'default' | 'pat' | 'service_principal';
  
  // For PAT authentication
  personal_access_token?: string;
  
  // For Service Principal authentication
  service_principal_client_id?: string;
  service_principal_client_secret?: string;
  
  // Vector configuration
  embedding_dimension?: number;
}

export interface LakebaseMemoryConfig {
  instance_name?: string;
  embedding_dimension?: number;
  short_term_table?: string;
  long_term_table?: string;
  entity_table?: string;
  tables_initialized?: boolean;
}

export interface MemoryBackendConfig {
  backend_type: MemoryBackendType;

  // Backend-specific configuration
  databricks_config?: DatabricksMemoryConfig;
  lakebase_config?: LakebaseMemoryConfig;

  // Common configuration
  enable_short_term?: boolean;
  enable_long_term?: boolean;
  enable_entity?: boolean;

  // Advanced configuration
  enable_relationship_retrieval?: boolean;

  // Database persistence fields
  is_default?: boolean;
  is_active?: boolean;

  // Advanced options
  custom_config?: Record<string, unknown>;
}

// Default configurations for easy setup
export const DEFAULT_MEMORY_BACKEND_CONFIG: MemoryBackendConfig = {
  backend_type: MemoryBackendType.DEFAULT,
  enable_short_term: true,
  enable_long_term: true,
  enable_entity: true,
  enable_relationship_retrieval: false,
};

export const DEFAULT_DATABRICKS_CONFIG: DatabricksMemoryConfig = {
  endpoint_name: '',
  short_term_index: '',
  embedding_dimension: 1024,
  auth_type: 'default',
};

export const DEFAULT_LAKEBASE_CONFIG: LakebaseMemoryConfig = {
  embedding_dimension: 1024,
  short_term_table: 'crew_short_term_memory',
  long_term_table: 'crew_long_term_memory',
  entity_table: 'crew_entity_memory',
  tables_initialized: false,
};

// Validation helpers
export const isValidMemoryBackendConfig = (config: unknown): config is MemoryBackendConfig => {
  if (!config || typeof config !== 'object' || config === null) return false;
  
  const configObj = config as Record<string, unknown>;
  
  if (!Object.values(MemoryBackendType).includes(configObj.backend_type as MemoryBackendType)) return false;
  
  if (configObj.backend_type === MemoryBackendType.DATABRICKS) {
    const databricksConfig = configObj.databricks_config as DatabricksMemoryConfig | undefined;
    if (!databricksConfig) return false;
    if (!databricksConfig.endpoint_name || !databricksConfig.short_term_index) {
      return false;
    }
  }
  
  return true;
};

// Helper to get display name for backend type
export const getBackendDisplayName = (type: MemoryBackendType): string => {
  const displayNames: Record<MemoryBackendType, string> = {
    [MemoryBackendType.DEFAULT]: 'Local (ChromaDB + SQLite)',
    [MemoryBackendType.DATABRICKS]: 'Databricks Vector Search',
    [MemoryBackendType.LAKEBASE]: 'Lakebase (pgvector)',
  };
  return displayNames[type] || type;
};

// Helper to get backend description
export const getBackendDescription = (type: MemoryBackendType): string => {
  const descriptions: Record<MemoryBackendType, string> = {
    [MemoryBackendType.DEFAULT]: 'Uses local ChromaDB for vector search and SQLite for long-term memory. No external infrastructure required.',
    [MemoryBackendType.DATABRICKS]: 'Uses Databricks Vector Search for scalable, enterprise-grade memory storage with Unity Catalog governance.',
    [MemoryBackendType.LAKEBASE]: 'Uses your configured Lakebase PostgreSQL instance with pgvector for memory storage. Zero additional infrastructure required.',
  };
  return descriptions[type] || '';
};

// Additional types for Databricks setup UI components
export interface EndpointInfo {
  name: string;
  type?: string;
  status?: string;
  error?: string;
  state?: string;
  ready?: boolean;
  can_delete_indexes?: boolean;
}

export interface IndexInfo {
  name: string;
  status?: string;
  index_type?: string;
}

export interface SavedConfigInfo {
  backend_id?: string;
  workspace_url?: string;
  catalog?: string;
  schema?: string;
  endpoints?: {
    memory?: EndpointInfo;
    document?: EndpointInfo;
  };
  indexes?: {
    short_term?: IndexInfo;
    long_term?: IndexInfo;
    entity?: IndexInfo;
    document?: IndexInfo;
  };
}

export interface SetupResult {
  success: boolean;
  message: string;
  endpoints?: {
    memory?: EndpointInfo;
    document?: EndpointInfo;
  };
  indexes?: {
    short_term?: IndexInfo;
    long_term?: IndexInfo;
    entity?: IndexInfo;
    document?: IndexInfo;
  };
  config?: {
    endpoint_name?: string;
    document_endpoint_name?: string;
    short_term_index?: string;
    long_term_index?: string;
    entity_index?: string;
    document_index?: string;
    workspace_url?: string;
    embedding_dimension?: number;
    catalog?: string;
    schema?: string;
  };
  catalog?: string;
  schema?: string;
  backend_id?: string;
  error?: string;
  warning?: string;
  info?: string;
}

export interface IndexInfoState {
  doc_count: number;
  loading: boolean;
  error?: string;
  status?: string;
  ready?: boolean;
  index_type?: string;
}

export interface ManualConfig {
  workspace_url: string;
  endpoint_name: string;
  document_endpoint_name: string;
  short_term_index: string;
  long_term_index: string;
  entity_index: string;
  document_index: string;
  embedding_model: string;
}