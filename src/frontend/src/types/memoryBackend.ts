/**
 * Memory backend configuration types for AI agent memory storage.
 */

export enum MemoryBackendType {
  DEFAULT = 'default', // CrewAI's default (ChromaDB + SQLite)
  DATABRICKS = 'databricks', // Databricks Vector Search
  // Future backends can be added here
  // PINECONE = 'pinecone',
  // QDRANT = 'qdrant',
  // WEAVIATE = 'weaviate',
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

export interface MemoryBackendConfig {
  backend_type: MemoryBackendType;
  
  // Backend-specific configuration
  databricks_config?: DatabricksMemoryConfig;
  
  // Common configuration
  enable_short_term?: boolean;
  enable_long_term?: boolean;
  enable_entity?: boolean;
  
  // Advanced options
  custom_config?: Record<string, unknown>;
}

// Default configurations for easy setup
export const DEFAULT_MEMORY_BACKEND_CONFIG: MemoryBackendConfig = {
  backend_type: MemoryBackendType.DEFAULT,
  enable_short_term: true,
  enable_long_term: true,
  enable_entity: true,
};

export const DEFAULT_DATABRICKS_CONFIG: DatabricksMemoryConfig = {
  endpoint_name: '',
  short_term_index: '',
  embedding_dimension: 768,
  auth_type: 'default',
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
    [MemoryBackendType.DEFAULT]: 'Default (ChromaDB + SQLite)',
    [MemoryBackendType.DATABRICKS]: 'Databricks Vector Search',
  };
  return displayNames[type] || type;
};

// Helper to get backend description
export const getBackendDescription = (type: MemoryBackendType): string => {
  const descriptions: Record<MemoryBackendType, string> = {
    [MemoryBackendType.DEFAULT]: 'Uses CrewAI\'s built-in memory storage with ChromaDB for vector storage and SQLite for long-term memory.',
    [MemoryBackendType.DATABRICKS]: 'Uses Databricks Vector Search for scalable, enterprise-grade memory storage with Unity Catalog governance.',
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