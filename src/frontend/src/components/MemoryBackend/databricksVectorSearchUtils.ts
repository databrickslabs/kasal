/**
 * Utility functions for Databricks Vector Search operations
 */

/**
 * Builds a URL for a Databricks Vector Search endpoint
 */
export const buildVectorSearchEndpointUrl = (workspaceUrl: string, endpointName: string): string => {
  // Extract workspace ID from URL if present
  const match = workspaceUrl.match(/\?o=(\d+)/);
  const workspaceId = match ? match[1] : '';
  const baseUrl = workspaceUrl.replace(/\?o=\d+/, ''); // Remove workspace ID from base URL
  const cleanUrl = baseUrl.endsWith('/') ? baseUrl.slice(0, -1) : baseUrl;
  
  return `${cleanUrl}/compute/vector-search/${endpointName}${workspaceId ? `?o=${workspaceId}` : ''}`;
};

/**
 * Builds a URL for a Databricks Vector Search index
 */
export const buildVectorSearchIndexUrl = (workspaceUrl: string, indexName: string): string => {
  // Extract workspace ID from URL if present
  const match = workspaceUrl.match(/\?o=(\d+)/);
  const workspaceId = match ? match[1] : '';
  const baseUrl = workspaceUrl.replace(/\?o=\d+/, ''); // Remove workspace ID from base URL
  const cleanUrl = baseUrl.endsWith('/') ? baseUrl.slice(0, -1) : baseUrl;
  
  // Parse the index name (catalog.schema.table)
  const parts = indexName.split('.');
  if (parts.length >= 3) {
    const catalog = parts[0];
    const schema = parts[1];
    const table = parts.slice(2).join('.'); // Handle cases where table name might contain dots
    return `${cleanUrl}/explore/data/${catalog}/${schema}/${table}${workspaceId ? `?o=${workspaceId}` : ''}`;
  }
  
  // Fallback if index name format is unexpected
  return `${cleanUrl}/explore/data${workspaceId ? `?o=${workspaceId}` : ''}`;
};

/**
 * Validates if a Vector Search index name follows the correct format: catalog.schema.indexname
 */
export const validateVectorSearchIndexName = (indexName: string): boolean => {
  // Check if index name follows the pattern: catalog.schema.indexname
  const parts = indexName.split('.');
  return parts.length >= 3 && parts.every(part => part.length > 0);
};

/**
 * Checks if an endpoint has active indexes
 */
export const hasActiveVectorSearchIndexes = (
  savedConfig: { indexes?: Record<string, { name?: string } | undefined> } | null,
  endpointType: 'memory' | 'document'
): boolean => {
  if (!savedConfig?.indexes) return false;
  
  if (endpointType === 'memory') {
    return !!(savedConfig.indexes.short_term || savedConfig.indexes.long_term || savedConfig.indexes.entity);
  } else {
    return !!savedConfig.indexes.document;
  }
};

/**
 * Renders the setup status for Vector Search resources
 */
export const getVectorSearchSetupStatus = (status?: string): 'success' | 'error' => {
  if (status === 'created' || status === 'already_exists') {
    return 'success';
  }
  return 'error';
};