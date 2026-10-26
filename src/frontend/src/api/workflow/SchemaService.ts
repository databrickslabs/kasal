import { apiClient } from '../../shared/api/client';
import { Schema, SchemaCreate, SchemaListResponse } from '../../types/workflow/schema';

interface CacheEntry<T> {
  data: T;
  timestamp: number;
  expiry: number;
}

export class SchemaService {
  private static instance: SchemaService;
  private readonly CACHE_TTL = 5 * 60 * 1000; // 5 minutes in milliseconds
  
  // Cache for schemas
  private schemasCache: CacheEntry<Schema[]> | null = null;

  private constructor() {
    // Initialize service
    this.clearCaches();
  }

  public static getInstance(): SchemaService {
    if (!SchemaService.instance) {
      SchemaService.instance = new SchemaService();
    }
    return SchemaService.instance;
  }

  /**
   * Check if cache is valid
   */
  private isCacheValid<T>(cache: CacheEntry<T> | null): boolean {
    if (!cache) return false;
    return Date.now() < cache.expiry;
  }

  /**
   * Set cache with expiry time
   */
  private setCache<T>(cache: CacheEntry<T> | null, data: T): CacheEntry<T> {
    const now = Date.now();
    return {
      data,
      timestamp: now,
      expiry: now + this.CACHE_TTL
    };
  }

  /**
   * Clear all caches
   */
  public clearCaches(): void {
    this.schemasCache = null;
  }

  /**
   * Get all schemas
   */
  public async getSchemas(): Promise<Schema[]> {
    // Check cache first
    if (this.isCacheValid(this.schemasCache) && this.schemasCache) {
      return this.schemasCache.data;
    }

    try {
      const response = await apiClient.get<SchemaListResponse>('/schemas');
      
      if (response.data && response.data.schemas) {
        let schemas = response.data.schemas;
        
        // Process schemas to ensure schema_definition is properly handled
        schemas = schemas.map(schema => {
          try {
            if (schema.schema_definition && typeof schema.schema_definition === 'string') {
              return {
                ...schema,
                schema_definition: JSON.parse(schema.schema_definition as unknown as string)
              };
            }
          } catch (error) {
            console.error(`Error parsing schema_definition for ${schema.name}:`, error);
          }
          return schema;
        });
        
        // Update cache
        this.schemasCache = this.setCache(this.schemasCache, schemas);
        
        return schemas;
      } else {
        console.error('API response did not contain valid schemas data', response.data);
        return [];
      }
    } catch (error) {
      console.error('Error fetching schemas:', error);
      return [];
    }
  }


  /**
   * Create a new schema
   */
  public async createSchema(schema: SchemaCreate): Promise<Schema | null> {
    try {
      
      // Ensure schema uses schema_definition field
      const schemaToSend = { ...schema };
      
      const response = await apiClient.post<Schema>('/schemas', schemaToSend);
      
      if (response.data) {
        
        // Clear caches to ensure fresh data on next fetch
        this.clearCaches();
        
        return response.data;
      } else {
        console.error('API response did not contain valid schema data', response.data);
        return null;
      }
    } catch (error: unknown) {
      // Improved error logging to capture validation errors
      console.error(`Error creating schema ${schema.name}:`, error);
      
      if (error && typeof error === 'object' && 'response' in error) {
        const errorResponse = error as { response?: { data?: unknown; status?: number } };
        console.error('Error response data:', errorResponse.response?.data);
        console.error('Error response status:', errorResponse.response?.status);
        
        // For 422 errors, extract and log the validation error details
        if (errorResponse.response?.status === 422 && errorResponse.response?.data) {
          const responseData = errorResponse.response.data as { detail?: unknown };
          console.error('Validation errors:', responseData.detail);
          
          // Re-throw with more specific message to be handled by the component
          throw new Error(`Validation error: ${JSON.stringify(responseData.detail)}`);
        }
      }
      
      // Re-throw the error to be handled by the component
      throw error;
    }
  }

  /**
   * Update an existing schema
   */
  public async updateSchema(schemaName: string, schema: SchemaCreate): Promise<Schema | null> {
    try {
      
      // Ensure schema uses schema_definition field
      const schemaToSend = { ...schema };
      
      const response = await apiClient.put<Schema>(`/schemas/${schemaName}`, schemaToSend);
      
      if (response.data) {
        
        // Clear caches to ensure fresh data on next fetch
        this.clearCaches();
        
        return response.data;
      } else {
        console.error('API response did not contain valid schema data', response.data);
        return null;
      }
    } catch (error) {
      console.error(`Error updating schema ${schemaName}:`, error);
      return null;
    }
  }

  /**
   * Delete a schema
   */
  public async deleteSchema(schemaName: string): Promise<boolean> {
    try {
      await apiClient.delete(`/schemas/${schemaName}`);
      
      
      // Clear caches to ensure fresh data on next fetch
      this.clearCaches();
      
      return true;
    } catch (error) {
      console.error(`Error deleting schema ${schemaName}:`, error);
      return false;
    }
  }
}
