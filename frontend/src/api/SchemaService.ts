import axios, { AxiosError } from 'axios';
import { apiClient } from '../config/api/ApiConfig';
import { Schema, SchemaCreate, SchemaListResponse } from '../types/schema';

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
  private schemasByTypeCache: Record<string, CacheEntry<Schema[]>> = {};

  private constructor() {
    // Initialize service
    this.clearCaches();
    console.log('SchemaService initialized');
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
    console.log('Clearing schema caches');
    this.schemasCache = null;
    this.schemasByTypeCache = {};
  }

  /**
   * Get all schemas
   */
  public async getSchemas(): Promise<Schema[]> {
    // Check cache first
    if (this.isCacheValid(this.schemasCache) && this.schemasCache) {
      console.log('Using cached schemas');
      return this.schemasCache.data;
    }

    try {
      console.log('Fetching schemas from API...');
      const response = await apiClient.get<SchemaListResponse>('/schemas');
      
      if (response.data && response.data.schemas) {
        let schemas = response.data.schemas;
        console.log(`Received ${schemas.length} schemas from API`);
        
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
   * Get schemas by type
   */
  public async getSchemasByType(schemaType: string): Promise<Schema[]> {
    // Check cache first
    const typeCache = this.schemasByTypeCache[schemaType];
    if (this.isCacheValid(typeCache)) {
      console.log(`Using cached schemas for type ${schemaType}`);
      return typeCache.data;
    }

    try {
      console.log(`Fetching schemas of type ${schemaType} from API...`);
      const response = await apiClient.get<SchemaListResponse>(`/schemas/by-type/${schemaType}`);
      
      if (response.data && response.data.schemas) {
        const schemas = response.data.schemas;
        console.log(`Received ${schemas.length} schemas of type ${schemaType} from API`);
        
        // Update cache
        this.schemasByTypeCache[schemaType] = this.setCache(typeCache, schemas);
        
        return schemas;
      } else {
        console.error('API response did not contain valid schemas data', response.data);
        return [];
      }
    } catch (error) {
      console.error(`Error fetching schemas of type ${schemaType}:`, error);
      return [];
    }
  }

  /**
   * Get a specific schema by name
   */
  public async getSchema(schemaName: string): Promise<Schema | null> {
    try {
      console.log(`Fetching schema ${schemaName} from API...`);
      const response = await apiClient.get<Schema>(`/schemas/${schemaName}`);
      
      if (response.data) {
        console.log(`Received schema ${schemaName} from API`);
        let schema = response.data;
        
        // Process schema to ensure schema_definition is properly handled
        try {
          if (schema.schema_definition && typeof schema.schema_definition === 'string') {
            schema = {
              ...schema,
              schema_definition: JSON.parse(schema.schema_definition as unknown as string)
            };
          }
        } catch (error) {
          console.error(`Error parsing schema_definition for ${schema.name}:`, error);
        }
        
        return schema;
      } else {
        console.error('API response did not contain valid schema data', response.data);
        return null;
      }
    } catch (error) {
      if (axios.isAxiosError(error)) {
        const axiosError = error as AxiosError;
        if (axiosError.response?.status === 404) {
          console.warn(`Schema ${schemaName} not found`);
          return null;
        }
      }
      console.error(`Error fetching schema ${schemaName}:`, error);
      return null;
    }
  }

  /**
   * Create a new schema
   */
  public async createSchema(schema: SchemaCreate): Promise<Schema | null> {
    try {
      console.log(`Creating schema ${schema.name}...`);
      
      // Ensure schema uses schema_definition field
      const schemaToSend = { ...schema };
      
      const response = await apiClient.post<Schema>('/schemas', schemaToSend);
      
      if (response.data) {
        console.log(`Schema ${schema.name} created successfully`);
        
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
      console.log(`Updating schema ${schemaName}...`);
      
      // Ensure schema uses schema_definition field
      const schemaToSend = { ...schema };
      
      const response = await apiClient.put<Schema>(`/schemas/${schemaName}`, schemaToSend);
      
      if (response.data) {
        console.log(`Schema ${schemaName} updated successfully`);
        
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
      console.log(`Deleting schema ${schemaName}...`);
      await apiClient.delete(`/schemas/${schemaName}`);
      
      console.log(`Schema ${schemaName} deleted successfully`);
      
      // Clear caches to ensure fresh data on next fetch
      this.clearCaches();
      
      return true;
    } catch (error) {
      console.error(`Error deleting schema ${schemaName}:`, error);
      return false;
    }
  }
} 