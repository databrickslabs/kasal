import { Models, ModelConfig } from '../types/models';
import { apiClient } from '../config/api/ApiConfig';
import axios, { AxiosError, AxiosResponse } from 'axios';
import { models as defaultModels } from '../config/models/models';

interface ApiModelResponse {
  id: number;
  key: string;
  name: string;
  provider?: string;
  temperature?: number;
  context_window?: number;
  max_output_tokens?: number;
  extended_thinking?: boolean;
  enabled?: boolean;
  created_at: string;
  updated_at: string;
}

// Response data structure from the backend API
interface ApiModelListResponse {
  models: ApiModelResponse[];
  count: number;
}

interface CacheEntry<T> {
  data: T;
  timestamp: number;
  expiry: number;
}

export class ModelService {
  private static instance: ModelService;
  private readonly CACHE_TTL = 30 * 60 * 1000; // 30 minutes in milliseconds
  
  // Cache for models
  private modelsCache: CacheEntry<Models> | null = null;
  private enabledModelsCache: CacheEntry<Models> | null = null;
  private hasInitialized = false;

  // Default fallback model for complete API failures - EMPTY TO PREVENT FALLBACKS
  private readonly DEFAULT_FALLBACK_MODEL: Models = {};

  // Private constructor for singleton pattern
  private constructor() {
    // Force cache clear on initialization
    this.clearCaches();
  }

  public static getInstance(): ModelService {
    if (!ModelService.instance) {
      ModelService.instance = new ModelService();
    }
    return ModelService.instance;
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
    this.modelsCache = null;
    this.enabledModelsCache = null;
  }

  /**
   * Initialize the database with default models if it's empty
   * This ensures we always have models to work with
   */
  private async initializeModelsIfNeeded(): Promise<void> {
    if (this.hasInitialized) {
      return;
    }

    try {
      // Check if any models exist in the database
      const response = await apiClient.get<ApiModelListResponse>('/models');
      const modelsData = this.extractModelsFromResponse(response);
      
      if (modelsData.length === 0) {
        // Create default models in the database
        const createPromises = Object.entries(defaultModels).map(([key, model]) => {
          return apiClient.post<ApiModelResponse>('/models', {
            key,
            name: model.name,
            provider: model.provider,
            temperature: model.temperature,
            context_window: model.context_window,
            max_output_tokens: model.max_output_tokens,
            extended_thinking: model.extended_thinking,
            enabled: true
          });
        });
        
        await Promise.all(createPromises);
      }
    } catch (error) {
      console.error('Error initializing models:', error);
    } finally {
      this.hasInitialized = true;
    }
  }

  /**
   * Convert API model response to Models format
   */
  private convertApiResponseToModels(apiModels: ApiModelResponse[]): Models {
    if (apiModels.length === 0) {
      console.warn('No models received from API');
      return this.DEFAULT_FALLBACK_MODEL;
    }
    
    const models: Models = {};
    
    apiModels.forEach((model: ApiModelResponse, index) => {
      // Make sure the model has valid key and name
      if (!model.key || !model.name) {
        console.warn(`Skipping model at index ${index} due to missing key or name:`, model);
        return;
      }
      
      models[model.key] = {
        name: model.name,
        provider: model.provider,
        temperature: model.temperature,
        context_window: model.context_window,
        max_output_tokens: model.max_output_tokens,
        extended_thinking: model.extended_thinking,
        enabled: model.enabled !== false // Default to enabled if not specified
      };
    });
    
    // If no valid models were processed, use the fallback
    if (Object.keys(models).length === 0) {
      console.warn('No valid models processed, using fallback');
      return this.DEFAULT_FALLBACK_MODEL;
    }
    
    return models;
  }

  /**
   * Extract model data from API response, handling different possible formats
   */
  private extractModelsFromResponse(response: AxiosResponse): ApiModelResponse[] {
    if (!response || !response.data) {
      console.warn('API returned empty response or no data');
      return [];
    }
    
    try {
      // Backend ApiModelListResponse format
      if (typeof response.data === 'object' && response.data.models && Array.isArray(response.data.models)) {
        return response.data.models;
      }
      
      // Direct array of models
      if (Array.isArray(response.data)) {
        return response.data;
      }
      
      // Models wrapped in results field
      if (typeof response.data === 'object' && response.data.results && Array.isArray(response.data.results)) {
        return response.data.results;
      }
      
      // Extract models from object format (key-value pairs where values are models)
      if (typeof response.data === 'object' && !Array.isArray(response.data)) {
        const potentialModels: ApiModelResponse[] = [];
        
        Object.entries(response.data).forEach(([key, value]) => {
          // Skip metadata-like fields
          if (key === 'count' || key === 'total' || key === 'page' || key === 'limit') return;
          
          // If value is an object with required model fields
          if (typeof value === 'object' && value !== null && 'name' in value) {
            // Either it already has a key property or we'll use the key from the object
            const modelKey = ('key' in value) ? String(value.key) : key;
            
            potentialModels.push({
              id: ('id' in value) ? Number(value.id) : 0,
              key: modelKey,
              name: String(value.name),
              provider: 'provider' in value ? String(value.provider) : undefined,
              temperature: 'temperature' in value ? Number(value.temperature) : undefined,
              context_window: 'context_window' in value ? Number(value.context_window) : undefined,
              max_output_tokens: 'max_output_tokens' in value ? Number(value.max_output_tokens) : undefined,
              extended_thinking: 'extended_thinking' in value ? Boolean(value.extended_thinking) : undefined,
              enabled: 'enabled' in value ? Boolean(value.enabled) : true,
              created_at: 'created_at' in value ? String(value.created_at) : new Date().toISOString(),
              updated_at: 'updated_at' in value ? String(value.updated_at) : new Date().toISOString()
            });
          }
        });
        
        if (potentialModels.length > 0) {
          return potentialModels;
        }
      }
      
      console.warn('Response data format not recognized, could not extract models');
    } catch (error) {
      console.error('Error extracting models from response:', error);
    }
    
    // Fallback for all unhandled formats
    console.warn('Could not extract any models from the API response, using fallback model');
    return [{
      id: 0,
      key: 'gpt-4o-mini',
      name: 'gpt-4o-mini',
      provider: 'openai',
      temperature: 0.7,
      context_window: 128000,
      max_output_tokens: 4096,
      extended_thinking: false,
      enabled: true,
      created_at: new Date().toISOString(),
      updated_at: new Date().toISOString()
    }];
  }

  /**
   * Get all models with their enabled status
   */
  public async getModels(forceRefresh = false): Promise<Models> {
    // First, ensure models are initialized
    await this.initializeModelsIfNeeded();
    
    // Check cache first, but allow force refresh with forceRefresh parameter
    if (!forceRefresh && this.isCacheValid(this.modelsCache) && this.modelsCache) {
      return this.modelsCache.data;
    }

    try {
      const response = await apiClient.get<ApiModelListResponse>('/models');
      const modelsData = this.extractModelsFromResponse(response);
      
      if (modelsData.length === 0) {
        console.warn('No models data found in API response, using fallback model');
        // Try loading from default static models as a fallback
        this.modelsCache = this.setCache(this.modelsCache, defaultModels);
        return defaultModels;
      }
      
      const models = this.convertApiResponseToModels(modelsData);
      
      // Update cache
      this.modelsCache = this.setCache(this.modelsCache, models);
      
      return models;
    } catch (error: unknown) {
      console.error('Error fetching models from API:', error);
      
      // More detailed error information
      if (axios.isAxiosError(error)) {
        const axiosError = error as AxiosError;
        if (axiosError.response) {
          console.error('Error response data:', axiosError.response.data);
          console.error('Error response status:', axiosError.response.status);
        }
      }
      
      // Return fallback model when API fails
      this.modelsCache = this.setCache(this.modelsCache, this.DEFAULT_FALLBACK_MODEL);
      return this.DEFAULT_FALLBACK_MODEL;
    }
  }

  /**
   * Save models configuration
   */
  public async saveModels(models: Models): Promise<Models> {
    try {
      // Update models in bulk by making individual API calls
      const updatePromises = Object.entries(models).map(([key, model]) => {
        return apiClient.put<ApiModelResponse>(`/models/${key}`, {
          key,
          name: model.name,
          provider: model.provider,
          temperature: model.temperature,
          context_window: model.context_window,
          max_output_tokens: model.max_output_tokens,
          extended_thinking: model.extended_thinking,
          enabled: model.enabled
        }).catch(error => {
          // If model doesn't exist, create it
          if (error.response && error.response.status === 404) {
            return apiClient.post<ApiModelResponse>('/models', {
              key,
              name: model.name,
              provider: model.provider,
              temperature: model.temperature,
              context_window: model.context_window,
              max_output_tokens: model.max_output_tokens,
              extended_thinking: model.extended_thinking,
              enabled: model.enabled
            });
          }
          throw error;
        });
      });
      
      await Promise.all(updatePromises);
      
      // Clear caches to force refresh
      this.clearCaches();
      
      // Get updated models
      return this.getModels(true);
    } catch (error) {
      console.error('Error saving models to API:', error);
      throw error;
    }
  }

  /**
   * Enable or disable a specific model
   */
  public async enableModel(modelKey: string, enabled: boolean): Promise<Models> {
    try {
      // Send request according to backend format
      await apiClient.patch<ApiModelResponse>(`/models/${modelKey}/toggle`, {
        enabled: enabled
      });
      
      // Clear caches to force refresh
      this.clearCaches();
      
      // Get updated models
      return this.getModels(true);
    } catch (error) {
      console.error(`Error ${enabled ? 'enabling' : 'disabling'} model ${modelKey}:`, error);
      
      if (axios.isAxiosError(error)) {
        const axiosError = error as AxiosError;
        if (axiosError.response) {
          console.error('Error response data:', axiosError.response.data);
          console.error('Error response status:', axiosError.response.status);
        }
      }
      
      throw error;
    }
  }

  /**
   * Enable all models at once using the dedicated API endpoint
   */
  public async enableAllModels(): Promise<Models> {
    try {
      // Call the enable-all endpoint
      const response = await apiClient.post<ApiModelListResponse>('/models/enable-all');
      const modelsData = this.extractModelsFromResponse(response);
      const models = this.convertApiResponseToModels(modelsData);
      
      // Clear caches to force refresh
      this.clearCaches();
      
      // Update cache
      this.modelsCache = this.setCache(this.modelsCache, models);
      
      return models;
    } catch (error) {
      console.error('Error enabling all models:', error);
      throw error;
    }
  }

  /**
   * Disable all models at once using the dedicated API endpoint
   */
  public async disableAllModels(): Promise<Models> {
    try {
      // Call the disable-all endpoint
      const response = await apiClient.post<ApiModelListResponse>('/models/disable-all');
      const modelsData = this.extractModelsFromResponse(response);
      const models = this.convertApiResponseToModels(modelsData);
      
      // Clear caches to force refresh
      this.clearCaches();
      
      // Update cache
      this.modelsCache = this.setCache(this.modelsCache, models);
      
      return models;
    } catch (error) {
      console.error('Error disabling all models:', error);
      throw error;
    }
  }

  /**
   * Get only enabled models
   */
  public async getEnabledModels(): Promise<Models> {
    // Check cache first
    if (this.isCacheValid(this.enabledModelsCache) && this.enabledModelsCache) {
      return this.enabledModelsCache.data;
    }

    try {
      // First, ensure models are initialized
      await this.initializeModelsIfNeeded();
      
      const response = await apiClient.get<ApiModelListResponse>('/models/enabled');
      const modelsData = this.extractModelsFromResponse(response);
      
      if (modelsData.length === 0) {
        console.warn('No enabled models found in API response, falling back to filtration');
        // Try filtering enabled models from all models
        const allModels = await this.getModels(true);
        const enabledModels: Models = {};
        
        for (const [key, model] of Object.entries(allModels)) {
          if (model.enabled) {
            enabledModels[key] = model;
          }
        }
        
        if (Object.keys(enabledModels).length > 0) {
          this.enabledModelsCache = this.setCache(this.enabledModelsCache, enabledModels);
          return enabledModels;
        }
        
        // If no enabled models found anywhere, use fallback
        this.enabledModelsCache = this.setCache(this.enabledModelsCache, this.DEFAULT_FALLBACK_MODEL);
        return this.DEFAULT_FALLBACK_MODEL;
      }
      
      const models = this.convertApiResponseToModels(modelsData);
      
      // Update cache
      this.enabledModelsCache = this.setCache(this.enabledModelsCache, models);
      
      return models;
    } catch (error: unknown) {
      console.error('Error fetching enabled models from API:', error);
      
      // Fall back to filtering enabled models from all models
      try {
        const allModels = await this.getModels(true);
        const enabledModels: Models = {};
        
        for (const [key, model] of Object.entries(allModels)) {
          if (model.enabled) {
            enabledModels[key] = model;
          }
        }
        
        // If we got any enabled models, use them
        if (Object.keys(enabledModels).length > 0) {
          this.enabledModelsCache = this.setCache(this.enabledModelsCache, enabledModels);
          return enabledModels;
        }
      } catch (innerError) {
        console.error('Error filtering enabled models:', innerError);
      }
      
      // If all else fails, return default models
      // Use our static fallback models
      const defaultEnabledModels: Models = {};
      
      for (const [key, model] of Object.entries(defaultModels)) {
        if (model.enabled) {
          defaultEnabledModels[key] = model;
        }
      }
      
      this.enabledModelsCache = this.setCache(this.enabledModelsCache, defaultEnabledModels);
      return defaultEnabledModels;
    }
  }

  /**
   * Get active models for use in the application
   * This is an alias for getEnabledModels() for backward compatibility
   */
  public async getActiveModels(): Promise<Models> {
    return this.getEnabledModels();
  }

  /**
   * Get active models synchronously - use this when you must have a synchronous result
   * This will use cached enabled models if available, otherwise fallback to the default models
   */
  public getActiveModelsSync(): Models {
    if (this.isCacheValid(this.enabledModelsCache) && this.enabledModelsCache) {
      return this.enabledModelsCache.data;
    }
    
    if (this.isCacheValid(this.modelsCache) && this.modelsCache) {
      const enabledModels: Models = {};
      
      for (const [key, model] of Object.entries(this.modelsCache.data)) {
        if (model.enabled) {
          enabledModels[key] = model;
        }
      }
      
      // If we have any enabled models, return them
      if (Object.keys(enabledModels).length > 0) {
        return enabledModels;
      }
    }
    
    // No valid cache, use default models
    const defaultEnabledModels: Models = {};
    
    for (const [key, model] of Object.entries(defaultModels)) {
      if (model.enabled) {
        defaultEnabledModels[key] = model;
      }
    }
    
    return defaultEnabledModels;
  }

  /**
   * Create a new model configuration directly using POST
   */
  public async createModel(key: string, model: ModelConfig): Promise<Models> {
    try {
      // Create the model using a direct POST request
      await apiClient.post<ApiModelResponse>('/models', {
        key,
        name: model.name,
        provider: model.provider,
        temperature: model.temperature,
        context_window: model.context_window,
        max_output_tokens: model.max_output_tokens,
        extended_thinking: model.extended_thinking,
        enabled: model.enabled !== false
      });
      
      // Clear caches to force refresh
      this.clearCaches();
      
      // Get updated models
      return this.getModels(true);
    } catch (error) {
      console.error('Error creating model:', error);
      throw error;
    }
  }

  /**
   * Delete a specific model by key
   */
  public async deleteModel(modelKey: string): Promise<Models> {
    console.log(`[ModelService.deleteModel] START - Deleting model with key: ${modelKey}`);
    
    try {
      // Make sure we have the model before trying to delete it
      console.log(`[ModelService.deleteModel] Fetching current models to verify model exists`);
      const existingModels = await this.getModels(true);
      
      console.log(`[ModelService.deleteModel] Checking if model ${modelKey} exists in:`, Object.keys(existingModels));
      if (!existingModels[modelKey]) {
        console.warn(`[ModelService.deleteModel] Model ${modelKey} doesn't exist or already deleted`);
        return existingModels; // Return current models if model doesn't exist
      }
      
      console.log(`[ModelService.deleteModel] Found model to delete:`, existingModels[modelKey]);
      
      // Call the delete endpoint
      console.log(`[ModelService.deleteModel] Sending DELETE request to: /models/${modelKey}`);
      const response = await apiClient.delete(`/models/${modelKey}`);
      
      console.log(`[ModelService.deleteModel] DELETE response:`, {
        status: response.status,
        statusText: response.statusText,
        headers: response.headers,
        data: response.data
      });
      
      // Clear caches to force refresh
      console.log(`[ModelService.deleteModel] Clearing caches after deletion`);
      this.clearCaches();
      
      // OPTIMIZATION: If backend returns 204/200, we'll create an optimistic update
      // by removing the model from our local data even if it still appears in the backend response
      if (response.status === 204 || response.status === 200) {
        console.log(`[ModelService.deleteModel] Using optimistic update to remove model ${modelKey}`);
        // Create optimistic update by manually removing the model from our copy
        const optimisticModels = { ...existingModels };
        delete optimisticModels[modelKey];
        
        // Update our cache with the optimistic data
        this.modelsCache = this.setCache(this.modelsCache, optimisticModels);
        
        console.log(`[ModelService.deleteModel] SUCCESS: Optimistically removed model ${modelKey}`);
        console.log(`[ModelService.deleteModel] END - Deletion complete for ${modelKey}`);
        
        return optimisticModels;
      } else {
        // If status is unexpected, use the regular approach
        console.log(`[ModelService.deleteModel] Unexpected status ${response.status}, fetching updated models list`);
        const updatedModels = await this.getModels(true);
        
        // Verify the model was actually deleted
        if (updatedModels[modelKey]) {
          console.warn(`[ModelService.deleteModel] WARNING: Model ${modelKey} still exists after deletion!`, updatedModels[modelKey]);
        } else {
          console.log(`[ModelService.deleteModel] SUCCESS: Model ${modelKey} confirmed deleted`);
        }
        
        console.log(`[ModelService.deleteModel] END - Deletion complete for ${modelKey}`);
        return updatedModels;
      }
    } catch (error) {
      console.error(`[ModelService.deleteModel] ERROR - Failed to delete model ${modelKey}:`, error);
      
      if (axios.isAxiosError(error)) {
        const axiosError = error as AxiosError;
        if (axiosError.response) {
          console.error('[ModelService.deleteModel] API Error Response:', {
            status: axiosError.response.status,
            statusText: axiosError.response.statusText,
            data: axiosError.response.data,
            headers: axiosError.response.headers
          });
        } else if (axiosError.request) {
          // The request was made but no response was received
          console.error('[ModelService.deleteModel] No response received:', axiosError.request);
        } else {
          // Something happened in setting up the request
          console.error('[ModelService.deleteModel] Request setup error:', axiosError.message);
        }
        
        console.error('[ModelService.deleteModel] Config used for request:', axiosError.config);
      }
      
      throw error;
    }
  }
} 