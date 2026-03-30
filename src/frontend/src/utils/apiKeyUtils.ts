import { ApiKey } from '../types/apiKeys';
import { DatabricksService } from '../api/DatabricksService';

/**
 * API Key Utility Functions
 * 
 * This module provides utility functions for checking API key requirements and configuration.
 * 
 * USAGE IN OTHER COMPONENTS:
 * 
 * 1. Import the utility functions:
 *    import * as ApiKeyUtils from '../utils/apiKeyUtils';
 * 
 * 2. Use the async functions to check if an API key is required or configured:
 * 
 *    // Example in generate agent/task/templates components:
 *    async function validateBeforeGeneration() {
 *      const provider = model.provider.toLowerCase();
 *      
 *      // First check if API key is even required
 *      if (await ApiKeyUtils.isApiKeyOptional(provider)) {
 *        // API key not required, proceed with generation
 *        return true;
 *      }
 *      
 *      // Otherwise check if API key is configured
 *      const apiKeyConfigured = await ApiKeyUtils.isApiKeyConfigured(provider, apiKeys);
 *      if (!apiKeyConfigured) {
 *        // Show error message about missing API key
 *        setError(`${provider.toUpperCase()} API key is required.`);
 *        return false;
 *      }
 *      
 *      // All checks passed
 *      return true;
 *    }
 */

/**
 * Interface for provider key mappings
 */
export interface ProviderKeyMapping {
  [provider: string]: string;
}

/**
 * Standard mapping of provider names to API key names
 */
export const providerToKeyName: ProviderKeyMapping = {
  'openai': 'OPENAI_API_KEY',
  'open-ai': 'OPENAI_API_KEY',
  'open ai': 'OPENAI_API_KEY',
  'anthropic': 'ANTHROPIC_API_KEY',
  'qwen': 'QWEN_API_KEY',
  'deepseek': 'DEEPSEEK_API_KEY',
  'grok': 'GROK_API_KEY',
  'databricks': 'DATABRICKS_API_KEY',
  'gemini': 'GEMINI_API_KEY'
};

/**
 * Checks if the API key is required for a given provider
 * 
 * @param provider - The AI provider name
 * @param apiKeys - Array of API keys
 * @returns true if API key is not required, false if it's required
 */
export async function isApiKeyOptional(provider: string): Promise<boolean> {
  const normalizedProvider = provider.toLowerCase();
  
  // Ollama doesn't require an API key
  if (normalizedProvider === 'ollama') {
    return true;
  }
  
  // Special case for Databricks provider
  if (normalizedProvider === 'databricks') {
    try {
      // Check if Databricks integration is enabled
      const databricksService = DatabricksService.getInstance();
      const databricksConfig = await databricksService.getDatabricksConfig();
      
      // If Databricks apps are enabled, API key is optional
      if (databricksConfig && databricksConfig.enabled) {
        return true;
      }
    } catch (error) {
      console.error('Error checking Databricks configuration:', error);
    }
  }
  
  // For all other providers, API key is required
  return false;
}

/**
 * Checks if an API key is configured for a given provider
 * 
 * @param provider - The AI provider name
 * @param apiKeys - Array of API keys
 * @returns true if API key is configured or not required, false otherwise
 */
export async function isApiKeyConfigured(provider: string, apiKeys: ApiKey[]): Promise<boolean> {
  const normalizedProvider = provider.toLowerCase();
  
  console.log(`Checking if API key is configured for provider: ${normalizedProvider}`);
  console.log(`Available API keys:`, apiKeys.map(k => ({ name: k.name, hasValue: !!k.value })));
  
  // First check if API key is optional for this provider
  if (await isApiKeyOptional(normalizedProvider)) {
    console.log(`API key is optional for provider: ${normalizedProvider}`);
    return true;
  }
  
  // Find the key name for this provider
  const keyName = providerToKeyName[normalizedProvider] || '';
  if (!keyName) {
    console.warn(`No API key mapping found for provider: ${provider}`);
    return false;
  }
  
  console.log(`Looking for API key with name: ${keyName}`);
  
  // Check if there's a key with this name and it has a value
  const hasKey = apiKeys.some(key => {
    const nameMatches = key.name.toUpperCase() === keyName.toUpperCase();
    const hasValue = key.value && key.value.trim() !== '';
    
    console.log(`Checking key ${key.name}: matches=${nameMatches}, hasValue=${hasValue}`);
    return nameMatches && hasValue;
  });
  
  console.log(`API key check result for ${normalizedProvider}: ${hasKey ? 'Valid key found' : 'No valid key found'}`);
  
  return hasKey;
} 