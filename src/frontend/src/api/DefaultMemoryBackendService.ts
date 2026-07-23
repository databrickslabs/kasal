/**
 * Service for managing default memory backend configuration
 * This allows setting a global default that all new agents will use
 */

import { MemoryBackendConfig } from '../types/memoryBackend';

const DEFAULT_CONFIG_KEY = 'default_memory_backend_config';

export class DefaultMemoryBackendService {
  private static instance: DefaultMemoryBackendService;

  private constructor() {
    // Private constructor for singleton pattern
  }

  static getInstance(): DefaultMemoryBackendService {
    if (!DefaultMemoryBackendService.instance) {
      DefaultMemoryBackendService.instance = new DefaultMemoryBackendService();
    }
    return DefaultMemoryBackendService.instance;
  }

  /**
   * Get the default memory backend configuration
   */
  getDefaultConfig(): MemoryBackendConfig | null {
    const stored = localStorage.getItem(DEFAULT_CONFIG_KEY);
    if (stored) {
      try {
        return JSON.parse(stored);
      } catch (e) {
        console.error('Failed to parse default memory backend config:', e);
        return null;
      }
    }
    return null;
  }

  /**
   * Set the default memory backend configuration
   */
  setDefaultConfig(config: MemoryBackendConfig): void {
    localStorage.setItem(DEFAULT_CONFIG_KEY, JSON.stringify(config));
  }

  /**
   * Clear the default memory backend configuration
   */
  clearDefaultConfig(): void {
    localStorage.removeItem(DEFAULT_CONFIG_KEY);
  }

  /**
   * Check if a default configuration is set
   */
  hasDefaultConfig(): boolean {
    return localStorage.getItem(DEFAULT_CONFIG_KEY) !== null;
  }
}