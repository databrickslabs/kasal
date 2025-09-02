import { DefaultMemoryBackendService } from './DefaultMemoryBackendService';
import { MemoryBackendConfig, MemoryBackendType } from '../types/memoryBackend';

describe('DefaultMemoryBackendService', () => {
  let service: DefaultMemoryBackendService;
  
  beforeEach(() => {
    // Clear localStorage before each test
    localStorage.clear();
    // Reset singleton instance
    (DefaultMemoryBackendService as any).instance = undefined;
    service = DefaultMemoryBackendService.getInstance();
    // Mock console.error
    jest.spyOn(console, 'error').mockImplementation(() => {});
  });

  afterEach(() => {
    (console.error as jest.Mock).mockRestore();
  });

  describe('getInstance', () => {
    it('should return the same instance (singleton)', () => {
      const instance1 = DefaultMemoryBackendService.getInstance();
      const instance2 = DefaultMemoryBackendService.getInstance();
      
      expect(instance1).toBe(instance2);
    });
  });

  describe('getDefaultConfig', () => {
    it('should return null when no config is stored', () => {
      const result = service.getDefaultConfig();
      expect(result).toBeNull();
    });

    it('should return stored config', () => {
      const mockConfig: MemoryBackendConfig = {
        backend_type: MemoryBackendType.DATABRICKS,
        enable_short_term: true,
        enable_long_term: true,
        enable_entity: true,
        databricks_config: {
          workspace_url: 'https://example.databricks.com',
          endpoint_name: 'test-endpoint',
          short_term_index: 'short_term_index',
          long_term_index: 'long_term_index',
          entity_index: 'entity_index',
        },
      };
      
      localStorage.setItem('default_memory_backend_config', JSON.stringify(mockConfig));
      
      const result = service.getDefaultConfig();
      expect(result).toEqual(mockConfig);
    });

    it('should return null and log error for invalid JSON', () => {
      localStorage.setItem('default_memory_backend_config', 'invalid-json');
      
      const result = service.getDefaultConfig();
      
      expect(result).toBeNull();
      expect(console.error).toHaveBeenCalledWith(
        'Failed to parse default memory backend config:',
        expect.any(Error)
      );
    });
  });

  describe('setDefaultConfig', () => {
    it('should store config in localStorage', () => {
      const mockConfig: MemoryBackendConfig = {
        backend_type: MemoryBackendType.DATABRICKS,
        enable_short_term: true,
        enable_long_term: true,
        enable_entity: true,
        databricks_config: {
          workspace_url: 'https://example.databricks.com',
          endpoint_name: 'test-endpoint',
          short_term_index: 'short_term_index',
          long_term_index: 'long_term_index',
          entity_index: 'entity_index',
        },
      };
      
      service.setDefaultConfig(mockConfig);
      
      const stored = localStorage.getItem('default_memory_backend_config');
      expect(stored).toBe(JSON.stringify(mockConfig));
    });

    it('should overwrite existing config', () => {
      const config1: MemoryBackendConfig = {
        backend_type: MemoryBackendType.DEFAULT,
        enable_short_term: false,
      };
      const config2: MemoryBackendConfig = {
        backend_type: MemoryBackendType.DATABRICKS,
        enable_short_term: true,
        databricks_config: {
          workspace_url: 'https://example.databricks.com',
          endpoint_name: 'new-endpoint',
          short_term_index: 'short_index',
        },
      };
      
      service.setDefaultConfig(config1);
      service.setDefaultConfig(config2);
      
      const stored = localStorage.getItem('default_memory_backend_config');
      expect(stored).toBe(JSON.stringify(config2));
    });
  });

  describe('clearDefaultConfig', () => {
    it('should remove config from localStorage', () => {
      const mockConfig: MemoryBackendConfig = {
        backend_type: MemoryBackendType.DATABRICKS,
        enable_short_term: true,
      };
      
      localStorage.setItem('default_memory_backend_config', JSON.stringify(mockConfig));
      expect(localStorage.getItem('default_memory_backend_config')).not.toBeNull();
      
      service.clearDefaultConfig();
      
      expect(localStorage.getItem('default_memory_backend_config')).toBeNull();
    });

    it('should handle clearing when no config exists', () => {
      expect(localStorage.getItem('default_memory_backend_config')).toBeNull();
      
      // Should not throw
      expect(() => service.clearDefaultConfig()).not.toThrow();
      
      expect(localStorage.getItem('default_memory_backend_config')).toBeNull();
    });
  });

  describe('hasDefaultConfig', () => {
    it('should return false when no config exists', () => {
      expect(service.hasDefaultConfig()).toBe(false);
    });

    it('should return true when config exists', () => {
      const mockConfig: MemoryBackendConfig = {
        backend_type: MemoryBackendType.DATABRICKS,
        enable_short_term: true,
      };
      
      localStorage.setItem('default_memory_backend_config', JSON.stringify(mockConfig));
      
      expect(service.hasDefaultConfig()).toBe(true);
    });

    it('should return true even for invalid JSON', () => {
      localStorage.setItem('default_memory_backend_config', 'invalid-json');
      
      expect(service.hasDefaultConfig()).toBe(true);
    });
  });

  describe('integration scenarios', () => {
    it('should handle full lifecycle of config management', () => {
      // Initially no config
      expect(service.hasDefaultConfig()).toBe(false);
      expect(service.getDefaultConfig()).toBeNull();
      
      // Set config
      const mockConfig: MemoryBackendConfig = {
        backend_type: MemoryBackendType.DATABRICKS,
        enable_short_term: true,
        enable_long_term: true,
        enable_entity: true,
        databricks_config: {
          workspace_url: 'https://example.databricks.com',
          endpoint_name: 'test-endpoint',
          short_term_index: 'short_term_index',
          long_term_index: 'long_term_index',
          entity_index: 'entity_index',
        },
      };
      service.setDefaultConfig(mockConfig);
      
      // Verify config exists
      expect(service.hasDefaultConfig()).toBe(true);
      expect(service.getDefaultConfig()).toEqual(mockConfig);
      
      // Clear config
      service.clearDefaultConfig();
      
      // Verify config is cleared
      expect(service.hasDefaultConfig()).toBe(false);
      expect(service.getDefaultConfig()).toBeNull();
    });
  });
});