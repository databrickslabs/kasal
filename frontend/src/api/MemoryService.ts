import ApiService from './ApiService';

/**
 * Memory interface types
 */
interface MemoryDetails {
  memory_path: string;
  size_bytes: number;
  creation_date: string;
  last_modified: string;
  long_term_memory?: {
    path: string;
    size_bytes: number;
    tables?: string[];
    records?: Array<{
      timestamp: string;
      content: string;
    }>;
  };
  short_term_memory?: {
    messages?: Array<{
      role: string;
      content: string;
    }>;
  };
}

interface CrewDetail {
  size: number;
  last_modified: string;
  messages_count?: number;
}

interface MemoryStats {
  total_crews: number;
  total_size: number;
  avg_size: number;
  oldest_memory: string;
  newest_memory?: string;
  crew_details?: Record<string, CrewDetail>;
}

/**
 * Memory Service - Provides functionality to manage CrewAI memory storage
 */
export class MemoryService {
  private static instance: MemoryService;
  private memoryPath: string | null = null;

  // Private constructor for singleton pattern
  private constructor() {
    // Try to load saved path from localStorage
    this.memoryPath = localStorage.getItem('memoryPath');
  }

  /**
   * Get singleton instance of MemoryService
   */
  public static getInstance(): MemoryService {
    if (!MemoryService.instance) {
      MemoryService.instance = new MemoryService();
    }
    return MemoryService.instance;
  }

  /**
   * Set custom memory path
   * @param path Custom path to look for memories
   */
  public setMemoryPath(path: string | null): void {
    this.memoryPath = path;
    if (path) {
      localStorage.setItem('memoryPath', path);
    } else {
      localStorage.removeItem('memoryPath');
    }
  }

  /**
   * Get current memory path
   * @returns Current memory path or null if using default
   */
  public getMemoryPath(): string | null {
    return this.memoryPath;
  }

  /**
   * List all crew memories
   * @returns Array of crew memory names
   */
  public async listMemories(): Promise<string[]> {
    try {
      console.log('Fetching memories from API...');
      const params = this.memoryPath ? { custom_path: this.memoryPath } : undefined;
      const response = await ApiService.get('/memory/list', params);
      console.log('API response:', response);
      
      // Handle different response formats
      if (Array.isArray(response.data)) {
        console.log('Response data is an array with length:', response.data.length);
        return response.data;
      } else if (response.data && typeof response.data === 'object') {
        console.log('Response data is an object:', response.data);
        // If response.data has a property that contains an array
        const possibleArrayProps = ['data', 'memories', 'crews', 'items', 'results'];
        for (const prop of possibleArrayProps) {
          if (Array.isArray(response.data[prop])) {
            console.log(`Found array in property ${prop} with length:`, response.data[prop].length);
            return response.data[prop];
          }
        }
      }
      
      // If we can't find an array, log warning and return empty array
      console.warn('Memory API response format unexpected:', response.data);
      return [];
    } catch (error) {
      console.error('Error listing memories:', error);
      return []; // Return empty array rather than throwing to prevent component crashes
    }
  }

  /**
   * Reset memory for a specific crew
   * @returns Result of the reset operation
   */
  public async resetMemory(crewName: string): Promise<{status: string; message: string}> {
    try {
      const params = this.memoryPath ? { custom_path: this.memoryPath } : undefined;
      const response = await ApiService.post(`/memory/reset/${crewName}`, params);
      return response.data || { status: 'success', message: `Memory for crew '${crewName}' has been reset` };
    } catch (error) {
      console.error(`Error resetting memory for crew '${crewName}':`, error);
      return { status: 'error', message: `Failed to reset memory for crew '${crewName}'` };
    }
  }

  /**
   * Reset all memories
   * @returns Result of the reset operation
   */
  public async resetAllMemories(): Promise<{status: string; message: string}> {
    try {
      const params = this.memoryPath ? { custom_path: this.memoryPath } : undefined;
      const response = await ApiService.post('/memory/reset-all', params);
      return response.data || { status: 'success', message: 'All crew memories have been reset' };
    } catch (error) {
      console.error('Error resetting all memories:', error);
      return { status: 'error', message: 'Failed to reset all memories' };
    }
  }

  /**
   * Get detailed memory info for a specific crew
   * @returns Memory details or null if none found
   */
  public async getMemoryDetails(crewName: string): Promise<MemoryDetails | null> {
    try {
      const params = this.memoryPath ? { custom_path: this.memoryPath } : undefined;
      const response = await ApiService.get(`/memory/details/${crewName}`, params);
      return response.data || null;
    } catch (error) {
      console.error(`Error getting memory details for crew '${crewName}':`, error);
      return null; // Return null instead of throwing
    }
  }

  /**
   * Get memory statistics
   * @returns Memory statistics or null if failed
   */
  public async getMemoryStats(detailed = false): Promise<MemoryStats | null> {
    try {
      const params = { 
        detailed,
        ...(this.memoryPath ? { custom_path: this.memoryPath } : {})
      };
      const response = await ApiService.get('/memory/stats', params);
      return response.data || null;
    } catch (error) {
      console.error('Error getting memory statistics:', error);
      return null; // Return null instead of throwing
    }
  }
  
  /**
   * Search memories for specific text
   * @returns Search results or empty array if none found
   */
  public async searchMemories(query: string): Promise<Array<{crew_name: string; snippet: string}>> {
    try {
      const params = { 
        query,
        ...(this.memoryPath ? { custom_path: this.memoryPath } : {})
      };
      const response = await ApiService.get('/memory/search', params);
      if (Array.isArray(response.data)) {
        return response.data;
      } else if (response.data && typeof response.data === 'object' && Array.isArray(response.data.results)) {
        return response.data.results;
      }
      return [];
    } catch (error) {
      console.error(`Error searching memories for '${query}':`, error);
      return []; // Return empty array instead of throwing
    }
  }

  /**
   * Clean up old memories
   * @returns Result of the cleanup operation
   */
  public async cleanupOldMemories(days = 30): Promise<{status: string; message: string; count?: number}> {
    try {
      const params = { 
        days,
        ...(this.memoryPath ? { custom_path: this.memoryPath } : {})
      };
      const response = await ApiService.post('/memory/cleanup', params);
      return response.data || { status: 'success', message: `Cleaned up memories older than ${days} days`, count: 0 };
    } catch (error) {
      console.error(`Error cleaning up old memories:`, error);
      return { status: 'error', message: 'Failed to clean up old memories' };
    }
  }

  /**
   * Delete a specific memory folder
   * @param crewName The name of the crew memory to delete
   * @returns Result of the delete operation
   */
  public async deleteMemory(crewName: string): Promise<{status: string; message: string}> {
    try {
      const params = this.memoryPath ? { custom_path: this.memoryPath } : undefined;
      // Using POST endpoint instead of DELETE which is having issues
      const response = await ApiService.post(`/memory/remove/${crewName}`, params);
      return response.data || { status: 'success', message: `Memory for crew '${crewName}' has been deleted` };
    } catch (error) {
      console.error(`Error deleting memory for crew '${crewName}':`, error);
      return { status: 'error', message: `Failed to delete memory for crew '${crewName}'` };
    }
  }
}

export default MemoryService; 