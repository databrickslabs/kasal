import apiClient from '../config/api/ApiConfig';

export interface PromptTemplate {
  id: number;
  name: string;
  description: string | null;
  template: string;
  is_active: boolean;
  created_at: string;
  updated_at: string;
}

export class PromptService {
  private static instance: PromptService;
  private readonly baseUrl = '/templates';

  // Private constructor for singleton pattern
  private constructor() {
    // Initialization is handled through instance properties
  }

  static getInstance(): PromptService {
    if (!PromptService.instance) {
      PromptService.instance = new PromptService();
    }
    return PromptService.instance;
  }

  /**
   * Get all prompt templates
   */
  async getAllPrompts(): Promise<PromptTemplate[]> {
    try {
      const response = await apiClient.get<PromptTemplate[]>(this.baseUrl);
      return response.data;
    } catch (error) {
      console.error('Error fetching prompts:', error);
      throw error;
    }
  }

  /**
   * Get a specific prompt template by ID
   */
  async getPromptById(id: number): Promise<PromptTemplate> {
    try {
      const response = await apiClient.get<PromptTemplate>(`${this.baseUrl}/${id}`);
      return response.data;
    } catch (error) {
      console.error(`Error fetching prompt with ID ${id}:`, error);
      throw error;
    }
  }

  /**
   * Get a specific prompt template by name
   */
  async getPromptByName(name: string): Promise<PromptTemplate> {
    try {
      const response = await apiClient.get<PromptTemplate>(`${this.baseUrl}/by-name/${name}`);
      return response.data;
    } catch (error) {
      console.error(`Error fetching prompt with name ${name}:`, error);
      throw error;
    }
  }

  /**
   * Update an existing prompt template
   */
  async updatePrompt(id: number, promptData: Partial<PromptTemplate>): Promise<PromptTemplate> {
    try {
      const response = await apiClient.put<PromptTemplate>(
        `${this.baseUrl}/${id}`, 
        promptData
      );
      return response.data;
    } catch (error) {
      console.error(`Error updating prompt with ID ${id}:`, error);
      throw error;
    }
  }

  /**
   * Reset all prompt templates to default values
   */
  async resetPromptTemplates(): Promise<{message: string, reset_count: number}> {
    try {
      const response = await apiClient.post<{message: string, reset_count: number}>(
        `${this.baseUrl}/reset`
      );
      return response.data;
    } catch (error) {
      console.error('Error resetting prompt templates:', error);
      throw error;
    }
  }
}

export default PromptService; 