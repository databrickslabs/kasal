import { apiClient } from '../config/api/ApiConfig';
import { Agent, KnowledgeSource, StepCallback } from '../types/agent';
import { ModelService } from './ModelService';
import { uploadService } from './UploadService';

// Re-export for backward compatibility
export type { Agent, KnowledgeSource, StepCallback };

export class AgentService {
  private static readonly modelService = ModelService.getInstance();

  static async getAgent(id: number | string): Promise<Agent | null> {
    try {
      const response = await apiClient.get<Agent>(`/agents/${id}`);
      // Check knowledge sources and verify files still exist
      if (response.data && response.data.knowledge_sources && response.data.knowledge_sources.length > 0) {
        await this.verifyKnowledgeSources(response.data.knowledge_sources);
      }
      return response.data;
    } catch (error) {
      console.error('Error fetching agent:', error);
      return null;
    }
  }

  static async findOrCreateAgent(agent: Omit<Agent, 'id' | 'created_at'>): Promise<Agent | null> {
    try {
      const defaultValues: Partial<Agent> = {
        llm: 'databricks-llama-4-maverick',
        tools: [],
        max_iter: 25,
        verbose: false,
        allow_delegation: false,
        cache: true,
        allow_code_execution: false,
        code_execution_mode: 'safe' as const,
        max_retry_limit: 2,
        use_system_prompt: true,
        respect_context_window: true,
        memory: true,
        max_tokens: 2000, // Default max output tokens
        max_context_window_size: 8192, // Default context window size
        max_rpm: 3, // Default RPM to prevent rate limiting with Anthropic models
      };

      // If a model is specified, get its configuration
      if (agent.llm) {
        try {
          // First try to get models from database via ModelService
          const models = await this.modelService.getActiveModels();
          if (models[agent.llm]) {
            // Update max_context_window_size based on model configuration
            const modelConfig = models[agent.llm];
            if (modelConfig.context_window) {
              defaultValues.max_context_window_size = modelConfig.context_window;
            }
            
            // Update max_tokens based on model configuration
            if (modelConfig.max_output_tokens) {
              defaultValues.max_tokens = modelConfig.max_output_tokens;
            }
          }
        } catch (error) {
          console.error('Error getting models from ModelService:', error);
        }
      }

      const agentToSend = { ...defaultValues, ...agent };
      
      // Use find-or-create endpoint to prevent duplicates
      const response = await apiClient.post<Agent>(`/agents/find-or-create`, agentToSend);
      return response.data;
    } catch (error) {
      console.error('Error in find-or-create agent:', error);
      return null;
    }
  }

  static async createAgent(agent: Omit<Agent, 'id' | 'created_at'>): Promise<Agent | null> {
    try {
      const defaultValues: Partial<Agent> = {
        llm: 'databricks-llama-4-maverick',
        tools: [],
        max_iter: 25,
        verbose: false,
        allow_delegation: false,
        cache: true,
        allow_code_execution: false,
        code_execution_mode: 'safe' as const,
        max_retry_limit: 2,
        use_system_prompt: true,
        respect_context_window: true,
        memory: true,
        max_tokens: 2000, // Default max output tokens
        max_context_window_size: 8192, // Default context window size
        max_rpm: 3, // Default RPM to prevent rate limiting with Anthropic models
      };

      // If a model is specified, get its configuration
      if (agent.llm) {
        try {
          // First try to get models from database via ModelService
          const models = await this.modelService.getActiveModels();
          if (models[agent.llm]) {
            // Update max_context_window_size based on model configuration
            const modelConfig = models[agent.llm];
            if (modelConfig.context_window) {
              defaultValues.max_context_window_size = modelConfig.context_window;
            }
            
            // Update max_tokens based on model configuration
            if (modelConfig.max_output_tokens) {
              defaultValues.max_tokens = modelConfig.max_output_tokens;
            }
          }
        } catch (error) {
          console.error('Error getting models from ModelService:', error);
          // Fallback to sync method if async fails
          const fallbackModels = this.modelService.getActiveModelsSync();
          if (fallbackModels[agent.llm]) {
            const modelConfig = fallbackModels[agent.llm];
            if (modelConfig.context_window) {
              defaultValues.max_context_window_size = modelConfig.context_window;
            }
            if (modelConfig.max_output_tokens) {
              defaultValues.max_tokens = modelConfig.max_output_tokens;
            }
          }
        }
      }

      // Set up default memory configuration if memory is enabled but embedder_config is missing
      // Default to Databricks embeddings to maintain consistency
      if (agent.memory && !agent.embedder_config) {
        defaultValues.embedder_config = {
          provider: 'databricks',
          config: {
            model: 'databricks-gte-large-en'
          }
        };
      }

      // Create a deep copy with verified knowledge sources
      const agentToCreate = {
        ...defaultValues,
        ...agent,
      };

      // Verify knowledge sources before creating
      if (agentToCreate.knowledge_sources && agentToCreate.knowledge_sources.length > 0) {
        await this.verifyKnowledgeSources(agentToCreate.knowledge_sources);
      }

      // Create a copy to ensure fileInfo is sent to the server
      const agentToSend = JSON.parse(JSON.stringify(agentToCreate));
      
      // Ensure fileInfo is sent with each knowledge source
      const response = await apiClient.post<Agent>(`/agents`, agentToSend);
      return response.data;
    } catch (error) {
      console.error('Error creating agent:', error);
      return null;
    }
  }

  static async listAgents(): Promise<Agent[]> {
    try {
      const response = await apiClient.get<Agent[]>(`/agents`);
      // For performance reasons, we don't verify knowledge sources for listing
      return response.data;
    } catch (error) {
      console.error('Error listing agents:', error);
      return [];
    }
  }

  static async updateAgentFull(id: number | string, agent: Omit<Agent, 'id' | 'created_at'>): Promise<Agent | null> {
    try {
      // Update context window and max tokens based on model configuration
      if (agent.llm) {
        try {
          // Get models from database via ModelService
          const models = await this.modelService.getActiveModels();
          if (models[agent.llm]) {
            const modelConfig = models[agent.llm];
            
            // Set max_context_window_size from model configuration
            if (modelConfig.context_window) {
              agent.max_context_window_size = modelConfig.context_window;
            } else if (!agent.max_context_window_size) {
              agent.max_context_window_size = 8192; // Default fallback
            }
            
            // Set max_tokens from model configuration
            if (modelConfig.max_output_tokens) {
              agent.max_tokens = modelConfig.max_output_tokens;
            } else if (!agent.max_tokens) {
              agent.max_tokens = 2000; // Default fallback
            }
          } else {
            // Model not in configuration, set defaults if not already set
            if (!agent.max_context_window_size) {
              agent.max_context_window_size = 8192;
            }
            
            if (!agent.max_tokens) {
              agent.max_tokens = 2000;
            }
          }
        } catch (error) {
          console.error('Error getting models from ModelService:', error);
          // Fallback to sync method if async fails
          const fallbackModels = this.modelService.getActiveModelsSync();
          if (fallbackModels[agent.llm]) {
            const modelConfig = fallbackModels[agent.llm];
            
            // Set max_context_window_size from model configuration
            if (modelConfig.context_window) {
              agent.max_context_window_size = modelConfig.context_window;
            } else if (!agent.max_context_window_size) {
              agent.max_context_window_size = 8192;
            }
            
            // Set max_tokens from model configuration
            if (modelConfig.max_output_tokens) {
              agent.max_tokens = modelConfig.max_output_tokens;
            } else if (!agent.max_tokens) {
              agent.max_tokens = 2000;
            }
          } else {
            // Set defaults if not already set
            if (!agent.max_context_window_size) {
              agent.max_context_window_size = 8192;
            }
            
            if (!agent.max_tokens) {
              agent.max_tokens = 2000;
            }
          }
        }
      } else {
        // No model specified, set defaults if not already set
        if (!agent.max_context_window_size) {
          agent.max_context_window_size = 8192;
        }
        
        if (!agent.max_tokens) {
          agent.max_tokens = 2000;
        }
      }
      
      // Set up default memory configuration if memory is enabled but embedder_config is missing
      // Default to Databricks embeddings to maintain consistency
      if (agent.memory && !agent.embedder_config) {
        agent.embedder_config = {
          provider: 'databricks',
          config: {
            model: 'databricks-gte-large-en'
          }
        };
      }
      
      // If memory is disabled, remove the embedder_config to keep the API payload clean
      if (!agent.memory && agent.embedder_config) {
        agent.embedder_config = undefined;
      }
      
      // Verify knowledge sources before updating
      if (agent.knowledge_sources && agent.knowledge_sources.length > 0) {
        await this.verifyKnowledgeSources(agent.knowledge_sources);
      }

      // Create a clone to avoid modifying the original object
      const agentToUpdate = JSON.parse(JSON.stringify(agent));
      
      // Explicitly ensure knowledge_sources array is included with fileInfo preserved
      if (agentToUpdate.knowledge_sources) {
        console.log('Updating with knowledge sources:', agentToUpdate.knowledge_sources);
      }
      
      const response = await apiClient.put<Agent>(`/agents/${id}/full`, agentToUpdate);
      return response.data;
    } catch (error) {
      console.error('Error updating agent:', error);
      return null;
    }
  }

  static async updateAgent(
    id: number | string, 
    agent: Pick<Agent, 'name' | 'role' | 'goal' | 'backstory'>
  ): Promise<Agent | null> {
    try {
      const response = await apiClient.put<Agent>(`/agents/${id}`, agent);
      return response.data;
    } catch (error) {
      console.error('Error updating agent:', error);
      return null;
    }
  }

  static async deleteAgent(id: number | string): Promise<boolean> {
    try {
      await apiClient.delete(`/agents/${id}`);
      return true;
    } catch (error) {
      console.error('Error deleting agent:', error);
      return false;
    }
  }

  static async deleteAllAgents(): Promise<boolean> {
    try {
      await apiClient.delete(`/agents`);
      return true;
    } catch (error) {
      console.error('Error deleting all agents:', error);
      // Re-throw the error so the calling component can handle it
      throw error;
    }
  }

  /**
   * Verifies knowledge sources to ensure file information is current
   * @param sources The knowledge sources to verify
   */
  private static async verifyKnowledgeSources(sources: KnowledgeSource[]): Promise<void> {
    for (const source of sources) {
      // Skip text and URL sources
      if (source.type === 'text' || source.type === 'url' || !source.source) {
        continue;
      }
      
      try {
        // For file sources, always verify the file exists
        const fileInfo = await uploadService.checkKnowledgeFile(source.source);
        
        // Update fileInfo regardless of previous state to ensure it's current
        source.fileInfo = fileInfo;
        
        // Log warning if file doesn't exist
        if (!fileInfo.exists) {
          console.warn(`Knowledge source file not found: ${source.source}`);
        }
      } catch (error) {
        console.error(`Error verifying knowledge source ${source.source}:`, error);
      }
    }
  }
}