import apiClient from '../config/api/ApiConfig';
import { AxiosError } from 'axios';
import { ConnectionAgent, ConnectionTask, ConnectionRequest, ConnectionResponse } from '../types/connection';
import { ModelService } from './ModelService';
import { Models } from '../types/models';

export class ConnectionService {
  private static readonly DEFAULT_MODEL = 'gpt-4';
  private static readonly modelService = ModelService.getInstance();

  private static formatTasks(tasks: ConnectionTask[]): ConnectionTask[] {
    console.log('=== Formatting Tasks ===');
    console.log('Tasks before formatting:', JSON.stringify(tasks, null, 2));
    
    const formattedTasks = tasks.map((task) => {
      const taskData = {
        name: task.name,
        description: task.description || task.name,
        expected_output: task.expected_output || '',
        human_input: task.human_input || false,
        tools: task.tools || [],
        context: {
          type: task.context?.type || 'general',
          priority: task.context?.priority || 'medium',
          complexity: task.context?.complexity || 'medium',
          required_skills: task.context?.required_skills || []
        }
      };

      // Add any additional task-specific fields
      if (task.type) {
        taskData.context.type = task.type;
      }
      if (task.priority) {
        taskData.context.priority = task.priority;
      }
      if (task.complexity) {
        taskData.context.complexity = task.complexity;
      }
      if (task.required_skills) {
        taskData.context.required_skills = task.required_skills;
      }

      return taskData;
    });
    
    console.log('Tasks after formatting:', JSON.stringify(formattedTasks, null, 2));
    return formattedTasks;
  }

  private static validateData(agents: ConnectionAgent[], tasks: ConnectionTask[]): void {
    console.log('=== Validating Input Data ===');
    console.log('Raw tasks received:', JSON.stringify(tasks, null, 2));
    
    agents.forEach((agent, index) => {
      if (!agent.name) throw new Error(`Agent ${index + 1} missing required field: name`);
      if (!agent.role) throw new Error(`Agent ${index + 1} missing required field: role`);
      if (!agent.goal) throw new Error(`Agent ${index + 1} missing required field: goal`);
    });

    tasks.forEach((task, index) => {
      console.log(`Validating task ${index + 1}:`, task);
      if (!task.name) {
        console.error(`Task ${index + 1} missing name. Full task object:`, task);
        throw new Error(`Task ${index + 1} missing required field: name`);
      }
      if (!task.description) {
        console.error(`Task ${index + 1} missing description. Full task object:`, task);
        throw new Error(`Task ${index + 1} missing required field: description`);
      }
    });
  }

  private static async formatModel(model: string): Promise<string> {
    const formattedModel = model.trim().toLowerCase();
    
    // Get models from ModelService
    let models: Models;
    try {
      models = await this.modelService.getActiveModels();
    } catch (error) {
      console.error('Error fetching models from ModelService:', error);
      // Fallback to synchronous method if async fails
      models = this.modelService.getActiveModelsSync();
    }
    
    // Debug the models object
    console.log('=== Model Validation ===');
    console.log('Available models:', Object.keys(models));
    console.log('Requested model:', formattedModel);
    
    // Check if the model exists in the retrieved models
    if (!Object.keys(models).includes(formattedModel)) {
      console.warn(`Invalid model "${model}" provided, using default model "${this.DEFAULT_MODEL}"`);
      return models[this.DEFAULT_MODEL]?.name || this.DEFAULT_MODEL;
    }
    
    // Get the actual model name from the models configuration
    const modelConfig = models[formattedModel];
    console.log('Model validated successfully:', formattedModel);
    console.log('Using model configuration:', modelConfig);
    
    // Return the actual model name, not the key
    return modelConfig.name;
  }

  static async generateConnections(agents: ConnectionAgent[], tasks: ConnectionTask[], model: string): Promise<ConnectionResponse> {
    console.log('=== Starting Connection Generation ===');
    console.log('Input parameters:', {
      agentsCount: agents.length,
      tasksCount: tasks.length,
      model,
      fullTasks: tasks
    });
    
    try {
      this.validateData(agents, tasks);

      const formattedTasks = this.formatTasks(tasks);
      const formattedModel = await this.formatModel(model);

      const requestData: ConnectionRequest = {
        agents: agents.map(agent => ({
          name: agent.name,
          role: agent.role,
          goal: agent.goal,
          backstory: agent.backstory || 'Not provided',
          tools: agent.tools || []
        })),
        tasks: formattedTasks,
        model: formattedModel
      };

      console.log('=== Making API Request ===');
      console.log('Full Request Data:', JSON.stringify(requestData, null, 2));

      const response = await apiClient.post<ConnectionResponse>(
        `/generate/generate-connections`,
        requestData,
        {
          headers: {
            'Content-Type': 'application/json',
          },
          timeout: 30000,
        }
      );
      
      console.log('=== API Response ===');
      console.log('Response status:', response.status);
      console.log('Response data:', JSON.stringify(response.data, null, 2));
      
      return response.data;
    } catch (error) {
      console.log('=== Connection Generation Error ===');
      console.error('Full error details:', {
        error,
        tasks,
        agents,
        model
      });
      
      if (error instanceof AxiosError) {
        const errorDetails = {
          message: error.message,
          status: error.response?.status,
          statusText: error.response?.statusText,
          responseData: error.response?.data,
          requestData: error.config?.data ? JSON.parse(error.config.data) : null,
          originalTasks: tasks
        };
        
        console.error('Detailed Error Information:', errorDetails);
        
        if (error.response?.status === 422) {
          throw new Error(
            `Validation error: ${error.response.data?.detail || 'Invalid request data'}`
          );
        }
        
        if (error.response?.status === 400) {
          throw new Error(
            `Bad request: ${error.response.data?.detail || 'Invalid request format'}`
          );
        }
        
        if (error.response?.status === 500) {
          console.error('Server error details:', error.response?.data);
          const errorDetail = error.response?.data?.detail || 'Unknown server error';
          throw new Error(
            `Server error (500): ${errorDetail}`
          );
        }
        
        throw new Error(
          `Failed to generate connections: ${error.response?.data?.detail || error.message}`
        );
      }
      
      throw new Error(`Unexpected error during connection generation: ${error instanceof Error ? error.message : 'Unknown error'}`);
    }
  }
} 