import axios from 'axios';
import { Agent } from './AgentService';
import { Task } from './TaskService';
import apiClient from '../config/api/ApiConfig';

export interface GenerationPrompt {
  prompt: string;
  model?: string;
}

export interface TemplateRequest {
  role: string;
  goal: string;
  backstory: string;
  model?: string;
}

export interface TemplateResponse {
  system_template: string;
  prompt_template: string;
  response_template: string;
}

interface OpenAITemplateResponse {
  'System Template'?: string;
  'Prompt Template'?: string;
  'Response Template'?: string;
  'system_template'?: string;
  'prompt_template'?: string;
  'response_template'?: string;
}

export class GenerateService {
  static async generateAgent(prompt: string, model?: string, tools: string[] = []): Promise<Agent | null> {
    try {
      console.log('Calling agent-generation API endpoint');
      // Ensure all tool IDs are strings
      const stringTools = tools.map(tool => String(tool));
      
      const response = await apiClient.post<Agent>('/agent-generation/generate', {
        prompt: prompt,
        model: model || "gpt-4o-mini",
        tools: stringTools
      });
      return response.data;
    } catch (error) {
      console.error('Error generating agent:', error);
      return null;
    }
  }

  static async generateTask(prompt: string, model?: string): Promise<Task | null> {
    try {
      const response = await apiClient.post<Task>(
        '/task-generation/generate-task',
        { 
          text: prompt.trim(),
          model: model || undefined
        },
        {
          headers: {
            'Content-Type': 'application/json'
          }
        }
      );
      return response.data;
    } catch (error) {
      console.error('Error generating task:', error);
      return null;
    }
  }

  static async generateTemplates(request: TemplateRequest): Promise<TemplateResponse | null> {
    try {
      const response = await apiClient.post<TemplateResponse>(
        '/template-generation/generate-templates',
        request,
        {
          headers: {
            'Content-Type': 'application/json',
            'Accept': 'application/json'
          },
          transformResponse: [(data) => {
            // Parse the raw response
            const rawData = JSON.parse(data) as OpenAITemplateResponse | TemplateResponse;
            
            // Check if it's already in the correct format
            if ('system_template' in rawData) {
              return rawData;
            }

            // Transform the response to the correct format
            const transformedData: TemplateResponse = {
              system_template: rawData['System Template'] || rawData['system_template'] || '',
              prompt_template: rawData['Prompt Template'] || rawData['prompt_template'] || '',
              response_template: rawData['Response Template'] || rawData['response_template'] || ''
            };

            return transformedData;
          }]
        }
      );

      return response.data;
    } catch (error) {
      console.error('Error generating templates:', error);
      if (axios.isAxiosError(error)) {
        console.error('Response data:', error.response?.data);
        console.error('Status code:', error.response?.status);
        if (error.response?.data?.detail) {
          console.error('Error detail:', error.response.data.detail);
        }
      }
      return null;
    }
  }
} 