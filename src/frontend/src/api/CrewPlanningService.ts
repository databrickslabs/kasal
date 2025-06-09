import { apiClient as API } from '../config/api/ApiConfig';
import { Crew } from '../types/crewPlan';

/**
 * Request parameters for creating a crew
 */
export interface CrewPlanningRequest {
  prompt: string;
  model: string;
  tools?: string[];
}

/**
 * Service for crew planning operations used in the CrewPlanningDialog
 */
export class CrewPlanningService {
  /**
   * Create a crew using a natural language prompt
   * Communicates with the backend to generate and save agents and tasks
   * 
   * @param request The planning request parameters
   * @returns A promise resolving to the created crew
   */
  static async createCrew(request: CrewPlanningRequest): Promise<Crew> {
    try {
      // Log the request (omitting any sensitive data)
      console.log('Creating crew with plan:', {
        prompt: request.prompt,
        model: request.model,
        toolsCount: request.tools?.length ?? 0
      });
      
      // Use the new create-crew endpoint that handles everything in the backend
      const response = await API.post('/crew/create-crew', {
        prompt: request.prompt,
        model: request.model,
        tools: request.tools || []
      });
      
      console.log('Created crew:', response.data);
      
      // Return the response with created agents and tasks
      return {
        agents: response.data.agents,
        tasks: response.data.tasks
      };
    } catch (error) {
      console.error('Error creating crew:', error);
      throw error;
    }
  }
} 