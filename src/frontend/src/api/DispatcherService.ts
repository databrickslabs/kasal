import { apiClient } from '../config/api/ApiConfig';

export interface DispatcherRequest {
  message: string;
  model?: string;
  tools?: string[];
}

export interface DispatcherResponse {
  intent: 'generate_agent' | 'generate_task' | 'generate_crew' | 'generate_plan' | 'execute_crew' | 'configure_crew' | 'conversation' | 'unknown';
  confidence: number;
  extracted_info: Record<string, unknown>;
  suggested_prompt?: string;
}

export interface ConfigureCrewResult {
  type: 'configure_crew';
  config_type: 'llm' | 'maxr' | 'tools' | 'general';
  message: string;
  actions: {
    open_llm_dialog: boolean;
    open_maxr_dialog: boolean;
    open_tools_dialog: boolean;
  };
  extracted_info: Record<string, unknown>;
}

export interface DispatchResult {
  dispatcher: DispatcherResponse;
  generation_result: unknown;
  service_called: string | null;
}

class DispatcherService {
  /**
   * Dispatch a natural language request to the appropriate generation service
   */
  async dispatch(request: DispatcherRequest): Promise<DispatchResult> {
    try {
      const response = await apiClient.post<DispatchResult>(
        '/dispatcher/dispatch',
        request
      );
      return response.data;
    } catch (error) {
      console.error('Error dispatching request:', error);
      throw error;
    }
  }

  /**
   * Detect intent only without executing generation
   */
  async detectIntent(request: DispatcherRequest): Promise<DispatcherResponse> {
    try {
      const response = await apiClient.post<DispatcherResponse>(
        '/dispatcher/detect-intent',
        request
      );
      return response.data;
    } catch (error) {
      console.error('Error detecting intent:', error);
      throw error;
    }
  }
}

export default new DispatcherService(); 