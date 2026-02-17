import { apiClient } from '../config/api/ApiConfig';

export interface DispatcherRequest {
  message: string;
  model?: string;
  tools?: string[];
}

export interface DispatcherResponse {
  intent: 'generate_agent' | 'generate_task' | 'generate_crew' | 'generate_plan'
    | 'execute_crew' | 'configure_crew' | 'conversation'
    | 'catalog_list' | 'catalog_load' | 'catalog_save' | 'catalog_schedule' | 'catalog_help'
    | 'flow_list' | 'flow_load' | 'flow_save'
    | 'execute_flow'
    | 'unknown';
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

export interface CatalogListResult {
  type: 'catalog_list';
  plans: Array<{
    id: string;
    name: string;
    agent_count?: number;
    task_count?: number;
    created_at?: string;
    updated_at?: string;
  }>;
  message: string;
}

export interface CatalogLoadResult {
  type: 'catalog_load';
  plan: {
    id: string;
    name: string;
    nodes: unknown[];
    edges: unknown[];
    process?: string;
    planning?: boolean;
    planning_llm?: string;
    memory?: boolean;
    verbose?: boolean;
    max_rpm?: number;
  } | null;
  message: string;
}

export interface CatalogSaveResult {
  type: 'catalog_save';
  action: 'open_save_dialog';
  suggested_name?: string;
  message: string;
}

export interface CatalogScheduleResult {
  type: 'catalog_schedule';
  action: 'open_schedule_dialog';
  message: string;
}

export interface FlowListResult {
  type: 'flow_list';
  flows: Array<{ id: string; name: string; node_count?: number; created_at?: string; updated_at?: string }>;
  message: string;
}

export interface FlowLoadResult {
  type: 'flow_load';
  flow: { id: string; name: string; nodes: unknown[]; edges: unknown[]; flow_config?: Record<string, unknown> } | null;
  message: string;
}

export interface FlowSaveResult {
  type: 'flow_save';
  action: 'open_save_flow_dialog';
  suggested_name?: string;
  message: string;
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