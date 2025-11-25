/**
 * AgentBricks Service
 *
 * Service for interacting with Databricks AgentBricks API endpoints.
 * Handles fetching AgentBricks endpoints and related operations.
 */

import { apiClient } from '../config/api/ApiConfig';

/**
 * Represents an AgentBricks endpoint
 */
export interface AgentBricksEndpoint {
  id: string;
  name: string;
  creator?: string;
  creation_timestamp?: number;
  last_updated_timestamp?: number;
  state?: string;
  config?: Record<string, unknown>;
  tags?: Array<Record<string, string>>;
  task?: string;
  endpoint_type?: string;
}

/**
 * Response from the AgentBricks endpoints API
 */
export interface AgentBricksEndpointsResponse {
  endpoints: AgentBricksEndpoint[];
  total_count: number;
  filtered: boolean;
}

/**
 * Request parameters for searching AgentBricks endpoints
 */
export interface AgentBricksEndpointsSearchRequest {
  search_query?: string;
  endpoint_ids?: string[];
  ready_only?: boolean;
  creator_filter?: string;
}

/**
 * Request to execute a query on an AgentBricks endpoint
 */
export interface AgentBricksExecutionRequest {
  endpoint_name: string;
  question: string;
  custom_inputs?: Record<string, unknown>;
  return_trace?: boolean;
  timeout?: number;
}

/**
 * Response from executing an AgentBricks query
 */
export interface AgentBricksExecutionResponse {
  endpoint_name: string;
  status: string;
  result?: string;
  error?: string;
  trace?: Record<string, unknown>;
}

/**
 * Service class for AgentBricks-related operations
 */
export class AgentBricksService {
  /**
   * Fetch available AgentBricks endpoints
   * @param readyOnly Only return ready endpoints (default true)
   * @param searchQuery Optional search query to filter endpoints
   * @returns Promise containing the response with endpoints
   */
  static async getEndpoints(readyOnly = true, searchQuery?: string): Promise<AgentBricksEndpointsResponse> {
    try {
      const params: Record<string, string | boolean> = { ready_only: readyOnly };
      if (searchQuery) {
        params.search_query = searchQuery;
      }
      const response = await apiClient.get<AgentBricksEndpointsResponse>('/api/agentbricks/endpoints', { params });
      return response.data;
    } catch (error) {
      console.error('Error fetching AgentBricks endpoints:', error);
      // Return empty response on error so UI can handle gracefully
      return { endpoints: [], total_count: 0, filtered: false };
    }
  }

  /**
   * Search and filter AgentBricks endpoints
   * @param searchParams Search parameters
   * @returns Promise containing the response with filtered endpoints
   */
  static async searchEndpoints(searchParams: AgentBricksEndpointsSearchRequest): Promise<AgentBricksEndpointsResponse> {
    try {
      const response = await apiClient.post<AgentBricksEndpointsResponse>(
        '/api/agentbricks/endpoints/search',
        searchParams
      );
      return response.data;
    } catch (error) {
      console.error('Error searching AgentBricks endpoints:', error);
      return { endpoints: [], total_count: 0, filtered: false };
    }
  }

  /**
   * Get details for a specific AgentBricks endpoint
   * @param endpointName The name of the endpoint to fetch
   * @returns Promise containing the endpoint details
   */
  static async getEndpointByName(endpointName: string): Promise<AgentBricksEndpoint> {
    try {
      const response = await apiClient.get(`/api/agentbricks/endpoints/${endpointName}`);
      return response.data;
    } catch (error) {
      console.error(`Error fetching AgentBricks endpoint ${endpointName}:`, error);
      throw error;
    }
  }

  /**
   * Execute a query on an AgentBricks endpoint
   * @param request The execution request
   * @returns Promise containing the execution response
   */
  static async executeQuery(request: AgentBricksExecutionRequest): Promise<AgentBricksExecutionResponse> {
    try {
      const response = await apiClient.post<AgentBricksExecutionResponse>(
        '/api/agentbricks/execute',
        request
      );
      return response.data;
    } catch (error) {
      console.error('Error executing AgentBricks query:', error);
      throw error;
    }
  }

  /**
   * Helper to format endpoint display name
   * @param endpoint The AgentBricks endpoint
   * @returns Formatted display name
   */
  static formatEndpointName(endpoint: AgentBricksEndpoint): string {
    if (endpoint.creator) {
      return `${endpoint.name} (by ${endpoint.creator})`;
    }
    return endpoint.name;
  }

  /**
   * Helper to check if an endpoint is ready
   * @param endpoint The AgentBricks endpoint
   * @returns True if the endpoint is ready
   */
  static isEndpointReady(endpoint: AgentBricksEndpoint): boolean {
    return endpoint.state === 'READY';
  }
}

export default AgentBricksService;
