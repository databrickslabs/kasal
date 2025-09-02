import apiClient from '../config/api/ApiConfig';
import { Trace, TaskDetails } from '../types/trace';

// List of known run IDs for development/testing - this should be removed in production
const KNOWN_RUN_IDS = [1]; // Based on the database, we only have run ID 1

// Define error interfaces
interface ApiError {
  response?: {
    status: number;
    data?: {
      detail?: string;
    };
  };
  message?: string;
}

// Define interfaces for return types
interface RunDetailsResponse {
  id: string;
  job_id: string;
  status: string;
  run_name: string;
  inputs?: Record<string, unknown>;
  result?: Record<string, unknown>;
  error?: string;
  created_at: string;
  completed_at?: string;
  [key: string]: unknown;
}

// Define interface for backend trace data
interface BackendTraceData {
  id: number;
  run_id?: number;
  job_id?: string;
  event_source?: string;
  event_context?: string;
  task_id?: string;
  event_type?: string;
  created_at?: string;
  timestamp?: string;
  output?: string | Record<string, unknown>;
  output_data?: string | Record<string, unknown>;
  [key: string]: unknown;
}

export const TraceService = {
  async checkRunExists(runId: string): Promise<boolean> {
    try {
      // Check if this is a UUID (contains dashes)
      const isUuid = typeof runId === 'string' && runId.includes('-');
      
      // Only convert to numeric if it's NOT a UUID
      const numericRunId = !isUuid && !isNaN(parseInt(runId)) ? parseInt(runId) : null;
      
      // If we're in development mode and we have a hardcoded list of known IDs
      // only apply this for numeric IDs, not UUIDs
      if (!isUuid && 
          process.env.NODE_ENV === 'development' && 
          numericRunId !== null &&
          KNOWN_RUN_IDS.includes(numericRunId)) {
        return true;
      }
      
      // Always use the /traces/job/ endpoint regardless of ID format
      // This is the most reliable endpoint that works for all ID types
      const endpoint = `/traces/job/${runId}`;
      
      // Use the traces endpoint to check if traces exist for this run ID
      const response = await apiClient.get(endpoint);
      return response.status === 200;
    } catch (error: unknown) {
      // Need to type cast to access properties
      const apiError = error as ApiError;
      
      // Check if it's a 404 - this is expected behavior when a run doesn't exist
      if (apiError.response && apiError.response.status === 404) {
        // For development, suggest using a known ID, but only for numeric IDs
        const isUuid = typeof runId === 'string' && runId.includes('-');
        if (!isUuid && process.env.NODE_ENV === 'development' && KNOWN_RUN_IDS.length > 0) {
          // Suggestion logic remains but without console.log
        }
        return false;
      }
      // Check for 422 (validation error) - likely means UUID format issue
      else if (apiError.response && apiError.response.status === 422) {
        return false;
      }
      // For other errors
      return false;
    }
  },

  async getRunDetails(runId: string): Promise<RunDetailsResponse> {
    try {
      // Check if this is a UUID (contains dashes)
      const isUuid = typeof runId === 'string' && runId.includes('-');
      
      // Check if we're already using a known run ID
      if (KNOWN_RUN_IDS.includes(Number(runId))) {
        // Using a known run ID
      }
      
      // Only convert to numeric if it's NOT a UUID
      const numericRunId = !isUuid && !isNaN(parseInt(runId)) ? parseInt(runId) : null;
      
      // For development purposes, only use fallback if it's a numeric ID (not UUID)
      // and it's not in the known IDs list
      if (!isUuid &&
          process.env.NODE_ENV === 'development' && 
          numericRunId !== null &&
          !KNOWN_RUN_IDS.includes(numericRunId) && 
          KNOWN_RUN_IDS.length > 0) {
        // Prevent infinite recursion
        if (KNOWN_RUN_IDS[0] === parseInt(runId)) {
          // Already using known ID, not redirecting again
        } else {
          return this.getRunDetails(KNOWN_RUN_IDS[0].toString());
        }
      }
      
      let endpoint;
      // If it's numeric and NOT a UUID, use the execution history endpoint
      if (!isUuid && numericRunId !== null) {
        endpoint = `/executions/history/${numericRunId}`;
      } else {
        // For UUID job_ids, use the executions endpoint
        endpoint = `/executions/${runId}`;
      }
      
      const response = await apiClient.get<RunDetailsResponse>(endpoint);
      return response.data;
    } catch (error: unknown) {
      // Need to type cast to access properties
      const apiError = error as ApiError;
      
      // For development, if we get a 404, try to use a known ID for numeric IDs only
      if (apiError.response && apiError.response.status === 404) {
        const isUuid = typeof runId === 'string' && runId.includes('-');
        
        if (!isUuid && 
            process.env.NODE_ENV === 'development' && 
            KNOWN_RUN_IDS.length > 0 &&
            !isNaN(parseInt(runId))) {
          // Prevent infinite recursion
          if (KNOWN_RUN_IDS[0] === parseInt(runId)) {
            throw error;
          } else {
            return this.getRunDetails(KNOWN_RUN_IDS[0].toString());
          }
        }
      }
      
      console.error(`Error fetching run details for ID ${runId}:`, apiError);
      // Re-throw to let the calling component handle the error
      throw error;
    }
  },

  async getTaskDetails(taskId: string): Promise<TaskDetails> {
    try {
      // Use taskId as is, without conversion
      const response = await apiClient.get<TaskDetails>(`/tasks/${taskId}`);
      return response.data;
    } catch (error) {
      console.error(`Error fetching task details for ID ${taskId}:`, error);
      throw error;
    }
  },

  async getTaskName(taskId: string): Promise<{ name: string }> {
    try {
      // Use taskId as is, without conversion
      const response = await apiClient.get<{ name: string }>(`/tasks/${taskId}/name`);
      return response.data;
    } catch (error) {
      console.error(`Error fetching task name for ID ${taskId}:`, error);
      throw error;
    }
  },

  async getTraces(runId: string): Promise<Trace[]> {
    try {
      // Check if this is a UUID (contains dashes)
      const isUuid = typeof runId === 'string' && runId.includes('-');
      
      // Only convert to numeric if it's NOT a UUID
      const numericRunId = !isUuid && !isNaN(parseInt(runId)) ? parseInt(runId) : null;
      
      // For development purposes, only use fallback if it's a numeric ID (not UUID)
      // and it's not in the known IDs list
      if (!isUuid && 
          process.env.NODE_ENV === 'development' && 
          numericRunId !== null &&
          !KNOWN_RUN_IDS.includes(numericRunId) && 
          KNOWN_RUN_IDS.length > 0) {
        return this.getTraces(KNOWN_RUN_IDS[0].toString());
      }
      
      let endpoint;
      // If the runId is numeric and NOT a UUID, use the execution endpoint
      if (!isUuid && numericRunId !== null) {
        endpoint = `/traces/execution/${numericRunId}`;
      } else {
        // For UUID job_ids, use the job endpoint
        endpoint = `/traces/job/${runId}`;
      }
      
      // The API returns an object with a 'traces' field for both endpoints:
      // ExecutionTraceResponseByRunId or ExecutionTraceResponseByJobId
      const response = await apiClient.get(endpoint);
      
      // Check if the response contains a traces field (from the API schemas)
      if (response.data && response.data.traces && Array.isArray(response.data.traces)) {
        // Process each trace to ensure it matches the frontend's expected format
        return response.data.traces.map((trace: BackendTraceData) => {
          // Map the backend trace model to the frontend Trace interface
          return {
            id: trace.id.toString(),
            event_source: trace.event_source || '',
            event_context: trace.event_context || '',
            event_type: trace.event_type || '',
            task_id: trace.task_id || undefined,
            created_at: trace.created_at || trace.timestamp || new Date().toISOString(),
            // Handle the case where the output might be in output_data, output, or directly in the trace
            output: trace.output || trace.output_data || '',
            // Include extra_data if present
            extra_data: trace.extra_data || undefined
          } as Trace;
        });
      } else {
        // Fallback in case the response format is different
        if (Array.isArray(response.data)) {
          // Map array items to match Trace interface
          return response.data.map((item: BackendTraceData) => ({
            id: item.id.toString(),
            event_source: item.event_source || '',
            event_context: item.event_context || '',
            event_type: item.event_type || '',
            task_id: item.task_id || undefined,
            created_at: item.created_at || item.timestamp || new Date().toISOString(),
            output: item.output || item.output_data || '',
            extra_data: item.extra_data || undefined
          } as Trace));
        }
        // Return empty array if no traces or invalid format
        return [];
      }
    } catch (error: unknown) {
      // Need to type cast to access properties
      const apiError = error as ApiError;
      
      // For development, if we get a 404, try to use a known ID only for numeric IDs
      if (apiError.response && apiError.response.status === 404) {
        const isUuid = typeof runId === 'string' && runId.includes('-');
        if (!isUuid && process.env.NODE_ENV === 'development' && KNOWN_RUN_IDS.length > 0 && !isNaN(parseInt(runId))) {
          return this.getTraces(KNOWN_RUN_IDS[0].toString());
        }
      }
      
      console.error(`Error fetching traces for ID ${runId}:`, apiError);
      console.error(`Error response:`, apiError.response?.data || 'No response data');
      console.error(`Error message:`, apiError.message || 'No error message');
      throw error;
    }
  }
};

export default TraceService; 