import apiClient from '../config/api/ApiConfig';
import { Run, RunsResponse, JobStatus } from '../types/run';

export type { Run, RunsResponse, JobStatus };

// Add cache control constants
const CACHE_TTL = 5000; // 5 seconds cache time-to-live

export function calculateDuration(run: Run): string {
  // Convert status to uppercase for case-insensitive comparison
  const status = (run.status || '').toUpperCase();
  
  // Only show duration for completed jobs (case-insensitive)
  if (status !== 'COMPLETED' && status !== 'FAILED' && status !== 'CANCELLED') {
    return '-';
  }
  
  // For terminal statuses, require created_at timestamp
  if (!run?.created_at) {
    return '-';
  }
  
  // For terminal statuses, use completed_at OR updated_at as fallback
  let endTimeStr: string;
  
  if (run.completed_at) {
    // Use completed_at if available
    endTimeStr = run.completed_at;
  } else if (run.updated_at) {
    // Use updated_at as fallback
    endTimeStr = run.updated_at;
  } else {
    // No valid end time available
    return '-';
  }
  
  try {
    // Parse timestamps as UTC dates
    const startTime = new Date(run.created_at);
    const endTime = new Date(endTimeStr);
    
    // Calculate duration in milliseconds
    const durationMs = Math.max(0, endTime.getTime() - startTime.getTime());
    
    // Format duration
    if (durationMs < 1000) {
      return '0s';
    } else if (durationMs < 60000) {
      return `${Math.floor(durationMs / 1000)}s`;
    } else if (durationMs < 3600000) {
      const minutes = Math.floor(durationMs / 60000);
      const seconds = Math.floor((durationMs % 60000) / 1000);
      return `${minutes}m ${seconds}s`;
    } else {
      const hours = Math.floor(durationMs / 3600000);
      const minutes = Math.floor((durationMs % 3600000) / 60000);
      return `${hours}h ${minutes}m`;
    }
  } catch (error) {
    return '-';
  }
}

// Define more specific types to replace 'any'
type InputDataType = Record<string, string | number | boolean | null | object>;
type OutputDataType = Record<string, string | number | boolean | null | object>;

// Interface for execution trace
interface TraceItem {
  id: number;
  run_id: number;
  timestamp: string;
  agent_name?: string;
  task_name?: string;
  input_data?: Record<string, InputDataType>;
  output_data?: Record<string, OutputDataType>;
}

// Interface for delete response
interface DeleteResponse {
  deleted_run_count: number;
  deleted_output_count: number;
  deleted_trace_count: number;
}

export class RunService {
  private static instance: RunService;
  private apiAvailable: boolean | null = null;
  // Add cache properties
  private runsCache: { data: RunsResponse; timestamp: number } | null = null;

  public static getInstance(): RunService {
    if (!RunService.instance) {
      RunService.instance = new RunService();
    }
    return RunService.instance;
  }

  // Check if the execution history API is available
  private async checkApiAvailability(): Promise<boolean> {
    if (this.apiAvailable !== null) {
      return this.apiAvailable;
    }

    try {
      // Try accessing the API with a reasonable timeout
      const _response = await apiClient.get('/executions', { 
        params: { limit: 1 },
        timeout: 5000 // 5 second timeout for better reliability
      });
      
      // If we get here, the API is definitely available
      this.apiAvailable = true;
      return true;
    } catch (error) {
      // One retry attempt before giving up
      try {
        await apiClient.get('/executions', {
          params: { limit: 1 },
          timeout: 5000
        });
        this.apiAvailable = true;
        return true;
      } catch (retryError) {
        this.apiAvailable = false;
        return false;
      }
    }
  }

  // Convert backend execution history item to frontend Run format
  private convertToRun(executionItem: Record<string, unknown>): Run {
    // The API might return execution_id instead of job_id
    const jobId = (executionItem.job_id as string) || (executionItem.execution_id as string);
    const name = (executionItem.name as string) || (executionItem.run_name as string);
    const status = (executionItem.status as string)?.toUpperCase() || 'UNKNOWN';
    
    // Handle timestamps
    const createdAt = executionItem.created_at as string;
    const completedAt = executionItem.completed_at as string;
    const updatedAt = executionItem.updated_at as string;
    
    // Extract the YAML data from the database record
    let agentsYamlData: unknown = null;
    let tasksYamlData: unknown = null;
    
    // Handle the case where 'inputs' is a direct field in the JSON response
    if (executionItem.inputs) {
      // If inputs is a string (common in SQLite database responses), parse it
      if (typeof executionItem.inputs === 'string') {
        try {
          const inputsStr = executionItem.inputs as string;
          const parsedInputs = JSON.parse(inputsStr);
          
          // Check if it contains the YAML data
          if (parsedInputs.agents_yaml) {
            agentsYamlData = parsedInputs.agents_yaml;
          }
          
          if (parsedInputs.tasks_yaml) {
            tasksYamlData = parsedInputs.tasks_yaml;
          }
        } catch (e) {
          // Error parsing inputs, continue with null values
        }
      } 
      // If inputs is already an object, check for YAML fields
      else if (typeof executionItem.inputs === 'object' && executionItem.inputs !== null) {
        const inputs = executionItem.inputs as Record<string, unknown>;
        
        if (inputs.agents_yaml) {
          agentsYamlData = inputs.agents_yaml;
        }
        
        if (inputs.tasks_yaml) {
          tasksYamlData = inputs.tasks_yaml;
        }
      }
    }
    
    // Check if YAML data is directly attached to the execution item
    if (!agentsYamlData && executionItem.agents_yaml) {
      agentsYamlData = executionItem.agents_yaml;
    }
    
    if (!tasksYamlData && executionItem.tasks_yaml) {
      tasksYamlData = executionItem.tasks_yaml;
    }
    
    // Try to extract YAML from any string field that might contain the data
    if ((!agentsYamlData || !tasksYamlData) && executionItem) {
      for (const [_key, value] of Object.entries(executionItem)) {
        if (typeof value === 'string' && 
            value.includes('agents_yaml') && 
            value.includes('tasks_yaml')) {
          try {
            const parsed = JSON.parse(value);
            if (!agentsYamlData && parsed.agents_yaml) {
              agentsYamlData = parsed.agents_yaml;
            }
            if (!tasksYamlData && parsed.tasks_yaml) {
              tasksYamlData = parsed.tasks_yaml;
            }
          } catch (e) {
            // Error parsing, continue with current values
          }
        }
      }
    }
    
    // Stringify the YAML data based on its type
    const stringifyYamlData = (data: unknown): string => {
      if (data === null || data === undefined) {
        return '';
      }
      
      if (typeof data === 'object') {
        return JSON.stringify(data);
      }
      
      if (typeof data === 'string') {
        return data.trim() ? data : '';
      }
      
      return String(data);
    };
    
    // Prepare the final YAML data
    const agents_yaml = stringifyYamlData(agentsYamlData);
    const tasks_yaml = stringifyYamlData(tasksYamlData);
    
    // Prepare object versions for inputs (preserve original structure if it's an object)
    const agents_yaml_object = (typeof agentsYamlData === 'object' && agentsYamlData !== null) 
      ? agentsYamlData as Record<string, unknown>
      : {};
    const tasks_yaml_object = (typeof tasksYamlData === 'object' && tasksYamlData !== null) 
      ? tasksYamlData as Record<string, unknown>
      : {};
    
    // Build inputs object if we have input data
    let inputs: {
      agents_yaml: Record<string, unknown>;
      tasks_yaml: Record<string, unknown>;
      inputs?: Record<string, unknown>;
      planning?: boolean;
      model?: string;
      execution_type?: string;
      schema_detection_enabled?: boolean;
      [key: string]: unknown;
    } | undefined = undefined;
    if (executionItem.inputs && typeof executionItem.inputs === 'object') {
      // Parse inputs if it's a string, otherwise use directly
      let parsedInputs = executionItem.inputs;
      if (typeof executionItem.inputs === 'string') {
        try {
          parsedInputs = JSON.parse(executionItem.inputs as string);
        } catch (e) {
          parsedInputs = {};
        }
      }
      
      inputs = {
        ...parsedInputs,
        agents_yaml: agents_yaml_object,
        tasks_yaml: tasks_yaml_object
      };
    } else if (Object.keys(agents_yaml_object).length > 0 || Object.keys(tasks_yaml_object).length > 0) {
      inputs = {
        agents_yaml: agents_yaml_object,
        tasks_yaml: tasks_yaml_object
      };
    }
    
    // Return the run object with all extracted data
    return {
      id: (executionItem.id as number | undefined)?.toString() || jobId,
      job_id: jobId,
      status: status,
      created_at: createdAt,
      updated_at: updatedAt,
      completed_at: completedAt,
      run_name: name || `Run ${jobId}`,
      agents_yaml,
      tasks_yaml,
      inputs,
      result: executionItem.result as Record<string, OutputDataType> | undefined,
      error: executionItem.error as string | undefined,
    };
  }

  public async getRunByJobId(jobId: string): Promise<Run | null> {
    try {
      // Only attempt API call if available
      if (await this.checkApiAvailability()) {
        try {
          // First try direct API endpoint by UUID
          const directResponse = await apiClient.get(`/executions/${jobId}`);
          return this.convertToRun(directResponse.data);
        } catch (directError) {
          // Fallback: Get all runs and filter by job_id
          const runsResponse = await this.getRuns(100, 0);
          const run = runsResponse.runs.find(r => r.job_id === jobId);
          
          if (run) {
            // Check if we have YAML data
            if ((!run.agents_yaml || !run.tasks_yaml) && run.job_id) {
              // Try to get the raw database record to extract the YAML directly
              try {
                const rawResponse = await apiClient.get(`/executions/history`, {
                  params: { job_id: run.job_id }
                });
                
                if (rawResponse.data && Array.isArray(rawResponse.data) && rawResponse.data.length > 0) {
                  // Reconvert the raw data to get the YAML properly
                  return this.convertToRun(rawResponse.data[0]);
                }
              } catch (rawError) {
                // Failed to get raw record, continue with what we have
              }
            }
          }
          
          return run || null;
        }
      }
      return null;
    } catch (error) {
      return null;
    }
  }

  public async getRuns(limit?: number, offset?: number, updated_since?: string): Promise<RunsResponse> {
    try {
      // Check if we have a valid cache entry
      const now = Date.now();
      if (this.runsCache && (now - this.runsCache.timestamp < CACHE_TTL)) {
        // Return cached data if it exists and hasn't expired
        return this.runsCache.data;
      }

      // Only attempt API call if available or we haven't checked yet
      if (this.apiAvailable === null || this.apiAvailable === true) {
        const params = new URLSearchParams();
        if (limit) params.append('limit', limit.toString());
        if (offset) params.append('offset', offset.toString());
        if (updated_since) params.append('updated_since', updated_since);
        
        try {
          // Using the correct endpoint
          const response = await apiClient.get(`/executions?${params.toString()}`);
          
          // API is available if we got here
          this.apiAvailable = true;
          
          // Convert the backend format to the frontend format
          const responseData = response.data;
          const runs: Run[] = Array.isArray(responseData) 
            ? responseData.map(item => this.convertToRun(item))
            : [];
          
          const result = {
            runs,
            total: runs.length,
            limit: limit || 50,
            offset: offset || 0
          };
          
          // Update the cache
          this.runsCache = {
            data: result,
            timestamp: now
          };
          
          return result;
        } catch (error) {
          // Only set apiAvailable to false on 404, not on server errors
          if (error && typeof error === 'object' && 'response' in error && 
              error.response && typeof error.response === 'object' && 
              'status' in error.response && error.response.status === 404) {
            this.apiAvailable = false;
          }
          // Fall through to return empty response
        }
      }
      
      // Return empty response if API not available or call failed
      const emptyResponse = {
        runs: [],
        total: 0,
        limit: limit || 50,
        offset: offset || 0
      };
      
      return emptyResponse;
    } catch (error) {
      return {
        runs: [],
        total: 0,
        limit: limit || 50,
        offset: offset || 0
      };
    }
  }

  public async getRunById(runId: string): Promise<Run | null> {
    try {
      if (await this.checkApiAvailability()) {
        try {
          // First try the direct endpoint
          const response = await apiClient.get(`/executions/${runId}`);
          return this.convertToRun(response.data);
        } catch (directError) {
          try {
            // Try numeric ID endpoint if it might be a numeric ID
            if (!isNaN(parseInt(runId, 10))) {
              const numericResponse = await apiClient.get(`/executions/history/${runId}`);
              return this.convertToRun(numericResponse.data);
            }
          } catch (numericError) {
            // Numeric ID endpoint failed, continue with next approach
          }
          
          // Last resort, try to find it in all runs
          const runsResponse = await this.getRuns(100, 0);
          const run = runsResponse.runs.find(r => r.id === runId || r.job_id === runId);
          
          if (run) {
            // If the run doesn't have YAML data, try one more approach
            if ((!run.agents_yaml || !run.tasks_yaml) && run.job_id) {
              try {
                const jobResponse = await apiClient.get(`/executions/history`, {
                  params: { job_id: run.job_id }
                });
                
                if (jobResponse.data && Array.isArray(jobResponse.data) && jobResponse.data.length > 0) {
                  return this.convertToRun(jobResponse.data[0]);
                }
              } catch (jobError) {
                // Job ID approach failed, continue with current run
              }
            }
            
            return run;
          }
          
          return null;
        }
      }
      return null;
    } catch (error) {
      return null;
    }
  }

  public async getRunTraces(runId: string): Promise<TraceItem[]> {
    try {
      if (await this.checkApiAvailability()) {
        // Using the execution traces endpoint
        const response = await apiClient.get(`/executions/${runId}/traces`);
        return response.data;
      }
      return [];
    } catch (error) {
      return [];
    }
  }

  public async deleteAllRuns(): Promise<DeleteResponse | null> {
    try {
      if (await this.checkApiAvailability()) {
        try {
          // Try direct DELETE first
          const response = await apiClient.delete('/executions');
          return response.data;
        } catch (error) {
          // If we get a 405 Method Not Allowed, try the alternative endpoint
          if (error && typeof error === 'object' && 'response' in error && 
              error.response && typeof error.response === 'object' && 
              'status' in error.response && error.response.status === 405) {
            
            // Try using history endpoint if available
            const historyResponse = await apiClient.delete('/executions/history');
            return historyResponse.data;
          }
          // If not a 405 error, rethrow
          throw error;
        }
      }
      return null;
    } catch (error) {
      return null;
    }
  }

  public async deleteRun(runId: string): Promise<DeleteResponse | null> {
    try {
      if (await this.checkApiAvailability()) {
        // Try direct deletion by UUID endpoint first
        try {
          const response = await apiClient.delete(`/executions/${runId}`);
          return response.data;
        } catch (directError: unknown) {
          // Type guard to check if error has expected structure
          const isAxiosError = directError && 
            typeof directError === 'object' && 
            'response' in directError;
          
          if (isAxiosError && directError.response) {
            const errorResponse = directError.response as { 
              status: number; 
              statusText: string; 
              data: unknown 
            };
            
            // Only proceed to fallback if this wasn't a server error
            if (errorResponse.status !== 500) {
              throw directError; // Rethrow for non-500 errors
            }
            
            // For 500 errors, try a workaround
            
            // Try to get the execution first to validate it exists
            try {
              const _getResponse = await apiClient.get(`/executions/${runId}`);
              
              // Now try deleting from /executions/history endpoint with job_id
              // Some backends support this pattern for deletion
              const altResponse = await apiClient.delete(`/executions/history?job_id=${runId}`);
              return altResponse.data;
            } catch (getError) {
              // Continue to numeric ID fallback
            }
          } else {
            // If it's not an Axios error with response, just rethrow
            throw directError;
          }
        }
        
        // Fallback to the history numeric ID endpoint
        const runs = await this.getRuns(100, 0);
        const run = runs.runs.find(r => r.job_id === runId);
        
        if (run && run.id) {
          const parsedId = parseInt(run.id, 10);
          
          if (!isNaN(parsedId) && parsedId > 0) {
            const historyResponse = await apiClient.delete(`/executions/history/${parsedId}`);
            return historyResponse.data;
          }
        }
        
        throw new Error(`Could not find a valid way to delete run with job_id ${runId}`);
      }
      return null;
    } catch (error) {
      return null;
    }
  }

  public async getJobStatus(jobId: string): Promise<JobStatus> {
    try {
      if (await this.checkApiAvailability()) {
        // Since execution_history_router doesn't have a direct endpoint for getting status,
        // we'll get the run by job_id and extract the status
        const run = await this.getRunByJobId(jobId);
        if (!run) {
          return {
            status: 'unknown',
            error: `Job with ID ${jobId} not found`
          };
        }
        
        return {
          status: run.status,
          error: run.error
        };
      }
      return {
        status: 'unknown',
        error: 'Execution history API not available'
      };
    } catch (error) {
      return {
        status: 'unknown',
        error: error instanceof Error ? error.message : String(error)
      };
    }
  }

  public async executeJob(agentsYaml: string, tasksYaml: string): Promise<{ job_id: string } | null> {
    try {
      // This endpoint might have a different availability than the history endpoints
      const response = await apiClient.post<{ job_id: string }>('/executions', {
        agents_yaml: agentsYaml,
        tasks_yaml: tasksYaml
      });
      
      // Invalidate cache since we've added a new job
      this.invalidateRunsCache();
      
      return response.data;
    } catch (error) {
      return null;
    }
  }

  // Add a method to invalidate the cache when we know data has changed
  public invalidateRunsCache(): void {
    this.runsCache = null;
  }

  // Public method to manually refresh API availability status
  public resetApiAvailability(): void {
    this.apiAvailable = null;
    this.invalidateRunsCache(); // Also clear the cache
  }
}

export const runService = RunService.getInstance(); 