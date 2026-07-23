import { apiClient } from '../config/api/ApiConfig';

export interface LogMessage {
  id?: number;
  job_id?: string;
  execution_id?: string;
  content?: string;
  output?: string;
  timestamp: string;
  type?: 'live' | 'historical';
}

export interface LogEntry {
  id?: number;
  output?: string;
  content?: string;
  timestamp: string;
  logType?: string;
}

// Add a type definition for the backend log response
interface BackendLogEntry {
  id?: number;
  content: string;
  timestamp: string;
}

class ExecutionLogService {
  async getHistoricalLogs(jobId: string, limit = 1000, offset = 0): Promise<LogMessage[]> {
    try {
      const response = await apiClient.get(`/runs/${jobId}/outputs`, {
        params: { limit, offset }
      });
      const data = response.data;

      const logs = data.logs || [];
      return logs.map((log: BackendLogEntry) => ({
        id: log.id || Date.now(),
        job_id: jobId,
        execution_id: jobId,
        output: log.content,
        content: log.content,
        timestamp: log.timestamp,
        type: 'historical'
      }));
    } catch (error: unknown) {
      const axiosError = error as { response?: { status?: number } };
      if (axiosError.response?.status === 404) {
        console.warn(`No logs found for job ${jobId}`);
        return [];
      }
      console.error('Error fetching historical logs:', error);
      throw error;
    }
  }
}

export const executionLogService = new ExecutionLogService();
export default executionLogService;
