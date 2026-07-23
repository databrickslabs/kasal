import { config } from '../config/api/ApiConfig';
import apiClient from '../config/api/ApiConfig';
import { LLMLog } from '../types/common';

interface LogParams {
  page: number;
  per_page: number;
  endpoint?: string;
}

class LogService {
  private apiUrl: string;

  constructor() {
    this.apiUrl = config.apiUrl;
  }

  async getLLMLogs(params: LogParams): Promise<LLMLog[]> {
    const { page, per_page, endpoint } = params;
    const response = await apiClient.get<LLMLog[]>('/llm-logs', {
      params: {
        page,
        per_page,
        ...(endpoint !== 'all' && { endpoint })
      }
    });
    return response.data;
  }
}

export const logService = new LogService();
export default logService; 