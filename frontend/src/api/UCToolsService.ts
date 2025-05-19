import axios from 'axios';
import apiClient from '../config/api/ApiConfig';

export interface InputParam {
  name: string;
  type: string;
  required: boolean;
}

export interface UCTool {
  name: string;
  full_name: string;
  catalog: string;
  schema: string;
  comment?: string;
  return_type: string;
  input_params: InputParam[];
}

export interface UCToolListResponse {
  tools: UCTool[];
  count: number;
}

export class UCToolsService {
  private static instance: UCToolsService;

  public static getInstance(): UCToolsService {
    if (!UCToolsService.instance) {
      UCToolsService.instance = new UCToolsService();
    }
    return UCToolsService.instance;
  }

  public async getUCTools(): Promise<UCTool[]> {
    try {
      const response = await apiClient.get<UCToolListResponse>('/uc-tools');
      return response.data.tools;
    } catch (error) {
      if (axios.isAxiosError(error)) {
        if (error.response?.status === 400) {
          throw new Error(`Configuration Error: ${error.response.data.error}`);
        }
        throw new Error(`Failed to load Unity Catalog tools: ${error.response?.data.error}`);
      }
      throw new Error('Failed to connect to the server. Please check if the backend is running.');
    }
  }
} 