import { getClient } from './client';

export interface ToolInfo {
  id: number;
  title: string;
  description: string;
  enabled: boolean;
}

interface ToolListResponse {
  tools: ToolInfo[];
  count: number;
}

export async function fetchEnabledTools(): Promise<ToolInfo[]> {
  const response = await getClient().get<ToolListResponse>('/tools/enabled');
  return response.data.tools;
}
