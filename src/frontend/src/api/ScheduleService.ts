import { apiClient } from '../config/api/ApiConfig';
import { AgentYaml, TaskYaml } from '../types/crew';

// Flow node/edge types for scheduling
export interface FlowNode {
  id: string;
  type: string;
  position: { x: number; y: number };
  data: Record<string, unknown>;
}

export interface FlowEdge {
  id: string;
  source: string;
  target: string;
  sourceHandle?: string;
  targetHandle?: string;
}

export interface Schedule {
  id: number;
  name: string;
  cron_expression: string;

  // Execution type (crew or flow)
  execution_type: 'crew' | 'flow';

  // Crew execution fields
  agents_yaml?: Record<string, AgentYaml>;
  tasks_yaml?: Record<string, TaskYaml>;

  // Flow execution fields
  flow_id?: string;
  nodes?: FlowNode[];
  edges?: FlowEdge[];
  flow_config?: Record<string, unknown>;

  // Common fields
  inputs?: Record<string, unknown>;
  is_active: boolean;
  last_run_at?: string;
  next_run_at?: string;
  created_at: string;
  updated_at: string;
  planning?: boolean;
  model?: string;
}

export interface ScheduleCreate {
  name: string;
  cron_expression: string;

  // Execution type (crew or flow)
  execution_type?: 'crew' | 'flow';

  // Crew execution fields (required for crew, optional for flow)
  agents_yaml?: Record<string, AgentYaml>;
  tasks_yaml?: Record<string, TaskYaml>;

  // Flow execution fields (required for flow, optional for crew)
  flow_id?: string;
  nodes?: FlowNode[];
  edges?: FlowEdge[];
  flow_config?: Record<string, unknown>;

  // Common fields
  inputs?: Record<string, unknown>;
  is_active?: boolean;
  planning?: boolean;
  model?: string;
}

export class ScheduleService {
  static async createSchedule(schedule: ScheduleCreate): Promise<Schedule> {
    const response = await apiClient.post<Schedule>('/schedules', schedule);
    return response.data;
  }

  static async listSchedules(): Promise<Schedule[]> {
    const response = await apiClient.get<Schedule[]>('/schedules');
    return response.data;
  }

  static async getSchedule(id: number): Promise<Schedule> {
    const response = await apiClient.get<Schedule>(`/schedules/${id}`);
    return response.data;
  }

  static async updateSchedule(id: number, schedule: ScheduleCreate): Promise<Schedule> {
    const response = await apiClient.put<Schedule>(`/schedules/${id}`, schedule);
    return response.data;
  }

  static async deleteSchedule(id: number): Promise<void> {
    await apiClient.delete(`/schedules/${id}`);
  }

  static async toggleSchedule(id: number): Promise<Schedule> {
    const response = await apiClient.post<Schedule>(`/schedules/${id}/toggle`);
    return response.data;
  }
}

export const scheduleService = new ScheduleService();
