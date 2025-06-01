import axios from 'axios';
import { apiClient as API } from '../config/api/ApiConfig';

import { CrewResponse, CrewCreate, Crew, CrewSaveData } from '../types/crews';
import { CrewTask } from '../types/crewPlan';

interface TaskNode {
  id: string;
  type: string;
  data?: {
    taskId?: string | number;
    id?: string | number;
    label?: string;
    name?: string;
    description?: string;
    expected_output?: string;
    tools?: string[];
    [key: string]: unknown;
  };
  [key: string]: unknown;
}

interface NodeData {
  label: string;
  tools: string[];
  context: string[];
  async_execution: boolean;
  description?: string;
  expected_output?: string;
  taskId?: string;
  agentId?: string;
  config?: {
    cache_response: boolean;
    cache_ttl: number;
    retry_on_fail: boolean;
    max_retries: number;
    timeout: null;
    priority: number;
    error_handling: string;
    output_file: null;
    output_json: boolean;
    output_pydantic: string | null;
    callback: null;
    human_input: boolean;
    markdown: boolean;
  };
  [key: string]: unknown;
}

interface NodeConfig {
  markdown?: boolean | string;
  [key: string]: unknown;
}

export class CrewService {
  static async getCrews(): Promise<CrewResponse[]> {
    try {
      const response = await API.get('/crews');
      return response.data;
    } catch (error) {
      console.error('Error fetching crews:', error);
      throw error;
    }
  }

  static async getCrew(id: string): Promise<CrewResponse> {
    try {
      const response = await API.get(`/crews/${id}`);
      return response.data;
    } catch (error) {
      console.error('Error fetching specific crew:', error);
      throw error;
    }
  }

  static async getTasks(crewId: string): Promise<CrewTask[]> {
    try {
      const response = await API.get(`/crews/${crewId}`);
      const crew = response.data;
      
      // Combine tasks from both sources
      let allTasks: CrewTask[] = [];
      
      // Extract from nodes
      if (crew.nodes && Array.isArray(crew.nodes)) {
        const taskNodes = crew.nodes.filter((node: TaskNode) => 
          node.type === 'taskNode' || 
          (node.data && typeof node.data === 'object' && 'taskId' in node.data)
        );
        
        taskNodes.forEach((node: TaskNode) => {
          if (node.data && typeof node.data === 'object') {
            const taskId = String(node.data.taskId || node.data.id || node.id);
            
            allTasks.push({
              id: taskId,
              name: String(node.data.label || node.data.name || 'Unnamed Task'),
              description: String(node.data.description || ''),
              expected_output: String(node.data.expected_output || ''),
              agent_id: String(crewId), // Ensure agent_id is always a string
              tools: Array.isArray(node.data.tools) ? node.data.tools : [],
              context: Array.isArray(node.data.context) ? node.data.context : [],
              markdown: (node.data.config as NodeConfig)?.markdown === true || (node.data.config as NodeConfig)?.markdown === 'true'
            });
          }
        });
      }
      
      // Extract from tasks array if present
      if (crew.tasks && Array.isArray(crew.tasks)) {
        crew.tasks.forEach((task: TaskNode) => {
          if (task.data) {
            const taskId = String(task.id);
            allTasks.push({
              id: taskId,
              name: String(task.data.name || task.data.label || 'Unnamed Task'),
              description: String(task.data.description || ''),
              expected_output: String(task.data.expected_output || ''),
              agent_id: String(crewId), // Ensure agent_id is always a string
              tools: Array.isArray(task.data.tools) ? task.data.tools : [],
              context: Array.isArray(task.data.context) ? task.data.context : [],
              markdown: (task.data.config as NodeConfig)?.markdown === true || (task.data.config as NodeConfig)?.markdown === 'true'
            });
          }
        });
      }
      
      // Filter out agent IDs - we only want tasks
      allTasks = allTasks.filter(task => !String(task.id).startsWith('agent-'));
      
      console.log(`Got ${allTasks.length} tasks for crew ${crewId}`);
      return allTasks;
    } catch (error) {
      console.error('Error fetching tasks:', error);
      return [];
    }
  }

  static async saveCrew(crew: CrewSaveData): Promise<Crew> {
    try {
      // Helper function to convert boolean values to strings recursively
      const convertBooleansToStrings = (obj: NodeData | Record<string, unknown>): unknown => {
        if (typeof obj !== 'object' || obj === null) {
          return typeof obj === 'boolean' ? String(obj) : obj;
        }
        
        if (Array.isArray(obj)) {
          return obj.map(item => convertBooleansToStrings(item as Record<string, unknown>));
        }
        
        const result: Record<string, unknown> = {};
        for (const [key, value] of Object.entries(obj)) {
          // Special handling for knowledge_sources to preserve fileInfo
          if (key === 'knowledge_sources' && Array.isArray(value)) {
            console.log(`Processing knowledge_sources for node, found ${value.length} sources:`, value);
            result[key] = value.map(source => {
              if (source && typeof source === 'object') {
                // Make sure fileInfo is preserved
                const processed = convertBooleansToStrings(source as Record<string, unknown>);
                return processed;
              }
              return source;
            });
            console.log('Processed knowledge_sources:', result[key]);
          }
          // Convert IDs to strings
          else if ((key === 'taskId' || key === 'agentId') && value !== null) {
            result[key] = String(value);
          } else if (typeof value === 'object' && value !== null) {
            result[key] = convertBooleansToStrings(value as Record<string, unknown>);
          } else {
            result[key] = typeof value === 'boolean' ? String(value) : value;
          }
        }
        return result;
      };

      const agentNodes = crew.nodes.filter(node => node.type === 'agentNode');
      const taskNodes = crew.nodes.filter(node => node.type === 'taskNode');
      
      // Log agent nodes for debugging
      console.log('Agent nodes before processing:', agentNodes.map(node => ({
        id: node.id,
        label: node.data?.label,
        knowledge_sources: node.data?.knowledge_sources || []
      })));
      
      // Add debugging
      console.log('Agent nodes before ID extraction:', agentNodes.map(node => ({
        id: node.id,
        agentId: node.data?.agentId,
        fullData: node.data
      })));
      console.log('Task nodes before ID extraction:', taskNodes.map(node => ({
        id: node.id,
        taskId: node.data?.taskId,
        fullData: node.data
      })));

      // Update the extractIdFromUuid function to follow TypeScript naming conventions
      // and mark it as unused with underscore prefix
      const _extractIdFromUuid = (id: string): string | null => {
        // Just return the full UUID as a string
        return id;
      };

      // Validate agent IDs
      const agent_ids = agentNodes.map(node => {
        // Check for agentId field first
        if (node.data?.agentId) {
          return String(node.data.agentId);
        }
        
        // If no agentId but node has an ID, use that
        if (node.id) {
          // If ID starts with "agent-", extract the uuid part
          if (node.id.startsWith('agent-')) {
            const parts = node.id.split('agent-');
            if (parts.length > 1) {
              return parts[1];
            }
          }
          return node.id;
        }
        
        console.warn(`Cannot extract valid ID from agent node ${node.id}`, node);
        return null;
      }).filter(Boolean) as string[];

      // Validate task IDs
      const task_ids = taskNodes.map(node => {
        // Check for taskId field first
        if (node.data?.taskId) {
          return String(node.data.taskId);
        }
        
        // If no taskId but node has an ID, use that
        if (node.id) {
          // If ID starts with "task-", extract the uuid part
          if (node.id.startsWith('task-')) {
            const parts = node.id.split('task-');
            if (parts.length > 1) {
              return parts[1];
            }
          }
          return node.id;
        }
        
        console.warn(`Cannot extract valid ID from task node ${node.id}`, node);
        return null;
      }).filter(Boolean) as string[];
      
      console.log('Extracted agent_ids:', agent_ids);
      console.log('Extracted task_ids:', task_ids);

      // Clean up node data
      const cleanedNodes = crew.nodes.map(node => {
        if (!node.data) {
          node.data = { label: node.id };
        }

        if (!node.data.label || typeof node.data.label !== 'string') {
          node.data.label = node.id; // Use node id as fallback label
        }

        // Ensure tools is always an array
        if (!node.data.tools || !Array.isArray(node.data.tools)) {
          node.data.tools = [];
        }

        // Ensure context is always an array
        if (!node.data.context || !Array.isArray(node.data.context)) {
          node.data.context = [];
        }

        // Set async_execution to a string of boolean if not already set
        if (node.data.async_execution === undefined) {
          node.data.async_execution = "false";
        } else {
          node.data.async_execution = String(node.data.async_execution);
        }

        // For agent nodes, ensure we preserve knowledge_sources
        if (node.type === 'agentNode' && node.data.knowledge_sources) {
          console.log(`Preserving knowledge_sources for agent node ${node.id}:`, node.data.knowledge_sources);
        }

        // Create a minimal node structure with original data
        const cleanedNode = {
          id: node.id,
          type: node.type,
          position: {
            x: node.position.x,
            y: node.position.y
          },
          data: convertBooleansToStrings(node.data as NodeData) as Record<string, unknown>
        };

        // Verify knowledge_sources are preserved after conversion
        if (node.type === 'agentNode' && node.data.knowledge_sources) {
          const processedData = cleanedNode.data as Record<string, unknown>;
          console.log(`After conversion, knowledge_sources for ${node.id}:`, 
            processedData.knowledge_sources || 'missing');
        }

        return cleanedNode;
      });

      // Clean up edges to bare minimum
      const cleanedEdges = crew.edges.map(edge => ({
        id: edge.id,
        source: edge.source,
        target: edge.target
      }));

      // Create the request data matching the PlanCreate interface
      const crewData: CrewCreate = {
        name: crew.name,
        agent_ids: agent_ids,
        task_ids: task_ids,
        nodes: cleanedNodes,
        edges: cleanedEdges
      };

      // Debug logs
      console.log('Complete crew data being sent:', JSON.stringify(crewData, null, 2));

      const response = await API.post('/crews', crewData);
      const savedCrew: CrewResponse = response.data;
      
      return {
        id: savedCrew.id.toString(),
        name: savedCrew.name,
        agent_ids: savedCrew.agent_ids,
        task_ids: savedCrew.task_ids,
        nodes: savedCrew.nodes || cleanedNodes,
        edges: savedCrew.edges || cleanedEdges,
        created_at: savedCrew.created_at,
        updated_at: savedCrew.updated_at,
      };
    } catch (error) {
      console.error('Error saving crew:', error);
      if (axios.isAxiosError(error) && error.response) {
        const errorData = error.response.data;
        console.error('Server validation error details:', {
          status: error.response.status,
          data: errorData,
          detail: errorData.detail,
          fullDetail: JSON.stringify(errorData.detail, null, 2)
        });
        
        // Log the request data that caused the error
        console.error('Request data that caused error:', {
          url: error.config?.url,
          method: error.config?.method,
          data: error.config?.data,
          rawData: error.config?.data ? JSON.parse(error.config.data) : null
        });
      }
      throw error;
    }
  }

  static async updateCrew(id: string, crew: CrewSaveData): Promise<Crew> {
    try {
      // Helper function to convert boolean values to strings recursively
      const convertBooleansToStrings = (obj: NodeData | Record<string, unknown>): unknown => {
        if (typeof obj !== 'object' || obj === null) {
          return typeof obj === 'boolean' ? String(obj) : obj;
        }
        
        if (Array.isArray(obj)) {
          return obj.map(item => convertBooleansToStrings(item as Record<string, unknown>));
        }
        
        const result: Record<string, unknown> = {};
        for (const [key, value] of Object.entries(obj)) {
          // Special handling for knowledge_sources to preserve fileInfo
          if (key === 'knowledge_sources' && Array.isArray(value)) {
            console.log(`Processing knowledge_sources for node, found ${value.length} sources:`, value);
            result[key] = value.map(source => {
              if (source && typeof source === 'object') {
                // Make sure fileInfo is preserved
                const processed = convertBooleansToStrings(source as Record<string, unknown>);
                return processed;
              }
              return source;
            });
            console.log('Processed knowledge_sources:', result[key]);
          }
          // Convert IDs to strings
          else if ((key === 'taskId' || key === 'agentId') && value !== null) {
            result[key] = String(value);
          } else if (typeof value === 'object' && value !== null) {
            result[key] = convertBooleansToStrings(value as Record<string, unknown>);
          } else {
            result[key] = typeof value === 'boolean' ? String(value) : value;
          }
        }
        return result;
      };

      const agentNodes = crew.nodes.filter(node => node.type === 'agentNode');
      const taskNodes = crew.nodes.filter(node => node.type === 'taskNode');
      
      // Validate agent IDs
      const agent_ids = agentNodes.map(node => {
        // Check for agentId field first
        if (node.data?.agentId) {
          return String(node.data.agentId);
        }
        
        // If no agentId but node has an ID, use that
        if (node.id) {
          // If ID starts with "agent-", extract the uuid part
          if (node.id.startsWith('agent-')) {
            const parts = node.id.split('agent-');
            if (parts.length > 1) {
              return parts[1];
            }
          }
          return node.id;
        }
        
        console.warn(`Cannot extract valid ID from agent node ${node.id}`, node);
        return null;
      }).filter(Boolean) as string[];

      // Validate task IDs
      const task_ids = taskNodes.map(node => {
        // Check for taskId field first
        if (node.data?.taskId) {
          return String(node.data.taskId);
        }
        
        // If no taskId but node has an ID, use that
        if (node.id) {
          // If ID starts with "task-", extract the uuid part
          if (node.id.startsWith('task-')) {
            const parts = node.id.split('task-');
            if (parts.length > 1) {
              return parts[1];
            }
          }
          return node.id;
        }
        
        console.warn(`Cannot extract valid ID from task node ${node.id}`, node);
        return null;
      }).filter(Boolean) as string[];

      // Clean up node data
      const cleanedNodes = crew.nodes.map(node => {
        if (!node.data) {
          node.data = { label: node.id };
        }

        if (!node.data.label || typeof node.data.label !== 'string') {
          node.data.label = node.id; // Use node id as fallback label
        }

        // Ensure tools is always an array
        if (!node.data.tools || !Array.isArray(node.data.tools)) {
          node.data.tools = [];
        }

        // Ensure context is always an array
        if (!node.data.context || !Array.isArray(node.data.context)) {
          node.data.context = [];
        }

        // Set async_execution to a string of boolean if not already set
        if (node.data.async_execution === undefined) {
          node.data.async_execution = "false";
        } else {
          node.data.async_execution = String(node.data.async_execution);
        }

        // Create a minimal node structure with original data
        const cleanedNode = {
          id: node.id,
          type: node.type,
          position: {
            x: node.position.x,
            y: node.position.y
          },
          data: convertBooleansToStrings(node.data as NodeData) as Record<string, unknown>
        };

        return cleanedNode;
      });

      // Clean up edges to bare minimum
      const cleanedEdges = crew.edges.map(edge => ({
        id: edge.id,
        source: edge.source,
        target: edge.target
      }));

      // Create the update data - only include changed fields
      const updateData = {
        name: crew.name,
        agent_ids: agent_ids,
        task_ids: task_ids,
        nodes: cleanedNodes,
        edges: cleanedEdges
      };

      console.log('Updating crew with data:', JSON.stringify(updateData, null, 2));

      const response = await API.put(`/crews/${id}`, updateData);
      const updatedCrew: CrewResponse = response.data;
      
      return {
        id: updatedCrew.id.toString(),
        name: updatedCrew.name,
        agent_ids: updatedCrew.agent_ids,
        task_ids: updatedCrew.task_ids,
        nodes: updatedCrew.nodes || cleanedNodes,
        edges: updatedCrew.edges || cleanedEdges,
        created_at: updatedCrew.created_at,
        updated_at: updatedCrew.updated_at,
      };
    } catch (error) {
      console.error('Error updating crew:', error);
      if (axios.isAxiosError(error) && error.response) {
        const errorData = error.response.data;
        console.error('Server validation error details:', {
          status: error.response.status,
          data: errorData,
          detail: errorData.detail,
          fullDetail: JSON.stringify(errorData.detail, null, 2)
        });
      }
      throw error;
    }
  }

  static async deleteCrew(id: string): Promise<boolean> {
    try {
      await API.delete(`/crews/${id}`);
      return true;
    } catch (error) {
      console.error('Error deleting crew:', error);
      return false;
    }
  }

  static async deleteAllCrews(): Promise<void> {
    try {
      await API.delete('/crews');
      console.log('All crews deleted successfully');
    } catch (error) {
      console.error('Error deleting all crews:', error);
      throw error;
    }
  }
}
