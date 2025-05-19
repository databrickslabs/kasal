import { apiClient } from '../config/api/ApiConfig';
import { FlowResponse, Flow, FlowSaveData } from '../types/flow';
import { Node } from 'reactflow';
import { v4 as uuidv4 } from 'uuid';
import { logger } from '../utils/logger';

// Create a specialized logger for this module
const flowLogger = logger.createChild('FlowService');

export class FlowService {
  static async getFlows(): Promise<FlowResponse[]> {
    try {
      const response = await apiClient.get('/flows');
      const flows = response.data;
      
      // Map flow_config to flowConfig for each flow
      return flows.map((flow: FlowResponse) => {
        if (flow.flow_config) {
          flow.flowConfig = flow.flow_config;
          
          // Ensure each node also has the flowConfig if possible
          if (flow.nodes && Array.isArray(flow.nodes)) {
            flow.nodes = flow.nodes.map((node: Node) => {
              if (node.data) {
                return {
                  ...node,
                  data: {
                    ...node.data,
                    flowConfig: flow.flowConfig
                  }
                };
              }
              return node;
            });
          }
        }
        return flow;
      });
    } catch (error) {
      console.error('Error fetching flows:', error);
      return [];
    }
  }

  static async getFlow(id: string): Promise<FlowResponse | null> {
    try {
      // Validate the ID before making the request
      if (!id || id.trim() === '') {
        console.error('Invalid flow ID provided:', id);
        return null;
      }
      
      // Format ID as UUID if needed
      let formattedId = id;
      if (id && !id.includes('-') && id.length >= 32) {
        // If we have a numeric ID but no dashes, try to add the dashes in
        console.log(`Converting string ID ${id} to proper UUID format`);
        try {
          // Attempt to add dashes in correct places (assuming standard UUID format)
          formattedId = [
            id.substring(0, 8),
            id.substring(8, 12),
            id.substring(12, 16),
            id.substring(16, 20),
            id.substring(20)
          ].join('-');
        } catch (err) {
          console.warn(`Failed to format UUID, using original: ${id}`);
          formattedId = id;
        }
      }
      
      console.log(`Fetching flow with formatted UUID: ${formattedId}`);
      const response = await apiClient.get(`/flows/${formattedId}`);
      
      // Validate the response data
      const flowData = response.data;
      if (!flowData || typeof flowData !== 'object') {
        console.error('Invalid flow data received from server');
        return null;
      }
      
      // Map flow_config to flowConfig if it exists
      if (flowData.flow_config) {
        flowData.flowConfig = flowData.flow_config;
        
        // Ensure each node also has the flowConfig if possible
        if (flowData.nodes && Array.isArray(flowData.nodes)) {
          flowData.nodes = flowData.nodes.map((node: Node) => {
            if (node.data) {
              return {
                ...node,
                data: {
                  ...node.data,
                  flowConfig: flowData.flowConfig
                }
              };
            }
            return node;
          });
        }
      }
      
      return flowData;
    } catch (error) {
      console.error('Error fetching flow:', error);
      // Add more specific error logging
      if (error && typeof error === 'object' && 'response' in error) {
        const errorWithResponse = error as { response?: { status?: number, data?: unknown } };
        console.error(`Server returned status ${errorWithResponse.response?.status}:`, errorWithResponse.response?.data);
      }
      return null;
    }
  }

  static async saveFlow(flow: FlowSaveData): Promise<Flow> {
    try {
      flowLogger.info('Saving new flow with name:', flow.name);
      
      // Validate required fields first
      if (!flow.name) {
        throw new Error('Flow name is required');
      }

      if (flow.crew_id === undefined || flow.crew_id === null) {
        throw new Error('Crew ID is required');
      }

      if (!Array.isArray(flow.nodes)) {
        throw new Error('Nodes must be an array');
      }

      if (!Array.isArray(flow.edges)) {
        throw new Error('Edges must be an array');
      }

      // Format crew_id as UUID if it's just a number
      let crew_id = flow.crew_id;
      if (typeof crew_id === 'number' || /^\d+$/.test(crew_id)) {
        // Create a UUID v4 for numeric IDs
        flowLogger.debug(`Converting numeric crew_id ${crew_id} to UUID format`);
        crew_id = uuidv4();
        flowLogger.debug(`Generated UUID for crew_id: ${crew_id}`);
      }

      // Log the incoming data for debugging
      flowLogger.debug('Incoming flow data:', {
        name: flow.name,
        crew_id: crew_id,
        nodes: flow.nodes.length,
        edges: flow.edges.length,
        hasFlowConfig: !!flow.flowConfig
      });
      
      // Normalize flow configuration
      const flow_config = flow.flowConfig ? {
        id: flow.flowConfig.id || `flow-${Date.now()}`,
        name: flow.flowConfig.name || flow.name,
        type: flow.flowConfig.type || 'default',
        listeners: (flow.flowConfig.listeners || []).map(listener => {
          flowLogger.debug('Processing listener:', listener.name);
          return {
            id: listener.id,
            name: listener.name,
            crewId: listener.crewId,
            crewName: listener.crewName,
            listenToTaskIds: listener.listenToTaskIds || [],
            listenToTaskNames: listener.listenToTaskNames || [],
            tasks: (listener.tasks || []).map(task => ({
              id: task.id,
              name: task.name,
              agent_id: task.agent_id,
              description: task.description || '',
              expected_output: task.expected_output || '',
              tools: task.tools || [],
              context: task.context || []
            })),
            state: listener.state || {
              stateType: 'unstructured',
              stateDefinition: '',
              stateData: {}
            },
            conditionType: listener.conditionType || 'NONE',
            // Include routerConfig for ROUTER type listeners
            ...(listener.conditionType === 'ROUTER' && listener.routerConfig ? {
              routerConfig: {
                defaultRoute: listener.routerConfig.defaultRoute,
                routes: listener.routerConfig.routes.map(route => ({
                  name: route.name,
                  condition: route.condition,
                  taskIds: route.taskIds
                }))
              }
            } : {})
          };
        }),
        actions: (flow.flowConfig.actions || []).map(action => {
          flowLogger.debug('Processing action:', action.id);
          return {
            id: action.id,
            crewId: action.crewId,
            crewName: action.crewName,
            taskId: action.taskId,
            taskName: action.taskName
          };
        }),
        startingPoints: (flow.flowConfig.startingPoints || [])
          .filter(sp => sp.isStartPoint)
          .map(point => {
            flowLogger.debug('Processing starting point:', point.taskName);
            return {
              crewId: point.crewId,
              crewName: point.crewName,
              taskId: point.taskId,
              taskName: point.taskName,
              isStartPoint: true
            };
          })
      } : {
        id: `flow-${Date.now()}`,
        name: flow.name,
        type: 'default',
        listeners: [],
        actions: [],
        startingPoints: []
      };

      // Ensure nodes and edges are properly formatted
      const nodes = flow.nodes.map(node => {
        flowLogger.debug('Processing node:', node.id);
        return {
          id: node.id,
          type: node.type,
          position: node.position || { x: 0, y: 0 },
          data: {
            ...node.data,
            label: node.data?.label || node.id
          }
        };
      });

      const edges = flow.edges.map(edge => {
        flowLogger.debug('Processing edge:', edge.id);
        return {
          id: edge.id,
          source: edge.source,
          target: edge.target,
          type: edge.type || 'default'
        };
      });
      
      const data = {
        name: flow.name,
        crew_id: crew_id,
        nodes,
        edges,
        flow_config
      };
      
      // Log the final data being sent
      flowLogger.debug('Sending flow data to backend:', {
        name: data.name,
        crew_id: data.crew_id,
        nodesCount: data.nodes.length,
        edgesCount: data.edges.length
      });
      
      try {
        const response = await apiClient.post('/flows', data);
        flowLogger.info('Flow created successfully:', response.data.id);
        
        // Convert response data to match frontend model
        const savedFlow = response.data;
        if (savedFlow.flow_config) {
          savedFlow.flowConfig = savedFlow.flow_config;
        }
        
        return savedFlow;
      } catch (error: unknown) {
        // Log the detailed error response
        if (error && typeof error === 'object' && 'response' in error) {
          const errorWithResponse = error as { 
            response?: { 
              status?: number, 
              statusText?: string,
              data?: unknown 
            } 
          };
          flowLogger.error('Server validation error details:', {
            status: errorWithResponse.response?.status,
            statusText: errorWithResponse.response?.statusText,
            data: errorWithResponse.response?.data
          });
          throw new Error(`Server validation error: ${JSON.stringify(errorWithResponse.response?.data)}`);
        }
        throw error instanceof Error ? error : new Error('Unknown error occurred');
      }
    } catch (error) {
      flowLogger.error('Error creating flow:', error);
      throw error instanceof Error ? error : new Error('Failed to save flow');
    }
  }

  static async updateFlow(id: string, flow: FlowSaveData): Promise<Flow> {
    try {
      // Format ID as UUID if needed
      let formattedId = id;
      if (id && !id.includes('-') && id.length >= 32) {
        console.log(`Converting string ID ${id} to proper UUID format for update`);
        try {
          // Add dashes in correct places (assuming standard UUID format)
          formattedId = [
            id.substring(0, 8),
            id.substring(8, 12),
            id.substring(12, 16),
            id.substring(16, 20),
            id.substring(20)
          ].join('-');
        } catch (err) {
          console.warn(`Failed to format UUID, using original: ${id}`);
          formattedId = id;
        }
      }
      
      console.log(`Updating flow with UUID: ${formattedId}`);
      
      // Normalize flow configuration
      const flow_config = flow.flowConfig ? {
        id: flow.flowConfig.id || `flow-${Date.now()}`,
        name: flow.flowConfig.name || flow.name,
        type: flow.flowConfig.type || 'default',
        listeners: (flow.flowConfig.listeners || []).map(listener => ({
          id: listener.id,
          name: listener.name,
          crewId: listener.crewId,
          crewName: listener.crewName,
          listenToTaskIds: listener.listenToTaskIds || [],
          listenToTaskNames: listener.listenToTaskNames || [],
          tasks: (listener.tasks || []).map(task => ({
            id: task.id,
            name: task.name,
            agent_id: task.agent_id,
            description: task.description || '',
            expected_output: task.expected_output || '',
            tools: task.tools || [],
            context: task.context || []
          })),
          state: listener.state || {
            stateType: 'unstructured',
            stateDefinition: '',
            stateData: {}
          },
          conditionType: listener.conditionType || 'NONE',
          // Include routerConfig for ROUTER type listeners
          ...(listener.conditionType === 'ROUTER' && listener.routerConfig ? {
            routerConfig: {
              defaultRoute: listener.routerConfig.defaultRoute,
              routes: listener.routerConfig.routes.map(route => ({
                name: route.name,
                condition: route.condition,
                taskIds: route.taskIds
              }))
            }
          } : {})
        })),
        actions: (flow.flowConfig.actions || []).map(action => ({
          id: action.id,
          crewId: action.crewId,
          crewName: action.crewName,
          taskId: action.taskId,
          taskName: action.taskName
        })),
        startingPoints: (flow.flowConfig.startingPoints || [])
          .filter(sp => sp.isStartPoint)
          .map(point => ({
            crewId: point.crewId,
            crewName: point.crewName,
            taskId: point.taskId,
            taskName: point.taskName,
            isStartPoint: true
          }))
      } : {};
      
      const data = {
        name: flow.name,
        flow_config
      };
      
      console.log('Sending flow update data:', data);
      
      const response = await apiClient.put(`/flows/${formattedId}`, data);
      console.log('Flow updated successfully:', response.data);
      
      // Convert response data to match frontend model
      const updatedFlow = response.data;
      if (updatedFlow.flow_config) {
        updatedFlow.flowConfig = updatedFlow.flow_config;
      }
      
      return updatedFlow;
    } catch (error: unknown) {
      console.error('Error updating flow:', error);
      // Cast error to specific type when needed
      if (error instanceof Error) {
        throw new Error(`Failed to update flow: ${error.message}`);
      }
      throw new Error('Failed to update flow: Unknown error occurred');
    }
  }

  static async deleteFlow(id: string): Promise<boolean> {
    try {
      // Format ID as UUID if needed
      let formattedId = id;
      if (id && !id.includes('-') && id.length >= 32) {
        console.log(`Converting string ID ${id} to proper UUID format for deletion`);
        try {
          // Add dashes in correct places (assuming standard UUID format)
          formattedId = [
            id.substring(0, 8),
            id.substring(8, 12),
            id.substring(12, 16),
            id.substring(16, 20),
            id.substring(20)
          ].join('-');
        } catch (err) {
          console.warn(`Failed to format UUID, using original: ${id}`);
          formattedId = id;
        }
      }
      
      // Always use force delete to avoid foreign key constraint issues
      console.log(`Force deleting flow with UUID: ${formattedId}`);
      await apiClient.delete(`/flows/${formattedId}?force=true`);
      
      // Signal UI to refresh the flows list
      window.dispatchEvent(new CustomEvent('refreshFlows'));
      
      window.dispatchEvent(new CustomEvent('showNotification', {
        detail: {
          message: 'Flow deleted successfully.',
          severity: 'success'
        }
      }));
      
      return true;
    } catch (error: unknown) {
      console.error('Error deleting flow:', error);
      
      // Extract error message if available
      let errorMessage = 'Failed to delete flow.';
      
      if (error && typeof error === 'object' && 'response' in error) {
        const errorWithResponse = error as { 
          response?: { 
            status?: number, 
            data?: unknown 
          } 
        };
        
        if (errorWithResponse.response?.data && 
            typeof errorWithResponse.response.data === 'object' && 
            errorWithResponse.response.data !== null && 
            'detail' in errorWithResponse.response.data) {
          const errorData = errorWithResponse.response.data as { detail?: string };
          errorMessage = errorData.detail || errorMessage;
        }
        
        // Show an error notification
        window.dispatchEvent(new CustomEvent('showNotification', {
          detail: {
            message: errorMessage,
            severity: 'error'
          }
        }));
      }
      
      return false;
    }
  }

  static async deleteAllFlows(): Promise<void> {
    try {
      await apiClient.delete('/flows');
    } catch (error) {
      console.error('Error deleting all flows:', error);
      throw error;
    }
  }
}