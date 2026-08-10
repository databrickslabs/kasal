import { apiClient } from '../../config/api/ApiConfig';
import { FlowResponse, Flow, FlowSaveData } from '../../types/workflow/flow';
import { Node } from 'reactflow';
import { v4 as uuidv4 } from 'uuid';
import { logger } from '../../utils/logger';

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
        // Spread FIRST, then normalise. This block used to list the keys it
        // kept — id, name, type, listeners, actions, startingPoints — which
        // silently dropped every other key the builder produces: `routers`
        // (so a saved flow lost its routing entirely), `persistence`, and the
        // `state` declaration. It went unnoticed because the chat path and
        // JobExecutionService REBUILD flow_config from nodes and edges before
        // every run, so the loss only showed up in what was stored.
        //
        // An allow-list here is a standing trap: every field added to
        // FlowConfiguration has to be remembered in two more places, and
        // forgetting is silent.
        ...flow.flowConfig,
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
              context: task.context || [],
              markdown: Boolean(task.markdown || false)
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
      // CRITICAL: Preserve ALL node properties to maintain visual state and custom data
      const nodes = flow.nodes.map(node => {
        flowLogger.debug('Processing node:', node.id);
        return {
          id: node.id,
          type: node.type,
          position: node.position || { x: 0, y: 0 },
          // Preserve additional top-level properties like width, height, style if they exist
          ...(node.width !== undefined && { width: node.width }),
          ...(node.height !== undefined && { height: node.height }),
          ...(node.style && { style: node.style }),
          ...(node.className && { className: node.className }),
          ...(node.dragging !== undefined && { dragging: node.dragging }),
          data: {
            ...node.data,  // Preserve ALL data properties (allTasks, selectedTasks, order, flowConfig, etc.)
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
          type: edge.type || 'default',
          // Preserve style and other edge properties
          ...(edge.style && { style: edge.style }),
          ...(edge.animated !== undefined && { animated: edge.animated }),
          ...(edge.label && { label: edge.label }),
          ...(edge.labelStyle && { labelStyle: edge.labelStyle }),
          ...(edge.labelBgStyle && { labelBgStyle: edge.labelBgStyle }),
          ...(edge.className && { className: edge.className }),
          data: edge.data || {},  // Preserve ALL edge data (configured, listenToTaskIds, targetTaskIds, logicType, routerConfig, etc.)
          sourceHandle: edge.sourceHandle,
          targetHandle: edge.targetHandle
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

      // Keys the CANVAS does not produce, but the flow has, must survive a
      // save. `outcomes` is written on the publish page and `state` in the edge
      // dialog; the config rebuilt from nodes and edges knows about neither, so
      // saving the canvas would silently drop both. Read the stored config and
      // merge the rebuild over it, rather than listing the keys to keep — that
      // list is what dropped `routers` for however long nobody noticed.
      let stored: Record<string, unknown> = {};
      try {
        const existing = await FlowService.getFlow(formattedId);
        stored = ((existing?.flowConfig ?? existing?.flow_config) || {}) as Record<
          string,
          unknown
        >;
      } catch {
        // A flow we cannot read is one we cannot merge with; the rebuild alone
        // is still a correct config, just without anything authored elsewhere.
      }

      
      // Normalize flow configuration
      const flow_config = flow.flowConfig ? {
        // The STORED config first, so keys authored outside the canvas
        // survive a save: `outcomes` from the publish page, `state` from the
        // edge dialog. The rebuild below knows about neither.
        ...stored,
        // Spread FIRST, then normalise. This block used to list the keys it
        // kept — id, name, type, listeners, actions, startingPoints — which
        // silently dropped every other key the builder produces: `routers`
        // (so a saved flow lost its routing entirely), `persistence`, and the
        // `state` declaration. It went unnoticed because the chat path and
        // JobExecutionService REBUILD flow_config from nodes and edges before
        // every run, so the loss only showed up in what was stored.
        //
        // An allow-list here is a standing trap: every field added to
        // FlowConfiguration has to be remembered in two more places, and
        // forgetting is silent.
        ...flow.flowConfig,
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
            context: task.context || [],
            markdown: Boolean(task.markdown || false)
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

      // Process nodes and edges if provided
      // CRITICAL: Preserve ALL node properties to maintain visual state and custom data
      const nodes = flow.nodes ? flow.nodes.map(node => ({
        id: node.id,
        type: node.type,
        position: node.position || { x: 0, y: 0 },
        // Preserve additional top-level properties like width, height, style if they exist
        ...(node.width !== undefined && { width: node.width }),
        ...(node.height !== undefined && { height: node.height }),
        ...(node.style && { style: node.style }),
        ...(node.className && { className: node.className }),
        ...(node.dragging !== undefined && { dragging: node.dragging }),
        data: {
          ...node.data,  // Preserve ALL data properties (allTasks, selectedTasks, order, flowConfig, etc.)
          label: node.data?.label || node.id
        }
      })) : undefined;

      const edges = flow.edges ? flow.edges.map(edge => ({
        id: edge.id,
        source: edge.source,
        target: edge.target,
        type: edge.type || 'default',
        // Preserve style and other edge properties
        ...(edge.style && { style: edge.style }),
        ...(edge.animated !== undefined && { animated: edge.animated }),
        ...(edge.label && { label: edge.label }),
        ...(edge.labelStyle && { labelStyle: edge.labelStyle }),
        ...(edge.labelBgStyle && { labelBgStyle: edge.labelBgStyle }),
        ...(edge.className && { className: edge.className }),
        data: edge.data || {},  // Preserve ALL edge data (configured, listenToTaskIds, targetTaskIds, logicType, routerConfig, etc.)
        sourceHandle: edge.sourceHandle,
        targetHandle: edge.targetHandle
      })) : undefined;

      const data = {
        name: flow.name,
        ...(nodes ? { nodes } : {}),
        ...(edges ? { edges } : {}),
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

  /**
   * Persist ONLY the flow's state declaration, leaving the canvas untouched.
   *
   * The declaration is a property of the FLOW, but it is edited from an edge
   * dialog whose Save button commits the edge to the canvas and nothing else.
   * Leaving it to ride along on the next flow save made it look like the toggle
   * did nothing: the user configured it, pressed the Save in front of them, and
   * the database never changed.
   *
   * `nodes` and `edges` are deliberately NOT sent. The backend writes only the
   * fields it is given, so an unsaved canvas edit is neither saved nor lost by
   * this call — which is what makes it safe to fire the moment the switch moves.
   */
  static async updateFlowState(
    id: string,
    state: Record<string, unknown> | undefined,
  ): Promise<boolean> {
    try {
      const existing = await FlowService.getFlow(id);
      if (!existing) return false;
      const current = (existing.flowConfig ?? existing.flow_config ?? {}) as Record<
        string,
        unknown
      >;
      // Read-modify-write on the SAVED config: anything else it carries —
      // routers, listeners, persistence — has to survive this.
      const flow_config = { ...current, ...(state ? { state } : {}) };
      if (!state) delete (flow_config as { state?: unknown }).state;

      await apiClient.put(`/flows/${id}`, { name: existing.name, flow_config });
      return true;
    } catch (error) {
      console.error('Error saving flow state declaration:', error);
      return false;
    }
  }

  /**
   * Persist what each crew in this flow DELIVERS, leaving the canvas alone.
   *
   * Written from the flow's publish page but stored on the FLOW, because the
   * selection that reads it runs for any conversational flow — published or
   * not. Only the one key is sent; `nodes` and `edges` are omitted, so an
   * unsaved canvas edit is neither saved nor lost by publishing.
   */
  static async updateFlowOutcomes(
    id: string,
    outcomes: Record<string, string>,
  ): Promise<boolean> {
    try {
      const existing = await FlowService.getFlow(id);
      if (!existing) return false;
      const current = (existing.flowConfig ?? existing.flow_config ?? {}) as Record<
        string,
        unknown
      >;
      // Empty lines are dropped rather than stored: a blank description is not
      // a description, and keeping it would make "described" look true.
      const kept = Object.fromEntries(
        Object.entries(outcomes).filter(([, text]) => (text || '').trim()),
      );
      await apiClient.put(`/flows/${id}`, {
        name: existing.name,
        flow_config: { ...current, outcomes: kept },
      });
      return true;
    } catch (error) {
      console.error('Error saving flow outcomes:', error);
      return false;
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

}